// scripts/lib/steam-enrichment.js
// Shared Steam enrichment module
//
// Pulls static metadata for a Steam game from two sources:
//   1. Steam Store API  — name, descriptions, release date, developers, publishers
//   2. Steam page HTML  — tags, social links, language table, controller support
// Then writes the combined record to the games_static table in Supabase.
//
// Used by:
//   - scripts/add-game-to-track.js        (manual game additions)
//   - scripts/promote-top10-to-games-static.js (automated top-riser promotion)
//
// Exports:
//   enrichGame(appId, { trackingSource, studyTags }, supabase)
//     Returns { success: true, name, tags, released } on success.
//     Returns { success: false, notFound: true } if app_id not found on Steam.
//     Throws on unexpected errors (network failures, Supabase write errors, etc.)
//
//   fetchWithRetry(url, options)
//     Shared fetch wrapper with retry logic — exported for use in other scripts.

// ---------------------------------------------------------------------------
// Retry configuration
// ---------------------------------------------------------------------------
const RETRY_DELAYS    = [2000, 4000, 8000]; // ms to wait before retries 1, 2, 3
const RATE_LIMIT_WAIT = 60000;              // ms to wait on HTTP 429

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// fetchWithRetry
// Wraps native fetch with retry logic and exponential backoff.
//
// Retry behaviour:
//   - Network errors     → retry with exponential backoff (2s / 4s / 8s)
//   - HTTP 5xx           → retry with exponential backoff
//   - HTTP 429           → retry after a fixed 60s wait
//   - Other HTTP 4xx     → throw immediately, no retry
//   - Max 3 retry attempts (4 total attempts including the initial request)
// ---------------------------------------------------------------------------
async function fetchWithRetry(url, options = {}) {
  for (let attempt = 0; attempt <= 3; attempt++) {
    let response;

    try {
      response = await fetch(url, options);
    } catch (err) {
      // Network-level failure (DNS, connection reset, timeout, etc.)
      if (attempt === 3) {
        throw new Error(`Network error after all retries: ${err.message}`);
      }
      console.warn(`Network error, retrying in ${RETRY_DELAYS[attempt] / 1000}s... (attempt ${attempt + 1})`);
      await sleep(RETRY_DELAYS[attempt]);
      continue;
    }

    if (response.ok) return response;

    if (response.status === 429) {
      // Rate limited — use a much longer fixed wait instead of exponential backoff
      if (attempt === 3) throw new Error('HTTP 429 rate limited — all retries exhausted');
      console.warn(`HTTP 429 rate limited — waiting ${RATE_LIMIT_WAIT / 1000}s before retry ${attempt + 1}...`);
      await sleep(RATE_LIMIT_WAIT);
      continue;
    }

    if (response.status >= 400 && response.status < 500) {
      // Other 4xx client errors — configuration or auth problem, do not retry
      throw new Error(`HTTP ${response.status}`);
    }

    // 5xx server error — retry with exponential backoff
    if (attempt === 3) {
      throw new Error(`HTTP ${response.status} — all retries exhausted`);
    }
    console.warn(`HTTP ${response.status}, retrying in ${RETRY_DELAYS[attempt] / 1000}s... (attempt ${attempt + 1})`);
    await sleep(RETRY_DELAYS[attempt]);
  }

  throw new Error('Exhausted all retry attempts');
}

// ---------------------------------------------------------------------------
// HTML parsing helpers
// ---------------------------------------------------------------------------

// Extract inner text from all <a> tags in an HTML fragment
function extractAnchorTexts(html) {
  const matches = [...html.matchAll(/<a[^>]*>([^<]+)<\/a>/g)];
  return matches.map(m => m[1].trim()).filter(Boolean);
}

// Strip all HTML tags and trim whitespace
function stripTags(html) {
  return html.replace(/<[^>]+>/g, '').trim();
}

// Classify a Discord URL and extract an invite code if present.
// Steam routes social links through a linkfilter proxy, so rawUrl is
// typically still URL-encoded when passed here.
function parseDiscordUrl(rawUrl) {
  const decoded = decodeURIComponent(rawUrl);
  if (/discord\.gg\/|discord\.com\/invite\//i.test(decoded)) {
    const inviteMatch = decoded.match(/discord\.(?:gg|com\/invite)\/([^/?#\s]+)/i);
    return {
      type:       'invite',
      url:        decoded,
      inviteCode: inviteMatch ? inviteMatch[1] : null,
    };
  }
  if (/discord\.com\/channels\//i.test(decoded)) {
    return { type: 'direct', url: decoded, inviteCode: null };
  }
  return { type: 'not_found', url: null, inviteCode: null };
}

// ---------------------------------------------------------------------------
// enrichGame
// Core enrichment function — pulls Steam data and writes to games_static.
//
// Parameters:
//   appId          {number}  Steam app ID
//   trackingSource {string}  e.g. 'manual', 'auto' (written to games_static)
//   studyTags      {string|null} internal grouping tag
//   supabase       Supabase client instance (caller is responsible for creating)
//
// Returns:
//   { success: true, name, tags, released }   on success
//   { success: false, notFound: true, error }  if app not found on Steam
//   Throws on unexpected errors
// ---------------------------------------------------------------------------
async function enrichGame(appId, { trackingSource = 'manual', studyTags = null } = {}, supabase) {
  const today = new Date().toISOString().split('T')[0];

  // -------------------------------------------------------------------------
  // Steam Store API — structured metadata
  // -------------------------------------------------------------------------
  let steamData;
  try {
    const apiUrl  = `https://store.steampowered.com/api/appdetails?appids=${appId}&l=english`;
    const response = await fetchWithRetry(apiUrl);
    const json     = await response.json();

    // Steam returns { "<appid>": { success: bool, data: {...} } }
    if (!json[appId] || json[appId].success === false) {
      return { success: false, notFound: true, error: `App ${appId} not found on Steam` };
    }

    steamData = json[appId].data;
  } catch (err) {
    throw new Error(`Steam Store API failed for ${appId}: ${err.message}`);
  }

  const name             = steamData.name;
  const shortDescription = steamData.short_description    || null;
  const longDescription  = steamData.detailed_description || null;
  const releaseDateText  = steamData.release_date?.date   || null;
  // These may be overridden by richer data scraped from the store page below
  let developersList = steamData.developers || [];
  let publishersList = steamData.publishers || [];
  let websiteUrl     = steamData.website    || null;

  // -------------------------------------------------------------------------
  // Determine released status
  // If the release date parses as a real past date, the game is out.
  // -------------------------------------------------------------------------
  let released         = false;
  let releaseDateParsed = null;

  if (releaseDateText) {
    try {
      const parsedDate = new Date(releaseDateText);
      if (!isNaN(parsedDate.getTime())) {
        releaseDateParsed = parsedDate.toISOString().split('T')[0];
        released = parsedDate < new Date();
      }
    } catch {
      // Unparseable date string (e.g. "Coming soon") — released stays false
    }
  }

  // -------------------------------------------------------------------------
  // Steam store page scrape — tags, social links, language table, controller
  // The Store API doesn't expose these fields, so we parse the HTML directly.
  // Scraping is best-effort: failure logs a warning but doesn't abort the run.
  // -------------------------------------------------------------------------
  let tags              = [];
  let discordUrl        = null;
  let discordInviteCode = null;
  let xUrl              = null;
  let redditUrl         = null;
  let languagesCount    = null;
  let fullAudioLanguages = null;
  let controllerSupport  = 'none';

  try {
    const pageUrl  = `https://store.steampowered.com/app/${appId}`;
    const response = await fetchWithRetry(pageUrl, {
      headers: {
        // Mimic a real browser to avoid bot-detection redirects
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        // Bypass age-gate for mature-rated games
        'Cookie':          'birthtime=0; lastagecheckage=1-January-1990; mature_content=1',
      },
    });

    const html = await response.text();

    // --- Tags ---------------------------------------------------------------
    // Steam embeds tag data in a JS call: InitAppTagModal( <appid>, [...], ...)
    const tagsMatch = html.match(/InitAppTagModal\s*\(\s*\d+,\s*(\[.*?\])/s);
    if (tagsMatch) {
      try {
        const tagsJson = JSON.parse(tagsMatch[1]);
        tags = tagsJson.slice(0, 20).map(t => t.name).filter(Boolean);
      } catch {
        console.warn(`[${appId}] Warning: could not parse tags JSON from store page.`);
      }
    }

    // --- Discord URL --------------------------------------------------------
    // Social links are routed through Steam's linkfilter proxy.
    // linkfilter/?u= — the ? must be \? in regex (not a quantifier).
    const discordMatch = html.match(/linkfilter\/\?u=([^"]+discord[^"]+)/i);
    if (discordMatch) {
      const parsed = parseDiscordUrl(discordMatch[1]);
      if (parsed.type !== 'not_found') {
        discordUrl        = parsed.url;
        discordInviteCode = parsed.inviteCode;
      }
    }

    // --- X (Twitter) URL ----------------------------------------------------
    const xMatch = html.match(/linkfilter\/\?u=([^"]+x\.com[^"]+)/i);
    if (xMatch) xUrl = decodeURIComponent(xMatch[1]);

    // --- Reddit URL ---------------------------------------------------------
    const redditMatch = html.match(/linkfilter\/\?u=([^"]+reddit\.com[^"]+)/i);
    if (redditMatch) redditUrl = decodeURIComponent(redditMatch[1]);

    // --- Website URL --------------------------------------------------------
    // External sites go through linkfilter; Steam publisher pages are direct hrefs.
    const linkfilterWebMatch = html.match(/linkfilter\/\?u=([^"]+)"[^>]*>\s*Visit the website/i);
    if (linkfilterWebMatch) {
      websiteUrl = decodeURIComponent(linkfilterWebMatch[1]);
    } else {
      const directWebMatch = html.match(/href="([^"]+)"[^>]*>\s*[\n\t]*Visit the website/i);
      if (directWebMatch && !directWebMatch[1].includes('store.steampowered.com')) {
        websiteUrl = directWebMatch[1];
      }
    }

    // --- Developers (from page) ---------------------------------------------
    const devBlockMatch = html.match(/id="developers_list"[^>]*>(.*?)<\/div>/s);
    if (devBlockMatch) {
      const fromPage = extractAnchorTexts(devBlockMatch[1]);
      if (fromPage.length > 0) developersList = fromPage;
    }

    // --- Publishers (from page) ---------------------------------------------
    const pubBlockMatch = html.match(/<b>Publisher:<\/b>(.*?)<\/div>/s);
    if (pubBlockMatch) {
      const fromPage = extractAnchorTexts(pubBlockMatch[1]);
      if (fromPage.length > 0) publishersList = fromPage;
    }

    // --- Language table -----------------------------------------------------
    // Third cell (index 2) per row = Full Audio column.
    // Steam uses &#10004; (U+2714 ✔ HEAVY CHECK MARK) — not ✓ (U+2713).
    const langTableMatch = html.match(/<table[^>]*class="game_language_options"[^>]*>(.*?)<\/table>/s);
    if (langTableMatch) {
      const tableHtml = langTableMatch[1];
      const rows      = [...tableHtml.matchAll(/<tr[^>]*>(.*?)<\/tr>/gs)];
      const dataRows  = rows.slice(1).filter(r => r[1].trim().length > 0); // skip header row
      languagesCount  = dataRows.length;

      const audioLangs = [];
      for (const row of dataRows) {
        const cells = [...row[1].matchAll(/<td[^>]*>(.*?)<\/td>/gs)];
        if (cells.length >= 3) {
          const langName  = stripTags(cells[0][1]);
          const audioCell = stripTags(cells[2][1]);
          if ((audioCell.includes('&#10004;') || audioCell.includes('✔') ||
               audioCell.includes('✓')        || audioCell.includes('&#10003;')) && langName) {
            audioLangs.push(langName);
          }
        }
      }
      fullAudioLanguages = audioLangs.length > 0 ? audioLangs.join(', ') : null;
    }

    // --- Controller support -------------------------------------------------
    // Encoded as a JSON blob in a data-props attribute; HTML entities must be decoded.
    const controllerMatch = html.match(
      /data-featuretarget="store-sidebar-controller-support-info"[^>]*data-props="([^"]+)"/
    );
    if (controllerMatch) {
      try {
        const controllerJson = JSON.parse(controllerMatch[1].replace(/&quot;/g, '"'));
        if (controllerJson.bFullXboxControllerSupport) {
          controllerSupport = 'full';
        } else if (controllerJson.bPartialXboxControllerSupport) {
          controllerSupport = 'partial';
        } else {
          controllerSupport = 'none';
        }
      } catch {
        console.warn(`[${appId}] Warning: could not parse controller support JSON.`);
      }
    }

  } catch (err) {
    // Page scraping is best-effort — continue with API data only
    console.warn(`[${appId}] Steam page scrape failed: ${err.message}. Continuing with API data only.`);
  }

  // -------------------------------------------------------------------------
  // Write to Supabase games_static
  // Upsert on app_id to make re-runs safe.
  // -------------------------------------------------------------------------
  const record = {
    app_id:               appId,
    name,
    first_seen_date:      today,
    release_date:         releaseDateText,
    release_date_parsed:  releaseDateParsed,
    released,
    last_static_update:   today,
    short_description:    shortDescription,
    long_description:     longDescription,

    // Tags — up to 20 slots; unfilled slots are NULL
    tag1:  tags[0]  || null,  tag2:  tags[1]  || null,
    tag3:  tags[2]  || null,  tag4:  tags[3]  || null,
    tag5:  tags[4]  || null,  tag6:  tags[5]  || null,
    tag7:  tags[6]  || null,  tag8:  tags[7]  || null,
    tag9:  tags[8]  || null,  tag10: tags[9]  || null,
    tag11: tags[10] || null,  tag12: tags[11] || null,
    tag13: tags[12] || null,  tag14: tags[13] || null,
    tag15: tags[14] || null,  tag16: tags[15] || null,
    tag17: tags[16] || null,  tag18: tags[17] || null,
    tag19: tags[18] || null,  tag20: tags[19] || null,

    // Developers — up to 3 slots
    developer1: developersList[0] || null,
    developer2: developersList[1] || null,
    developer3: developersList[2] || null,

    // Publishers — up to 3 slots
    publisher1: publishersList[0] || null,
    publisher2: publishersList[1] || null,
    publisher3: publishersList[2] || null,

    languages_count:      languagesCount,
    full_audio_languages: fullAudioLanguages,
    controller_support:   controllerSupport,

    discord_url:          discordUrl,
    discord_invite_code:  discordInviteCode,
    x_url:                xUrl,
    reddit_url:           redditUrl,
    website_url:          websiteUrl,

    tracking_source:      trackingSource,
    study_tags:           studyTags || null,
    data_sources:         'steam_store,steam_page',
  };

  const { error } = await supabase
    .from('games_static')
    .upsert(record, { onConflict: 'app_id' });

  if (error) throw new Error(`Supabase upsert failed: ${error.message}`);

  return { success: true, name, tags: tags.length, released };
}

module.exports = { enrichGame, fetchWithRetry };
