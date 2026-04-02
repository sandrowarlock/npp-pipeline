// add-game-to-track.js
// Workflow: add-game-to-track
//
// Pulls static metadata for a single Steam game and writes it to the
// games_static table in Supabase. Combines data from the Steam Store API
// (structured fields) and the Steam store page HTML (tags, social links,
// language table, controller support).
//
// Inputs (via environment variables):
//   STEAM_APP_ID       — Steam app ID to track (required)
//   TRACKING_SOURCE    — Where this lead came from (default: 'manual')
//   STUDY_TAGS         — Internal grouping tags, comma-separated (optional)
//   SUPABASE_URL       — NPP Supabase project URL
//   SUPABASE_SERVICE_KEY — Supabase service role key (write access)

const { createClient } = require('@supabase/supabase-js');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const STEAM_APP_ID     = process.env.STEAM_APP_ID;
const TRACKING_SOURCE  = process.env.TRACKING_SOURCE || 'manual';
const STUDY_TAGS       = process.env.STUDY_TAGS || '';
const SUPABASE_URL     = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!STEAM_APP_ID) {
  console.error('Error: STEAM_APP_ID environment variable is required.');
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Error: SUPABASE_URL and SUPABASE_SERVICE_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Extract all inner text values from <a> tags in an HTML fragment
function extractAnchorTexts(html) {
  const matches = [...html.matchAll(/<a[^>]*>([^<]+)<\/a>/g)];
  return matches.map(m => m[1].trim()).filter(Boolean);
}

// Strip all HTML tags from a string
function stripTags(html) {
  return html.replace(/<[^>]+>/g, '').trim();
}

// Classify a Discord URL and extract an invite code if present
function parseDiscordUrl(rawUrl) {
  const decoded = decodeURIComponent(rawUrl);
  if (/discord\.gg\/|discord\.com\/invite\//i.test(decoded)) {
    const inviteMatch = decoded.match(/discord\.(?:gg|com\/invite)\/([^/?#\s]+)/i);
    return {
      type: 'invite',
      url: decoded,
      inviteCode: inviteMatch ? inviteMatch[1] : null,
    };
  }
  if (/discord\.com\/channels\//i.test(decoded)) {
    return { type: 'direct', url: decoded, inviteCode: null };
  }
  return { type: 'not_found', url: null, inviteCode: null };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const appId = parseInt(STEAM_APP_ID, 10);
  const today = new Date().toISOString().split('T')[0];

  // -------------------------------------------------------------------------
  // Step 1 — Check if the game already exists in games_static
  // We skip re-adding games that are already being tracked to avoid
  // overwriting data (especially first_seen_date).
  // -------------------------------------------------------------------------
  try {
    const { data, error } = await supabase
      .from('games_static')
      .select('app_id, name')
      .eq('app_id', appId)
      .single();

    // PGRST116 = "no rows returned" — that's fine, means game is new
    if (error && error.code !== 'PGRST116') {
      throw new Error(`Supabase lookup failed: ${error.message}`);
    }

    if (data) {
      console.log(`Game already tracked: "${data.name}" (app_id: ${appId}). Nothing to do.`);
      process.exit(0);
    }
  } catch (err) {
    console.error(`Step 1 failed (checking for existing game): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 2 — Call Steam Store API for structured metadata
  // Returns: name, descriptions, release date, developers, publishers, website
  // -------------------------------------------------------------------------
  let steamData;
  try {
    const apiUrl = `https://store.steampowered.com/api/appdetails?appids=${appId}&l=english`;
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`Steam API returned HTTP ${response.status}`);
    }
    const json = await response.json();

    // Steam returns { "<appid>": { success: bool, data: {...} } }
    if (!json[appId] || json[appId].success === false) {
      console.log(`Game not found on Steam (app_id: ${appId}). Exiting.`);
      process.exit(0);
    }

    steamData = json[appId].data;
  } catch (err) {
    console.error(`Step 2 failed (Steam Store API): ${err.message}`);
    process.exit(1);
  }

  const name             = steamData.name;
  const shortDescription = steamData.short_description    || null;
  const longDescription  = steamData.detailed_description || null;
  const releaseDateText  = steamData.release_date?.date   || null;
  // Fallback developer/publisher lists — may be overridden by page scrape
  let developersList = steamData.developers || [];
  let publishersList = steamData.publishers || [];
  let websiteUrl     = steamData.website    || null;

  // -------------------------------------------------------------------------
  // Step 3 — Determine whether the game has already been released
  // If the release date string can be parsed as a real date that's in the
  // past, mark it as released. Store the ISO date for range queries.
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
      // Unparseable date (e.g. "Coming soon") — released stays false
    }
  }

  // -------------------------------------------------------------------------
  // Step 4 — Scrape the Steam store page for richer signals
  // The store API doesn't expose tags, social links, language tables, or
  // controller support — those require parsing the rendered HTML directly.
  // -------------------------------------------------------------------------
  let tags             = [];
  let discordUrl       = null;
  let discordInviteCode = null;
  let xUrl             = null;
  let redditUrl        = null;
  let languagesCount   = null;
  let fullAudioLanguages = null;
  let controllerSupport  = 'none';

  try {
    const pageUrl  = `https://store.steampowered.com/app/${appId}`;
    const response = await fetch(pageUrl, {
      headers: {
        // Mimic a real browser to avoid bot-detection redirects
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        // Bypass age-gate for mature-rated games
        'Cookie':          'birthtime=0; lastagecheckage=1-January-1990; mature_content=1',
      },
    });

    if (!response.ok) {
      throw new Error(`Steam page returned HTTP ${response.status}`);
    }

    const html = await response.text();

    // --- Tags ---------------------------------------------------------------
    // Steam embeds tag data in a JS call: InitAppTagModal( <appid>, [...], ...)
    // The JSON array contains objects with { tagid, name, count, ... }
    const tagsMatch = html.match(/InitAppTagModal\s*\(\s*\d+,\s*(\[.*?\])/s);
    if (tagsMatch) {
      try {
        const tagsJson = JSON.parse(tagsMatch[1]);
        tags = tagsJson.slice(0, 20).map(t => t.name).filter(Boolean);
      } catch {
        console.warn('Warning: could not parse tags JSON from store page.');
      }
    }

    // --- Discord URL --------------------------------------------------------
    // Social links on Steam store pages are routed through a linkfilter proxy.
    // The URL format is linkfilter/?u=... — the ? must be escaped as \? in regex,
    // otherwise it acts as a quantifier making the / optional instead of literal.
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
    if (xMatch) {
      xUrl = decodeURIComponent(xMatch[1]);
    }

    // --- Reddit URL ---------------------------------------------------------
    const redditMatch = html.match(/linkfilter\/\?u=([^"]+reddit\.com[^"]+)/i);
    if (redditMatch) {
      redditUrl = decodeURIComponent(redditMatch[1]);
    }

    // --- Website URL --------------------------------------------------------
    // "Visit the website" links come in two forms on Steam store pages:
    // 1. External sites routed through linkfilter: linkfilter/?u=https%3A%2F%2F...
    // 2. Steam-hosted publisher pages as a direct href (no linkfilter proxy)
    // Try linkfilter first; fall back to direct href but exclude Steam URLs since
    // those are publisher index pages, not the game's own website.
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
    // The page sometimes includes more detail than the API array
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
    // The language options table has one row per language. The third cell
    // (index 2) contains a checkmark (✓ or &#10003;) if full audio is available.
    const langTableMatch = html.match(/<table[^>]*class="game_language_options"[^>]*>(.*?)<\/table>/s);
    if (langTableMatch) {
      const tableHtml = langTableMatch[1];
      const rows = [...tableHtml.matchAll(/<tr[^>]*>(.*?)<\/tr>/gs)];

      // First row is the header (Interface / Full Audio / Subtitles)
      const dataRows = rows.slice(1).filter(r => r[1].trim().length > 0);
      languagesCount = dataRows.length;

      const audioLangs = [];
      for (const row of dataRows) {
        const cells = [...row[1].matchAll(/<td[^>]*>(.*?)<\/td>/gs)];
        if (cells.length >= 3) {
          const langName  = stripTags(cells[0][1]);
          const audioCell = stripTags(cells[2][1]);
          // Steam uses &#10004; (U+2714 ✔ HEAVY CHECK MARK) in the language table,
          // not ✓ (U+2713) or &#10003; as originally assumed from the spec.
          if ((audioCell.includes('&#10004;') || audioCell.includes('✔') || audioCell.includes('✓') || audioCell.includes('&#10003;')) && langName) {
            audioLangs.push(langName);
          }
        }
      }
      fullAudioLanguages = audioLangs.length > 0 ? audioLangs.join(', ') : null;
    }

    // --- Controller support -------------------------------------------------
    // Steam encodes controller support as a JSON blob in a data-props attribute
    const controllerMatch = html.match(
      /data-featuretarget="store-sidebar-controller-support-info"[^>]*data-props="([^"]+)"/
    );
    if (controllerMatch) {
      try {
        // Attribute is HTML-encoded — replace &quot; before parsing
        const controllerJson = JSON.parse(controllerMatch[1].replace(/&quot;/g, '"'));
        if (controllerJson.bFullXboxControllerSupport) {
          controllerSupport = 'full';
        } else if (controllerJson.bPartialXboxControllerSupport) {
          controllerSupport = 'partial';
        } else {
          controllerSupport = 'none';
        }
      } catch {
        console.warn('Warning: could not parse controller support JSON.');
      }
    }

  } catch (err) {
    // Store page scraping is best-effort — log the failure but continue so
    // that the API data (name, description, etc.) is still written to Supabase.
    console.warn(`Step 4 warning (Steam page scrape): ${err.message}`);
    console.warn('Continuing with data from Steam Store API only.');
  }

  // -------------------------------------------------------------------------
  // Step 5 — Write to Supabase games_static
  // Upsert on app_id so re-runs are safe (though step 1 normally prevents
  // that). Spreads tag1–tag20 and developer/publisher slots from arrays.
  // -------------------------------------------------------------------------
  try {
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

      tracking_source:      TRACKING_SOURCE,
      study_tags:           STUDY_TAGS || null,
      data_sources:         'steam_store,steam_page',
    };

    const { error } = await supabase
      .from('games_static')
      .upsert(record, { onConflict: 'app_id' });

    if (error) {
      throw new Error(`Supabase upsert failed: ${error.message}`);
    }
  } catch (err) {
    console.error(`Step 5 failed (Supabase write): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 6 — Log success
  // -------------------------------------------------------------------------
  console.log(`✓ Successfully added game: "${name}" (app_id: ${appId})`);
  console.log(`  tracking_source: ${TRACKING_SOURCE}`);
  if (STUDY_TAGS) console.log(`  study_tags: ${STUDY_TAGS}`);
  console.log(`  tags found: ${tags.length}`);
  console.log(`  released: ${released}${releaseDateParsed ? ` (${releaseDateParsed})` : ''}`);
}

main();
