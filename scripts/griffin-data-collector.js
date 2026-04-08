// griffin-data-collector.js
//
// One-off data collection script for a genre study covering 100 Steam games.
// Reads app IDs from griffin/Steam_AppIDs_for_Listed_Games_-_Steam_AppIDs_for_Listed_Games.csv
// and collects DAU history, wishlist history, tags, language distribution,
// and most-helpful reviews from Gamalytic and Steam APIs.
//
// Output files (all written to griffin/):
//   dau_history.csv        — daily active/monthly active users per game
//   wishlist_history.csv   — daily wishlist counts per game
//   tags.csv               — up to 20 Steam tags per game
//   languages.csv          — review count per language per game
//   games_index.csv        — current DAU, peak DAU, current wishlists per game
//   reviews.json           — 100 most helpful reviews per game (text only)
//
// Run locally:
//   node --env-file=../.env griffin-data-collector.js
//   (from scripts/ directory)

const fs   = require('fs');
const path = require('path');
const { fetchWithRetry } = require('./lib/steam-enrichment');

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const GAMALYTIC_API_KEY = process.env.GAMALYTIC_API_KEY;
if (!GAMALYTIC_API_KEY) {
  console.error('Error: GAMALYTIC_API_KEY is required.');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
const GRIFFIN_DIR = path.resolve(__dirname, '../griffin');
const INPUT_CSV   = path.join(GRIFFIN_DIR, 'Steam AppIDs for Listed Games - Steam AppIDs for Listed Games.csv');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const BETWEEN_GAMES_MS  = 1_000;  // 1 second between games
const BETWEEN_CALLS_MS  =   500;  // 500 ms between API calls within a game

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Escape a single CSV field value: wrap in quotes if it contains commas,
// quotes, or newlines; double any internal quotes.
function csvField(val) {
  const s = val == null ? '' : String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// Join an array of values into a CSV row string.
function csvRow(values) {
  return values.map(csvField).join(',');
}

// Write an array of row-arrays to a CSV file with a header row.
function writeCsv(filePath, header, rows) {
  const lines = [csvRow(header), ...rows.map(csvRow)];
  fs.writeFileSync(filePath, lines.join('\n') + '\n', 'utf8');
}

// ---------------------------------------------------------------------------
// CSV parser — simple split on comma (handles the input file which has no
// quoted fields with commas).
// ---------------------------------------------------------------------------
function parseInputCsv(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines   = content.trim().split('\n');
  const header  = lines[0].split(',');

  const titleIdx  = header.indexOf('Game Title');
  const appIdIdx  = header.indexOf('Steam AppID');

  if (titleIdx === -1 || appIdIdx === -1) {
    throw new Error('Required columns "Game Title" or "Steam AppID" not found in input CSV.');
  }

  return lines.slice(1).map(line => {
    const cols = line.split(',');
    return {
      name:  cols[titleIdx]?.trim()  || '',
      appId: cols[appIdIdx]?.trim()  || '',
    };
  }).filter(g => g.appId && g.name);
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

// 1. DAU / MAU history from Gamalytic
async function fetchDauHistory(appId) {
  const url = `https://api.gamalytic.com/game/${appId}/active-users-history`;
  const res  = await fetchWithRetry(url, { headers: { 'api-key': GAMALYTIC_API_KEY } });
  const data = await res.json();
  // Response is an array of { timestamp, dau, mau }
  if (!Array.isArray(data)) return [];
  return data.map(entry => ({
    date: new Date(entry.timestamp).toISOString().split('T')[0],
    dau:  entry.dau  ?? null,
    mau:  entry.mau  ?? null,
  }));
}

// 2. Wishlist history from Gamalytic
async function fetchWishlistHistory(appId) {
  const url = `https://api.gamalytic.com/game/${appId}?fields=history`;
  const res  = await fetchWithRetry(url, { headers: { 'api-key': GAMALYTIC_API_KEY } });
  const data = await res.json();
  const history = data?.history;
  if (!Array.isArray(history)) return [];
  return history.map(entry => {
    // Date may be an ISO string or a timestamp in ms
    let date;
    if (typeof entry.date === 'string') {
      date = entry.date.slice(0, 10);
    } else if (typeof entry.date === 'number') {
      date = new Date(entry.date).toISOString().split('T')[0];
    } else if (typeof entry.timeStamp === 'number') {
      date = new Date(entry.timeStamp).toISOString().split('T')[0];
    } else {
      date = '';
    }
    return { date, wishlists: entry.wishlists ?? null };
  }).filter(e => e.date);
}

// 3. Tags — scrape store page HTML for InitAppTagModal data, then fall back
//    to the Store API name field for the game name.
async function fetchTagsAndName(appId) {
  // Store API for the name
  const apiUrl = `https://store.steampowered.com/api/appdetails?appids=${appId}&l=english`;
  const apiRes  = await fetchWithRetry(apiUrl);
  const apiBody = await apiRes.json();
  const appData = apiBody?.[appId]?.data;
  const name    = appData?.name || null;

  await sleep(BETWEEN_CALLS_MS);

  // Scrape store page for tags
  const pageUrl = `https://store.steampowered.com/app/${appId}`;
  const pageRes = await fetchWithRetry(pageUrl, {
    headers: {
      'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cookie':          `birthtime=0; lastagecheckage=1-January-1990; mature_content=1; wants_mature_content_apps=${appId}`,
    },
  });
  const html = await pageRes.text();

  const tagMatch = /InitAppTagModal\(\s*\d+,\s*(\[.*?\]),/s.exec(html);
  let tags = [];
  if (tagMatch) {
    try {
      const parsed = JSON.parse(tagMatch[1]);
      tags = parsed.slice(0, 20).map(t => t.name || t.tagid || '').filter(Boolean);
    } catch {
      // leave tags empty
    }
  }

  return { name, tags };
}

// 4. Language distribution from Steam reviews summary
async function fetchLanguages(appId) {
  const url = `https://store.steampowered.com/appreviews/${appId}?json=1&filter=all&language=all&num_per_page=1&purchase_type=all`;
  const res  = await fetchWithRetry(url);
  const data = await res.json();
  const langs = data?.query_summary?.languages;
  if (!langs || typeof langs !== 'object') return [];
  return Object.entries(langs).map(([language, review_count]) => ({
    language,
    review_count,
  }));
}

// 5. Most helpful reviews (up to 100, text only)
async function fetchReviews(appId) {
  const url = `https://store.steampowered.com/appreviews/${appId}?json=1&filter=helpful&language=all&num_per_page=100&review_type=all&purchase_type=all`;
  const res  = await fetchWithRetry(url);
  const data = await res.json();
  const reviews = data?.reviews;
  if (!Array.isArray(reviews)) return [];
  return reviews.map(r => r.review).filter(Boolean);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('Griffin data collector\n');

  // Read input
  const games = parseInputCsv(INPUT_CSV);
  console.log(`Loaded ${games.length} games from input CSV.\n`);

  // Accumulators
  const dauRows       = [];   // [app_id, name, date, dau, mau]
  const wishlistRows  = [];   // [app_id, name, date, wishlists]
  const tagRows       = [];   // [app_id, name, tag1..tag20]
  const languageRows  = [];   // [app_id, name, language, review_count]
  const indexRows     = [];   // [app_id, name, current_dau, peak_dau, current_wishlists]
  const reviewsMap    = {};   // { appId: [text, ...] }

  // Failure tracking
  const failed = { dau: [], wishlists: [], tags: [], languages: [], reviews: [] };

  for (let i = 0; i < games.length; i++) {
    const { name: csvName, appId } = games[i];
    const prefix = `[${String(i + 1).padStart(3, ' ')}/${games.length}] ${csvName} (${appId})`;

    if (i > 0) await sleep(BETWEEN_GAMES_MS);

    const status = { dau: '✗', wishlists: '✗', tags: '✗', languages: '✗', reviews: '✗' };
    let resolvedName = csvName;  // use CSV name as fallback

    // ---- 1. DAU history ----
    let currentDau = null;
    let peakDau    = null;
    try {
      const entries = await fetchDauHistory(appId);
      for (const e of entries) {
        dauRows.push([appId, csvName, e.date, e.dau, e.mau]);
      }
      if (entries.length > 0) {
        // Most recent first or last — find max and last
        const dauValues = entries.map(e => e.dau).filter(v => v != null);
        currentDau = entries[entries.length - 1].dau;
        peakDau    = dauValues.length ? Math.max(...dauValues) : null;
      }
      status.dau = '✓';
    } catch (err) {
      console.error(`  DAU failed: ${err.message}`);
      failed.dau.push(csvName);
    }

    await sleep(BETWEEN_CALLS_MS);

    // ---- 2. Wishlist history ----
    let currentWishlists = null;
    try {
      const entries = await fetchWishlistHistory(appId);
      for (const e of entries) {
        wishlistRows.push([appId, csvName, e.date, e.wishlists]);
      }
      if (entries.length > 0) {
        currentWishlists = entries[entries.length - 1].wishlists;
      }
      status.wishlists = '✓';
    } catch (err) {
      console.error(`  Wishlists failed: ${err.message}`);
      failed.wishlists.push(csvName);
    }

    await sleep(BETWEEN_CALLS_MS);

    // ---- 3. Tags + name ----
    try {
      const { name: apiName, tags } = await fetchTagsAndName(appId);
      if (apiName) resolvedName = apiName;
      // Pad to 20 tag columns
      const padded = [...tags];
      while (padded.length < 20) padded.push('');
      tagRows.push([appId, resolvedName, ...padded]);
      status.tags = '✓';
    } catch (err) {
      console.error(`  Tags failed: ${err.message}`);
      failed.tags.push(csvName);
      // Still add a row with empty tags so the game appears in the file
      tagRows.push([appId, resolvedName, ...Array(20).fill('')]);
    }

    await sleep(BETWEEN_CALLS_MS);

    // ---- 4. Language distribution ----
    try {
      const langs = await fetchLanguages(appId);
      for (const { language, review_count } of langs) {
        languageRows.push([appId, resolvedName, language, review_count]);
      }
      status.languages = '✓';
    } catch (err) {
      console.error(`  Languages failed: ${err.message}`);
      failed.languages.push(csvName);
    }

    await sleep(BETWEEN_CALLS_MS);

    // ---- 5. Reviews ----
    try {
      const texts = await fetchReviews(appId);
      reviewsMap[appId] = texts;
      status.reviews = '✓';
    } catch (err) {
      console.error(`  Reviews failed: ${err.message}`);
      failed.reviews.push(csvName);
      reviewsMap[appId] = [];
    }

    // ---- Index row ----
    indexRows.push([appId, resolvedName, currentDau, peakDau, currentWishlists]);

    console.log(`${prefix}: DAU ${status.dau} wishlists ${status.wishlists} tags ${status.tags} languages ${status.languages} reviews ${status.reviews}`);
  }

  // ---------------------------------------------------------------------------
  // Write output files
  // ---------------------------------------------------------------------------
  console.log('\nWriting output files...');

  // 1. dau_history.csv
  writeCsv(
    path.join(GRIFFIN_DIR, 'dau_history.csv'),
    ['app_id', 'name', 'date', 'dau', 'mau'],
    dauRows
  );
  console.log(`  dau_history.csv       — ${dauRows.length} rows`);

  // 2. wishlist_history.csv
  writeCsv(
    path.join(GRIFFIN_DIR, 'wishlist_history.csv'),
    ['app_id', 'name', 'date', 'wishlists'],
    wishlistRows
  );
  console.log(`  wishlist_history.csv  — ${wishlistRows.length} rows`);

  // 3. tags.csv
  const tagHeader = ['app_id', 'name', ...Array.from({ length: 20 }, (_, i) => `tag${i + 1}`)];
  writeCsv(path.join(GRIFFIN_DIR, 'tags.csv'), tagHeader, tagRows);
  console.log(`  tags.csv              — ${tagRows.length} rows`);

  // 4. languages.csv
  writeCsv(
    path.join(GRIFFIN_DIR, 'languages.csv'),
    ['app_id', 'name', 'language', 'review_count'],
    languageRows
  );
  console.log(`  languages.csv         — ${languageRows.length} rows`);

  // 5. games_index.csv
  writeCsv(
    path.join(GRIFFIN_DIR, 'games_index.csv'),
    ['app_id', 'name', 'current_dau', 'peak_dau', 'current_wishlists'],
    indexRows
  );
  console.log(`  games_index.csv       — ${indexRows.length} rows`);

  // 6. reviews.json
  fs.writeFileSync(
    path.join(GRIFFIN_DIR, 'reviews.json'),
    JSON.stringify(reviewsMap, null, 2),
    'utf8'
  );
  const totalReviews = Object.values(reviewsMap).reduce((s, r) => s + r.length, 0);
  console.log(`  reviews.json          — ${totalReviews} reviews across ${Object.keys(reviewsMap).length} games`);

  // ---------------------------------------------------------------------------
  // Final summary
  // ---------------------------------------------------------------------------
  const n = games.length;
  console.log(`
Collection complete.
DAU: ${n - failed.dau.length}/${n} | Wishlists: ${n - failed.wishlists.length}/${n} | Tags: ${n - failed.tags.length}/${n} | Languages: ${n - failed.languages.length}/${n} | Reviews: ${n - failed.reviews.length}/${n}`);

  if (failed.dau.length)       console.log(`  DAU failures:       ${failed.dau.join(', ')}`);
  if (failed.wishlists.length) console.log(`  Wishlist failures:  ${failed.wishlists.join(', ')}`);
  if (failed.tags.length)      console.log(`  Tag failures:       ${failed.tags.join(', ')}`);
  if (failed.languages.length) console.log(`  Language failures:  ${failed.languages.join(', ')}`);
  if (failed.reviews.length)   console.log(`  Review failures:    ${failed.reviews.join(', ')}`);
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
