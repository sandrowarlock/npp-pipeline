// daily-game-snapshot.js
// Workflow: daily-game-snapshot
//
// Runs daily and records one snapshot row per tracked game, capturing the
// current wishlist count (from Gamalytic) and Discord stats (from the
// Discord scraper Supabase project).
//
// Steps:
//   1. Read all tracked games from games_static
//   2. Fetch wishlist counts from Gamalytic (bulk by app_id list)
//   3. Fetch Discord stats from scraper Supabase (read-only)
//   4. Merge and write one row per game to games_daily_snapshots (upsert)
//   5. Log pipeline run outcome (null counts, discord gaps)
//
// Exit codes:
//   0 — all games processed (partial data counts as success)
//   1 — fatal error (cannot read games list or complete step fails)

const { createClient } = require('@supabase/supabase-js');
const { logRun }       = require('./lib/pipeline-logger');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GAMALYTIC_API_KEY    = process.env.GAMALYTIC_API_KEY;
const SCRAPER_SUPABASE_URL = process.env.SCRAPER_SUPABASE_URL;
const SCRAPER_SUPABASE_KEY = process.env.SCRAPER_SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !GAMALYTIC_API_KEY) {
  console.error('Error: SUPABASE_URL, SUPABASE_SERVICE_KEY, and GAMALYTIC_API_KEY are required.');
  process.exit(1);
}

if (!SCRAPER_SUPABASE_URL || !SCRAPER_SUPABASE_KEY) {
  console.warn('Warning: SCRAPER_SUPABASE_URL or SCRAPER_SUPABASE_KEY not set — Discord data will be skipped.');
}

const supabase        = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const scraperSupabase = (SCRAPER_SUPABASE_URL && SCRAPER_SUPABASE_KEY)
  ? createClient(SCRAPER_SUPABASE_URL, SCRAPER_SUPABASE_KEY)
  : null;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const GAMALYTIC_BASE = 'https://api.gamalytic.com/steam-games/list';
const RETRY_DELAYS   = [2000, 4000, 8000]; // delays before retries 1, 2, 3

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch Gamalytic data for a batch of app IDs.
// Returns an array of { steamId, wishlists, followers } objects.
// On failure, throws so the caller can decide how to handle it.
async function fetchGamalyticBatch(appIds) {
  const params = new URLSearchParams({
    fields:  'steamId,wishlists,followers',
    appids:  appIds.join(','),
    limit:   String(appIds.length),
  });

  const url     = `${GAMALYTIC_BASE}?${params}`;
  const headers = { 'api-key': GAMALYTIC_API_KEY };

  for (let attempt = 0; attempt <= 3; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_DELAYS[attempt - 1];
      console.warn(`  Gamalytic batch: retry attempt ${attempt}, waiting ${delay / 1000}s...`);
      await sleep(delay);
    }

    let response;
    try {
      response = await fetch(url, { headers });
    } catch (err) {
      if (attempt < 3) continue;
      throw new Error(`Network error after all retries: ${err.message}`);
    }

    if (response.status === 429) {
      console.warn(`  Gamalytic rate limit hit — waiting 60s...`);
      await sleep(60000);
      attempt--; // don't count this as a retry
      continue;
    }

    if (response.ok) {
      const data = await response.json();
      // Gamalytic wraps results in { pages, total, result: [...] }
      return Array.isArray(data.result) ? data.result : [];
    }

    if (response.status >= 400 && response.status < 500) {
      throw new Error(`Gamalytic HTTP ${response.status} client error — check API key or params`);
    }

    // 5xx — retry if attempts remain
    if (attempt < 3) {
      console.warn(`  Gamalytic HTTP ${response.status}, will retry...`);
      continue;
    }

    throw new Error(`Gamalytic HTTP ${response.status} after all retries`);
  }

  throw new Error('Exhausted all Gamalytic retry attempts');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today = new Date().toISOString().split('T')[0];
  console.log(`Daily game snapshot for: ${today}\n`);

  // -------------------------------------------------------------------------
  // Step 1 — Read all tracked games from games_static
  // -------------------------------------------------------------------------
  console.log('Step 1: Reading tracked games from games_static...');
  let games;
  try {
    const { data, error } = await supabase
      .from('games_static')
      .select('app_id, name, discord_invite_code')
      .order('app_id', { ascending: true });

    if (error) throw new Error(error.message);

    if (!data || data.length === 0) {
      console.log('No tracked games found. Exiting.');
      process.exit(0);
    }

    games = data;
    console.log(`Found ${games.length} tracked game(s).`);
  } catch (err) {
    console.error(`Step 1 failed (reading games_static): ${err.message}`);
    process.exit(1);
  }

  const appIds = games.map(g => g.app_id);

  // -------------------------------------------------------------------------
  // Step 2 — Fetch wishlist + followers counts from Gamalytic
  // TODO: Verify that the Gamalytic 'appids' bulk parameter works correctly
  //       for large tracked-game lists (hundreds of IDs). If not, paginate
  //       in batches of 100 and merge the results.
  // TODO: Confirm with Gamalytic support whether there is a dedicated
  //       backfill endpoint to retrieve historical daily data for specific
  //       app IDs, in case this workflow misses a day.
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Fetching Gamalytic wishlist data...');
  const gamalyticMap = new Map(); // app_id -> { wishlists, followers }
  try {
    const results = await fetchGamalyticBatch(appIds);
    for (const r of results) {
      gamalyticMap.set(r.steamId, {
        wishlist_count:   r.wishlists  ?? null,
        followers_count:  r.followers  ?? null,
      });
    }
    console.log(`Gamalytic returned data for ${gamalyticMap.size} of ${appIds.length} games.`);
  } catch (err) {
    console.error(`Step 2 failed (Gamalytic fetch): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Fetch Discord stats from scraper Supabase
  // Only games in games_static with a discord_invite_code are expected to
  // have Discord data. Missing rows are recorded as gaps, not errors.
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Fetching Discord stats from scraper project...');
  const discordMap = new Map(); // app_id -> { member_count, online_count }

  if (!scraperSupabase) {
    console.warn('  Skipped — scraper Supabase credentials not configured.');
  } else {
    try {
      // The scraper project stores the latest snapshot per steam_id.
      // We read all rows and build a lookup by steam_id (= app_id).
      const { data, error } = await scraperSupabase
        .from('discord_snapshots')
        .select('steam_id, member_count, active_user_count')
        .in('steam_id', appIds);

      if (error) throw new Error(error.message);

      for (const row of (data || [])) {
        discordMap.set(row.steam_id, {
          discord_member_count: row.member_count        ?? null,
          discord_online_count: row.active_user_count   ?? null,
        });
      }
      console.log(`Discord data found for ${discordMap.size} game(s).`);
    } catch (err) {
      // Discord data is non-critical — log a warning and continue without it.
      console.warn(`  Step 3 warning (Discord fetch failed): ${err.message}`);
      console.warn('  Proceeding without Discord data for this run.');
    }
  }

  // -------------------------------------------------------------------------
  // Step 4 — Merge and upsert one row per game into games_daily_snapshots
  // Conflict on (date, app_id) makes re-runs idempotent.
  // -------------------------------------------------------------------------
  console.log('\nStep 4: Writing daily snapshots...');
  let rowsWritten       = 0;
  let nullWishlistCount = 0;
  let nullFollowersCount = 0;
  let discordGaps       = 0;

  const rows = games.map(game => {
    const gamalytic = gamalyticMap.get(game.app_id) ?? {};
    const discord   = discordMap.get(game.app_id)   ?? {};

    const wishlistCount  = gamalytic.wishlist_count  ?? null;
    const followersCount = gamalytic.followers_count ?? null;
    const memberCount    = discord.discord_member_count ?? null;
    const onlineCount    = discord.discord_online_count ?? null;

    if (wishlistCount  === null) nullWishlistCount++;
    if (followersCount === null) nullFollowersCount++;

    // Count a Discord gap only if the game has a known Discord server
    if (game.discord_invite_code && memberCount === null) discordGaps++;

    return {
      date:                 today,
      app_id:               game.app_id,
      wishlist_count:       wishlistCount,
      followers_count:      followersCount,
      discord_member_count: memberCount,
      discord_online_count: onlineCount,
      data_source:          'live',
    };
  });

  try {
    const { error } = await supabase
      .from('games_daily_snapshots')
      .upsert(rows, { onConflict: 'date,app_id' });

    if (error) throw new Error(error.message);

    rowsWritten = rows.length;
    console.log(`Wrote ${rowsWritten} rows to games_daily_snapshots.`);
  } catch (err) {
    console.error(`Step 4 failed (upserting snapshots): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Report data quality
  // -------------------------------------------------------------------------
  if (nullWishlistCount > 0) {
    console.warn(`  Warning: ${nullWishlistCount} game(s) have null wishlist_count.`);
  }
  if (nullFollowersCount > 0) {
    console.warn(`  Warning: ${nullFollowersCount} game(s) have null followers_count.`);
  }
  if (discordGaps > 0) {
    console.warn(`  Warning: ${discordGaps} game(s) with a Discord server have no Discord data.`);
  }

  // -------------------------------------------------------------------------
  // Step 5 — Log pipeline run outcome
  // -------------------------------------------------------------------------
  try {
    await logRun(supabase, {
      workflowName:       'daily-game-snapshot',
      status:             'success',
      rowsWritten,
      rowsExpected:       games.length,
      nullWishlistCount,
      nullFollowersCount,
      discordGaps,
    });
    console.log('\nPipeline run logged.');
  } catch (logErr) {
    console.warn(`Pipeline log failed: ${logErr.message}`);
  }

  console.log('\n✓ daily-game-snapshot complete.');
}

main();
