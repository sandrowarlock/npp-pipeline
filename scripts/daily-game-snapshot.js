// daily-game-snapshot.js
// Workflow: daily-game-snapshot
//
// Backfills and maintains daily snapshots for all tracked games.
//
// Core principle: for every tracked game, fetch the full history from all
// sources (Gamalytic and Discord scraper), compare against what already
// exists in games_daily_snapshots, and write only the missing dates.
//
//   First run      → writes all historical data
//   Normal daily   → writes only today
//   After a gap    → automatically fills missed days
//
// All writes upsert on (date, app_id) so running the script twice is safe.
//
// Steps:
//   1. Get all tracked games from games_static
//   2. Get existing snapshot dates per game from games_daily_snapshots
//   3. Fetch Gamalytic history per game, skip already-stored dates
//   4. Bulk upsert Gamalytic rows (chunks of 100)
//   5. Fetch Discord history from scraper Supabase, skip already-stored dates
//   6. Calculate sanity-check metrics for today
//   7. Log pipeline run to pipeline_runs
//   8. Exit non-zero if >50% of games are missing today's wishlist count

const { createClient }   = require('@supabase/supabase-js');
const { fetchWithRetry } = require('./lib/steam-enrichment');
const { logRun }         = require('./lib/pipeline-logger');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const SCRAPER_SUPABASE_URL = process.env.SCRAPER_SUPABASE_URL;
const SCRAPER_SUPABASE_KEY = process.env.SCRAPER_SUPABASE_KEY;
const GAMALYTIC_API_KEY    = process.env.GAMALYTIC_API_KEY;

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
// Helpers
// ---------------------------------------------------------------------------

// Convert a Unix timestamp in milliseconds to a YYYY-MM-DD string.
function msToDateStr(ms) {
  return new Date(ms).toISOString().split('T')[0];
}

// Fetch all rows from a Supabase query builder in batches of 1,000,
// paginating with .range() until a batch returns fewer than 1,000 rows.
// `queryFn` receives (from, to) and must return a Supabase query with
// .select() already applied — range is appended here.
// Returns a flat array of all rows across all pages.
async function fetchAllRows(queryFn, batchSize = 1000) {
  const all = [];
  let offset = 0;

  while (true) {
    const { data, error } = await queryFn(offset, offset + batchSize - 1);
    if (error) throw new Error(error.message);

    const rows = data || [];
    all.push(...rows);

    if (rows.length < batchSize) break;
    offset += batchSize;
  }

  return all;
}

// Upsert an array of rows in chunks on the given conflict target.
// Returns the total number of rows attempted.
async function upsertInChunks(client, table, rows, chunkSize, onConflict) {
  let total = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const { error } = await client
      .from(table)
      .upsert(chunk, { onConflict, ignoreDuplicates: false });
    if (error) throw new Error(`Upsert to ${table} failed: ${error.message}`);
    total += chunk.length;
  }
  return total;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today = new Date().toISOString().split('T')[0];
  console.log(`daily-game-snapshot — ${today}\n`);

  // -------------------------------------------------------------------------
  // Step 1 — Get all tracked games from games_static
  // -------------------------------------------------------------------------
  console.log('Step 1: Loading tracked games...');

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
    console.log(`${games.length} tracked game(s).`);
  } catch (err) {
    console.error(`Step 1 failed: ${err.message}`);
    process.exit(1);
  }

  const appIds = games.map(g => g.app_id);

  // -------------------------------------------------------------------------
  // Step 2 — Get existing snapshot dates and Discord values per game
  //
  // Single bulk query — no per-game queries.
  // We build:
  //   existingWishlistDates : app_id → Set<YYYY-MM-DD>
  //     Dates where wishlist_count is already stored. Gamalytic rows skip
  //     these dates entirely rather than overwriting with duplicate data.
  //
  //   existingDiscordData   : app_id → Map<YYYY-MM-DD, {member, online}>
  //     Dates that already have Discord data, along with the actual values.
  //     Used in Step 3 so Gamalytic rows copy existing Discord values
  //     instead of writing null — the Supabase JS client has no partial-
  //     column-update syntax, so we merge at the application layer instead.
  //
  //   existingDiscordDates  : app_id → Set<YYYY-MM-DD>
  //     Convenience set used in Step 5 to skip Discord rows already stored.
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Loading existing snapshot dates...');

  const existingWishlistDates = new Map(); // app_id → Set<string>
  const existingDiscordDates  = new Map(); // app_id → Set<string>
  const existingDiscordData   = new Map(); // app_id → Map<string, {member, online}>

  for (const id of appIds) {
    existingWishlistDates.set(id, new Set());
    existingDiscordDates.set(id, new Set());
    existingDiscordData.set(id, new Map());
  }

  try {
    const allRows = await fetchAllRows((from, to) =>
      supabase
        .from('games_daily_snapshots')
        .select('app_id, date, wishlist_count, discord_member_count, discord_online_count')
        .in('app_id', appIds)
        .range(from, to)
    );

    for (const row of allRows) {
      const dateStr = typeof row.date === 'string' ? row.date.split('T')[0] : row.date;

      if (row.wishlist_count !== null) {
        existingWishlistDates.get(row.app_id)?.add(dateStr);
      }

      if (row.discord_member_count !== null) {
        existingDiscordDates.get(row.app_id)?.add(dateStr);
        existingDiscordData.get(row.app_id)?.set(dateStr, {
          member: row.discord_member_count,
          online: row.discord_online_count ?? null,
        });
      }
    }

    console.log(`Found ${allRows.length} existing snapshot rows across all games.`);
  } catch (err) {
    console.error(`Step 2 failed: ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Fetch Gamalytic history per game
  //
  // TODO: verify Gamalytic per-game history endpoint and bulk appids endpoint
  // work correctly before enabling schedule.
  // Test locally with real games first.
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Fetching Gamalytic history per game...');

  // Use a Map keyed by "app_id:date" to deduplicate within the batch.
  // Gamalytic's history array often includes today's date; we also always
  // push a live row for today. The Map ensures the live row (written last)
  // wins for today, and Postgres never sees two rows with the same
  // (date, app_id) in a single upsert — which would cause an error.
  const gamalyticRowMap = new Map(); // "app_id:date" → row object
  let gamalyticErrors   = 0;

  for (const game of games) {
    const { app_id, name } = game;
    const knownDates = existingWishlistDates.get(app_id);

    const url = `https://api.gamalytic.com/game/${app_id}?fields=history,wishlists,followers&include_pre_release_history=true`;

    let data;
    try {
      const response = await fetchWithRetry(url, { headers: { 'api-key': GAMALYTIC_API_KEY } });
      data = await response.json();
    } catch (err) {
      console.error(`  ${name} (${app_id}): Gamalytic fetch failed — ${err.message}`);
      gamalyticErrors++;
      continue;
    }

    const history      = Array.isArray(data.history) ? data.history : [];
    const currentWish  = data.wishlists ?? null;
    const currentFollo = data.followers ?? null;

    let alreadyExisted = 0;
    let newRows        = 0;

    // Discord data already stored for this game, keyed by date string.
    // We copy existing discord values into Gamalytic rows so the upsert
    // never overwrites real Discord data with null. The Supabase JS client
    // cannot do partial-column updates on conflict, so we merge here.
    const discordByDate = existingDiscordData.get(app_id);

    // Process historical entries
    for (const entry of history) {
      const dateStr    = msToDateStr(entry.timeStamp);
      const discord    = discordByDate.get(dateStr);

      if (knownDates.has(dateStr)) {
        alreadyExisted++;
        continue;
      }

      gamalyticRowMap.set(`${app_id}:${dateStr}`, {
        date:                 dateStr,
        app_id,
        wishlist_count:       entry.wishlists         ?? null,
        followers_count:      entry.followers         ?? null,
        discord_member_count: discord?.member         ?? null,
        discord_online_count: discord?.online         ?? null,
        data_source:          'gamalytic_backfill',
      });
      newRows++;
    }

    // Always write today using the top-level current values — history array
    // can lag by a day, so this guarantees today's row is always fresh.
    // Writing into the Map last means the live row overwrites any history
    // entry for today, eliminating duplicates within the batch.
    const todayDiscord = discordByDate.get(today);
    gamalyticRowMap.set(`${app_id}:${today}`, {
      date:                 today,
      app_id,
      wishlist_count:       currentWish,
      followers_count:      currentFollo,
      discord_member_count: todayDiscord?.member      ?? null,
      discord_online_count: todayDiscord?.online      ?? null,
      data_source:          'live',
    });
    // Count today as new only if it wasn't already stored
    if (knownDates.has(today)) {
      alreadyExisted++;
    } else {
      newRows++;
    }

    console.log(`  ${name} (${app_id}): ${history.length} historical rows, ${alreadyExisted} already existed, ${newRows} new rows to write`);
  }

  // -------------------------------------------------------------------------
  // Step 4 — Bulk upsert Gamalytic rows
  //
  // On conflict (date, app_id), update wishlist and followers but never
  // overwrite discord columns that already have data.
  // -------------------------------------------------------------------------
  const gamalyticRows = [...gamalyticRowMap.values()];
  console.log(`\nStep 4: Upserting ${gamalyticRows.length} Gamalytic row(s)...`);

  let gamalyticRowsWritten = 0;
  try {
    gamalyticRowsWritten = await upsertInChunks(
      supabase,
      'games_daily_snapshots',
      gamalyticRows,
      100,
      'date,app_id',
    );
    console.log(`Wrote ${gamalyticRowsWritten} Gamalytic rows.`);
  } catch (err) {
    console.error(`Step 4 failed: ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 5 — Fetch Discord history from scraper Supabase
  //
  // The scraper stores historical daily_snapshots going back weeks.
  // We pull all of them and backfill any missing Discord dates.
  //
  // Scraper schema (read-only):
  //   games              : id, steam_app_id
  //   discord_servers    : id, game_id, is_active
  //   daily_snapshots    : discord_server_id, snapshot_date, member_count, online_count
  // -------------------------------------------------------------------------
  console.log('\nStep 5: Fetching Discord history from scraper...');

  let discordRowsWritten = 0;
  let discordGames       = 0; // games matched in scraper
  let discordNotFound    = 0; // games not in scraper at all
  const notInScraper     = [];

  if (!scraperSupabase) {
    console.warn('  Skipped — scraper Supabase credentials not configured.');
  } else {
    try {
      // 5a — Map tracked app_ids to scraper games.id
      const { data: scraperGames, error: gErr } = await scraperSupabase
        .from('games')
        .select('id, steam_app_id')
        .in('steam_app_id', appIds);

      if (gErr) throw new Error(`Scraper games query failed: ${gErr.message}`);

      const scraperGameIdByAppId = new Map(); // app_id → scraper games.id
      for (const row of (scraperGames || [])) {
        scraperGameIdByAppId.set(row.steam_app_id, row.id);
      }

      // Log which games are missing from the scraper
      for (const id of appIds) {
        if (!scraperGameIdByAppId.has(id)) {
          const game = games.find(g => g.app_id === id);
          notInScraper.push(id);
          console.warn(`  app_id ${id} (${game?.name ?? '?'}) not in scraper — add to scraper tracking`);
          discordNotFound++;
        }
      }

      const scraperGameIds = [...scraperGameIdByAppId.values()];

      if (scraperGameIds.length === 0) {
        console.warn('  No tracked games found in scraper.');
      } else {
        // 5b — Map scraper games.id → discord_servers.id (active servers only)
        const { data: servers, error: sErr } = await scraperSupabase
          .from('discord_servers')
          .select('id, game_id')
          .in('game_id', scraperGameIds)
          .eq('is_active', true);

        if (sErr) throw new Error(`Scraper discord_servers query failed: ${sErr.message}`);

        const serverIdByGameId  = new Map(); // scraper games.id → discord_servers.id
        const appIdByServerId   = new Map(); // discord_servers.id → app_id (NPP)

        for (const row of (servers || [])) {
          serverIdByGameId.set(row.game_id, row.id);
        }

        // Build reverse map: server_id → app_id
        for (const [appId, gameId] of scraperGameIdByAppId.entries()) {
          const serverId = serverIdByGameId.get(gameId);
          if (serverId !== undefined) {
            appIdByServerId.set(serverId, appId);
            discordGames++;
          }
        }

        const serverIds = [...appIdByServerId.keys()];

        if (serverIds.length === 0) {
          console.warn('  No active Discord servers found in scraper.');
        } else {
          // 5c — Pull all historical daily_snapshots for those servers.
          // Paginated — this table grows continuously as the scraper runs daily.
          let snapshots;
          try {
            snapshots = await fetchAllRows((from, to) =>
              scraperSupabase
                .from('daily_snapshots')
                .select('discord_server_id, snapshot_date, member_count, online_count')
                .in('discord_server_id', serverIds)
                .range(from, to)
            );
          } catch (snErr) {
            throw new Error(`Scraper daily_snapshots query failed: ${snErr.message}`);
          }

          const discordRows = [];

          for (const snap of (snapshots || [])) {
            const appId   = appIdByServerId.get(snap.discord_server_id);
            if (appId === undefined) continue;

            const dateStr = typeof snap.snapshot_date === 'string'
              ? snap.snapshot_date.split('T')[0]
              : new Date(snap.snapshot_date).toISOString().split('T')[0];

            // Skip dates where we already have Discord data for this game
            if (existingDiscordDates.get(appId)?.has(dateStr)) continue;

            discordRows.push({
              date:                 dateStr,
              app_id:               appId,
              discord_member_count: snap.member_count  ?? null,
              discord_online_count: snap.online_count  ?? null,
            });
          }

          console.log(`  ${snapshots.length} scraper snapshot(s) found, ${discordRows.length} new Discord rows to write.`);

          if (discordRows.length > 0) {
            discordRowsWritten = await upsertInChunks(
              supabase,
              'games_daily_snapshots',
              discordRows,
              100,
              'date,app_id',
            );
            console.log(`  Wrote ${discordRowsWritten} Discord rows.`);
          }
        }
      }
    } catch (err) {
      // Discord data is supplementary — log warning and continue
      console.warn(`  Step 5 warning (Discord fetch failed): ${err.message}`);
      console.warn('  Proceeding without Discord data for this run.');
    }
  }

  // -------------------------------------------------------------------------
  // Step 6 — Sanity-check metrics for today's rows
  // -------------------------------------------------------------------------
  console.log('\nStep 6: Checking today\'s data quality...');

  let nullWishlistCount  = 0;
  let nullFollowersCount = 0;
  let discordGaps        = 0; // games with a Discord server but no today's data

  try {
    const { data: todayRows, error } = await supabase
      .from('games_daily_snapshots')
      .select('app_id, wishlist_count, followers_count, discord_member_count')
      .in('app_id', appIds)
      .eq('date', today);

    if (error) throw new Error(error.message);

    const todayByAppId = new Map();
    for (const row of (todayRows || [])) todayByAppId.set(row.app_id, row);

    for (const game of games) {
      const row = todayByAppId.get(game.app_id);
      if (!row || row.wishlist_count  === null) nullWishlistCount++;
      if (!row || row.followers_count === null) nullFollowersCount++;
      if (game.discord_invite_code && (!row || row.discord_member_count === null)) discordGaps++;
    }

    console.log(`  null wishlist_count:  ${nullWishlistCount}`);
    console.log(`  null followers_count: ${nullFollowersCount}`);
    console.log(`  discord gaps:         ${discordGaps}`);
  } catch (err) {
    console.warn(`  Step 6 warning (sanity check failed): ${err.message}`);
  }

  // -------------------------------------------------------------------------
  // Step 7 — Log pipeline run
  // -------------------------------------------------------------------------
  const totalRowsWritten = gamalyticRowsWritten + discordRowsWritten;
  const missingRatio     = nullWishlistCount / games.length;

  const status = missingRatio === 0 && nullFollowersCount === 0 && discordGaps === 0
    ? 'success'
    : missingRatio > 0.5
      ? 'failure'
      : 'partial';

  const notes = [
    `${games.length} games`,
    `${totalRowsWritten} new rows`,
    `${gamalyticRowsWritten} gamalytic`,
    `${discordRowsWritten} discord`,
    `${discordGames} discord matched`,
    discordNotFound > 0 ? `${discordNotFound} not in scraper` : null,
    gamalyticErrors > 0 ? `${gamalyticErrors} gamalytic errors` : null,
  ].filter(Boolean).join(', ');

  try {
    await logRun(supabase, {
      workflowName:       'daily-game-snapshot',
      status,
      rowsWritten:        totalRowsWritten,
      rowsExpected:       games.length,
      nullWishlistCount,
      nullFollowersCount,
      discordGaps,
      notes,
    });
    console.log(`\nPipeline run logged: ${status}`);
    console.log(`Notes: ${notes}`);
  } catch (logErr) {
    console.warn(`Pipeline log failed: ${logErr.message}`);
  }

  // -------------------------------------------------------------------------
  // Step 8 — Exit
  // -------------------------------------------------------------------------
  if (missingRatio > 0.5) {
    console.error(`\nMore than 50% of games are missing today's wishlist data (${nullWishlistCount}/${games.length}). Exiting with error.`);
    process.exit(1);
  }

  console.log('\n✓ daily-game-snapshot complete.');
}

main();
