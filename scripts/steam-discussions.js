// steam-discussions.js
// Workflow: steam-discussions
//
// Runs daily and scrapes Steam discussion threads for all tracked games
// via Apify, writing only new topics to steam_discussions. Already-stored
// topic_ids are skipped so the script is safe to re-run at any time.
//
// On success, the workflow triggers steam-discussion-replies.yml to pull
// replies for all tracked discussion threads.
//
// Steps:
//   1. Get all tracked games from games_static
//   2. Get existing topic IDs per game from steam_discussions (paginated)
//   3. Fetch discussions per game via Apify actor (sync run)
//   4. Upsert new discussions to steam_discussions in chunks of 100
//   5. Log pipeline run to pipeline_runs
//   6. Exit non-zero if all games failed

const { createClient }   = require('@supabase/supabase-js');
const { fetchWithRetry } = require('./lib/steam-enrichment');
const { logRun }         = require('./lib/pipeline-logger');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const APIFY_API_KEY        = process.env.APIFY_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !APIFY_API_KEY) {
  console.error('Error: SUPABASE_URL, SUPABASE_SERVICE_KEY, and APIFY_API_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const APIFY_URL =
  'https://api.apify.com/v2/acts/shahidirfan~steam-reviews-scraper/run-sync-get-dataset-items';

const APIFY_TIMEOUT_MS  = 120_000; // Apify sync runs can be slow
const BETWEEN_CALLS_MS  = 2_000;   // Respectful delay between per-game Apify calls

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch all rows from a Supabase query in pages of 1,000 using .range().
// queryFn(from, to) must return a Supabase query with .select() applied.
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

// Upsert rows in chunks, returning total rows attempted.
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
  console.log(`steam-discussions — ${today}\n`);

  let gamesProcessed  = 0;
  let gamesFailed     = 0;
  let totalNewRows    = 0;

  // -------------------------------------------------------------------------
  // Step 1 — Get all tracked games
  // -------------------------------------------------------------------------
  console.log('Step 1: Loading tracked games...');

  let games;
  try {
    const { data, error } = await supabase
      .from('games_static')
      .select('app_id, name')
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
  // Step 2 — Get existing topic IDs per game (paginated bulk query)
  //
  // Paginated to handle large datasets — steam_discussions grows continuously.
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Loading existing topic IDs...');

  const existingTopics = new Map(); // app_id → Set<topic_id string>
  for (const id of appIds) existingTopics.set(id, new Set());

  try {
    const rows = await fetchAllRows((from, to) =>
      supabase
        .from('steam_discussions')
        .select('app_id, topic_id')
        .in('app_id', appIds)
        .range(from, to)
    );

    for (const row of rows) {
      existingTopics.get(row.app_id)?.add(String(row.topic_id));
    }

    console.log(`Found ${rows.length} existing discussion(s) across all games.`);
  } catch (err) {
    console.error(`Step 2 failed: ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Fetch discussions per game via Apify
  //
  // Uses the steam-reviews-scraper actor in sync mode — the request blocks
  // until the actor finishes and returns the dataset items directly.
  // A 120s timeout is set via AbortSignal since Apify sync runs can be slow.
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Fetching discussions from Apify...');

  const allNewRows = []; // collected across all games, written in Step 4

  for (const game of games) {
    const { app_id, name } = game;

    // Respectful delay between calls — skip on first iteration
    if (gamesProcessed + gamesFailed > 0) await sleep(BETWEEN_CALLS_MS);

    let items;
    try {
      const response = await fetchWithRetry(
        `${APIFY_URL}?token=${APIFY_API_KEY}`,
        {
          method:  'POST',
          headers: { 'content-type': 'application/json' },
          body:    JSON.stringify({
            appId:          String(app_id),
            results_wanted: 100,
            max_pages:      20,
          }),
          signal: AbortSignal.timeout(APIFY_TIMEOUT_MS),
        }
      );

      items = await response.json();
      if (!Array.isArray(items)) {
        throw new Error(`Unexpected response shape: ${JSON.stringify(items).slice(0, 100)}`);
      }
    } catch (err) {
      console.error(`  ${name} (${app_id}): Apify fetch failed — ${err.message}`);
      gamesFailed++;
      continue;
    }

    const known = existingTopics.get(app_id);
    let newCount      = 0;
    let skippedCount  = 0;

    for (const item of items) {
      const topicId = String(item.topic_id);

      if (known.has(topicId)) {
        skippedCount++;
        continue;
      }

      allNewRows.push({
        app_id,
        topic_id:          topicId,
        title:             item.title             || '',
        author_name:       item.author_name       || '',
        reply_count:       parseInt(item.reply_count || 0, 10),
        opening_post_body: item.opening_post_body  || '',
        last_posted_at:    item.last_posted_at     || null,
        is_pinned:         item.is_pinned          || false,
        is_locked:         item.is_locked          || false,
        scraped_date:      today,
      });
      newCount++;
    }

    console.log(`  ${name} (${app_id}): ${items.length} fetched, ${newCount} new, ${skippedCount} already exist`);
    gamesProcessed++;
  }

  // -------------------------------------------------------------------------
  // Step 4 — Upsert new discussions in chunks of 100
  // -------------------------------------------------------------------------
  console.log(`\nStep 4: Writing ${allNewRows.length} new discussion(s)...`);

  if (allNewRows.length > 0) {
    try {
      totalNewRows = await upsertInChunks(
        supabase,
        'steam_discussions',
        allNewRows,
        100,
        'topic_id'
      );
      console.log(`Wrote ${totalNewRows} discussion(s).`);
    } catch (err) {
      console.error(`Step 4 failed: ${err.message}`);
      // Don't exit — log the partial result and continue to pipeline log
    }
  } else {
    console.log('No new discussions to write.');
  }

  // -------------------------------------------------------------------------
  // Step 5 — Log pipeline run
  // -------------------------------------------------------------------------
  const status = gamesFailed === 0
    ? 'success'
    : gamesFailed === games.length
      ? 'failure'
      : 'partial';

  const notes = [
    `${games.length} games`,
    `${totalNewRows} new discussions`,
    gamesFailed > 0 ? `${gamesFailed} games failed` : null,
  ].filter(Boolean).join(', ');

  try {
    await logRun(supabase, {
      workflowName: 'steam-discussions',
      status,
      rowsWritten:  totalNewRows,
      rowsExpected: games.length,
      notes,
    });
    console.log(`\nPipeline run logged: ${status}`);
    console.log(`Notes: ${notes}`);
  } catch (logErr) {
    console.warn(`Pipeline log failed: ${logErr.message}`);
  }

  // -------------------------------------------------------------------------
  // Step 6 — Exit
  // -------------------------------------------------------------------------
  if (gamesFailed === games.length) {
    console.error('\nAll games failed. Exiting with error.');
    process.exit(1);
  }

  console.log('\n✓ steam-discussions complete.');
}

main();
