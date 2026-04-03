// wishlist-bracket-snapshots.js
// Workflow: wishlist-bracket-snapshots
//
// Pulls all unreleased Steam games with 5,000–20,000 wishlists from the
// Gamalytic API, writes a daily snapshot for each game, calculates the top 10
// fastest-growing games by 7-day wishlist growth, and purges snapshot rows
// older than 7 days to keep the table lean.
//
// Steps:
//   1. Paginate Gamalytic API (with retry + exponential backoff per page)
//   2. Write all games to wishlist_bracket_snapshots (batched upsert)
//   3. Calculate top 10 by wishlists_7d, write to daily_top10_risers
//   4. Purge wishlist_bracket_snapshots rows older than 7 days

const { createClient } = require('@supabase/supabase-js');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const GAMALYTIC_API_KEY  = process.env.GAMALYTIC_API_KEY;
const SUPABASE_URL       = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!GAMALYTIC_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Error: GAMALYTIC_API_KEY, SUPABASE_URL, and SUPABASE_SERVICE_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const GAMALYTIC_BASE    = 'https://api.gamalytic.com/steam-games/list';
const LIMIT             = 1000;
const PAGE_DELAY_MS     = 2000;          // delay between pages (rate limit safety)
const RETRY_DELAYS      = [2000, 4000, 8000]; // delays before retries 1, 2, 3
const BATCH_SIZE        = 500;           // max rows per Supabase upsert payload
const PURGE_DAYS        = 7;            // delete bracket snapshots older than this

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// ---------------------------------------------------------------------------
// fetchPage
// Fetches a single page from the Gamalytic API with retry + exponential backoff.
// Returns { success: true, data: [...] } on success.
// Returns { success: false, error: string, fatal?: true } on failure.
// fatal=true means a 4xx was received and we should stop pagination entirely.
// ---------------------------------------------------------------------------
async function fetchPage(page) {
  const params = new URLSearchParams({
    release_status: 'unreleased',
    wishlists_min:  '5000',
    wishlists_max:  '20000',
    sort:           'wishlists',
    sort_mode:      'desc',
    limit:          String(LIMIT),
    fields:         'steamId,name,wishlists,wishlists7d,followers',
    page:           String(page),
  });

  const url     = `${GAMALYTIC_BASE}?${params}`;
  const headers = { 'api-key': GAMALYTIC_API_KEY };

  // attempt 0 = initial request; attempts 1-3 = retries
  for (let attempt = 0; attempt <= 3; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_DELAYS[attempt - 1];
      console.warn(`  Page ${page}: retry attempt ${attempt}, waiting ${delay / 1000}s...`);
      await sleep(delay);
    }

    let response;
    try {
      response = await fetch(url, { headers });
    } catch (err) {
      // Network-level error (DNS failure, connection reset, etc.) — retry
      if (attempt < 3) continue;
      return { success: false, error: `Network error: ${err.message}` };
    }

    if (response.ok) {
      const data = await response.json();
      return { success: true, data };
    }

    if (response.status >= 400 && response.status < 500) {
      // Client error (bad API key, invalid params, etc.) — do not retry
      return {
        success: false,
        error:   `HTTP ${response.status} client error`,
        fatal:   true,
      };
    }

    // 5xx server error — retry if attempts remain
    if (attempt < 3) {
      console.warn(`  Page ${page}: HTTP ${response.status}, will retry...`);
      continue;
    }

    return { success: false, error: `HTTP ${response.status} after all retries` };
  }

  // Should not be reached, but guards against unexpected loop exit
  return { success: false, error: 'Exhausted all retry attempts' };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today = new Date().toISOString().split('T')[0];

  // -------------------------------------------------------------------------
  // Step 1 — Paginate through Gamalytic API
  // Keeps fetching pages until the result is empty or shorter than the page
  // limit. Failed pages are skipped (logged as warnings) rather than crashing
  // the run, so a transient error on one page doesn't lose the whole batch.
  // -------------------------------------------------------------------------
  console.log('Step 1: Fetching games from Gamalytic API...');
  const allGames = [];
  let page = 0;

  try {
    while (true) {
      const result = await fetchPage(page);

      if (!result.success) {
        if (result.fatal) {
          // 4xx errors indicate a configuration problem — stop immediately
          console.error(`Page ${page}: fatal error — ${result.error}. Stopping pagination.`);
          break;
        }
        // Transient failure after all retries — skip page and continue
        console.warn(`Page ${page}: all retries failed — ${result.error}. Skipping page.`);
        page++;
        await sleep(PAGE_DELAY_MS);
        continue;
      }

      // Gamalytic wraps results in { pages, total, result: [...] }
      const games = result.data.result;

      if (!Array.isArray(games) || games.length === 0) {
        console.log(`Page ${page}: empty result array — pagination complete.`);
        break;
      }

      allGames.push(...games);
      console.log(`Fetched page ${page}, ${allGames.length} games so far`);

      if (games.length < LIMIT) {
        // Fewer results than the page limit means this is the last page
        console.log(`Page ${page}: ${games.length} results (< ${LIMIT}) — pagination complete.`);
        break;
      }

      page++;
      await sleep(PAGE_DELAY_MS);
    }
  } catch (err) {
    console.error(`Step 1 failed unexpectedly: ${err.message}`);
    process.exit(1);
  }

  // If Gamalytic returned nothing at all, something is wrong — fail the run
  // so GitHub Actions marks it as failed and sends a notification email.
  if (allGames.length === 0) {
    console.error('Error: Gamalytic returned no games. Exiting with failure.');
    process.exit(1);
  }

  console.log(`\nTotal games fetched: ${allGames.length}`);

  // -------------------------------------------------------------------------
  // Step 2 — Write to wishlist_bracket_snapshots
  // Upserted in batches of 500 to stay within Supabase payload limits.
  // Conflict on (date, app_id) makes re-runs idempotent.
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Writing bracket snapshots...');
  try {
    const rows = allGames.map(game => ({
      date:          today,
      app_id:        game.steamId,
      game_name:     game.name,
      wishlist_count: game.wishlists,
      wishlists_7d:  game.wishlists7d ?? null,
    }));

    const chunks = chunkArray(rows, BATCH_SIZE);
    let totalWritten = 0;

    for (const chunk of chunks) {
      const { error } = await supabase
        .from('wishlist_bracket_snapshots')
        .upsert(chunk, { onConflict: 'date,app_id' });

      if (error) throw new Error(error.message);
      totalWritten += chunk.length;
    }

    console.log(`Wrote ${totalWritten} rows to wishlist_bracket_snapshots`);
  } catch (err) {
    console.error(`Step 2 failed (writing bracket snapshots): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Calculate top 10 risers and write to daily_top10_risers
  // Sorted by wishlists_7d descending. Games with null wishlists7d sort last.
  // Conflict on (date, rank) makes re-runs idempotent.
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Calculating top 10 risers...');
  try {
    const top10 = [...allGames]
      .sort((a, b) => (b.wishlists7d ?? 0) - (a.wishlists7d ?? 0))
      .slice(0, 10);

    const top10Rows = top10.map((game, i) => ({
      date:          today,
      rank:          i + 1,
      app_id:        game.steamId,
      game_name:     game.name,
      wishlist_count: game.wishlists,
      wishlists_7d:  game.wishlists7d ?? null,
    }));

    const { error } = await supabase
      .from('daily_top10_risers')
      .upsert(top10Rows, { onConflict: 'date,rank' });

    if (error) throw new Error(error.message);

    console.log('Top 10 risers today:');
    top10Rows.forEach(r => {
      const growth = r.wishlists_7d != null ? r.wishlists_7d.toLocaleString() : 'N/A';
      console.log(`  ${r.rank}. ${r.game_name} — ${growth} wishlists (7d)`);
    });
  } catch (err) {
    console.error(`Step 3 failed (writing top 10 risers): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 4 — Purge bracket snapshot rows older than 7 days
  // The bracket snapshots table is only needed for short-term trend analysis,
  // so we purge old rows daily to keep the table size manageable.
  // -------------------------------------------------------------------------
  console.log('\nStep 4: Purging old bracket snapshots...');
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - PURGE_DAYS);
    const cutoffDate = cutoff.toISOString().split('T')[0];

    const { data, error } = await supabase
      .from('wishlist_bracket_snapshots')
      .delete()
      .lt('date', cutoffDate)
      .select();

    if (error) throw new Error(error.message);

    console.log(`Purged ${data?.length ?? 0} rows older than ${cutoffDate}`);
  } catch (err) {
    console.error(`Step 4 failed (purging old snapshots): ${err.message}`);
    process.exit(1);
  }

  console.log('\n✓ wishlist-bracket-snapshots complete.');
}

main();
