// promote-top10-to-games-static.js
// Workflow: promote-top10-to-games-static
//
// Reads today's top 10 fastest-rising games from daily_top10_risers, checks
// which are not yet in games_static, and enriches + adds the new ones.
// After adding each new game, placeholder log lines stand in for downstream
// workflow triggers (daily-game-snapshot, youtube-enrichment, steam-discussions,
// steam-discussion-replies) that will be wired up once those workflows exist.
//
// Steps:
//   1. Read today's top 10 from daily_top10_risers
//   2. Check which games are not yet in games_static
//   3. Enrich and add each new game via shared steam-enrichment module
//   4. Log placeholder triggers for downstream enrichment workflows
//
// Exit codes:
//   0 — all games processed successfully (skipped games count as success)
//   1 — one or more enrichment failures occurred

const { createClient } = require('@supabase/supabase-js');
const { enrichGame }   = require('./lib/steam-enrichment');
const { logRun }       = require('./lib/pipeline-logger');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Error: SUPABASE_URL and SUPABASE_SERVICE_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Placeholder downstream workflow triggers
// These will be replaced with real GitHub Actions API calls once the
// corresponding workflows (youtube-enrichment, steam-discussions, etc.) exist.
// ---------------------------------------------------------------------------
async function runDownstreamWorkflows(appId, gameName) {
  console.log(`  → Would run daily-game-snapshot for ${appId} (${gameName})`);
  console.log(`  → Would run youtube-enrichment for ${appId} (${gameName})`);
  console.log(`  → Would run steam-discussions for ${appId} (${gameName})`);
  console.log(`  → Would run steam-discussion-replies for ${appId} (${gameName})`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today = new Date().toISOString().split('T')[0];

  // Counters for the summary log
  let totalInTop10    = 0;
  let alreadyTracked  = 0;
  let newlyAdded      = 0;
  let failed          = 0;

  // -------------------------------------------------------------------------
  // Step 1 — Read today's top 10 risers
  // -------------------------------------------------------------------------
  console.log(`Step 1: Reading today's top 10 risers for ${today}...`);
  let top10;
  try {
    const { data, error } = await supabase
      .from('daily_top10_risers')
      .select('rank, app_id, game_name, wishlist_count, wishlists_7d')
      .eq('date', today)
      .order('rank', { ascending: true });

    if (error) throw new Error(error.message);

    if (!data || data.length === 0) {
      console.log('No top 10 risers found for today. Exiting.');
      try {
        await logRun(supabase, {
          workflowName: 'promote-top10-to-games-static',
          status:       'failure',
          notes:        'No top 10 risers found for today',
        });
      } catch (logErr) {
        console.warn(`Pipeline log failed: ${logErr.message}`);
      }
      process.exit(0);
    }

    top10 = data;
    totalInTop10 = top10.length;
    console.log(`Found ${totalInTop10} games in today's top 10.`);
  } catch (err) {
    console.error(`Step 1 failed (reading top 10): ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 2 — Check which games are not yet in games_static
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Checking which games are already tracked...');
  let newGames;
  try {
    const appIds = top10.map(g => g.app_id);

    const { data, error } = await supabase
      .from('games_static')
      .select('app_id')
      .in('app_id', appIds);

    if (error) throw new Error(error.message);

    const trackedIds = new Set((data || []).map(r => r.app_id));
    newGames       = top10.filter(g => !trackedIds.has(g.app_id));
    alreadyTracked = top10.length - newGames.length;

    console.log(`  Already tracked: ${alreadyTracked}`);
    console.log(`  New to add:      ${newGames.length}`);
  } catch (err) {
    console.error(`Step 2 failed (checking existing games): ${err.message}`);
    process.exit(1);
  }

  if (newGames.length === 0) {
    console.log('\nAll top 10 games are already tracked. Nothing to do.');
    console.log('\n--- Summary ---');
    console.log(`Total in today's top 10: ${totalInTop10}`);
    console.log(`Already tracked (skipped): ${alreadyTracked}`);
    console.log(`Newly added: 0`);
    console.log(`Failed: 0`);
    try {
      await logRun(supabase, {
        workflowName: 'promote-top10-to-games-static',
        status:       'success',
        rowsWritten:  0,
        notes:        `${totalInTop10} in top 10, ${alreadyTracked} already tracked, 0 new, 0 failed`,
      });
    } catch (logErr) {
      console.warn(`Pipeline log failed: ${logErr.message}`);
    }
    process.exit(0);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Enrich and add each new game
  // Failures are logged and counted but never stop processing of remaining games.
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Enriching new games...');
  for (const game of newGames) {
    console.log(`\n[${game.rank}/10] ${game.game_name} (app_id: ${game.app_id})`);

    try {
      const result = await enrichGame(
        game.app_id,
        { trackingSource: 'auto', studyTags: null },
        supabase
      );

      if (result.notFound) {
        // Game exists in our top 10 data but is no longer on Steam — skip it
        console.warn(`  Game not found on Steam — skipping.`);
        failed++;
        continue;
      }

      console.log(`  ✓ Added: "${result.name}" — ${result.tags} tags, released: ${result.released}`);
      newlyAdded++;

      // -----------------------------------------------------------------------
      // Step 4 — Trigger downstream enrichment workflows
      // Placeholders until the individual workflow scripts are built.
      // Each runs in order, waiting for the previous to complete.
      // -----------------------------------------------------------------------
      await runDownstreamWorkflows(game.app_id, result.name);

    } catch (err) {
      console.error(`  ✗ Enrichment failed for ${game.app_id}: ${err.message}`);
      failed++;
    }
  }

  // -------------------------------------------------------------------------
  // Summary log
  // -------------------------------------------------------------------------
  console.log('\n--- Summary ---');
  console.log(`Total in today's top 10: ${totalInTop10}`);
  console.log(`Already tracked (skipped): ${alreadyTracked}`);
  console.log(`Newly added: ${newlyAdded}`);
  console.log(`Failed: ${failed}`);

  // Log pipeline run outcome — always attempt, never crash on failure
  try {
    await logRun(supabase, {
      workflowName: 'promote-top10-to-games-static',
      status:       failed > 0 ? 'partial' : 'success',
      rowsWritten:  newlyAdded,
      notes:        `${totalInTop10} in top 10, ${alreadyTracked} already tracked, ${newlyAdded} new, ${failed} failed`,
    });
  } catch (logErr) {
    console.warn(`Pipeline log failed: ${logErr.message}`);
  }

  // Exit non-zero if any enrichment failed so GitHub marks the run as failed
  // and sends a notification email — but don't fail just because games were skipped.
  if (failed > 0) {
    console.error(`\n${failed} game(s) failed enrichment. Exiting with error.`);
    process.exit(1);
  }

  console.log('\n✓ promote-top10-to-games-static complete.');
}

main();
