// daily-heartbeat.js
// Workflow: daily-heartbeat
//
// Runs daily at 09:00 UTC — after all upstream workflows have had time to
// complete — and checks yesterday's pipeline_runs rows for red flags.
// Exits with a non-zero code if any issues are found so GitHub Actions marks
// the run as failed and sends a notification email.
//
// Checks performed:
//   1. wishlist-bracket-snapshots — ran, succeeded, rows within 5% of expected
//   2. promote-top10-to-games-static — ran, succeeded, no enrichment failures
//   3. daily-game-snapshot — ran, succeeded, no null wishlist/follower/discord gaps

const { createClient } = require('@supabase/supabase-js');
const { getRun }       = require('./lib/pipeline-logger');

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
// Helpers
// ---------------------------------------------------------------------------

// Parse a notes string like "10 in top 10, 7 already tracked, 3 new, 2 failed"
// and extract the integer before "failed". Returns 0 if not found.
function parseFailedCount(notes) {
  if (!notes) return 0;
  const match = notes.match(/(\d+)\s+failed/);
  return match ? parseInt(match[1], 10) : 0;
}

// Format a percentage change for display
function pct(actual, expected) {
  if (!expected) return 'N/A';
  return (((expected - actual) / expected) * 100).toFixed(1) + '%';
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  // "Yesterday" in UTC — all upstream workflows run before 09:00 UTC
  const yesterday = new Date(Date.now() - 864e5).toISOString().split('T')[0];

  console.log(`Daily heartbeat check for: ${yesterday}\n`);

  const alerts = [];

  // -------------------------------------------------------------------------
  // Check 1 — wishlist-bracket-snapshots
  // -------------------------------------------------------------------------
  try {
    const run = await getRun(supabase, 'wishlist-bracket-snapshots', yesterday);

    if (!run) {
      alerts.push(
        '[wishlist-bracket-snapshots] RED FLAG: No pipeline_runs row found — workflow did not run.'
      );
    } else if (run.status === 'failure') {
      alerts.push(
        `[wishlist-bracket-snapshots] RED FLAG: status = 'failure'. Notes: ${run.notes ?? 'none'}`
      );
    } else if (run.rows_written != null && run.rows_expected != null && run.rows_expected > 0) {
      const drop = (run.rows_expected - run.rows_written) / run.rows_expected;
      if (drop > 0.05) {
        alerts.push(
          `[wishlist-bracket-snapshots] RED FLAG: rows written ${run.rows_written.toLocaleString()} ` +
          `vs expected ${run.rows_expected.toLocaleString()} — drop: ${pct(run.rows_written, run.rows_expected)} (threshold: 5%)`
        );
      }
    }
  } catch (err) {
    alerts.push(`[wishlist-bracket-snapshots] ERROR reading pipeline_runs: ${err.message}`);
  }

  // -------------------------------------------------------------------------
  // Check 2 — promote-top10-to-games-static
  // -------------------------------------------------------------------------
  try {
    const run = await getRun(supabase, 'promote-top10-to-games-static', yesterday);

    if (!run) {
      alerts.push(
        '[promote-top10-to-games-static] RED FLAG: No pipeline_runs row found — workflow did not run.'
      );
    } else if (run.status === 'failure') {
      alerts.push(
        `[promote-top10-to-games-static] RED FLAG: status = 'failure'. Notes: ${run.notes ?? 'none'}`
      );
    } else {
      const failedCount = parseFailedCount(run.notes);
      if (failedCount > 0) {
        alerts.push(
          `[promote-top10-to-games-static] RED FLAG: ${failedCount} game(s) failed enrichment. ` +
          `Notes: ${run.notes}`
        );
      }
    }
  } catch (err) {
    alerts.push(`[promote-top10-to-games-static] ERROR reading pipeline_runs: ${err.message}`);
  }

  // -------------------------------------------------------------------------
  // Check 3 — daily-game-snapshot
  // -------------------------------------------------------------------------
  try {
    const run = await getRun(supabase, 'daily-game-snapshot', yesterday);

    if (!run) {
      alerts.push(
        '[daily-game-snapshot] RED FLAG: No pipeline_runs row found — workflow did not run.'
      );
    } else if (run.status === 'failure') {
      alerts.push(
        `[daily-game-snapshot] RED FLAG: status = 'failure'. Notes: ${run.notes ?? 'none'}`
      );
    } else {
      if (run.null_wishlist_count > 0) {
        alerts.push(
          `[daily-game-snapshot] RED FLAG: ${run.null_wishlist_count} game(s) have null wishlist_count.`
        );
      }
      if (run.null_followers_count > 0) {
        alerts.push(
          `[daily-game-snapshot] RED FLAG: ${run.null_followers_count} game(s) have null followers_count.`
        );
      }
      if (run.discord_gaps > 0) {
        alerts.push(
          `[daily-game-snapshot] RED FLAG: ${run.discord_gaps} game(s) have no Discord data.`
        );
      }
    }
  } catch (err) {
    alerts.push(`[daily-game-snapshot] ERROR reading pipeline_runs: ${err.message}`);
  }

  // -------------------------------------------------------------------------
  // Report
  // -------------------------------------------------------------------------
  if (alerts.length > 0) {
    console.error(`${alerts.length} red flag(s) found for ${yesterday}:\n`);
    alerts.forEach(a => console.error(`  ✗ ${a}`));
    console.error('');
    process.exit(1);
  }

  console.log(`✓ All pipeline checks passed for ${yesterday}`);
}

main();
