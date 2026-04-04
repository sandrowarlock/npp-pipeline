// scripts/lib/pipeline-logger.js
// Shared pipeline run logging module.
//
// Provides two functions for writing and reading pipeline run records in the
// pipeline_runs table. Each workflow should call logRun() at the end of its
// main() function to record its outcome for monitoring and auditing.
//
// Error handling is intentionally left to the caller — this module does not
// catch errors from Supabase, so callers can decide how to handle failures
// (e.g. log and continue, or exit with a non-zero code).
//
// Exports:
//   logRun(supabase, options) — upsert a run record
//   getRun(supabase, workflowName, date) — fetch a run record

// ---------------------------------------------------------------------------
// logRun
// Upserts a row to pipeline_runs for the given workflow and date.
// On conflict of (workflow_name, run_date), updates all fields.
//
// Parameters:
//   supabase        — Supabase client instance
//   workflowName    — name of the workflow (e.g. 'daily-game-snapshot')
//   runDate         — ISO date string YYYY-MM-DD (default: today)
//   status          — 'success' | 'partial' | 'failure'
//   rowsWritten     — number of rows written to the target table (optional)
//   rowsExpected    — number of rows expected to be written (optional)
//   nullWishlistCount  — games with no wishlist data (optional)
//   nullFollowersCount — games with no followers data (optional)
//   discordGaps     — games with no Discord data (optional)
//   notes           — free-text notes or error summary (optional)
//
// Returns the upserted row.
// ---------------------------------------------------------------------------
async function logRun(supabase, {
  workflowName,
  runDate = new Date().toISOString().split('T')[0],
  status,
  rowsWritten        = null,
  rowsExpected       = null,
  nullWishlistCount  = null,
  nullFollowersCount = null,
  discordGaps        = null,
  notes              = null,
} = {}) {
  const { data, error } = await supabase
    .from('pipeline_runs')
    .upsert(
      {
        workflow_name:        workflowName,
        run_date:             runDate,
        status,
        rows_written:         rowsWritten,
        rows_expected:        rowsExpected,
        null_wishlist_count:  nullWishlistCount,
        null_followers_count: nullFollowersCount,
        discord_gaps:         discordGaps,
        notes,
      },
      { onConflict: 'workflow_name,run_date' }
    )
    .select()
    .single();

  if (error) throw error;
  return data;
}

// ---------------------------------------------------------------------------
// getRun
// Fetches a single pipeline_runs row for the given workflow and date.
// Returns null if no row exists for that (workflow_name, run_date) pair.
//
// Parameters:
//   supabase      — Supabase client instance
//   workflowName  — name of the workflow to look up
//   date          — ISO date string YYYY-MM-DD
// ---------------------------------------------------------------------------
async function getRun(supabase, workflowName, date) {
  const { data, error } = await supabase
    .from('pipeline_runs')
    .select('*')
    .eq('workflow_name', workflowName)
    .eq('run_date', date)
    .single();

  // PGRST116 = no rows found — return null rather than throwing
  if (error && error.code === 'PGRST116') return null;
  if (error) throw error;
  return data;
}

module.exports = { logRun, getRun };
