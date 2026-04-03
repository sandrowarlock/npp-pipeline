// add-game-to-track.js
// Workflow: add-game-to-track
//
// Manually adds a single Steam game to the tracking pipeline.
// Core enrichment logic (Steam API + page scrape + Supabase write) lives in
// scripts/lib/steam-enrichment.js and is shared with promote-top10-to-games-static.js.
//
// Inputs (via environment variables):
//   STEAM_APP_ID         — Steam app ID to track (required)
//   TRACKING_SOURCE      — Where this lead came from (default: 'manual')
//   STUDY_TAGS           — Internal grouping tags, comma-separated (optional)
//   SUPABASE_URL         — NPP Supabase project URL
//   SUPABASE_SERVICE_KEY — Supabase service role key (write access)

const { createClient } = require('@supabase/supabase-js');
const { enrichGame }   = require('./lib/steam-enrichment');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const STEAM_APP_ID       = process.env.STEAM_APP_ID;
const TRACKING_SOURCE    = process.env.TRACKING_SOURCE || 'manual';
const STUDY_TAGS         = process.env.STUDY_TAGS || '';
const SUPABASE_URL       = process.env.SUPABASE_URL;
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

async function main() {
  const appId = parseInt(STEAM_APP_ID, 10);

  // -------------------------------------------------------------------------
  // Step 1 — Check if the game already exists in games_static
  // Skip re-adding tracked games to avoid overwriting first_seen_date and
  // other fields that should only be set once.
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
  // Step 2 — Enrich game and write to games_static
  // Delegates to the shared enrichGame function which handles:
  //   - Steam Store API (name, descriptions, release date, developers, publishers)
  //   - Steam page scrape (tags, social links, languages, controller support)
  //   - Supabase upsert
  // -------------------------------------------------------------------------
  try {
    const result = await enrichGame(
      appId,
      { trackingSource: TRACKING_SOURCE, studyTags: STUDY_TAGS || null },
      supabase
    );

    if (result.notFound) {
      console.log(`Game not found on Steam (app_id: ${appId}). Exiting.`);
      process.exit(0);
    }

    console.log(`✓ Successfully added game: "${result.name}" (app_id: ${appId})`);
    console.log(`  tracking_source: ${TRACKING_SOURCE}`);
    if (STUDY_TAGS) console.log(`  study_tags: ${STUDY_TAGS}`);
    console.log(`  tags found: ${result.tags}`);
    console.log(`  released: ${result.released}`);
  } catch (err) {
    console.error(`Enrichment failed for app_id ${appId}: ${err.message}`);
    process.exit(1);
  }
}

main();
