// bulk-add-games.js
//
// Bulk-adds a list of Steam games to the tracking pipeline using the same
// enrichment logic as add-game-to-track.js (Steam Store API + page scrape).
//
// Each game is upserted into games_static with:
//   tracking_source: "griffin-genre-study"
//   study_tags:      "pipeline-candidates"
//
// Usage (from scripts/ directory):
//   node --env-file=../.env bulk-add-games.js ../pipeline_app_ids.json
//
// Input JSON format:
//   [ { "app_id": 123456, "name": "Game Name" }, ... ]

'use strict';

const fs   = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const { enrichGame }   = require('./lib/steam-enrichment');

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Error: SUPABASE_URL and SUPABASE_SERVICE_KEY are required.');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const TRACKING_SOURCE  = 'griffin-genre-study';
const STUDY_TAGS       = 'pipeline-candidates';
const BETWEEN_GAMES_MS = 1_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  // ---- Parse CLI argument ----
  const inputPath = process.argv[2];
  if (!inputPath) {
    console.error('Error: input JSON file path is required as the first argument.');
    console.error('Usage: node bulk-add-games.js <path-to-json>');
    process.exit(1);
  }

  const resolvedPath = path.resolve(process.cwd(), inputPath);
  if (!fs.existsSync(resolvedPath)) {
    console.error(`Error: file not found — ${resolvedPath}`);
    process.exit(1);
  }

  let games;
  try {
    games = JSON.parse(fs.readFileSync(resolvedPath, 'utf8'));
  } catch (err) {
    console.error(`Error: failed to parse input JSON — ${err.message}`);
    process.exit(1);
  }

  if (!Array.isArray(games) || games.length === 0) {
    console.error('Error: input file must be a non-empty JSON array of { app_id, name } objects.');
    process.exit(1);
  }

  console.log(`bulk-add-games — ${new Date().toISOString().split('T')[0]}`);
  console.log(`Input: ${resolvedPath}`);
  console.log(`Games to process: ${games.length}`);
  console.log(`tracking_source: ${TRACKING_SOURCE} | study_tags: ${STUDY_TAGS}\n`);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const failures = []; // { app_id, name, reason }
  let   succeeded = 0;
  let   skipped   = 0;

  for (let i = 0; i < games.length; i++) {
    if (i > 0) await sleep(BETWEEN_GAMES_MS);

    const game  = games[i];
    const appId = parseInt(game.app_id, 10);
    const name  = game.name ?? `app_id ${appId}`;
    const label = `[${i + 1}/${games.length}] ${name} (${appId})`;

    if (isNaN(appId)) {
      console.log(`${label} — SKIP: invalid app_id`);
      skipped++;
      continue;
    }

    try {
      const result = await enrichGame(
        appId,
        { trackingSource: TRACKING_SOURCE, studyTags: STUDY_TAGS },
        supabase
      );

      if (result.notFound) {
        console.log(`${label} — SKIP: not found on Steam`);
        skipped++;
        continue;
      }

      console.log(`${label} — ✓  tags: ${result.tags} | released: ${result.released}`);
      succeeded++;
    } catch (err) {
      console.error(`${label} — ✗  ${err.message}`);
      failures.push({ app_id: appId, name, reason: err.message });
    }
  }

  // ---- Summary ----
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Total: ${games.length} | Succeeded: ${succeeded} | Skipped: ${skipped} | Failed: ${failures.length}`);

  if (failures.length > 0) {
    console.log('\nFailed app_ids:');
    for (const f of failures) {
      console.log(`  ${f.app_id}  ${f.name}  — ${f.reason}`);
    }
  }
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
