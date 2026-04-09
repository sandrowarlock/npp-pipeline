// youtube-pipeline-backfill.js
//
// One-time script to backfill YouTube data for all 124 pipeline candidate games
// (tracking_source = 'griffin-genre-study') across two consecutive days, staying
// within the YouTube Data API quota of 10,000 units/day (100 units per search).
//
// Run order:
//   Day 1: node --env-file=../.env youtube-pipeline-backfill.js  → processes Batch 1 (games 0–61)
//   Day 2: node --env-file=../.env youtube-pipeline-backfill.js  → processes Batch 2 (games 62–123)
//
// State is persisted in griffin/.pipeline_backfill_state.json between runs.
// Games are sorted by app_id ascending for a stable, deterministic split.

'use strict';

const fs   = require('fs');
const path = require('path');
const { createClient }   = require('@supabase/supabase-js');
const { fetchWithRetry } = require('./lib/steam-enrichment');

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const YOUTUBE_API_KEY      = process.env.YOUTUBE_API_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !YOUTUBE_API_KEY || !ANTHROPIC_API_KEY) {
  console.error('Error: SUPABASE_URL, SUPABASE_SERVICE_KEY, YOUTUBE_API_KEY, and ANTHROPIC_API_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const YOUTUBE_SEARCH_URL = 'https://www.googleapis.com/youtube/v3/search';
const YOUTUBE_VIDEOS_URL = 'https://www.googleapis.com/youtube/v3/videos';
const ANTHROPIC_API_URL  = 'https://api.anthropic.com/v1/messages';
const GRIFFIN_DIR        = path.resolve(__dirname, '../griffin');
const STATE_FILE         = path.join(GRIFFIN_DIR, '.pipeline_backfill_state.json');
const BETWEEN_GAMES_MS   = 1_000;
const TRACKING_SOURCE    = 'griffin-genre-study';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function joinNonNull(values, sep = ', ') {
  return values.filter(Boolean).join(sep) || 'Unknown';
}

function loadState() {
  if (!fs.existsSync(STATE_FILE)) {
    return { batch1_completed_date: null, batch2_completed_date: null };
  }
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return { batch1_completed_date: null, batch2_completed_date: null };
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

// ---------------------------------------------------------------------------
// Claude relevance filtering — same criteria as youtube-enrichment.js
// ---------------------------------------------------------------------------
const CLAUDE_SYSTEM_PROMPT = `You are a video relevance filter for a gaming market intelligence system. Given a game's details and a list of YouTube search results, identify which videos are genuinely about that specific game.

A video IS relevant if it is any of the following:
- Gameplay footage or gameplay compilations
- Reviews (positive, negative, or mixed)
- Official trailers or teasers
- Trailer reposts by any channel (including aggregators, fan channels, or news outlets)
- Developer diaries or devlogs
- News coverage or announcements
- Influencer or streamer videos featuring the game
- Walkthrough or guide videos
- Comparison videos that feature this game
- Videos discussing the game even if not exclusively about it

A video is NOT relevant if:
- It is about a completely different game that happens to share words with this game's name
- It only mentions the game title in passing without meaningful coverage
- It is spam or clearly unrelated content

When in doubt, include the video. It is better to include a borderline video than to miss relevant content.

Respond with ONLY a JSON array of relevant video IDs, no other text. Example:
["abc123", "def456"]`;

async function filterWithClaude(game, videos) {
  const developers = joinNonNull([game.developer1, game.developer2, game.developer3]);
  const publishers = joinNonNull([game.publisher1, game.publisher2, game.publisher3]);

  const videoList = videos
    .map(v => `${v.videoId} | ${v.title} | ${v.channelName} | ${v.description}`)
    .join('\n');

  const userMessage =
    `Game: ${game.name}\n` +
    `Developers: ${developers}\n` +
    `Publishers: ${publishers}\n` +
    `Description: ${game.short_description ?? 'No description available'}\n\n` +
    `Videos to evaluate:\n${videoList}`;

  const response = await fetchWithRetry(ANTHROPIC_API_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      'claude-sonnet-4-5',
      max_tokens: 1000,
      system:     CLAUDE_SYSTEM_PROMPT,
      messages:   [{ role: 'user', content: userMessage }],
    }),
  });

  const data    = await response.json();
  const text    = data.content?.[0]?.text ?? '';
  const cleaned = text.trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();

  let ids;
  try {
    ids = JSON.parse(cleaned);
  } catch {
    throw new Error(`Failed to parse Claude response as JSON: ${text.slice(0, 200)}`);
  }

  if (!Array.isArray(ids)) {
    throw new Error(`Claude returned non-array: ${text.slice(0, 200)}`);
  }

  return ids;
}

// ---------------------------------------------------------------------------
// Process a single game — search, filter, fetch details, upsert
// Returns the number of videos upserted.
// ---------------------------------------------------------------------------
async function processGame(game, today) {
  const { app_id, name } = game;

  // Step 1 — Search YouTube (100 units)
  let searchResults;
  try {
    const params = new URLSearchParams({
      part:       'snippet',
      q:          `${name} game Steam`,
      type:       'video',
      maxResults: '50',
      key:        YOUTUBE_API_KEY,
    });

    const response = await fetchWithRetry(`${YOUTUBE_SEARCH_URL}?${params}`);
    const data     = await response.json();

    if (data.error) {
      // Quota exceeded — surface clearly so caller can abort
      if (data.error.code === 403) throw new Error('HTTP 403');
      throw new Error(`YouTube API error: ${data.error.message}`);
    }

    searchResults = (data.items || []).map(item => ({
      videoId:     item.id.videoId,
      title:       item.snippet.title,
      channelName: item.snippet.channelTitle,
      description: item.snippet.description,
    }));
  } catch (err) {
    if (err.message.includes('403')) throw err; // propagate quota errors
    throw new Error(`Search failed: ${err.message}`);
  }

  if (searchResults.length === 0) {
    console.log(`    No search results.`);
    return 0;
  }

  console.log(`    YouTube: ${searchResults.length} result(s)`);

  // Step 2 — Claude relevance filter
  let relevantIds;
  try {
    relevantIds = await filterWithClaude(game, searchResults);
    console.log(`    Claude:  ${relevantIds.length} relevant`);
  } catch (err) {
    throw new Error(`Claude filtering failed: ${err.message}`);
  }

  if (relevantIds.length === 0) return 0;

  // Step 3 — Fetch full video details (1 unit per batch of 50)
  const videoRows = [];
  for (let i = 0; i < relevantIds.length; i += 50) {
    const batch = relevantIds.slice(i, i + 50);
    try {
      const params = new URLSearchParams({
        part: 'snippet,statistics',
        id:   batch.join(','),
        key:  YOUTUBE_API_KEY,
      });

      const response = await fetchWithRetry(`${YOUTUBE_VIDEOS_URL}?${params}`);
      const data     = await response.json();

      for (const item of (data.items || [])) {
        videoRows.push({
          video_id:      item.id,
          app_id,
          title:         item.snippet.title,
          channel_name:  item.snippet.channelTitle,
          published_at:  item.snippet.publishedAt,
          views:         parseInt(item.statistics.viewCount    ?? '0', 10),
          likes:         parseInt(item.statistics.likeCount    ?? '0', 10),
          comment_count: parseInt(item.statistics.commentCount ?? '0', 10),
          language:      item.snippet.defaultAudioLanguage ?? item.snippet.defaultLanguage ?? null,
          has_transcript: false,
          last_updated:  today,
        });
      }
    } catch (err) {
      if (err.message.includes('403')) throw err;
      console.warn(`    Video details batch failed: ${err.message}`);
    }
  }

  // Step 4 — Upsert to youtube_videos
  if (videoRows.length > 0) {
    const { error } = await supabase
      .from('youtube_videos')
      .upsert(videoRows, { onConflict: 'video_id' });
    if (error) throw new Error(`Supabase upsert failed: ${error.message}`);
  }

  return videoRows.length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today = new Date().toISOString().split('T')[0];
  console.log(`youtube-pipeline-backfill — ${today}\n`);

  // ---- Load state ----
  const state = loadState();
  console.log(`State: batch1=${state.batch1_completed_date ?? 'pending'} | batch2=${state.batch2_completed_date ?? 'pending'}`);

  if (state.batch1_completed_date && state.batch2_completed_date) {
    console.log('\nBackfill complete — both batches already done. Exiting.');
    process.exit(0);
  }

  // ---- Load pipeline candidate games ----
  let allGames;
  try {
    const { data, error } = await supabase
      .from('games_static')
      .select('app_id, name, developer1, developer2, developer3, publisher1, publisher2, publisher3, short_description')
      .eq('tracking_source', TRACKING_SOURCE)
      .order('app_id', { ascending: true });

    if (error) throw new Error(error.message);
    allGames = data || [];
  } catch (err) {
    console.error(`Failed to load games: ${err.message}`);
    process.exit(1);
  }

  console.log(`Pipeline candidate games found: ${allGames.length}\n`);

  if (allGames.length === 0) {
    console.log('No games to process. Exiting.');
    process.exit(0);
  }

  // ---- Determine which batch to run ----
  const midpoint  = Math.ceil(allGames.length / 2);
  const batch1    = allGames.slice(0, midpoint);
  const batch2    = allGames.slice(midpoint);

  let batchNum, gamesToProcess;

  if (!state.batch1_completed_date) {
    batchNum       = 1;
    gamesToProcess = batch1;
  } else {
    batchNum       = 2;
    gamesToProcess = batch2;
  }

  console.log(`Running Batch ${batchNum} — ${gamesToProcess.length} games (app_ids ${gamesToProcess[0].app_id}–${gamesToProcess[gamesToProcess.length - 1].app_id})`);
  console.log(`Estimated YouTube quota usage: ${gamesToProcess.length * 100} units of 10,000\n`);

  // ---- Process games ----
  let totalVideos  = 0;
  let succeeded    = 0;
  let failed       = 0;

  for (let i = 0; i < gamesToProcess.length; i++) {
    if (i > 0) await sleep(BETWEEN_GAMES_MS);

    const game = gamesToProcess[i];
    console.log(`[${i + 1}/${gamesToProcess.length}] ${game.name} (${game.app_id})`);

    try {
      const videosFound = await processGame(game, today);
      totalVideos += videosFound;
      console.log(`    ✓ ${videosFound} video(s) upserted`);
      succeeded++;
    } catch (err) {
      // Quota exceeded — abort immediately to avoid wasting remaining units
      if (err.message.includes('403')) {
        console.error(`\nYouTube quota exceeded at game ${i + 1}/${gamesToProcess.length}. Aborting.`);
        console.error(`Processed ${succeeded} games before quota hit. Re-run tomorrow to continue.`);
        process.exit(1);
      }
      console.error(`    ✗ ${err.message}`);
      failed++;
    }
  }

  // ---- Update state file ----
  if (batchNum === 1) {
    state.batch1_completed_date = today;
  } else {
    state.batch2_completed_date = today;
  }
  saveState(state);

  // ---- Summary ----
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Batch ${batchNum} complete — ${today}`);
  console.log(`Games: ${succeeded} succeeded | ${failed} failed | ${totalVideos} total videos upserted`);

  if (batchNum === 1) {
    console.log(`\nRun again tomorrow to process Batch 2 (${batch2.length} games).`);
  } else {
    console.log('\nBackfill complete — both batches done.');
  }
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
