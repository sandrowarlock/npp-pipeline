// youtube-enrichment.js
// Workflow: youtube-enrichment
//
// Runs weekly and enriches tracked games with YouTube data:
// videos, comments, and transcripts.
//
// Core principle: only process games that haven't been searched in the last
// 7 days, based on the most recent last_updated date in youtube_videos.
//
// Steps:
//   1. Get games to process (not searched within last 7 days)
//   2. Search YouTube for each game (100 units/call)
//   3. Filter results with Claude API for relevance (~$0.001–0.003/game)
//   4. Fetch video details in batches of 50 (1 unit/batch)
//   5. Fetch comments per video — up to 500 (1 unit/page)
//   6. Fetch transcripts per video via Supadata API
//   7. Log pipeline run
//   8. Exit

const { createClient }   = require('@supabase/supabase-js');
const { fetchWithRetry } = require('./lib/steam-enrichment');
const { logRun }         = require('./lib/pipeline-logger');

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const YOUTUBE_API_KEY      = process.env.YOUTUBE_API_KEY;
const TRANSCRIPT_API_KEY   = process.env.TRANSCRIPT_API_KEY;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !YOUTUBE_API_KEY || !ANTHROPIC_API_KEY) {
  console.error('Error: SUPABASE_URL, SUPABASE_SERVICE_KEY, YOUTUBE_API_KEY, and ANTHROPIC_API_KEY are required.');
  process.exit(1);
}

if (!TRANSCRIPT_API_KEY) {
  console.warn('Warning: TRANSCRIPT_API_KEY not set — transcript fetching will be skipped.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const YOUTUBE_SEARCH_URL   = 'https://www.googleapis.com/youtube/v3/search';
const YOUTUBE_VIDEOS_URL   = 'https://www.googleapis.com/youtube/v3/videos';
const YOUTUBE_COMMENTS_URL = 'https://www.googleapis.com/youtube/v3/commentThreads';
const TRANSCRIPT_API_URL   = 'https://transcriptapi.com/api/v2/youtube/transcript';
const ANTHROPIC_API_URL    = 'https://api.anthropic.com/v1/messages';

// Only process games whose youtube_videos rows are older than this many days.
const STALE_DAYS = 7;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Join non-null values from an array with a separator, falling back to 'Unknown'.
function joinNonNull(values, sep = ', ') {
  return values.filter(Boolean).join(sep) || 'Unknown';
}

// ---------------------------------------------------------------------------
// Claude relevance filtering
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

  const data = await response.json();
  const text = data.content?.[0]?.text ?? '';

  // Strip markdown code fences if Claude wraps the response (e.g. ```json ... ```)
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
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today     = new Date().toISOString().split('T')[0];
  const staleDate = new Date(Date.now() - STALE_DAYS * 864e5).toISOString().split('T')[0];

  console.log(`youtube-enrichment — ${today}\n`);

  let totalVideosWritten      = 0;
  let totalCommentsWritten    = 0;
  let totalTranscriptsWritten = 0;
  let gamesProcessed          = 0;
  let gamesFailed             = 0;

  // -------------------------------------------------------------------------
  // Step 1 — Get games to process
  //
  // Only process games whose most recent youtube_videos row is older than
  // STALE_DAYS, or games that have never been searched (no rows at all).
  // -------------------------------------------------------------------------
  console.log('Step 1: Loading games to process...');

  let allGames;
  try {
    const { data, error } = await supabase
      .from('games_static')
      .select('app_id, name, developer1, developer2, developer3, publisher1, publisher2, publisher3, short_description')
      .order('app_id', { ascending: true });

    if (error) throw new Error(error.message);
    allGames = data || [];
  } catch (err) {
    console.error(`Step 1 failed (reading games_static): ${err.message}`);
    process.exit(1);
  }

  if (allGames.length === 0) {
    console.log('No tracked games found. Exiting.');
    process.exit(0);
  }

  const allAppIds = allGames.map(g => g.app_id);

  // Find the most recent last_updated per game from youtube_videos.
  // A simple query is sufficient here — missing rows due to the 1,000-row
  // default limit only causes a game to be re-processed unnecessarily,
  // which is harmless since all writes are upserts.
  const recentByAppId = new Map(); // app_id → most recent last_updated string
  try {
    const { data, error } = await supabase
      .from('youtube_videos')
      .select('app_id, last_updated')
      .in('app_id', allAppIds);

    if (error) throw new Error(error.message);

    for (const row of (data || [])) {
      const existing = recentByAppId.get(row.app_id);
      if (!existing || row.last_updated > existing) {
        recentByAppId.set(row.app_id, row.last_updated);
      }
    }
  } catch (err) {
    console.warn(`Step 1 warning (reading youtube_videos): ${err.message}`);
    console.warn('Treating all games as unprocessed.');
  }

  const gamesToProcess = allGames.filter(g => {
    const lastUpdated = recentByAppId.get(g.app_id);
    return !lastUpdated || lastUpdated < staleDate;
  });

  const skipped = allGames.length - gamesToProcess.length;
  console.log(`${gamesToProcess.length} game(s) to process, ${skipped} skipped (searched within last ${STALE_DAYS} days).`);

  if (gamesToProcess.length === 0) {
    console.log('All games are up to date. Exiting.');
    process.exit(0);
  }

  // video_id → app_id — built up during Steps 2 & 3, used in Steps 4–6.
  const videoAppIdMap = new Map();

  // -------------------------------------------------------------------------
  // Steps 2 & 3 — YouTube search and Claude filtering, per game
  // -------------------------------------------------------------------------
  console.log('\nSteps 2 & 3: Searching YouTube and filtering with Claude...');

  for (const game of gamesToProcess) {
    const { app_id, name } = game;
    console.log(`\n  ${name} (${app_id}):`);

    // Step 2 — Search YouTube (100 units per call)
    let searchResults;
    try {
      const params = new URLSearchParams({
        part:       'snippet',
        q:          `${name} game`,
        type:       'video',
        maxResults: '50',
        key:        YOUTUBE_API_KEY,
      });

      const response = await fetchWithRetry(`${YOUTUBE_SEARCH_URL}?${params}`);
      const data     = await response.json();

      searchResults = (data.items || []).map(item => ({
        videoId:     item.id.videoId,
        title:       item.snippet.title,
        channelName: item.snippet.channelTitle,
        description: item.snippet.description,
      }));

      console.log(`    YouTube: ${searchResults.length} result(s)`);
    } catch (err) {
      // 403 on search = quota exceeded — no point continuing
      if (err.message.includes('403')) {
        console.error(`    YouTube quota exceeded. Aborting.`);
        process.exit(1);
      }
      console.error(`    Search failed: ${err.message}`);
      gamesFailed++;
      continue;
    }

    if (searchResults.length === 0) {
      console.log('    No results. Skipping.');
      gamesProcessed++;
      continue;
    }

    // Step 3 — Filter with Claude (~$0.001–0.003 per game)
    let relevantIds;
    try {
      relevantIds = await filterWithClaude(game, searchResults);
      console.log(`    Claude: ${relevantIds.length} of ${searchResults.length} video(s) relevant`);
    } catch (err) {
      console.warn(`    Claude filtering failed: ${err.message}. Skipping game.`);
      gamesFailed++;
      continue;
    }

    if (relevantIds.length === 0) {
      console.warn('    No relevant videos found by Claude. Skipping.');
      gamesProcessed++;
      continue;
    }

    for (const videoId of relevantIds) {
      videoAppIdMap.set(videoId, app_id);
    }

    gamesProcessed++;
  }

  const allRelevantIds = [...videoAppIdMap.keys()];
  console.log(`\nTotal relevant videos: ${allRelevantIds.length}`);

  if (allRelevantIds.length === 0) {
    console.log('No relevant videos to process.');
    try {
      await logRun(supabase, {
        workflowName: 'youtube-enrichment',
        status:       gamesFailed === gamesToProcess.length ? 'failure' : 'success',
        rowsWritten:  0,
        rowsExpected: gamesToProcess.length,
        notes:        `${gamesProcessed} games processed, 0 videos found`,
      });
    } catch (logErr) {
      console.warn(`Pipeline log failed: ${logErr.message}`);
    }
    process.exit(gamesFailed === gamesToProcess.length ? 1 : 0);
  }

  // -------------------------------------------------------------------------
  // Step 4 — Fetch video details in batches of 50 (1 unit per batch)
  // -------------------------------------------------------------------------
  console.log('\nStep 4: Fetching video details...');

  const videoRows = []; // will be used by Steps 5 & 6 as well

  for (let i = 0; i < allRelevantIds.length; i += 50) {
    const batch = allRelevantIds.slice(i, i + 50);
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
          app_id:        videoAppIdMap.get(item.id),
          title:         item.snippet.title,
          channel_name:  item.snippet.channelTitle,
          published_at:  item.snippet.publishedAt,
          views:         parseInt(item.statistics.viewCount   ?? '0', 10),
          likes:         parseInt(item.statistics.likeCount   ?? '0', 10),
          comment_count: parseInt(item.statistics.commentCount ?? '0', 10),
          language:      item.snippet.defaultAudioLanguage ?? item.snippet.defaultLanguage ?? null,
          has_transcript: false,
          last_updated:  today,
        });
      }
    } catch (err) {
      if (err.message.includes('403')) {
        console.error('YouTube quota exceeded fetching video details. Aborting.');
        process.exit(1);
      }
      console.error(`  Video details batch failed (ids ${i}–${i + batch.length - 1}): ${err.message}`);
    }
  }

  if (videoRows.length > 0) {
    try {
      const { error } = await supabase
        .from('youtube_videos')
        .upsert(videoRows, { onConflict: 'video_id' });
      if (error) throw new Error(error.message);
      totalVideosWritten = videoRows.length;
    } catch (err) {
      console.error(`  Step 4 upsert failed: ${err.message}`);
    }
  }

  console.log(`Upserted ${totalVideosWritten} video(s).`);

  // -------------------------------------------------------------------------
  // Step 5 — Fetch comments per video (up to 500 comments, 1 unit per page)
  //
  // Some videos have comments disabled — 403 on the first page is expected
  // and handled gracefully by breaking the pagination loop.
  // -------------------------------------------------------------------------
  console.log('\nStep 5: Fetching comments...');

  for (const { video_id, app_id } of videoRows) {
    const commentRows = [];
    let pageToken     = null;
    let pages         = 0;

    while (pages < 5) {
      try {
        const params = new URLSearchParams({
          part:       'snippet',
          videoId:    video_id,
          maxResults: '100',
          order:      'relevance',
          key:        YOUTUBE_API_KEY,
        });
        if (pageToken) params.set('pageToken', pageToken);

        const response = await fetchWithRetry(`${YOUTUBE_COMMENTS_URL}?${params}`);
        const data     = await response.json();

        for (const item of (data.items || [])) {
          const top = item.snippet.topLevelComment;
          commentRows.push({
            comment_id:    top.id,
            video_id,
            app_id,
            comment_text:  top.snippet.textDisplay,
            comment_likes: top.snippet.likeCount ?? 0,
            published_at:  top.snippet.publishedAt,
          });
        }

        pages++;
        pageToken = data.nextPageToken ?? null;
        if (!pageToken) break;
      } catch (err) {
        if (err.message.includes('403')) {
          // Comments disabled on this video — not an error
          break;
        }
        console.warn(`  Comments failed for ${video_id}: ${err.message}`);
        break;
      }
    }

    if (commentRows.length > 0) {
      try {
        const { error } = await supabase
          .from('youtube_comments')
          .upsert(commentRows, { onConflict: 'comment_id' });
        if (error) throw new Error(error.message);
        totalCommentsWritten += commentRows.length;
      } catch (err) {
        console.warn(`  Comment upsert failed for ${video_id}: ${err.message}`);
      }
    }
  }

  console.log(`Fetched ${totalCommentsWritten} comment(s) across ${videoRows.length} video(s).`);

  // -------------------------------------------------------------------------
  // Step 6 — Fetch transcripts via Supadata API (500ms delay between calls)
  //
  // Upserts a row to youtube_transcripts for every video regardless of
  // whether a transcript was found, so we don't re-attempt unavailable ones.
  // Updates youtube_videos.has_transcript = true when a transcript is found.
  // -------------------------------------------------------------------------
  console.log('\nStep 6: Fetching transcripts...');

  if (!TRANSCRIPT_API_KEY) {
    console.log('  Skipped — TRANSCRIPT_API_KEY not configured.');
  } else {
    for (const { video_id, app_id } of videoRows) {
      await sleep(500); // stay within rate limits

      try {
        const params = new URLSearchParams({
          video_url: video_id,
          format:    'json',
        });

        let transcriptText = null;
        let language       = null;
        let hasTranscript  = false;

        try {
          const response = await fetchWithRetry(`${TRANSCRIPT_API_URL}?${params}`, {
            headers: { 'Authorization': `Bearer ${TRANSCRIPT_API_KEY}` },
          });
          const data = await response.json();

          // transcriptapi.com wraps segments under one of several fields depending
          // on the response shape — try each in order until we find a non-empty array.
          const segments =
            (Array.isArray(data.segments)            && data.segments.length            ? data.segments            : null) ??
            (Array.isArray(data.transcript)          && data.transcript.length          ? data.transcript          : null) ??
            (Array.isArray(data.data?.segments)      && data.data.segments.length       ? data.data.segments       : null) ??
            null;

          if (segments) {
            // Store the full segments array as a JSON string — all fields intact
            // (text, start, duration, etc.) with no character limit, so report
            // generation can correlate specific moments with external signals.
            transcriptText = JSON.stringify(segments);
            language       = data.language ?? data.lang ?? null;
            hasTranscript  = true;
          }
        } catch (err) {
          // 404 = no transcript available — expected for many videos
          if (!err.message.includes('404')) {
            console.warn(`  Transcript fetch error for ${video_id}: ${err.message}`);
          }
        }

        // Always upsert a transcript row so we don't re-attempt this video
        const transcriptRow = {
          video_id,
          app_id,
          language,
          has_transcript: hasTranscript,
          ...(transcriptText ? { transcript_text: transcriptText } : {}),
        };

        const { error: tErr } = await supabase
          .from('youtube_transcripts')
          .upsert(transcriptRow, { onConflict: 'video_id' });
        if (tErr) throw new Error(tErr.message);

        if (hasTranscript) {
          totalTranscriptsWritten++;
          // Keep youtube_videos in sync so reports can filter by has_transcript
          await supabase
            .from('youtube_videos')
            .update({ has_transcript: true })
            .eq('video_id', video_id);
        }
      } catch (err) {
        console.warn(`  Transcript processing failed for ${video_id}: ${err.message}`);
      }
    }
  }

  console.log(`Fetched ${totalTranscriptsWritten} transcript(s).`);

  // -------------------------------------------------------------------------
  // Step 7 — Log pipeline run
  // -------------------------------------------------------------------------
  const totalRowsWritten = totalVideosWritten + totalCommentsWritten + totalTranscriptsWritten;

  const status = gamesFailed === 0
    ? 'success'
    : gamesFailed === gamesToProcess.length
      ? 'failure'
      : 'partial';

  const notes =
    `${gamesProcessed} games processed, ` +
    `${totalVideosWritten} videos, ` +
    `${totalCommentsWritten} comments, ` +
    `${totalTranscriptsWritten} transcripts`;

  try {
    await logRun(supabase, {
      workflowName: 'youtube-enrichment',
      status,
      rowsWritten:  totalRowsWritten,
      rowsExpected: gamesToProcess.length,
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
  if (gamesFailed === gamesToProcess.length) {
    console.error('\nAll games failed. Exiting with error.');
    process.exit(1);
  }

  console.log('\n✓ youtube-enrichment complete.');
}

main();
