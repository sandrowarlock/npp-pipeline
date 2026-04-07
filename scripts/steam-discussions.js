// steam-discussions.js
// Workflow: steam-discussions
//
// Scrapes Steam discussion threads for all tracked games by paginating through
// the Steam community discussion pages directly (no Apify dependency).
//
// For each topic, compares against what is already stored to determine whether
// replies need re-scraping (new topic, or reply_count / last_posted_at changed).
// Sets needs_reply_scrape = true on those rows so steam-discussion-replies can
// query only the threads that have new activity.
//
// Steps:
//   1. Get all tracked games from games_static
//   2. Get existing topics per game from steam_discussions (paginated bulk query)
//   3. Scrape Steam discussion pages per game (paginate until stale or capped)
//   4. Upsert all scraped topics, updating needs_reply_scrape flag
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

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Error: SUPABASE_URL and SUPABASE_SERVICE_KEY are required.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const STEAM_DISCUSSIONS_URL = 'https://steamcommunity.com/app/{appId}/discussions/0/?fp={page}';

// Stop paginating if the oldest thread on the current page was last posted
// more than this many days ago — avoids pulling years of stale history.
const MAX_AGE_DAYS = 30;

// Hard cap on threads per game — safety net against infinite pagination.
const MAX_THREADS = 1000;

// Polite delays to avoid hammering Steam's servers.
const BETWEEN_PAGES_MS = 1_000;
const BETWEEN_GAMES_MS = 2_000;

// Browser-like request headers so Steam serves the page rather than blocking.
// Built per-game so wants_mature_content_apps can be set to the current app_id.
// Two separate age-gate cookies are required:
//   - birthtime + lastagecheckage  → store page age gate
//   - wants_mature_content_apps    → community hub age gate (different system)
function steamHeaders(appId) {
  return {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection':      'keep-alive',
    'Sec-Fetch-Dest':  'document',
    'Sec-Fetch-Mode':  'navigate',
    'Sec-Fetch-Site':  'none',
    'Cookie':          `birthtime=0; lastagecheckage=1-January-1990; mature_content=1; wants_mature_content_apps=${appId}`,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Paginated bulk fetch from Supabase — same pattern as daily-game-snapshot.js.
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

// Decode common HTML entities and strip all tags — used for opening_post_body.
function htmlToPlainText(html) {
  if (!html) return '';
  return html
    .replace(/&amp;/g,  '&')
    .replace(/&lt;/g,   '<')
    .replace(/&gt;/g,   '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g,  "'")
    .replace(/&#10;/g,  '\n')
    .replace(/&#13;/g,  '')
    .replace(/&nbsp;/g, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .trim();
}

// ---------------------------------------------------------------------------
// HTML parsing helpers
// ---------------------------------------------------------------------------

// Extract all discussion topic blocks from a Steam discussions page.
// Returns an array of raw topic objects, or an empty array if none found.
function parseDiscussionsPage(html) {
  const topics = [];

  // Each topic is a <div> with a data-gidforumtopic attribute.
  // We match from that opening tag to the closing </div> of the block.
  // Steam's HTML is not perfectly nested, so we look for the key fields
  // within a generous window after each topic container opening.
  // Capture the full opening tag (group 1) to read class attributes, plus the
  // inner block (group 2) for all child-element fields.
  const topicPattern = /(<div[^>]+data-gidforumtopic="(\d+)"[^>]*>)([\s\S]*?)(?=<div[^>]+data-gidforumtopic=|$)/g;

  let match;
  while ((match = topicPattern.exec(html)) !== null) {
    const openingTag = match[1];
    const topicId    = match[2];
    const block      = match[3];

    // Pinned / locked: detected from CSS classes on the topic container div.
    // Examples:
    //   class="forum_topic unread sticky"         → pinned
    //   class="forum_topic unread locked sticky"  → pinned + locked
    const classMatch = /class="([^"]*)"/i.exec(openingTag);
    const classList  = classMatch ? classMatch[1] : '';
    const isPinned   = /\bsticky\b/.test(classList);
    const isLocked   = /\blocked\b/.test(classList);

    // Title: .forum_topic_name
    const titleMatch = /class="forum_topic_name[^"]*"[^>]*>([\s\S]*?)<\/a>/i.exec(block);
    const title = titleMatch ? titleMatch[1].replace(/<[^>]+>/g, '').trim() : '';

    // Author: .forum_topic_op
    const authorMatch = /class="forum_topic_op[^"]*"[^>]*>([\s\S]*?)<\/(?:div|span|a)>/i.exec(block);
    const authorName = authorMatch ? authorMatch[1].replace(/<[^>]+>/g, '').trim() : '';

    // Reply count: .forum_topic_reply_count
    const replyMatch = /class="forum_topic_reply_count[^"]*"[^>]*>([\s\S]*?)<\/(?:div|span)>/i.exec(block);
    const replyText  = replyMatch ? replyMatch[1].replace(/<[^>]+>/g, '').trim() : '0';
    const replyCount = parseInt(replyText.replace(/[^0-9]/g, '') || '0', 10);

    // Last posted: data-timestamp inside .forum_topic_lastpost
    const lastPostMatch = /class="forum_topic_lastpost[^"]*"[\s\S]*?data-timestamp="(\d+)"/i.exec(block);
    const lastPostedAt = lastPostMatch
      ? new Date(parseInt(lastPostMatch[1], 10) * 1000).toISOString()
      : null;

    // Opening post body: data-tooltip-forum attribute on the topic container
    const tooltipMatch = /data-tooltip-forum="([^"]*)"/i.exec(openingTag);
    const openingPostBody = tooltipMatch
      ? htmlToPlainText(tooltipMatch[1])
      : '';

    topics.push({ topicId, title, authorName, replyCount, lastPostedAt, openingPostBody, isPinned, isLocked });
  }

  return topics;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const today       = new Date().toISOString().split('T')[0];
  const cutoffMs    = Date.now() - MAX_AGE_DAYS * 864e5; // 30-day threshold in ms

  console.log(`steam-discussions — ${today}\n`);

  let gamesProcessed       = 0;
  let gamesFailed          = 0;
  let totalTopicsWritten   = 0;
  let totalNew             = 0;
  let totalUpdated         = 0;
  let totalUnchanged       = 0;
  let totalNeedsScrape     = 0;

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
  // Step 2 — Get existing topics per game (paginated)
  //
  // Builds: app_id → Map<topic_id → { reply_count, last_posted_at }>
  // Used in Step 3 to detect new topics and changed threads.
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Loading existing topic data...');

  const existingTopics = new Map(); // app_id → Map<topic_id, {reply_count, last_posted_at}>
  for (const id of appIds) existingTopics.set(id, new Map());

  try {
    const rows = await fetchAllRows((from, to) =>
      supabase
        .from('steam_discussions')
        .select('app_id, topic_id, reply_count, last_posted_at')
        .in('app_id', appIds)
        .range(from, to)
    );

    for (const row of rows) {
      existingTopics.get(row.app_id)?.set(String(row.topic_id), {
        reply_count:    row.reply_count,
        last_posted_at: row.last_posted_at,
      });
    }

    const totalTopics = rows.length;
    const totalWithActivity = rows.filter(r => r.reply_count > 0).length;
    console.log(`Found ${totalTopics} existing topic(s) across all games (${totalWithActivity} with replies).`);
  } catch (err) {
    console.error(`Step 2 failed: ${err.message}`);
    process.exit(1);
  }

  // -------------------------------------------------------------------------
  // Step 3 — Scrape Steam discussion pages per game
  // -------------------------------------------------------------------------
  console.log('\nStep 3: Scraping Steam discussions...');

  const allRows = []; // collected across all games, written in Step 4

  for (const game of games) {
    const { app_id, name } = game;

    if (gamesProcessed + gamesFailed > 0) await sleep(BETWEEN_GAMES_MS);

    const knownTopics = existingTopics.get(app_id);
    const isFirstRun  = knownTopics.size === 0;
    const gameRows    = [];

    let page        = 1;
    let stopReason  = 'no topics';
    let hitCap      = false;

    console.log(`  ${name} (${app_id}): ${isFirstRun ? 'first run — no 30-day cutoff' : 'subsequent run — 30-day cutoff active'}`);

    try {
      pageLoop: while (true) {
        if (page > 1) await sleep(BETWEEN_PAGES_MS);

        const url = STEAM_DISCUSSIONS_URL
          .replace('{appId}', app_id)
          .replace('{page}', page);

        const response = await fetchWithRetry(url, { headers: steamHeaders(app_id) });
        const html     = await response.text();

        // Detect age gate — Steam returned a content-check page instead of discussions.
        if (html.includes('contentcheck_desc_ctn')) {
          console.warn(`  Age gate not bypassed for ${name} — may need login`);
          break pageLoop;
        }

        // Detect login wall — two distinct cases:
        //   1. Steam redirects to the login page (title is "Sign In")
        //   2. Steam shows an error page with an inline login-required message
        //      e.g. "You must be logged in to view this Community Hub"
        // Note: do NOT check for "login/home" in body — that string appears in the
        // nav header of every public Steam page for unauthenticated users.
        if (
          html.includes('<title>Sign In') ||
          html.includes('You must be logged in to view this Community Hub')
        ) {
          console.warn(`  Login required for ${name} — skipping`);
          break pageLoop;
        }

        const pageTopics = parseDiscussionsPage(html);

        if (pageTopics.length === 0) {
          stopReason = 'no topics';
          break;
        }

        for (const t of pageTopics) {
          // Check 30-day threshold against last_posted_at.
          // Skipped on first run so the full discussion history is collected.
          // Pinned topics are always exempt — they can be years old but appear
          // at the top of every page, so they must never trigger pagination stop.
          if (!isFirstRun && !t.isPinned && t.lastPostedAt) {
            const lastMs = new Date(t.lastPostedAt).getTime();
            if (lastMs < cutoffMs) {
              stopReason = '30-day threshold';
              break pageLoop;
            }
          }

          // Safety cap
          if (gameRows.length >= MAX_THREADS) {
            stopReason = '1000 cap';
            hitCap     = true;
            break pageLoop;
          }

          const existing          = knownTopics.get(t.topicId);
          const isNew             = !existing;
          const replyCountChanged = existing && existing.reply_count    !== t.replyCount;
          const lastPostChanged   = existing && existing.last_posted_at !== t.lastPostedAt;
          const needsReplyScrape  = isNew || replyCountChanged || lastPostChanged;

          gameRows.push({
            topic_id:           t.topicId,
            app_id,
            title:              t.title,
            author_name:        t.authorName,
            reply_count:        t.replyCount,
            opening_post_body:  t.openingPostBody,
            last_posted_at:     t.lastPostedAt,
            is_pinned:          t.isPinned,
            is_locked:          t.isLocked,
            scraped_date:       today,
            needs_reply_scrape: needsReplyScrape,
          });
        }

        if (!hitCap) {
          stopReason = `${page} page(s)`;
          page++;
        }
      }
    } catch (err) {
      console.error(`  ${name} (${app_id}): scrape failed — ${err.message}`);
      gamesFailed++;
      continue;
    }

    // Tally per-game stats
    let newCount       = 0;
    let updatedCount   = 0;
    let unchangedCount = 0;
    let needsCount     = 0;

    for (const row of gameRows) {
      const existing = knownTopics.get(row.topic_id);
      if (!existing) {
        newCount++;
      } else if (row.needs_reply_scrape) {
        updatedCount++;
      } else {
        unchangedCount++;
      }
      if (row.needs_reply_scrape) needsCount++;
    }

    console.log(
      `  ${name} (${app_id}): ${page - 1} page(s), ${gameRows.length} topics, ` +
      `${newCount} new, ${updatedCount} updated, ${unchangedCount} unchanged, ` +
      `${needsCount} flagged for reply scraping, stop reason: ${stopReason}`
    );

    allRows.push(...gameRows);
    totalNew       += newCount;
    totalUpdated   += updatedCount;
    totalUnchanged += unchangedCount;
    totalNeedsScrape += needsCount;
    gamesProcessed++;
  }

  // -------------------------------------------------------------------------
  // Step 4 — Upsert all scraped topics
  // -------------------------------------------------------------------------
  console.log(`\nStep 4: Writing ${allRows.length} topic(s)...`);

  if (allRows.length > 0) {
    try {
      totalTopicsWritten = await upsertInChunks(
        supabase,
        'steam_discussions',
        allRows,
        100,
        'topic_id'
      );
      console.log(`Wrote ${totalTopicsWritten} topic(s).`);
    } catch (err) {
      console.error(`Step 4 failed: ${err.message}`);
    }
  } else {
    console.log('No topics to write.');
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
    `${totalTopicsWritten} topics`,
    `${totalNew} new`,
    `${totalUpdated} updated`,
    `${totalUnchanged} unchanged`,
    `${totalNeedsScrape} flagged for reply scraping`,
    gamesFailed > 0 ? `${gamesFailed} games failed` : null,
  ].filter(Boolean).join(', ');

  try {
    await logRun(supabase, {
      workflowName: 'steam-discussions',
      status,
      rowsWritten:  totalTopicsWritten,
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
