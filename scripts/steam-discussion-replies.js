// steam-discussion-replies.js
// Workflow: steam-discussion-replies
//
// Scrapes replies for Steam discussion topics that have needs_reply_scrape = true.
// Topics are flagged by steam-discussions.js when they are new or when their
// reply_count or last_posted_at has changed since the previous scrape.
//
// After successfully scraping a topic, sets needs_reply_scrape = false so the
// topic is not re-scraped on the next run unless it changes again.
//
// Steps:
//   1. Load topics with needs_reply_scrape = true from steam_discussions
//   2. Scrape replies per topic from Steam community HTML
//   3. Upsert replies to steam_discussion_replies, clear needs_reply_scrape flag
//   4. Log pipeline run to pipeline_runs
//   5. Exit non-zero if all topics failed

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
const STEAM_TOPIC_URL  = 'https://steamcommunity.com/app/{appId}/discussions/0/{topicId}/';
const BETWEEN_TOPICS_MS = 2_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Build browser-like headers per request, including the community hub age-gate
// cookie keyed to the current app_id.
function steamHeaders(appId) {
  return {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cookie':          `birthtime=0; lastagecheckage=1-January-1990; mature_content=1; wants_mature_content_apps=${appId}`,
  };
}

// Decode common HTML entities and strip all tags — used for reply body text.
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
// HTML parsing
// ---------------------------------------------------------------------------

// Parse all reply blocks from a Steam discussion thread page.
// Returns an array of reply objects.
//
// Steam thread pages use this structure per comment:
//   <div class="commentthread_comment ..." id="comment_{id}">
//     <div class="commentthread_comment_content">
//       <div class="commentthread_comment_author">...</div>
//       <div class="commentthread_comment_text">...</div>
//       <span class="commentthread_comment_timestamp" data-timestamp="{unix}">
//     </div>
//   </div>
//
// The opening post (forum_op) uses the same id="comment_..." pattern but has
// an additional class "forum_op" — we skip it since its content is already
// stored in steam_discussions.opening_post_body.
function parseThreadPage(html) {
  const results = [];

  // Match every commentthread_comment div. Capture:
  //   group 1 — full opening tag (for class inspection and id extraction)
  //   group 2 — inner block content
  const blockPattern = /(<div[^>]+class="[^"]*commentthread_comment[^"]*"[^>]*>)([\s\S]*?)(?=<div[^>]+class="[^"]*commentthread_comment[^"]*"|$)/g;

  let match;
  while ((match = blockPattern.exec(html)) !== null) {
    const openingTag = match[1];
    const block      = match[2];

    // Skip the opening post — identified by the forum_op class.
    const classMatch = /class="([^"]*)"/i.exec(openingTag);
    const classList  = classMatch ? classMatch[1] : '';
    if (/\bforum_op\b/.test(classList)) continue;

    // Skip non-comment divs that match the pattern but lack an id="comment_..."
    const idMatch = /\bid="comment_(\d+)"/.exec(openingTag);
    if (!idMatch) continue;
    const postId = idMatch[1];

    // Author: first anchor/bdi inside .commentthread_comment_author
    const authorBlockMatch = /class="[^"]*commentthread_comment_author[^"]*"[^>]*>([\s\S]*?)<\/div>/i.exec(block);
    const author = authorBlockMatch
      ? authorBlockMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim()
      : '';

    // Reply body: .commentthread_comment_text
    const bodyMatch = /class="[^"]*commentthread_comment_text[^"]*"[^>]*>([\s\S]*?)<\/div>/i.exec(block);
    const text = bodyMatch ? htmlToPlainText(bodyMatch[1]) : '';

    // Timestamp: data-timestamp attribute on the timestamp span
    const tsMatch  = /data-timestamp="(\d+)"/i.exec(block);
    const postedAt = tsMatch
      ? new Date(parseInt(tsMatch[1], 10) * 1000).toISOString()
      : null;

    results.push({ postId, author, text, postedAt });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Upsert helper
// ---------------------------------------------------------------------------
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
  console.log(`steam-discussion-replies — ${today}\n`);

  let topicsProcessed = 0;
  let topicsSkipped   = 0;
  let topicsFailed    = 0;
  let totalReplies    = 0;

  // -------------------------------------------------------------------------
  // Step 1 — Load topics needing reply scraping
  // -------------------------------------------------------------------------
  console.log('Step 1: Loading topics with needs_reply_scrape = true...');

  let topics;
  try {
    const { data, error } = await supabase
      .from('steam_discussions')
      .select('topic_id, app_id, title, reply_count')
      .eq('needs_reply_scrape', true)
      .order('app_id', { ascending: true });

    if (error) throw new Error(error.message);

    topics = data || [];
  } catch (err) {
    console.error(`Step 1 failed: ${err.message}`);
    process.exit(1);
  }

  if (topics.length === 0) {
    console.log('No topics need reply scraping. Exiting.');

    try {
      await logRun(supabase, {
        workflowName: 'steam-discussion-replies',
        status:       'success',
        rowsWritten:  0,
        rowsExpected: 0,
        notes:        'No topics flagged for reply scraping',
      });
    } catch (logErr) {
      console.warn(`Pipeline log failed: ${logErr.message}`);
    }

    process.exit(0);
  }

  console.log(`${topics.length} topic(s) to process.`);

  // -------------------------------------------------------------------------
  // Step 2 & 3 — Scrape replies per topic, upsert, clear flag
  // -------------------------------------------------------------------------
  console.log('\nStep 2: Scraping replies...');

  for (const topic of topics) {
    const { topic_id, app_id, title, reply_count } = topic;

    if (topicsProcessed + topicsSkipped + topicsFailed > 0) {
      await sleep(BETWEEN_TOPICS_MS);
    }

    const url = STEAM_TOPIC_URL
      .replace('{appId}',   app_id)
      .replace('{topicId}', topic_id);

    let html;
    try {
      const response = await fetchWithRetry(url, { headers: steamHeaders(app_id) });
      html = await response.text();
    } catch (err) {
      console.error(`  [${topic_id}] Fetch failed: ${err.message}`);
      topicsFailed++;
      continue;
    }

    // Detect access blocks — same checks as steam-discussions.js
    if (html.includes('contentcheck_desc_ctn')) {
      console.warn(`  [${topic_id}] Age gate not bypassed — skipping`);
      topicsSkipped++;
      continue;
    }
    if (
      html.includes('<title>Sign In') ||
      html.includes('You must be logged in to view this Community Hub')
    ) {
      console.warn(`  [${topic_id}] Login required — skipping`);
      topicsSkipped++;
      continue;
    }

    const parsed = parseThreadPage(html);

    if (parsed.length === 0) {
      console.log(`  [${topic_id}] "${title}": no posts parsed`);
      // Still clear the flag so we don't re-attempt a page that returned nothing
    } else {
      // Build rows for steam_discussion_replies — opening posts already filtered
      // out inside parseThreadPage, so every entry here is a genuine reply.
      const replyRows = parsed.map(p => ({
        comment_id:   p.postId,
        topic_id,
        app_id,
        author:       p.author,
        text:         p.text,
        scraped_date: today,
      }));

      try {
        const written = await upsertInChunks(
          supabase,
          'steam_discussion_replies',
          replyRows,
          100,
          'comment_id'
        );
        totalReplies += written;
        console.log(`  [${topic_id}] "${title}": ${written} post(s) written (expected ~${reply_count + 1})`);
      } catch (err) {
        console.error(`  [${topic_id}] Upsert failed: ${err.message}`);
        topicsFailed++;
        continue;
      }
    }

    // Clear needs_reply_scrape flag so this topic is skipped on the next run
    // unless steam-discussions flags it again due to new activity.
    try {
      const { error } = await supabase
        .from('steam_discussions')
        .update({ needs_reply_scrape: false })
        .eq('topic_id', topic_id);
      if (error) throw new Error(error.message);
    } catch (err) {
      console.warn(`  [${topic_id}] Failed to clear needs_reply_scrape: ${err.message}`);
    }

    topicsProcessed++;
  }

  console.log(`\n${topicsProcessed} processed, ${topicsSkipped} skipped, ${topicsFailed} failed.`);
  console.log(`Total replies written: ${totalReplies}`);

  // -------------------------------------------------------------------------
  // Step 4 — Log pipeline run
  // -------------------------------------------------------------------------
  const totalTopics = topics.length;
  const status = topicsFailed === 0
    ? 'success'
    : topicsFailed === totalTopics
      ? 'failure'
      : 'partial';

  const notes = [
    `${totalTopics} topics`,
    `${topicsProcessed} processed`,
    `${totalReplies} replies written`,
    topicsSkipped > 0 ? `${topicsSkipped} skipped (login/age gate)` : null,
    topicsFailed  > 0 ? `${topicsFailed} failed` : null,
  ].filter(Boolean).join(', ');

  try {
    await logRun(supabase, {
      workflowName: 'steam-discussion-replies',
      status,
      rowsWritten:  totalReplies,
      rowsExpected: totalTopics,
      notes,
    });
    console.log(`\nPipeline run logged: ${status}`);
    console.log(`Notes: ${notes}`);
  } catch (logErr) {
    console.warn(`Pipeline log failed: ${logErr.message}`);
  }

  // -------------------------------------------------------------------------
  // Step 5 — Exit
  // -------------------------------------------------------------------------
  if (topicsFailed === totalTopics) {
    console.error('\nAll topics failed. Exiting with error.');
    process.exit(1);
  }

  console.log('\n✓ steam-discussion-replies complete.');
}

main();
