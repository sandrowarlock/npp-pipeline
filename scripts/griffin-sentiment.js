// griffin-sentiment.js
//
// Two-phase analysis of Steam reviews for roguelike games in the Griffin genre study.
//
// Phase 1 — Translation
//   Detects language of each review using franc. Sends non-English reviews to
//   Claude in batches of 20 for translation to English.
//
// Phase 2 — Analysis
//   Sends all translated reviews to Claude in batches of 50 for sentiment
//   classification and open-ended thematic tagging. Then sends one final
//   aggregation prompt to group themes and write per-category insights.
//
// Output: griffin/roguelike_sentiment.json
//
// Run from scripts/ directory:
//   node --env-file=../.env griffin-sentiment.js

'use strict';

const fs   = require('fs');
const path = require('path');
const franc = require('franc');

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_API_KEY) {
  console.error('Error: ANTHROPIC_API_KEY is required.');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
const GRIFFIN_DIR  = path.resolve(__dirname, '../griffin');
const TAGS_CSV     = path.join(GRIFFIN_DIR, 'tags.csv');
const REVIEWS_JSON = path.join(GRIFFIN_DIR, 'reviews.json');
const OUTPUT_JSON  = path.join(GRIFFIN_DIR, 'roguelike_sentiment.json');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const ANTHROPIC_API_URL    = 'https://api.anthropic.com/v1/messages';
const MODEL                = 'claude-sonnet-4-20250514';
const MAX_TOKENS           = 4096;
const BETWEEN_CALLS_MS     = 500;
const TRANSLATION_BATCH    = 20;
const ANALYSIS_BATCH       = 50;

const ROGUELIKE_TAGS = new Set(['Roguelike', 'Roguelite', 'Action Roguelike']);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function stripFences(text) {
  return text.trim()
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();
}

async function claudeCall(userContent) {
  const response = await fetch(ANTHROPIC_API_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: MAX_TOKENS,
      messages:   [{ role: 'user', content: userContent }],
    }),
  });

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    throw new Error(`Claude API HTTP ${response.status}: ${errText.slice(0, 300)}`);
  }

  const data = await response.json();
  return data.content?.[0]?.text ?? '';
}

function parseJsonResponse(text, context) {
  const cleaned = stripFences(text);
  try {
    return JSON.parse(cleaned);
  } catch {
    throw new Error(`Failed to parse JSON for ${context}: ${text.slice(0, 300)}`);
  }
}

// ---------------------------------------------------------------------------
// CSV loader — returns Set of roguelike app_ids from tags.csv
// ---------------------------------------------------------------------------
function loadRoguelikeAppIds(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines   = content.trim().split('\n');
  const header  = lines[0].split(',');

  const appIdIdx = header.indexOf('app_id');
  const tagCols  = header
    .map((col, i) => ({ col, i }))
    .filter(({ col }) => /^tag\d+$/.test(col))
    .map(({ i }) => i);

  if (appIdIdx === -1 || tagCols.length === 0) {
    throw new Error('tags.csv missing expected columns (app_id, tag1..tag20)');
  }

  const roguelikeIds = new Set();
  for (const line of lines.slice(1)) {
    if (!line.trim()) continue;
    const cols  = line.split(',');
    const appId = cols[appIdIdx]?.trim();
    if (!appId) continue;
    const isRoguelike = tagCols.some(i => ROGUELIKE_TAGS.has(cols[i]?.trim()));
    if (isRoguelike) roguelikeIds.add(appId);
  }

  return roguelikeIds;
}

// ---------------------------------------------------------------------------
// Phase 1 — Language detection + translation
// ---------------------------------------------------------------------------

function detectLanguage(text) {
  // franc needs at least ~10 chars to be reliable; short reviews default to "und"
  if (!text || text.trim().length < 10) return 'und';
  return franc(text);
}

async function translateBatch(reviews) {
  // reviews is an array of { index, text } — index used to re-align results
  const numbered = reviews
    .map((r, i) => `${i + 1}. ${r.text.replace(/\n+/g, ' ').slice(0, 800)}`)
    .join('\n');

  const prompt =
    `Translate each of the following reviews to English.\n` +
    `Return ONLY a JSON array of strings, one translated string per review,\n` +
    `in the same order as the input. Do not add any explanation or commentary.\n` +
    `Preserve the meaning and tone as closely as possible.\n\n` +
    `Reviews:\n${numbered}`;

  const text   = await claudeCall(prompt);
  const parsed = parseJsonResponse(text, 'translation batch');

  if (!Array.isArray(parsed) || parsed.length !== reviews.length) {
    throw new Error(
      `Translation returned ${Array.isArray(parsed) ? parsed.length : typeof parsed} items, expected ${reviews.length}`
    );
  }

  return parsed; // array of translated strings, same order as input
}

async function runPhase1(reviewEntries) {
  console.log('\n── Phase 1: Language detection & translation ──\n');

  // Detect language for every review
  for (const entry of reviewEntries) {
    entry.original_language = detectLanguage(entry.text);
    // "und" = undetermined (too short) → treat as English, no translation needed
    entry.was_translated = entry.original_language !== 'eng' && entry.original_language !== 'und';
  }

  const toTranslate = reviewEntries.filter(e => e.was_translated);
  const langBreakdown = {};
  for (const e of reviewEntries) {
    langBreakdown[e.original_language] = (langBreakdown[e.original_language] || 0) + 1;
  }

  console.log(`Language breakdown (top 10):`);
  Object.entries(langBreakdown)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([lang, n]) => console.log(`  ${lang.padEnd(6)} ${n}`));

  console.log(`\nReviews needing translation: ${toTranslate.length} / ${reviewEntries.length}`);

  if (toTranslate.length === 0) {
    console.log('No translation needed — all reviews in English or undetermined.\n');
    return;
  }

  const totalBatches  = Math.ceil(toTranslate.length / TRANSLATION_BATCH);
  let   translated    = 0;
  let   failed        = 0;

  console.log(`Translating in ${totalBatches} batches of up to ${TRANSLATION_BATCH}...\n`);

  for (let b = 0; b < totalBatches; b++) {
    if (b > 0) await sleep(BETWEEN_CALLS_MS);

    const slice = toTranslate.slice(b * TRANSLATION_BATCH, (b + 1) * TRANSLATION_BATCH);

    try {
      const results = await translateBatch(slice);
      for (let i = 0; i < slice.length; i++) {
        slice[i].text = results[i] ?? slice[i].text; // fallback to original on missing entry
      }
      translated += slice.length;
    } catch (err) {
      console.error(`  Translation batch ${b + 1}/${totalBatches} failed: ${err.message}`);
      failed++;
      // Keep original text — analysis will still run on it
    }

    if ((b + 1) % 20 === 0 || b + 1 === totalBatches) {
      console.log(`  Translation batch ${b + 1}/${totalBatches} — ${translated} reviews translated so far`);
    }
  }

  console.log(`\nPhase 1 complete. Translated: ${translated} | Batches failed: ${failed}\n`);
}

// ---------------------------------------------------------------------------
// Phase 2a — Sentiment + open-ended theme analysis
// ---------------------------------------------------------------------------

async function analyzeBatch(reviews) {
  // reviews: array of { text, original_language }
  const numbered = reviews
    .map((r, i) =>
      `${i + 1}. [lang: ${r.original_language}] ${r.text.replace(/\n+/g, ' ').slice(0, 600)}`
    )
    .join('\n\n');

  const prompt =
    `Below are Steam game reviews for roguelike games, translated to English.\n` +
    `For each review, return a JSON array with one object per review containing:\n` +
    `- "sentiment": "positive", "negative", or "mixed"\n` +
    `- "themes": array of short theme strings describing what the review is actually about — use YOUR OWN WORDS, do not use a predefined list. Examples: "satisfying difficulty curve", "unfair RNG", "great build variety", "too short", "excellent soundtrack", "pay to win", "boring after 10 hours", "amazing co-op experience". Be specific and descriptive. Use 1-4 themes per review.\n` +
    `- "language": the original_language code passed in\n\n` +
    `Return ONLY the JSON array, no preamble.\n\n` +
    `Reviews:\n${numbered}`;

  const text   = await claudeCall(prompt);
  const parsed = parseJsonResponse(text, 'analysis batch');

  if (!Array.isArray(parsed)) {
    throw new Error(`Analysis returned non-array: ${text.slice(0, 200)}`);
  }

  return parsed;
}

async function runPhase2a(reviewEntries) {
  console.log('── Phase 2a: Sentiment & theme analysis ──\n');

  const totalBatches = Math.ceil(reviewEntries.length / ANALYSIS_BATCH);
  console.log(`Analyzing ${reviewEntries.length} reviews in ${totalBatches} batches of up to ${ANALYSIS_BATCH}...\n`);

  const sentimentCounts = { positive: 0, negative: 0, mixed: 0 };
  const allThemes       = []; // { theme: string, language: string }
  let   totalAnalyzed   = 0;
  let   failed          = 0;

  for (let b = 0; b < totalBatches; b++) {
    if (b > 0) await sleep(BETWEEN_CALLS_MS);

    const slice = reviewEntries.slice(b * ANALYSIS_BATCH, (b + 1) * ANALYSIS_BATCH);

    try {
      const results = await analyzeBatch(slice);

      for (const result of results) {
        const s = result.sentiment;
        if (s === 'positive' || s === 'negative' || s === 'mixed') {
          sentimentCounts[s]++;
        }
        const lang = result.language ?? 'und';
        if (Array.isArray(result.themes)) {
          for (const theme of result.themes) {
            if (theme && typeof theme === 'string') {
              allThemes.push({ theme: theme.toLowerCase().trim(), language: lang });
            }
          }
        }
      }

      totalAnalyzed += results.length;
    } catch (err) {
      console.error(`  Analysis batch ${b + 1}/${totalBatches} failed: ${err.message}`);
      failed++;
    }

    if ((b + 1) % 10 === 0 || b + 1 === totalBatches) {
      console.log(`  Batch ${b + 1}/${totalBatches} — ${totalAnalyzed} analyzed, ${allThemes.length} themes collected`);
    }
  }

  console.log(`\nPhase 2a complete. Analyzed: ${totalAnalyzed} | Batches failed: ${failed}`);
  console.log(`Total raw theme strings: ${allThemes.length}\n`);

  return { sentimentCounts, allThemes, totalAnalyzed };
}

// ---------------------------------------------------------------------------
// Phase 2b — Theme aggregation via final Claude call
// ---------------------------------------------------------------------------

async function runPhase2b(allThemes, totalAnalyzed) {
  console.log('── Phase 2b: Theme aggregation ──\n');

  // Build frequency map: "theme|language" → count
  const themeFreq = {};
  for (const { theme, language } of allThemes) {
    const key = `${theme}|||${language}`;
    themeFreq[key] = (themeFreq[key] || 0) + 1;
  }

  // Format for the aggregation prompt
  const themeLines = Object.entries(themeFreq)
    .sort((a, b) => b[1] - a[1])
    .map(([key, count]) => {
      const [theme, lang] = key.split('|||');
      return `"${theme}" | lang:${lang} | count:${count}`;
    })
    .join('\n');

  console.log(`Sending ${Object.keys(themeFreq).length} distinct theme×language pairs to Claude for aggregation...`);

  const prompt =
    `Below is a list of theme strings extracted from roguelike game reviews across multiple languages.\n` +
    `Some themes are similar and should be grouped (e.g. "unfair RNG", "luck-based outcomes", "random feels cheap" are the same thing).\n\n` +
    `Your job:\n` +
    `1. Group similar themes into canonical categories\n` +
    `2. Count how many reviews mention each category\n` +
    `3. Identify if any category is disproportionately mentioned in a specific language — flag it if a theme appears more than 2x the average rate in one language group\n` +
    `4. Write a short paragraph (3-5 sentences) of insight for each major category\n\n` +
    `Total reviews analyzed: ${totalAnalyzed}\n\n` +
    `Input — each line is: "{theme}" | lang:{language_code} | count:{N}\n\n` +
    `${themeLines}\n\n` +
    `Return a JSON object:\n` +
    `{\n` +
    `  "categories": [\n` +
    `    {\n` +
    `      "name": "category name",\n` +
    `      "total_mentions": N,\n` +
    `      "percent_of_reviews": X,\n` +
    `      "language_skew": "zho players mention this 3x more than average" or null,\n` +
    `      "insight": "paragraph of analysis"\n` +
    `    }\n` +
    `  ],\n` +
    `  "global_insight": "2-3 paragraph summary of what roguelike players across all languages care about most, and any notable regional differences"\n` +
    `}`;

  const text   = await claudeCall(prompt);
  const parsed = parseJsonResponse(text, 'theme aggregation');

  if (!parsed.categories || !Array.isArray(parsed.categories)) {
    throw new Error(`Aggregation response missing categories array: ${text.slice(0, 300)}`);
  }

  console.log(`Aggregation complete. ${parsed.categories.length} canonical categories identified.\n`);
  return parsed;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const startTime = Date.now();
  console.log('Griffin sentiment analysis — two-phase\n');

  // ---- Setup ----
  console.log('Loading roguelike app IDs from tags.csv...');
  const roguelikeIds = loadRoguelikeAppIds(TAGS_CSV);
  console.log(`Found ${roguelikeIds.size} roguelike games.`);

  console.log('Loading reviews from reviews.json...');
  const allReviews   = JSON.parse(fs.readFileSync(REVIEWS_JSON, 'utf8'));

  const reviewEntries = [];
  for (const appId of roguelikeIds) {
    const texts = allReviews[appId];
    if (!Array.isArray(texts) || texts.length === 0) continue;
    for (const text of texts) {
      if (text && text.trim()) {
        reviewEntries.push({ appId, text: text.trim(), original_language: 'und', was_translated: false });
      }
    }
  }

  console.log(`Total reviews: ${reviewEntries.length}\n`);
  if (reviewEntries.length === 0) { console.log('Nothing to analyze. Exiting.'); process.exit(0); }

  // ---- Phase 1: Translation ----
  await runPhase1(reviewEntries);
  const translatedCount = reviewEntries.filter(e => e.was_translated).length;

  // ---- Phase 2a: Analysis ----
  const { sentimentCounts, allThemes, totalAnalyzed } = await runPhase2a(reviewEntries);

  // ---- Phase 2b: Aggregation ----
  let aggregation;
  try {
    aggregation = await runPhase2b(allThemes, totalAnalyzed);
  } catch (err) {
    console.error(`Phase 2b aggregation failed: ${err.message}`);
    aggregation = { categories: [], global_insight: 'Aggregation failed — see logs.' };
  }

  // ---- Write output ----
  const total = totalAnalyzed;
  const pct   = n => total > 0 ? Math.round((n / total) * 1000) / 10 : 0;

  const output = {
    generated_at:      new Date().toISOString(),
    total_reviews:     reviewEntries.length,
    translated_reviews: translatedCount,
    sentiment: {
      positive: { count: sentimentCounts.positive, percent: pct(sentimentCounts.positive) },
      negative: { count: sentimentCounts.negative, percent: pct(sentimentCounts.negative) },
      mixed:    { count: sentimentCounts.mixed,    percent: pct(sentimentCounts.mixed)    },
    },
    categories:    aggregation.categories,
    global_insight: aggregation.global_insight,
  };

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(output, null, 2), 'utf8');

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const mins    = Math.floor(elapsed / 60);
  const secs    = elapsed % 60;

  console.log(`Output written to roguelike_sentiment.json`);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Complete in ${mins}m ${secs}s`);
  console.log(`Reviews: ${reviewEntries.length} total | ${translatedCount} translated | ${totalAnalyzed} analyzed`);
  console.log(`Sentiment: ${sentimentCounts.positive} positive (${pct(sentimentCounts.positive)}%) | ${sentimentCounts.negative} negative (${pct(sentimentCounts.negative)}%) | ${sentimentCounts.mixed} mixed (${pct(sentimentCounts.mixed)}%)`);
  console.log(`Categories: ${aggregation.categories.length}`);
  if (aggregation.categories.length > 0) {
    console.log('\nTop categories by mentions:');
    aggregation.categories
      .slice()
      .sort((a, b) => b.total_mentions - a.total_mentions)
      .slice(0, 10)
      .forEach(c => console.log(`  ${c.name.padEnd(35)} ${String(c.total_mentions).padStart(5)} mentions (${c.percent_of_reviews}%)`));
  }
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
