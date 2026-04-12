// griffin-sentiment.js
//
// Multi-phase sentiment analysis of Steam reviews for a given genre in the
// Griffin genre study, using the Claude API.
//
// Usage (from scripts/ directory):
//   node --env-file=../.env griffin-sentiment.js <genre>
//
// Supported genres: roguelike, deckbuilder, metroidvania
//
// Phases:
//   Phase 1  — Language detection (franc) + translation to English
//   Phase 2a — Theme extraction per review (open-ended, no predefined list)
//   Phase 2b — Theme-level sentiment scoring against global theme vocabulary
//   Phase 2c — Aggregation: group themes into categories, compute insights
//
// Checkpoint files (all in griffin/):
//   {genre}_reviews_translated.json  — Phase 1 output (skip if exists)
//   {genre}_themes_raw.json          — Phase 2a output (skip if exists)
//   {genre}_theme_sentiments.json    — Phase 2b output (skip if exists)
//   {genre}_sentiment.json           — Final output

'use strict';

const fs    = require('fs');
const path  = require('path');
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
// Genre configuration
// ---------------------------------------------------------------------------
const GENRE_CONFIG = {
  roguelike: {
    tags: new Set(['Roguelike', 'Roguelite', 'Action Roguelike']),
    // Backward compat: if new-format cache doesn't exist, try the old filename
    legacyTranslatedCache: 'roguelike_translated.json',
  },
  deckbuilder: {
    tags: new Set(['Deckbuilder', 'Deck Building', 'Card Game', 'Card Battler']),
  },
  metroidvania: {
    tags: new Set(['Metroidvania']),
  },
};

// Parse and validate the genre argument
const genre = process.argv[2]?.toLowerCase();
if (!genre || !GENRE_CONFIG[genre]) {
  console.error(`Error: genre argument required. Supported: ${Object.keys(GENRE_CONFIG).join(', ')}`);
  console.error(`Usage: node griffin-sentiment.js <genre>`);
  process.exit(1);
}

const { tags: GENRE_TAGS, legacyTranslatedCache } = GENRE_CONFIG[genre];

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
const GRIFFIN_DIR          = path.resolve(__dirname, '../griffin');
const TAGS_CSV             = path.join(GRIFFIN_DIR, 'tags.csv');
const REVIEWS_JSON         = path.join(GRIFFIN_DIR, 'reviews.json');
const TRANSLATED_CACHE     = path.join(GRIFFIN_DIR, `${genre}_reviews_translated.json`);
const LEGACY_TRANSLATED    = legacyTranslatedCache ? path.join(GRIFFIN_DIR, legacyTranslatedCache) : null;
const THEMES_RAW           = path.join(GRIFFIN_DIR, `${genre}_themes_raw.json`);
const THEME_SENTIMENTS     = path.join(GRIFFIN_DIR, `${genre}_theme_sentiments.json`);
const OUTPUT_JSON          = path.join(GRIFFIN_DIR, `${genre}_sentiment.json`);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const ANTHROPIC_API_URL = 'https://api.anthropic.com/v1/messages';
const MODEL             = 'claude-sonnet-4-20250514';
const MAX_TOKENS        = 4096;
const BETWEEN_CALLS_MS  = 500;
const TRANSLATION_BATCH = 20;
const ANALYSIS_BATCH    = 50;

// Phase 2b: max themes in global list sent with each scoring batch.
// Each theme ≈ 8 tokens; 250 themes ≈ 2k tokens — keeps prompts manageable.
const MAX_GLOBAL_THEMES = 250;
const MIN_THEME_FREQ    = 2; // drop themes that appear in fewer than N reviews

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

async function claudeCall(userContent, maxTokens = MAX_TOKENS) {
  const response = await fetch(ANTHROPIC_API_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      MODEL,
      max_tokens: maxTokens,
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
// Load genre app_ids from tags.csv
// ---------------------------------------------------------------------------
function loadGenreAppIds() {
  const content = fs.readFileSync(TAGS_CSV, 'utf8');
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

  const ids = new Set();
  for (const line of lines.slice(1)) {
    if (!line.trim()) continue;
    const cols  = line.split(',');
    const appId = cols[appIdIdx]?.trim();
    if (!appId) continue;
    if (tagCols.some(i => GENRE_TAGS.has(cols[i]?.trim()))) ids.add(appId);
  }

  return ids;
}

// ---------------------------------------------------------------------------
// Phase 1 — Language detection + translation
// ---------------------------------------------------------------------------
function detectLanguage(text) {
  if (!text || text.trim().length < 10) return 'und';
  return franc(text);
}

async function translateBatch(reviews) {
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

  return parsed;
}

async function runPhase1(reviewEntries) {
  console.log('\n── Phase 1: Language detection & translation ──\n');

  for (const entry of reviewEntries) {
    entry.original_language = detectLanguage(entry.text);
    entry.was_translated    = entry.original_language !== 'eng' && entry.original_language !== 'und';
  }

  const toTranslate    = reviewEntries.filter(e => e.was_translated);
  const langBreakdown  = {};
  for (const e of reviewEntries) {
    langBreakdown[e.original_language] = (langBreakdown[e.original_language] || 0) + 1;
  }

  console.log('Language breakdown (top 10):');
  Object.entries(langBreakdown)
    .sort((a, b) => b[1] - a[1]).slice(0, 10)
    .forEach(([lang, n]) => console.log(`  ${lang.padEnd(6)} ${n}`));

  console.log(`\nReviews needing translation: ${toTranslate.length} / ${reviewEntries.length}`);

  if (toTranslate.length === 0) {
    console.log('No translation needed.\n');
  } else {
    const totalBatches = Math.ceil(toTranslate.length / TRANSLATION_BATCH);
    let translated = 0, failed = 0;
    console.log(`Translating in ${totalBatches} batches of up to ${TRANSLATION_BATCH}...\n`);

    for (let b = 0; b < totalBatches; b++) {
      if (b > 0) await sleep(BETWEEN_CALLS_MS);
      const slice = toTranslate.slice(b * TRANSLATION_BATCH, (b + 1) * TRANSLATION_BATCH);
      try {
        const results = await translateBatch(slice);
        for (let i = 0; i < slice.length; i++) slice[i].text = results[i] ?? slice[i].text;
        translated += slice.length;
      } catch (err) {
        console.error(`  Translation batch ${b + 1}/${totalBatches} failed: ${err.message}`);
        failed++;
      }
      if ((b + 1) % 20 === 0 || b + 1 === totalBatches) {
        console.log(`  Batch ${b + 1}/${totalBatches} — ${translated} translated so far`);
      }
    }
    console.log(`\nPhase 1 complete. Translated: ${translated} | Batches failed: ${failed}\n`);
  }

  fs.writeFileSync(TRANSLATED_CACHE, JSON.stringify(reviewEntries, null, 2), 'utf8');
  console.log(`Checkpoint saved → ${genre}_reviews_translated.json\n`);
}

// ---------------------------------------------------------------------------
// Phase 2a — Theme extraction (open-ended, no sentiment in this pass)
// ---------------------------------------------------------------------------
async function extractThemesBatch(reviews, batchOffset) {
  const numbered = reviews
    .map((r, i) =>
      `${i + 1}. [lang: ${r.original_language}] ${r.text.replace(/\n+/g, ' ').slice(0, 600)}`
    )
    .join('\n\n');

  const prompt =
    `Below are Steam game reviews. For each review, extract 1-8 themes\n` +
    `that describe what the reviewer is talking about. Use your own words,\n` +
    `be specific. Do not use a predefined list.\n\n` +
    `Return ONLY a JSON array, one object per review:\n` +
    `[\n` +
    `  { "themes": ["great build variety", "unfair RNG"], "language": "eng" },\n` +
    `  ...\n` +
    `]\n\n` +
    `Reviews:\n${numbered}`;

  const text   = await claudeCall(prompt);
  const parsed = parseJsonResponse(text, 'theme extraction batch');

  if (!Array.isArray(parsed)) throw new Error(`Theme extraction returned non-array: ${text.slice(0, 200)}`);

  // Attach global review_index to each result
  return parsed.map((result, i) => ({
    review_index: batchOffset + i,
    themes:       (result.themes || []).map(t => t.toLowerCase().trim()).filter(Boolean),
    language:     result.language ?? reviews[i]?.original_language ?? 'und',
  }));
}

async function runPhase2a(reviewEntries) {
  console.log('── Phase 2a: Theme extraction ──\n');

  const totalBatches = Math.ceil(reviewEntries.length / ANALYSIS_BATCH);
  console.log(`Extracting themes from ${reviewEntries.length} reviews in ${totalBatches} batches...\n`);

  const reviewThemeMap = []; // { review_index, themes, language }
  let   totalProcessed = 0;
  let   failed         = 0;

  for (let b = 0; b < totalBatches; b++) {
    if (b > 0) await sleep(BETWEEN_CALLS_MS);

    const offset = b * ANALYSIS_BATCH;
    const slice  = reviewEntries.slice(offset, offset + ANALYSIS_BATCH);

    try {
      const results = await extractThemesBatch(slice, offset);
      reviewThemeMap.push(...results);
      totalProcessed += results.length;
    } catch (err) {
      console.error(`  Batch ${b + 1}/${totalBatches} failed: ${err.message}`);
      failed++;
    }

    if ((b + 1) % 10 === 0 || b + 1 === totalBatches) {
      const themeCount = reviewThemeMap.reduce((s, r) => s + r.themes.length, 0);
      console.log(`  Batch ${b + 1}/${totalBatches} — ${totalProcessed} processed, ${themeCount} raw themes`);
    }
  }

  // Build deduplicated global theme list with frequency counts
  const themeFreq = {};
  for (const { themes } of reviewThemeMap) {
    for (const t of themes) {
      themeFreq[t] = (themeFreq[t] || 0) + 1;
    }
  }

  const allThemes = Object.keys(themeFreq); // full deduplicated list
  const totalRawThemes = reviewThemeMap.reduce((s, r) => s + r.themes.length, 0);

  console.log(`\nPhase 2a complete. Processed: ${totalProcessed} | Batches failed: ${failed}`);
  console.log(`Unique themes: ${allThemes.length} | Raw theme mentions: ${totalRawThemes}\n`);

  const checkpoint = { themes: allThemes, themeFreq, reviewThemeMap, totalProcessed };
  fs.writeFileSync(THEMES_RAW, JSON.stringify(checkpoint, null, 2), 'utf8');
  console.log(`Checkpoint saved → ${genre}_themes_raw.json\n`);

  return checkpoint;
}

// ---------------------------------------------------------------------------
// Phase 2b — Theme-level sentiment scoring
// ---------------------------------------------------------------------------
async function scoreThemeSentimentBatch(reviews, globalThemesList) {
  const themeListText = globalThemesList
    .map((t, i) => `${i + 1}. ${t}`)
    .join('\n');

  const numbered = reviews
    .map((r, i) =>
      `${i + 1}. [lang: ${r.original_language}] ${r.text.replace(/\n+/g, ' ').slice(0, 600)}`
    )
    .join('\n\n');

  const prompt =
    `Below are Steam game reviews and a global list of themes identified\n` +
    `across all reviews.\n\n` +
    `For each review, identify which themes from the global list are mentioned\n` +
    `(directly or implicitly), and whether the reviewer feels positive, negative,\n` +
    `or neutral about each one.\n\n` +
    `Global themes:\n${themeListText}\n\n` +
    `For each review return a JSON array of theme sentiments:\n` +
    `[\n` +
    `  {\n` +
    `    "review_index": N,\n` +
    `    "theme_sentiments": [\n` +
    `      { "theme": "great build variety", "sentiment": "positive" },\n` +
    `      { "theme": "unfair RNG", "sentiment": "negative" }\n` +
    `    ]\n` +
    `  },\n` +
    `  ...\n` +
    `]\n\n` +
    `Only include themes that are actually present in the review.\n` +
    `Return ONLY the JSON array.\n\n` +
    `Reviews:\n${numbered}`;

  const text   = await claudeCall(prompt);
  const parsed = parseJsonResponse(text, 'theme sentiment batch');

  if (!Array.isArray(parsed)) throw new Error(`Sentiment scoring returned non-array: ${text.slice(0, 200)}`);
  return parsed;
}

async function runPhase2b(reviewEntries, phase2aResult) {
  console.log('── Phase 2b: Theme-level sentiment scoring ──\n');

  const { themeFreq } = phase2aResult;

  // Build the global theme list: filter by min frequency, sort by freq desc, cap
  const globalThemesList = Object.entries(themeFreq)
    .filter(([, freq]) => freq >= MIN_THEME_FREQ)
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_GLOBAL_THEMES)
    .map(([theme]) => theme);

  const globalThemeSet = new Set(globalThemesList);

  console.log(`Global theme vocabulary: ${globalThemesList.length} themes (freq ≥ ${MIN_THEME_FREQ}, capped at ${MAX_GLOBAL_THEMES})`);
  console.log(`Scoring ${reviewEntries.length} reviews in batches of ${ANALYSIS_BATCH}...\n`);

  const totalBatches = Math.ceil(reviewEntries.length / ANALYSIS_BATCH);

  // Accumulators
  // Per theme: { positive, negative, neutral, total, langs: { langCode: count } }
  const themeSentimentCounts = {};
  for (const t of globalThemesList) {
    themeSentimentCounts[t] = { positive: 0, negative: 0, neutral: 0, total: 0, langs: {} };
  }

  // Per-review majority sentiment (for top-level positive/negative/mixed counts)
  const reviewSentiments = []; // 'positive' | 'negative' | 'mixed'

  let totalScored = 0;
  let failed      = 0;

  for (let b = 0; b < totalBatches; b++) {
    if (b > 0) await sleep(BETWEEN_CALLS_MS);

    const offset = b * ANALYSIS_BATCH;
    const slice  = reviewEntries.slice(offset, offset + ANALYSIS_BATCH);

    try {
      const results = await scoreThemeSentimentBatch(slice, globalThemesList);

      for (let i = 0; i < slice.length; i++) {
        const reviewResult = results.find(r => r.review_index === i) ?? results[i];
        const lang         = slice[i].original_language ?? 'und';
        const sentiments   = reviewResult?.theme_sentiments ?? [];

        let pos = 0, neg = 0, neu = 0;
        for (const { theme, sentiment } of sentiments) {
          const normTheme = theme?.toLowerCase().trim();
          if (!normTheme || !globalThemeSet.has(normTheme)) continue;
          const entry = themeSentimentCounts[normTheme];
          if (!entry) continue;
          const s = sentiment?.toLowerCase();
          if (s === 'positive') { entry.positive++; pos++; }
          else if (s === 'negative') { entry.negative++; neg++; }
          else { entry.neutral++; neu++; }
          entry.total++;
          entry.langs[lang] = (entry.langs[lang] || 0) + 1;
        }

        // Review-level overall sentiment: majority vote
        if (pos > neg && pos > neu)       reviewSentiments.push('positive');
        else if (neg > pos && neg > neu)  reviewSentiments.push('negative');
        else if (pos === 0 && neg === 0)  reviewSentiments.push('mixed'); // no themes matched
        else                              reviewSentiments.push('mixed');
      }

      totalScored += slice.length;
    } catch (err) {
      console.error(`  Batch ${b + 1}/${totalBatches} failed: ${err.message}`);
      failed++;
      // Fill missing reviews with 'mixed' to keep counts aligned
      for (let i = 0; i < slice.length; i++) reviewSentiments.push('mixed');
    }

    if ((b + 1) % 10 === 0 || b + 1 === totalBatches) {
      const scored = Object.values(themeSentimentCounts).reduce((s, t) => s + t.total, 0);
      console.log(`  Batch ${b + 1}/${totalBatches} — ${totalScored} reviews scored, ${scored} theme sentiments recorded`);
    }
  }

  console.log(`\nPhase 2b complete. Scored: ${totalScored} | Batches failed: ${failed}\n`);

  const checkpoint = { globalThemesList, themeSentimentCounts, reviewSentiments, totalScored };
  fs.writeFileSync(THEME_SENTIMENTS, JSON.stringify(checkpoint, null, 2), 'utf8');
  console.log(`Checkpoint saved → ${genre}_theme_sentiments.json\n`);

  return checkpoint;
}

// ---------------------------------------------------------------------------
// Phase 2c — Aggregation: group themes into canonical categories
// ---------------------------------------------------------------------------
async function runPhase2c(phase2bResult, totalReviews) {
  console.log('── Phase 2c: Category aggregation ──\n');

  const { themeSentimentCounts, totalScored } = phase2bResult;

  // Format theme data for Claude — include sentiment breakdown and lang info
  const themeLines = Object.entries(themeSentimentCounts)
    .filter(([, v]) => v.total >= MIN_THEME_FREQ)
    .sort((a, b) => b[1].total - a[1].total)
    .map(([theme, { positive, negative, neutral, total, langs }]) => {
      const posPct = total > 0 ? Math.round((positive / total) * 100) : 0;
      const negPct = total > 0 ? Math.round((negative / total) * 100) : 0;
      const neuPct = 100 - posPct - negPct;
      const langSummary = Object.entries(langs)
        .sort((a, b) => b[1] - a[1]).slice(0, 4)
        .map(([l, n]) => `${l}:${n}`).join(', ');
      return `"${theme}" | total:${total} | pos:${posPct}% neg:${negPct}% neu:${neuPct}% | langs:{${langSummary}}`;
    })
    .join('\n');

  console.log(`Sending themes to Claude for aggregation...`);

  const prompt =
    `Below is a list of theme strings extracted from Steam game reviews (${genre} genre) across multiple languages.\n` +
    `Each line shows: theme | total mentions | sentiment split (pos/neg/neu %) | top languages\n\n` +
    `Your job:\n` +
    `1. Group similar themes into canonical categories (e.g. "unfair RNG", "luck-based outcomes", "random feels cheap" → "RNG & Randomness")\n` +
    `2. For each category: sum total mentions, compute weighted positive/negative/neutral % across constituent themes\n` +
    `3. Flag language skew if a category is mentioned 1.5x+ above average rate in one language group\n` +
    `4. Write a short insight paragraph (3-5 sentences) per major category\n` +
    `5. Write a 2-3 paragraph global_insight summary covering what players across all languages care about most and any regional differences\n\n` +
    `Total reviews analyzed: ${totalScored}\n\n` +
    `${themeLines}\n\n` +
    `Return a JSON object:\n` +
    `{\n` +
    `  "categories": [\n` +
    `    {\n` +
    `      "name": "category name",\n` +
    `      "total_mentions": N,\n` +
    `      "percent_of_reviews": X.X,\n` +
    `      "positive_pct": N,\n` +
    `      "neutral_pct": N,\n` +
    `      "negative_pct": N,\n` +
    `      "language_skew": "cmn players mention this 2x more than average" or null,\n` +
    `      "insight": "..."\n` +
    `    }\n` +
    `  ],\n` +
    `  "global_insight": "..."\n` +
    `}`;

  const text   = await claudeCall(prompt, 4096);
  const parsed = parseJsonResponse(text, 'category aggregation');

  if (!parsed.categories || !Array.isArray(parsed.categories)) {
    throw new Error(`Aggregation missing categories array: ${text.slice(0, 300)}`);
  }

  console.log(`Aggregation complete. ${parsed.categories.length} canonical categories identified.\n`);
  return parsed;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const startTime = Date.now();
  console.log(`Griffin sentiment analysis — ${genre}\n`);

  // ---- Load genre app_ids ----
  console.log(`Loading ${genre} app IDs from tags.csv...`);
  const genreIds = loadGenreAppIds();
  console.log(`Found ${genreIds.size} ${genre} games.`);

  // ---- Phase 1: Translation ----
  let reviewEntries, translatedCount;

  // Check for translated cache (new-format name first, then legacy for roguelike)
  const translatedCachePath =
    fs.existsSync(TRANSLATED_CACHE)                       ? TRANSLATED_CACHE :
    LEGACY_TRANSLATED && fs.existsSync(LEGACY_TRANSLATED) ? LEGACY_TRANSLATED :
    null;

  if (translatedCachePath) {
    console.log(`\nCheckpoint found — loading translated reviews from ${path.basename(translatedCachePath)} (skipping Phase 1)...`);
    reviewEntries   = JSON.parse(fs.readFileSync(translatedCachePath, 'utf8'));
    translatedCount = reviewEntries.filter(e => e.was_translated).length;
    console.log(`Loaded ${reviewEntries.length} reviews (${translatedCount} translated)\n`);
  } else {
    console.log('Loading reviews from reviews.json...');
    const allReviews = JSON.parse(fs.readFileSync(REVIEWS_JSON, 'utf8'));

    reviewEntries = [];
    for (const appId of genreIds) {
      const texts = allReviews[appId];
      if (!Array.isArray(texts) || texts.length === 0) continue;
      for (const text of texts) {
        if (text && text.trim()) {
          reviewEntries.push({ appId, text: text.trim(), original_language: 'und', was_translated: false });
        }
      }
    }

    console.log(`Total reviews: ${reviewEntries.length}\n`);
    if (reviewEntries.length === 0) { console.log('No reviews found. Exiting.'); process.exit(0); }

    await runPhase1(reviewEntries);
    translatedCount = reviewEntries.filter(e => e.was_translated).length;
  }

  // ---- Phase 2a: Theme extraction ----
  let phase2aResult;

  if (fs.existsSync(THEMES_RAW)) {
    console.log(`Checkpoint found — loading themes from ${genre}_themes_raw.json (skipping Phase 2a)...`);
    phase2aResult = JSON.parse(fs.readFileSync(THEMES_RAW, 'utf8'));
    console.log(`Loaded: ${phase2aResult.reviewThemeMap.length} reviews | ${phase2aResult.themes.length} unique themes\n`);
  } else {
    phase2aResult = await runPhase2a(reviewEntries);
  }

  // ---- Phase 2b: Theme-level sentiment scoring ----
  let phase2bResult;

  if (fs.existsSync(THEME_SENTIMENTS)) {
    console.log(`Checkpoint found — loading theme sentiments from ${genre}_theme_sentiments.json (skipping Phase 2b)...`);
    phase2bResult = JSON.parse(fs.readFileSync(THEME_SENTIMENTS, 'utf8'));
    const scored = Object.values(phase2bResult.themeSentimentCounts).reduce((s, t) => s + t.total, 0);
    console.log(`Loaded: ${phase2bResult.totalScored} reviews | ${scored} theme sentiments\n`);
  } else {
    phase2bResult = await runPhase2b(reviewEntries, phase2aResult);
  }

  // ---- Phase 2c: Aggregation ----
  let aggregation;
  try {
    aggregation = await runPhase2c(phase2bResult, reviewEntries.length);
  } catch (err) {
    console.error(`Phase 2c aggregation failed: ${err.message}`);
    aggregation = { categories: [], global_insight: 'Aggregation failed — see logs.' };
  }

  // ---- Compute top-level sentiment from Phase 2b review sentiments ----
  const sentimentCounts = { positive: 0, negative: 0, mixed: 0 };
  for (const s of phase2bResult.reviewSentiments) {
    if (s === 'positive')      sentimentCounts.positive++;
    else if (s === 'negative') sentimentCounts.negative++;
    else                       sentimentCounts.mixed++;
  }
  const total = phase2bResult.totalScored;
  const pct   = n => total > 0 ? Math.round((n / total) * 1000) / 10 : 0;

  // ---- Write final output ----
  const output = {
    generated_at:       new Date().toISOString(),
    genre,
    total_reviews:      reviewEntries.length,
    translated_reviews: translatedCount,
    reviews_analyzed:   total,
    sentiment: {
      positive: { count: sentimentCounts.positive, percent: pct(sentimentCounts.positive) },
      negative: { count: sentimentCounts.negative, percent: pct(sentimentCounts.negative) },
      mixed:    { count: sentimentCounts.mixed,    percent: pct(sentimentCounts.mixed)    },
    },
    categories:     aggregation.categories,
    global_insight: aggregation.global_insight,
  };

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(output, null, 2), 'utf8');

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const mins    = Math.floor(elapsed / 60);
  const secs    = elapsed % 60;

  console.log(`Output written to ${genre}_sentiment.json`);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Complete in ${mins}m ${secs}s`);
  console.log(`Reviews: ${reviewEntries.length} total | ${translatedCount} translated | ${total} analyzed`);
  console.log(`Sentiment: ${sentimentCounts.positive} positive (${pct(sentimentCounts.positive)}%) | ${sentimentCounts.negative} negative (${pct(sentimentCounts.negative)}%) | ${sentimentCounts.mixed} mixed (${pct(sentimentCounts.mixed)}%)`);
  console.log(`Categories: ${aggregation.categories.length}`);

  if (aggregation.categories.length > 0) {
    console.log('\nTop categories by mentions:');
    aggregation.categories
      .slice()
      .sort((a, b) => b.total_mentions - a.total_mentions)
      .slice(0, 10)
      .forEach(c => {
        const bar = `pos:${c.positive_pct}% neg:${c.negative_pct}% neu:${c.neutral_pct}%`;
        console.log(`  ${c.name.padEnd(38)} ${String(c.total_mentions).padStart(4)} mentions | ${bar}`);
      });
  }
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
