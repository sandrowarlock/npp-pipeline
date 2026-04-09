// griffin-sentiment.js
//
// Analyzes Steam reviews for roguelike games in the Griffin genre study
// using the Claude API. Identifies roguelikes from griffin/tags.csv
// (any game with tag 'Roguelike', 'Roguelite', or 'Action Roguelike'),
// then sends their reviews from griffin/reviews.json to Claude in batches
// of 50 for sentiment classification and thematic tagging.
//
// Output:
//   griffin/roguelike_sentiment.json — aggregated counts and percentages
//
// Run locally:
//   node --env-file=../.env griffin-sentiment.js
//   (from scripts/ directory)

const fs   = require('fs');
const path = require('path');

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
const GRIFFIN_DIR   = path.resolve(__dirname, '../griffin');
const TAGS_CSV      = path.join(GRIFFIN_DIR, 'tags.csv');
const REVIEWS_JSON  = path.join(GRIFFIN_DIR, 'reviews.json');
const OUTPUT_JSON   = path.join(GRIFFIN_DIR, 'roguelike_sentiment.json');

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const ANTHROPIC_API_URL  = 'https://api.anthropic.com/v1/messages';
const MODEL              = 'claude-sonnet-4-20250514';
const MAX_TOKENS         = 2000;
const BATCH_SIZE         = 50;
const BETWEEN_BATCHES_MS = 500;

const ROGUELIKE_TAGS = new Set(['Roguelike', 'Roguelite', 'Action Roguelike']);

// The 15 themes to detect
const THEMES = [
  'replayability',
  'difficulty',
  'progression',
  'build_variety',
  'randomness',
  'story_lore',
  'art_style',
  'music_sound',
  'controls_feel',
  'performance_bugs',
  'price_value',
  'early_access',
  'multiplayer_coop',
  'community',
  'content_volume',
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// CSV parser — reads tags.csv and returns app_ids for roguelike games.
// Header: app_id, name, tag1..tag20
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

    const isRoguelike = tagCols.some(i => {
      const tag = cols[i]?.trim();
      return tag && ROGUELIKE_TAGS.has(tag);
    });

    if (isRoguelike) roguelikeIds.add(appId);
  }

  return roguelikeIds;
}

// ---------------------------------------------------------------------------
// Claude API call — analyze a batch of review texts.
// Returns an array of per-review result objects:
//   { sentiment: 'positive'|'negative'|'mixed', themes: string[] }
// ---------------------------------------------------------------------------
const SYSTEM_PROMPT = `You are a game review analyst specializing in roguelike games.
You will receive a numbered list of Steam reviews. For each review, classify:
1. sentiment: "positive", "negative", or "mixed"
2. themes: an array of themes present in the review, chosen from this fixed list:
   replayability, difficulty, progression, build_variety, randomness,
   story_lore, art_style, music_sound, controls_feel, performance_bugs,
   price_value, early_access, multiplayer_coop, community, content_volume

Respond ONLY with a JSON array — one object per review, in the same order as the input.
Each object must have exactly two keys: "sentiment" (string) and "themes" (array of strings).
Do not include any explanation or markdown formatting.

Example output format:
[
  {"sentiment": "positive", "themes": ["replayability", "difficulty", "build_variety"]},
  {"sentiment": "negative", "themes": ["performance_bugs", "price_value"]},
  {"sentiment": "mixed",    "themes": ["difficulty", "progression"]}
]`;

async function analyzeBatch(reviews) {
  const numbered = reviews
    .map((text, i) => `[${i + 1}] ${text.replace(/\n+/g, ' ').slice(0, 500)}`)
    .join('\n\n');

  const response = await fetch(ANTHROPIC_API_URL, {
    method: 'POST',
    headers: {
      'x-api-key':         ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:    MODEL,
      max_tokens: MAX_TOKENS,
      system:   SYSTEM_PROMPT,
      messages: [{ role: 'user', content: `Analyze these ${reviews.length} roguelike game reviews:\n\n${numbered}` }],
    }),
  });

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    throw new Error(`Claude API HTTP ${response.status}: ${errText.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data.content?.[0]?.text ?? '';

  // Strip markdown code fences if present
  const cleaned = text.trim()
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();

  let parsed;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    throw new Error(`Failed to parse Claude response as JSON: ${text.slice(0, 300)}`);
  }

  if (!Array.isArray(parsed)) {
    throw new Error(`Claude returned non-array response: ${text.slice(0, 300)}`);
  }

  return parsed;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('Griffin sentiment analysis\n');

  // ---- Load roguelike app IDs from tags.csv ----
  console.log('Loading roguelike app IDs from tags.csv...');
  const roguelikeIds = loadRoguelikeAppIds(TAGS_CSV);
  console.log(`Found ${roguelikeIds.size} roguelike games.\n`);

  // ---- Load reviews from reviews.json ----
  console.log('Loading reviews from reviews.json...');
  const allReviews = JSON.parse(fs.readFileSync(REVIEWS_JSON, 'utf8'));

  // Collect all reviews for roguelike games, keeping track of app_id
  const reviewEntries = []; // { appId, text }
  for (const appId of roguelikeIds) {
    const texts = allReviews[appId];
    if (!Array.isArray(texts) || texts.length === 0) {
      console.warn(`  No reviews found for app_id ${appId}`);
      continue;
    }
    for (const text of texts) {
      if (text && text.trim()) {
        reviewEntries.push({ appId, text: text.trim() });
      }
    }
  }

  console.log(`Total reviews to analyze: ${reviewEntries.length}\n`);

  if (reviewEntries.length === 0) {
    console.log('No reviews to analyze. Exiting.');
    process.exit(0);
  }

  // ---- Batch processing ----
  const totalBatches = Math.ceil(reviewEntries.length / BATCH_SIZE);
  console.log(`Sending ${totalBatches} batches of up to ${BATCH_SIZE} reviews each...\n`);

  // Accumulators
  const sentimentCounts = { positive: 0, negative: 0, mixed: 0 };
  const themeCounts     = {};
  for (const theme of THEMES) themeCounts[theme] = 0;

  let totalAnalyzed = 0;
  let batchesFailed = 0;

  for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
    if (batchNum > 0) await sleep(BETWEEN_BATCHES_MS);

    const start  = batchNum * BATCH_SIZE;
    const slice  = reviewEntries.slice(start, start + BATCH_SIZE);
    const texts  = slice.map(e => e.text);

    try {
      const results = await analyzeBatch(texts);

      // Aggregate results
      for (const result of results) {
        const sentiment = result.sentiment;
        if (sentiment === 'positive' || sentiment === 'negative' || sentiment === 'mixed') {
          sentimentCounts[sentiment]++;
        }
        if (Array.isArray(result.themes)) {
          for (const theme of result.themes) {
            if (themeCounts.hasOwnProperty(theme)) {
              themeCounts[theme]++;
            }
          }
        }
      }

      totalAnalyzed += results.length;
    } catch (err) {
      console.error(`  Batch ${batchNum + 1}/${totalBatches} failed: ${err.message}`);
      batchesFailed++;
      // Continue — don't abort the whole run for one bad batch
    }

    // Progress log every 10 batches
    if ((batchNum + 1) % 10 === 0 || batchNum + 1 === totalBatches) {
      console.log(`  Batch ${batchNum + 1}/${totalBatches} complete — ${totalAnalyzed} reviews analyzed so far`);
    }
  }

  // ---- Build output ----
  const total = totalAnalyzed;

  const pct = (n) => total > 0 ? Math.round((n / total) * 1000) / 10 : 0;

  const sentimentSummary = {
    positive: { count: sentimentCounts.positive, percent: pct(sentimentCounts.positive) },
    negative: { count: sentimentCounts.negative, percent: pct(sentimentCounts.negative) },
    mixed:    { count: sentimentCounts.mixed,    percent: pct(sentimentCounts.mixed)    },
  };

  const themeSummary = {};
  for (const theme of THEMES) {
    themeSummary[theme] = {
      count:   themeCounts[theme],
      percent: pct(themeCounts[theme]),
    };
  }

  const output = {
    generated_at:      new Date().toISOString(),
    roguelike_games:   roguelikeIds.size,
    total_reviews:     reviewEntries.length,
    reviews_analyzed:  totalAnalyzed,
    batches_failed:    batchesFailed,
    sentiment:         sentimentSummary,
    themes:            themeSummary,
  };

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(output, null, 2), 'utf8');
  console.log(`\nOutput written to roguelike_sentiment.json`);

  // ---- Summary ----
  console.log(`
Collection complete.
Games: ${roguelikeIds.size} roguelikes | Reviews: ${reviewEntries.length} total | Analyzed: ${totalAnalyzed} | Batches failed: ${batchesFailed}

Sentiment breakdown:
  Positive: ${sentimentSummary.positive.count} (${sentimentSummary.positive.percent}%)
  Negative: ${sentimentSummary.negative.count} (${sentimentSummary.negative.percent}%)
  Mixed:    ${sentimentSummary.mixed.count} (${sentimentSummary.mixed.percent}%)

Top themes:`);

  // Sort themes by count descending for the summary
  const sortedThemes = THEMES.slice().sort((a, b) => themeCounts[b] - themeCounts[a]);
  for (const theme of sortedThemes) {
    const { count, percent } = themeSummary[theme];
    console.log(`  ${theme.padEnd(20)} ${String(count).padStart(4)} reviews (${percent}%)`);
  }
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
