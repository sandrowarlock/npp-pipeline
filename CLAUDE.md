# NPP Pipeline — Claude Code Briefing

## What Is This Project?
NPP (New Player Pipeline) is a B2B gaming market intelligence
product. It tracks upcoming Steam games to identify breakout
titles for publisher, developer, and investor clients.

The pipeline collects daily data signals across multiple sources,
stores them in Supabase, and feeds into AI-generated
intelligence reports.

## Tech Stack
- **Orchestration:** GitHub Actions (scheduled + manual workflows)
- **Database:** Supabase (PostgreSQL)
  - Project URL: https://kldikruuurzrujmvvumq.supabase.co
  - Never hardcode API keys — use GitHub Secrets for Actions,
    .env for local development
- **Language:** JavaScript (Node.js) for all workflow scripts
- **Package management:** npm

## Data Sources
### Steam
- Store API (no key required):
  https://store.steampowered.com/api/appdetails?appids={appid}
- Reviews API (no key required):
  https://store.steampowered.com/appreviews/{appid}
- Some endpoints require web scraping — use cheerio for parsing

### Gamalytic API
- Primary wishlist data source
- Base URL: https://gamalytic.com/api
- API key required — store as GAMALYTIC_API_KEY in secrets
- Rate limit: 30 requests/minute
- Key endpoint: /steam-games/list with filters:
  - wishlist_min, wishlist_max, release_status
  - Returns max 1,000 games per call
  - Paginate using wishlist range bands to get full 0-5,000 range

### YouTube Data API v3
- API key required — store as YOUTUBE_API_KEY in secrets
- Used for: searching videos by game name, fetching view counts,
  likes, comments, publish dates, engagement rates

### YouTube Transcript API
- API key required — store as TRANSCRIPT_API_KEY in secrets
- Used for: pulling transcripts from relevant YouTube videos

### Discord
- Invite API (no key required):
  https://discord.com/api/v9/invites/{invite_code}?with_counts=true
- Returns: approximate_member_count, approximate_presence_count
- Discord data is also pulled from a separate scraper project
  hosted on a different Supabase instance
- Scraper collects: steam_id, game name, discord invite link,
  member count, active user count — updated daily
- If active user count drops below 50, stop tracking that server

### Anthropic Claude API
- Used for: filtering YouTube search results for relevance,
  generating intelligence reports
- API key required — store as ANTHROPIC_API_KEY in secrets
- Always use claude-sonnet-4-5 model
- Reports must be generated via API with no conversation history
  to avoid context bias

## GitHub Secrets
All sensitive credentials are stored as GitHub Secrets
and accessed via process.env in workflow scripts.
Never hardcode these values.

- YOUTUBE_API_KEY — YouTube Data API v3
- TRANSCRIPT_API_KEY — YouTube transcript API
- ANTHROPIC_API_KEY — Claude API for filtering and reports
- SUPABASE_URL — NPP Supabase project URL
- SUPABASE_SERVICE_KEY — NPP Supabase service role key (write access)
- GAMALYTIC_API_KEY — Gamalytic API for wishlist data
- APIFY_API_KEY — Apify for Steam discussions scraping
- SCRAPER_SUPABASE_URL — Discord scraper Supabase project URL
- SCRAPER_SUPABASE_KEY — Discord scraper Supabase anon key (read only)

## Workflows (GitHub Actions)

### 1. add-games-to-track
- Trigger: manual (workflow_dispatch) with input: steam_app_id
- Pulls static data from Steam Store API:
  tags, description, developers, publishers, release date
- Writes to: games_static table in Supabase
- Also initializes a row in games_tracking table

### 2. daily-wishlist-snapshot
- Trigger: scheduled daily
- Source: Gamalytic API
- Pulls wishlist estimates for all tracked games
- Writes to: games_daily_snapshots table

### 3. daily-fastest-risers
- Trigger: scheduled daily (runs after daily-wishlist-snapshot)
- Source: Gamalytic API
- Pulls all unreleased games with 0-5,000 wishlists
- Uses wishlist range band pagination:
  Band 1: 0-500
  Band 2: 500-1,000
  Band 3: 1,000-2,000
  Band 4: 2,000-3,500
  Band 5: 3,500-5,000
- Calculates day-over-day percentage growth per game
- Flags games above growth threshold for review
- Writes to: fastest_risers table

### 4. daily-discord-snapshot
- Trigger: scheduled daily
- Source: scraper Supabase project (read via REST API)
- Pulls latest Discord stats for tracked games only
- Writes to: discord_snapshots table in NPP Supabase
- Secondary check: flag games where active users < 50

### 5. youtube-enrichment
- Trigger: manual or scheduled weekly
- For each tracked game: search YouTube, score relevance
  via Claude API, store top videos
- Writes to: youtube_videos table

### 6. youtube-re-search
- Trigger: manual
- Re-runs YouTube enrichment for specific games
- Same logic as youtube-enrichment but single-game scope

### 7. steam-discussions
- Trigger: scheduled daily
- Pulls Steam discussion threads for tracked games
- Writes to: steam_discussions table

### 8. steam-discussion-replies
- Trigger: scheduled daily (runs after steam-discussions)
- Pulls replies to tracked discussion threads
- Writes to: steam_discussion_replies table

### 9. steam-discussions-re-search
- Trigger: manual
- Re-runs discussion pull for specific games

### 10. daily-top-10
- Trigger: scheduled daily (runs after daily-wishlist-snapshot)
- Ranks tracked games by wishlist count
- Writes top 10 to: daily_top10 table

## Database Tables (Supabase)
Full schema lives in /database/schema.sql. Summary of all tables:

### games_static
Static metadata for each tracked game, sourced from the Steam Store API.
- app_id (integer, PK)
- name (text, not null)
- first_seen_date (date)
- release_date (text)
- release_date_parsed (date)
- released (boolean, default false)
- last_static_update (date)
- short_description (text)
- long_description (text)
- tag1–tag20 (text, nullable)
- developer1–developer3 (text, nullable)
- publisher1–publisher3 (text, nullable)
- languages_count (integer)
- full_audio_languages (text)
- controller_support (text)
- discord_url (text)
- discord_invite_code (text)
- x_url (text)
- reddit_url (text)
- website_url (text)
- tracking_source (text)
- study_tags (text)
- data_sources (text)

### games_static_history
Audit log of field-level changes to games_static. Populated automatically
by a Postgres trigger — do not write to directly.
- id (bigint, PK, auto-increment)
- app_id (integer)
- changed_at (timestamptz, default now())
- changed_fields (jsonb) — format: {"field": {"old": ..., "new": ...}}

### games_daily_snapshots
One row per (date, app_id). Daily wishlist count and Discord stats per
tracked game. Unique constraint on (date, app_id).
- id (bigint, PK, auto-increment)
- date (date)
- app_id (integer, FK → games_static)
- wishlist_count (integer)
- discord_member_count (integer)
- discord_online_count (integer)
- data_source (text, default 'live')

### wishlist_bracket_snapshots
Daily snapshot of all unreleased games in the 0–5,000 wishlist range,
used to calculate day-over-day growth. Unique constraint on (date, app_id).
- id (bigint, PK, auto-increment)
- date (date)
- app_id (integer)
- game_name (text)
- wishlist_count (integer)
- previous_wishlist_count (integer)
- daily_growth_absolute (integer)
- daily_growth_percent (float)

### daily_top10_risers
Top 10 fastest-growing tracked games per day, ranked by wishlist growth.
Unique constraint on (date, rank).
- id (bigint, PK, auto-increment)
- date (date)
- rank (integer)
- app_id (integer)
- game_name (text)
- wishlist_count (integer)
- daily_growth_absolute (integer)
- daily_growth_percent (float)

### youtube_videos
YouTube videos associated with tracked games, relevance-scored via Claude API.
- video_id (text, PK)
- app_id (integer, FK → games_static)
- title (text)
- channel_name (text)
- published_at (timestamptz)
- views (integer)
- likes (integer)
- comment_count (integer)
- language (text)
- has_transcript (boolean)
- last_updated (date)

### youtube_comments
Comments scraped from tracked YouTube videos.
- comment_id (text, PK)
- video_id (text, FK → youtube_videos)
- app_id (integer, FK → games_static)
- comment_text (text)
- comment_likes (integer)
- published_at (timestamptz)

### youtube_transcripts
Full transcripts for tracked YouTube videos. One row per video.
- video_id (text, PK, FK → youtube_videos)
- app_id (integer, FK → games_static)
- language (text)
- transcript_text (text)
- has_transcript (boolean)

### steam_discussions
Steam discussion threads scraped for each tracked game.
- topic_id (text, PK)
- app_id (integer, FK → games_static)
- title (text)
- author_name (text)
- reply_count (integer)
- opening_post_body (text)
- last_posted_at (timestamptz)
- is_pinned (boolean)
- is_locked (boolean)
- scraped_date (date)

### steam_discussion_replies
Individual replies within tracked Steam discussion threads.
- comment_id (text, PK)
- topic_id (text, FK → steam_discussions)
- app_id (integer, FK → games_static)
- author (text)
- text (text)
- scraped_date (date)

## Key Business Rules
- Wishlist velocity uses percentage growth, not absolute numbers
- Video virality precedes wishlist spikes by 1-2 days (embed in reports)
- Discord active users < 50 = stop tracking that server
- All intelligence reports generated via Claude API, never in conversation
- Discord is the only non-retroactive signal — all others can be backfilled
- YouTube, Steam discussions, Steam news can be retrieved retroactively

## Coding Conventions
- All scripts in /scripts folder
- All GitHub Actions workflows in /.github/workflows folder
- Use async/await throughout, no callbacks
- Always wrap Supabase writes in try/catch with meaningful error logging
- Environment variables via process.env — never hardcoded
- Add comments explaining business logic, not just what the code does
- Keep each workflow script focused on one job

## Git Commits
Do not add "Co-authored-by" or any Claude/AI attribution to git commits. Write clean commit messages only.
