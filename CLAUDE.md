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
Schema to be defined — see separate schema design document.
Key tables:
- games_static
- games_tracking
- games_daily_snapshots
- discord_snapshots
- youtube_videos
- steam_discussions
- steam_discussion_replies
- fastest_risers
- daily_top10

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
