-- =============================================================================
-- NPP Pipeline — Database Schema
-- =============================================================================
-- This file defines all Supabase/PostgreSQL tables for the New Player Pipeline.
-- NPP is a B2B gaming market intelligence product that tracks upcoming Steam
-- games across multiple data sources (Gamalytic, YouTube, Discord, Steam).
--
-- Tables:
--   1. games_static                — static game metadata from Steam
--   2. games_static_history        — audit log of changes to games_static
--   3. games_daily_snapshots       — daily wishlist + Discord stats per game
--   4. wishlist_bracket_snapshots  — daily snapshot of all 0–5,000 wishlist games
--   5. daily_top10_risers          — top 10 fastest-growing games per day
--   6. youtube_videos              — YouTube videos associated with tracked games
--   7. youtube_comments            — comments on tracked YouTube videos
--   8. youtube_transcripts         — transcripts for tracked YouTube videos
--   9. steam_discussions           — Steam discussion threads for tracked games
--  10. steam_discussion_replies    — replies to tracked Steam discussion threads
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. games_static
-- Stores static metadata for each tracked game, sourced from the Steam Store
-- API. Updated manually or when the add-games-to-track workflow runs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS games_static (
    app_id                  INTEGER         PRIMARY KEY,
    name                    TEXT            NOT NULL,
    first_seen_date         DATE,
    release_date            TEXT,
    release_date_parsed     DATE,
    released                BOOLEAN         DEFAULT FALSE,
    last_static_update      DATE,
    short_description       TEXT,
    long_description        TEXT,

    -- Steam tags (up to 20)
    tag1                    TEXT,
    tag2                    TEXT,
    tag3                    TEXT,
    tag4                    TEXT,
    tag5                    TEXT,
    tag6                    TEXT,
    tag7                    TEXT,
    tag8                    TEXT,
    tag9                    TEXT,
    tag10                   TEXT,
    tag11                   TEXT,
    tag12                   TEXT,
    tag13                   TEXT,
    tag14                   TEXT,
    tag15                   TEXT,
    tag16                   TEXT,
    tag17                   TEXT,
    tag18                   TEXT,
    tag19                   TEXT,
    tag20                   TEXT,

    -- Developers (up to 3)
    developer1              TEXT,
    developer2              TEXT,
    developer3              TEXT,

    -- Publishers (up to 3)
    publisher1              TEXT,
    publisher2              TEXT,
    publisher3              TEXT,

    languages_count         INTEGER,
    full_audio_languages    TEXT,
    controller_support      TEXT,

    -- Social / community links
    discord_url             TEXT,
    discord_invite_code     TEXT,
    x_url                   TEXT,
    reddit_url              TEXT,
    website_url             TEXT,

    -- Internal metadata
    tracking_source         TEXT,
    study_tags              TEXT,
    data_sources            TEXT
);


-- -----------------------------------------------------------------------------
-- 2. games_static_history
-- Audit log that captures field-level changes to games_static rows.
-- Populated automatically by the trigger below — do not write to directly.
-- Changed fields are stored in changed_fields as {"field": {"old": ..., "new": ...}}.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS games_static_history (
    id              BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    app_id          INTEGER,
    changed_at      TIMESTAMPTZ     DEFAULT NOW(),
    changed_fields  JSONB
);

-- Trigger function: compares OLD and NEW rows, builds a jsonb object of only
-- the fields that changed (storing their old values), then inserts a history row.
CREATE OR REPLACE FUNCTION log_games_static_changes()
RETURNS TRIGGER AS $$
DECLARE
    changed JSONB := '{}';
    old_val TEXT;
    new_val TEXT;
    col     TEXT;
BEGIN
    FOR col IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'games_static'
          AND table_schema = TG_TABLE_SCHEMA
    LOOP
        EXECUTE format('SELECT ($1).%I::TEXT', col) INTO old_val USING OLD;
        EXECUTE format('SELECT ($1).%I::TEXT', col) INTO new_val USING NEW;

        IF old_val IS DISTINCT FROM new_val THEN
            changed := changed || jsonb_build_object(col, jsonb_build_object('old', old_val, 'new', new_val));
        END IF;
    END LOOP;

    -- Only insert a history row if something actually changed
    IF changed <> '{}' THEN
        INSERT INTO games_static_history (app_id, changed_fields)
        VALUES (OLD.app_id, changed);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to games_static
CREATE OR REPLACE TRIGGER trg_games_static_history
AFTER UPDATE ON games_static
FOR EACH ROW EXECUTE FUNCTION log_games_static_changes();


-- -----------------------------------------------------------------------------
-- 3. games_daily_snapshots
-- One row per (date, app_id). Captures wishlist count and Discord stats for
-- each tracked game each day. Written by the daily-wishlist-snapshot and
-- daily-discord-snapshot workflows.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS games_daily_snapshots (
    id                      BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date                    DATE,
    app_id                  INTEGER     REFERENCES games_static (app_id),
    wishlist_count          INTEGER,
    discord_member_count    INTEGER,
    discord_online_count    INTEGER,
    data_source             TEXT        DEFAULT 'live',

    CONSTRAINT uq_daily_snapshot UNIQUE (date, app_id)
);


-- -----------------------------------------------------------------------------
-- 4. wishlist_bracket_snapshots
-- Daily snapshot of all unreleased games in the 0–5,000 wishlist range,
-- sourced from Gamalytic via range-band pagination. Used to calculate
-- day-over-day growth and populate the fastest-risers feed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wishlist_bracket_snapshots (
    id             BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date           DATE,
    app_id         INTEGER,
    game_name      TEXT,
    wishlist_count INTEGER,

    CONSTRAINT uq_bracket_snapshot UNIQUE (date, app_id)
);


-- -----------------------------------------------------------------------------
-- 5. daily_top10_risers
-- The top 10 fastest-growing tracked games for each day, ranked by wishlist
-- growth. Written by the daily-top-10 workflow after snapshots are taken.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_top10_risers (
    id                      BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date                    DATE,
    rank                    INTEGER,
    app_id                  INTEGER,
    game_name               TEXT,
    wishlist_count          INTEGER,
    daily_growth_absolute   INTEGER,
    daily_growth_percent    FLOAT,

    CONSTRAINT uq_top10_rank UNIQUE (date, rank)
);


-- -----------------------------------------------------------------------------
-- 6. youtube_videos
-- YouTube videos surfaced for each tracked game via the YouTube Data API v3.
-- Relevance is scored via the Claude API before storing. Refreshed by the
-- youtube-enrichment and youtube-re-search workflows.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id        TEXT            PRIMARY KEY,
    app_id          INTEGER         REFERENCES games_static (app_id),
    title           TEXT,
    channel_name    TEXT,
    published_at    TIMESTAMPTZ,
    views           INTEGER,
    likes           INTEGER,
    comment_count   INTEGER,
    language        TEXT,
    has_transcript  BOOLEAN,
    last_updated    DATE
);


-- -----------------------------------------------------------------------------
-- 7. youtube_comments
-- Comments scraped from tracked YouTube videos, linked to both the video
-- and the game for easy cross-referencing in reports.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS youtube_comments (
    comment_id      TEXT            PRIMARY KEY,
    video_id        TEXT            REFERENCES youtube_videos (video_id),
    app_id          INTEGER         REFERENCES games_static (app_id),
    comment_text    TEXT,
    comment_likes   INTEGER,
    published_at    TIMESTAMPTZ
);


-- -----------------------------------------------------------------------------
-- 8. youtube_transcripts
-- Full transcripts for tracked YouTube videos, pulled via the Transcript API.
-- One row per video; video_id is both PK and FK to youtube_videos.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS youtube_transcripts (
    video_id        TEXT            PRIMARY KEY REFERENCES youtube_videos (video_id),
    app_id          INTEGER         REFERENCES games_static (app_id),
    language        TEXT,
    transcript_text TEXT,
    has_transcript  BOOLEAN
);


-- -----------------------------------------------------------------------------
-- 9. steam_discussions
-- Discussion threads scraped from Steam for each tracked game.
-- Written by the steam-discussions and steam-discussions-re-search workflows.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS steam_discussions (
    topic_id            TEXT            PRIMARY KEY,
    app_id              INTEGER         REFERENCES games_static (app_id),
    title               TEXT,
    author_name         TEXT,
    reply_count         INTEGER,
    opening_post_body   TEXT,
    last_posted_at      TIMESTAMPTZ,
    is_pinned           BOOLEAN,
    is_locked           BOOLEAN,
    scraped_date        DATE
);


-- -----------------------------------------------------------------------------
-- 10. steam_discussion_replies
-- Individual replies within tracked Steam discussion threads.
-- Written by the steam-discussion-replies workflow.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS steam_discussion_replies (
    comment_id      TEXT            PRIMARY KEY,
    topic_id        TEXT            REFERENCES steam_discussions (topic_id),
    app_id          INTEGER         REFERENCES games_static (app_id),
    author          TEXT,
    text            TEXT,
    scraped_date    DATE
);
