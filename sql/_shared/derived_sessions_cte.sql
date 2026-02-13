-- derived_sessions_cte.sql
-- Reusable CTE: Derives sessions from time-gap logic (30-min inactivity threshold).
-- No session ID column is populated, so sessions are inferred from event timestamps.
-- Groups consecutive events per user where gaps > 30 minutes start a new session.
--
-- Usage: Copy into WITH block. Requires activity_events CTE (or inline the event source).
-- Parameters:
--   {{START_DATE}} - date literal, e.g., '2025-01-01'

-- Raw events with timestamps per user
raw_events AS (
    SELECT
        user_id,
        TIMESTAMP,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP) AS event_ts,
        LAG(TIMESTAMP) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS prev_ts
    FROM (
        SELECT user_id, TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS
        WHERE event_text NOT IN (
            'atlan_analaytics_aggregateinfo_fetch',
            'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
            'Experiment Started', '$experiment_started',
            'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
            'performance_metric_user_timing_discovery_search',
            'performance_metric_user_timing_app_bootstrap',
            'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
        )
        AND event_text NOT LIKE 'workflow_%'
        AND TIMESTAMP >= {{START_DATE}}

        UNION ALL

        SELECT user_id, TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES
        WHERE TIMESTAMP >= {{START_DATE}}
    ) AS combined
),

-- Mark session boundaries (new session when gap > 30 min or first event)
session_boundaries AS (
    SELECT
        user_id,
        TIMESTAMP,
        event_ts,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, TIMESTAMP) > 1800 THEN 1
            ELSE 0
        END AS is_new_session
    FROM raw_events
),

-- Assign session IDs using cumulative sum of boundaries
session_numbered AS (
    SELECT
        user_id,
        TIMESTAMP,
        event_ts,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS session_id
    FROM session_boundaries
),

-- Aggregate to session level
derived_sessions AS (
    SELECT
        user_id,
        session_id,
        DATE(MIN(event_ts)) AS session_date,
        MIN(event_ts) AS session_start,
        MAX(event_ts) AS session_end,
        COUNT(*) AS event_count,
        DATEDIFF('second', MIN(TIMESTAMP), MAX(TIMESTAMP)) AS duration_seconds
    FROM session_numbered
    GROUP BY user_id, session_id
)
