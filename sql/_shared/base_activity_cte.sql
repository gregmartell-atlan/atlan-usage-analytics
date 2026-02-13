-- base_activity_cte.sql
-- Reusable CTE: Combined TRACKS + PAGES into unified activity stream.
-- Filters out noise events. Converts timestamps to IST.
-- Uses PAGES.domain as the domain source (always populated).
-- TRACKS rows get domain via subquery join to PAGES (same user_id).
--
-- Usage: Copy this CTE into the WITH block of any analytics query.
-- Parameters:
--   {{START_DATE}} - date literal, e.g., '2025-01-01'

-- Step 1: Build a user-to-domain lookup from PAGES (most reliable domain source)
user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- Step 2: Unified activity stream
activity_events AS (
    SELECT
        t.user_id,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP) AS event_ts,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_week,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_month,
        'track' AS source,
        t.event_text AS activity_name,
        ud.domain
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    LEFT JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'workflows_run_ended',
        'atlan_analaytics_aggregateinfo_fetch',
        'workflow_run_finished',
        'workflow_step_finished',
        'api_error_emit',
        'api_evaluator_cancelled',
        'api_evaluator_succeeded',
        'Experiment Started',
        '$experiment_started',
        'web_vital_metric_inp_track',
        'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search',
        'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track',
        'web_vital_metric_lcp_track'
    )
    AND t.TIMESTAMP >= {{START_DATE}}

    UNION ALL

    SELECT
        p.user_id,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP) AS event_ts,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_week,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_month,
        'page' AS source,
        p.name AS activity_name,
        p.domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
)
