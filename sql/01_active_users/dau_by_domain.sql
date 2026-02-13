-- dau_by_domain.sql
-- Daily Active Users per customer domain.
-- Uses PAGES.domain for filtering. Counts by user_id.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com' (or replace filter line with 1=1 for all)

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity_events AS (
    SELECT
        t.user_id,
        ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'workflows_run_ended', 'atlan_analaytics_aggregateinfo_fetch',
        'workflow_run_finished', 'workflow_step_finished', 'api_error_emit',
        'api_evaluator_cancelled', 'api_evaluator_succeeded', 'Experiment Started',
        '$experiment_started',
        'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search',
        'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
    )
    AND t.TIMESTAMP >= {{START_DATE}}

    UNION ALL

    SELECT
        p.user_id,
        p.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
)

SELECT
    domain,
    event_date,
    COUNT(DISTINCT user_id) AS dau
FROM activity_events
WHERE domain = {{DOMAIN}}
GROUP BY domain, event_date
ORDER BY domain, event_date DESC;
