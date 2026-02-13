-- wau_by_domain.sql
-- Weekly Active Users per customer domain.
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
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_week
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.event_text NOT IN (
        'atlan_analaytics_aggregateinfo_fetch',
        'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
        'Experiment Started', '$experiment_started',
        'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
        'performance_metric_user_timing_discovery_search',
        'performance_metric_user_timing_app_bootstrap',
        'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
    )
    AND t.event_text NOT LIKE 'workflow_%'
    AND t.TIMESTAMP >= {{START_DATE}}

    UNION ALL

    SELECT
        p.user_id,
        p.domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_week
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
)

SELECT
    domain,
    event_week,
    COUNT(DISTINCT user_id) AS wau
FROM activity_events
WHERE domain = {{DOMAIN}}
GROUP BY domain, event_week
ORDER BY domain, event_week DESC;
