-- mau_dau_ratio.sql
-- DAU/MAU stickiness ratio per domain per month.
-- >0.3 = strong daily engagement, <0.1 = episodic usage.
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
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_month
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
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
),

daily_users AS (
    SELECT
        domain,
        event_month,
        event_date,
        COUNT(DISTINCT user_id) AS dau
    FROM activity_events
    WHERE domain = {{DOMAIN}}
    GROUP BY domain, event_month, event_date
),

monthly_users AS (
    SELECT
        domain,
        event_month,
        COUNT(DISTINCT user_id) AS mau
    FROM activity_events
    WHERE domain = {{DOMAIN}}
    GROUP BY domain, event_month
)

SELECT
    m.domain,
    m.event_month,
    m.mau,
    ROUND(AVG(d.dau), 1) AS avg_dau,
    ROUND(AVG(d.dau) / NULLIF(m.mau, 0), 3) AS stickiness_ratio,
    CASE
        WHEN AVG(d.dau) / NULLIF(m.mau, 0) >= 0.3 THEN 'Strong'
        WHEN AVG(d.dau) / NULLIF(m.mau, 0) >= 0.1 THEN 'Moderate'
        ELSE 'Episodic'
    END AS engagement_level
FROM monthly_users m
JOIN daily_users d ON d.domain = m.domain AND d.event_month = m.event_month
GROUP BY m.domain, m.event_month, m.mau
ORDER BY m.domain, m.event_month DESC;
