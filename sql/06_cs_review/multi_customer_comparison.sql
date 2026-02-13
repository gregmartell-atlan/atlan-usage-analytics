-- multi_customer_comparison.sql
-- Side-by-side key metrics for multiple domains.
-- Shows MAU, stickiness, feature breadth, license utilization.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
-- (No domain filter - returns all domains, sorted by MAU)

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity_events AS (
    SELECT sub.user_id, ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud2 ON ud2.user_id = t.user_id
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
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    ) sub
    INNER JOIN user_domains ud ON ud.user_id = sub.user_id
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),

total_users AS (
    SELECT domain, COUNT(DISTINCT user_id) AS total_users
    FROM user_domains GROUP BY domain
),

current_month AS (
    SELECT
        domain,
        COUNT(DISTINCT user_id) AS mau,
        COUNT(DISTINCT CONCAT(user_id, event_date)) AS user_days
    FROM activity_events
    WHERE event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    GROUP BY domain
),

prev_month AS (
    SELECT
        domain,
        COUNT(DISTINCT user_id) AS prev_mau
    FROM activity_events
    WHERE event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
    GROUP BY domain
),

features AS (
    SELECT
        ud.domain,
        COUNT(DISTINCT CASE
            WHEN p.name IN ('discovery') THEN 'Discovery'
            WHEN p.name IN ('saved_query', 'insights') THEN 'Insights'
            WHEN p.name IN ('glossary', 'term', 'category') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            ELSE NULL END) AS feature_breadth
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) AND p.name IS NOT NULL
    GROUP BY ud.domain
)

SELECT
    tu.domain,
    tu.total_users,
    COALESCE(cm.mau, 0) AS current_mau,
    COALESCE(pm.prev_mau, 0) AS prev_mau,
    COALESCE(cm.mau, 0) - COALESCE(pm.prev_mau, 0) AS mau_delta,
    ROUND(100.0 * COALESCE(cm.mau, 0) / NULLIF(tu.total_users, 0), 1) AS license_util_pct,
    ROUND(COALESCE(cm.user_days, 0)::FLOAT / NULLIF(COALESCE(cm.mau, 1) * DAY(LAST_DAY(CURRENT_TIMESTAMP())), 0), 3) AS stickiness,
    COALESCE(f.feature_breadth, 0) AS feature_breadth
FROM total_users tu
LEFT JOIN current_month cm ON cm.domain = tu.domain
LEFT JOIN prev_month pm ON pm.domain = tu.domain
LEFT JOIN features f ON f.domain = tu.domain
ORDER BY COALESCE(cm.mau, 0) DESC;
