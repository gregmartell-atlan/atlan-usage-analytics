-- mau_by_domain.sql
-- Monthly Active Users per customer domain with month-over-month delta.
-- Uses PAGES.domain for domain filtering. Counts by user_id (not email).
-- LEFT JOINs USERS for optional email enrichment.
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
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_month
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
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_month
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
),

mau_counts AS (
    SELECT
        domain,
        event_month,
        COUNT(DISTINCT user_id) AS mau
    FROM activity_events
    WHERE domain = {{DOMAIN}}
    GROUP BY domain, event_month
)

SELECT
    domain,
    event_month,
    mau,
    LAG(mau) OVER (PARTITION BY domain ORDER BY event_month) AS prev_month_mau,
    mau - LAG(mau) OVER (PARTITION BY domain ORDER BY event_month) AS mau_delta,
    ROUND(100.0 * (mau - LAG(mau) OVER (PARTITION BY domain ORDER BY event_month))
        / NULLIF(LAG(mau) OVER (PARTITION BY domain ORDER BY event_month), 0), 1) AS mau_change_pct
FROM mau_counts
ORDER BY domain, event_month DESC;
