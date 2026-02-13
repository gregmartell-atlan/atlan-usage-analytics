-- domain_summary_snapshot.sql
-- One-row summary per domain with the latest month's key metrics.
-- Designed for quick CS leadership review.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity AS (
    SELECT sub.user_id, sub.TIMESTAMP, sub.activity_name, ud.domain
    FROM (
        SELECT user_id, TIMESTAMP, name AS activity_name
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES
        WHERE TIMESTAMP >= {{START_DATE}} AND name IS NOT NULL

        UNION ALL

        SELECT user_id, TIMESTAMP, event_text AS activity_name
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS
        WHERE TIMESTAMP >= {{START_DATE}}
          AND event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch',
              'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started',
              'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search',
              'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
          )
          AND event_text NOT LIKE 'workflow_%'
    ) sub
    INNER JOIN user_domains ud ON ud.user_id = sub.user_id
),

domain_stats AS (
    SELECT
        ud.domain,
        COUNT(DISTINCT ud.user_id) AS total_users,
        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.TIMESTAMP))
                 = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
            THEN a.user_id END) AS active_this_month,
        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.TIMESTAMP))
                 = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
            THEN a.user_id END) AS active_last_month,
        MAX(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.TIMESTAMP)) AS last_activity
    FROM user_domains ud
    LEFT JOIN activity a ON a.user_id = ud.user_id
    GROUP BY ud.domain
)

SELECT
    ds.domain,
    ds.total_users,
    ds.active_this_month,
    ds.active_last_month,
    ds.active_this_month - ds.active_last_month AS mau_delta,
    ROUND(100.0 * (ds.active_this_month - ds.active_last_month) / NULLIF(ds.active_last_month, 0), 1) AS mau_change_pct,
    ROUND(100.0 * ds.active_this_month / NULLIF(ds.total_users, 0), 1) AS license_util_pct,
    ds.last_activity,
    DATEDIFF('day', ds.last_activity, CURRENT_TIMESTAMP()) AS days_since_last_activity
FROM domain_stats ds
WHERE ds.total_users > 0
ORDER BY ds.active_this_month DESC;
