-- retention_rate_aggregate.sql
-- Aggregate retention rate: of users with any activity, what % had a pageview
-- within 7 days? Returns a single per-week summary (not per-cohort).
-- Uses daily user activity instead of amplitude session IDs.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- First activity date per user (entry point)
first_activity AS (
    SELECT
        sub.user_id,
        MIN(DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP))) AS first_activity_date
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.TIMESTAMP >= {{START_DATE}}
          AND t.event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch',
              'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started',
              'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search',
              'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
          )
          AND t.event_text NOT LIKE 'workflow_%'
          AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.TIMESTAMP >= {{START_DATE}}
          AND p.domain = {{DOMAIN}}
    ) sub
    GROUP BY sub.user_id
),

-- First pageview within 7 days of first activity
pageview_within_7d AS (
    SELECT
        fa.user_id,
        fa.first_activity_date,
        MIN(DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP))) AS first_pv_date
    FROM first_activity fa
    INNER JOIN {{DATABASE}}.{{SCHEMA}}.PAGES p
        ON p.user_id = fa.user_id
        AND DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP))
            BETWEEN fa.first_activity_date AND DATEADD('day', 7, fa.first_activity_date)
    WHERE p.TIMESTAMP >= {{START_DATE}}
      AND p.domain = {{DOMAIN}}
    GROUP BY fa.user_id, fa.first_activity_date
)

SELECT
    DATE_TRUNC('WEEK', fa.first_activity_date) AS cohort_week,
    COUNT(DISTINCT fa.user_id) AS users_with_activity,
    COUNT(DISTINCT pv.user_id) AS users_with_pageview_7d,
    ROUND(100.0 * COUNT(DISTINCT pv.user_id) / NULLIF(COUNT(DISTINCT fa.user_id), 0), 1) AS retention_rate_pct
FROM first_activity fa
LEFT JOIN pageview_within_7d pv ON pv.user_id = fa.user_id
WHERE fa.first_activity_date <= DATEADD('day', -7, CURRENT_DATE())  -- Only complete 7-day windows
GROUP BY cohort_week
ORDER BY cohort_week DESC;
