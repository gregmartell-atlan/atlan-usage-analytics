-- monthly_retention_cohort.sql
-- Cohort retention: what % of users who first appeared in month X returned in month X+1, X+2, etc.
-- Output is a triangular retention matrix.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com' (or replace filter with 1=1 for all)

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

user_months AS (
    SELECT DISTINCT
        sub.user_id,
        ud.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud2 ON ud2.user_id = t.user_id
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
        AND ud2.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.domain = {{DOMAIN}}
    ) sub
    INNER JOIN user_domains ud ON ud.user_id = sub.user_id
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),

cohorts AS (
    SELECT
        user_id,
        domain,
        MIN(activity_month) AS cohort_month
    FROM user_months
    GROUP BY user_id, domain
),

retention AS (
    SELECT
        c.cohort_month,
        DATEDIFF('month', c.cohort_month, um.activity_month) AS months_since_start,
        COUNT(DISTINCT um.user_id) AS active_users
    FROM cohorts c
    INNER JOIN user_months um ON um.user_id = c.user_id AND um.domain = c.domain
    GROUP BY c.cohort_month, months_since_start
),

cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS cohort_size
    FROM cohorts
    GROUP BY cohort_month
)

SELECT
    r.cohort_month,
    cs.cohort_size,
    r.months_since_start,
    r.active_users,
    ROUND(100.0 * r.active_users / cs.cohort_size, 1) AS retention_pct
FROM retention r
JOIN cohort_sizes cs ON cs.cohort_month = r.cohort_month
ORDER BY r.cohort_month, r.months_since_start;
