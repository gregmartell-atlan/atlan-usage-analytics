-- churned_users.sql
-- Users active in the prior month but NOT in the current month.
-- Includes last activity date, role, and top features used.
--
-- Parameters:
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

user_months AS (
    SELECT DISTINCT
        sub.user_id,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP
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
        AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.domain = {{DOMAIN}}
    ) sub
    WHERE sub.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
),

prev_month_users AS (
    SELECT user_id
    FROM user_months
    WHERE activity_month = DATE_TRUNC('MONTH', DATEADD('month', -1, CURRENT_TIMESTAMP()))
),

curr_month_users AS (
    SELECT DISTINCT user_id
    FROM user_months
    WHERE activity_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
)

SELECT
    p.user_id,
    u.email,
    u.username,
    u.role,
    u.job_role,
    DATE_TRUNC('MONTH', DATEADD('month', -1, CURRENT_TIMESTAMP())) AS last_active_month
FROM prev_month_users p
LEFT JOIN curr_month_users c ON c.user_id = p.user_id
LEFT JOIN (
    SELECT id, MAX(email) AS email, MAX(username) AS username, MAX(role) AS role, MAX(job_role) AS job_role
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    GROUP BY id
) u ON u.id = p.user_id
WHERE c.user_id IS NULL
ORDER BY p.user_id;
