-- reactivated_users.sql
-- Users who were inactive for 30+ days and then returned.
-- Shows the gap duration and what they did on return.
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

user_activity_days AS (
    SELECT DISTINCT
        sub.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS activity_date
    FROM (
        SELECT t.user_id, t.TIMESTAMP
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
        AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.domain = {{DOMAIN}}
    ) sub
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),

with_gaps AS (
    SELECT
        user_id,
        activity_date,
        LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date) AS prev_activity_date,
        DATEDIFF('day', LAG(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date), activity_date) AS gap_days
    FROM user_activity_days
)

SELECT
    g.user_id,
    u.email,
    u.username,
    u.role,
    g.prev_activity_date AS last_active_before_gap,
    g.activity_date AS reactivation_date,
    g.gap_days
FROM with_gaps g
LEFT JOIN (
    SELECT id, MAX(email) AS email, MAX(username) AS username, MAX(role) AS role
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    GROUP BY id
) u ON u.id = g.user_id
WHERE g.gap_days >= 30
ORDER BY g.activity_date DESC, g.gap_days DESC;
