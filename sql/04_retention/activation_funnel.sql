-- activation_funnel.sql
-- New user activation: how quickly do new users take their first action?
-- Shows % activated within 1 day, 7 days, 14 days, 30 days of account creation.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01' (for user creation date filter)
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

users_deduped AS (
    SELECT
        id,
        MAX(role) AS role,
        MIN(created_at) AS user_created_at
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    GROUP BY id
),

first_activity AS (
    SELECT
        user_id,
        MIN(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS first_event_ts
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
    )
    GROUP BY user_id
),

new_users AS (
    SELECT
        ud.user_id,
        u.role,
        u.user_created_at,
        DATE_TRUNC('MONTH', u.user_created_at) AS creation_month,
        fa.first_event_ts,
        DATEDIFF('day', u.user_created_at, fa.first_event_ts) AS days_to_first_action
    FROM user_domains ud
    INNER JOIN users_deduped u ON u.id = ud.user_id
    LEFT JOIN first_activity fa ON fa.user_id = ud.user_id
    WHERE ud.domain = {{DOMAIN}}
      AND u.user_created_at >= {{START_DATE}}
)

SELECT
    creation_month,
    COUNT(*) AS total_new_users,
    COUNT(CASE WHEN days_to_first_action <= 1 THEN 1 END) AS activated_1d,
    COUNT(CASE WHEN days_to_first_action <= 7 THEN 1 END) AS activated_7d,
    COUNT(CASE WHEN days_to_first_action <= 14 THEN 1 END) AS activated_14d,
    COUNT(CASE WHEN days_to_first_action <= 30 THEN 1 END) AS activated_30d,
    COUNT(CASE WHEN days_to_first_action IS NULL THEN 1 END) AS never_activated,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 1 THEN 1 END) / COUNT(*), 1) AS pct_1d,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 7 THEN 1 END) / COUNT(*), 1) AS pct_7d,
    ROUND(100.0 * COUNT(CASE WHEN days_to_first_action <= 30 THEN 1 END) / COUNT(*), 1) AS pct_30d
FROM new_users
GROUP BY creation_month
ORDER BY creation_month DESC;
