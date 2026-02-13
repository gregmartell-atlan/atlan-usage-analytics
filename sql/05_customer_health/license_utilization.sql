-- license_utilization.sql
-- Active vs total users by domain, broken down by role and license type.
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

active_users AS (
    SELECT DISTINCT sub.user_id
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
    WHERE sub.TIMESTAMP >= DATEADD('month', -1, CURRENT_TIMESTAMP())
)

SELECT
    ud.domain,
    u.role,
    u.license_type,
    COUNT(DISTINCT ud.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END) AS active_users,
    COUNT(DISTINCT CASE WHEN a.user_id IS NULL THEN ud.user_id END) AS inactive_users,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN a.user_id IS NOT NULL THEN ud.user_id END)
        / NULLIF(COUNT(DISTINCT ud.user_id), 0), 1) AS utilization_pct
FROM user_domains ud
LEFT JOIN (
    SELECT id, MAX(role) AS role, MAX(license_type) AS license_type
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    GROUP BY id
) u ON u.id = ud.user_id
LEFT JOIN active_users a ON a.user_id = ud.user_id
WHERE ud.domain = {{DOMAIN}}
GROUP BY ud.domain, u.role, u.license_type
ORDER BY total_users DESC;
