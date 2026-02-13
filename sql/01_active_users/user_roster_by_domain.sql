-- user_roster_by_domain.sql
-- Full user list for a domain with activity status, last activity, and event counts.
-- Starts from active user_ids in PAGES, LEFT JOINs USERS for metadata.
-- Shows all active users even if they have no USERS record.
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

activity_events AS (
    SELECT
        t.user_id,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP) AS event_ts
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id AND ud.domain = {{DOMAIN}}
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
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP) AS event_ts
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.domain = {{DOMAIN}}
      AND p.TIMESTAMP >= {{START_DATE}}
),

user_activity AS (
    SELECT
        user_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT DATE(event_ts)) AS active_days,
        MIN(event_ts) AS first_activity,
        MAX(event_ts) AS last_activity
    FROM activity_events
    GROUP BY user_id
),

user_meta AS (
    SELECT id, email, username, role,
           MAX(license_type) AS license_type,
           MAX(job_role) AS job_role,
           MIN(created_at) AS user_created_at
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    WHERE email IS NOT NULL
    GROUP BY id, email, username, role
)

SELECT
    a.user_id,
    um.email,
    um.username,
    um.role,
    um.license_type,
    um.job_role,
    um.user_created_at,
    a.total_events,
    a.active_days,
    a.first_activity,
    a.last_activity,
    DATEDIFF('day', a.last_activity, CURRENT_TIMESTAMP()) AS days_since_last_activity,
    CASE
        WHEN a.last_activity >= DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN 'Active'
        WHEN a.last_activity >= DATEADD('day', -90, CURRENT_TIMESTAMP()) THEN 'Inactive'
        WHEN a.last_activity IS NULL THEN 'Never Active'
        ELSE 'Churned'
    END AS status
FROM user_activity a
LEFT JOIN user_meta um ON um.id = a.user_id
ORDER BY a.total_events DESC;
