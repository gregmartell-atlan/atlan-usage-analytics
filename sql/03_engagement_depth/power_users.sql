-- power_users.sql
-- Top users by composite activity score for a domain and time range.
-- Score = weighted sum of total events, active days, and feature breadth.
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

all_activity AS (
    SELECT p.user_id, p.TIMESTAMP, ud.domain,
        CASE
            WHEN p.name = 'discovery' THEN 'Discovery'
            WHEN p.name IN ('saved_query', 'insights') THEN 'Insights'
            WHEN p.name IN ('glossary', 'term', 'category') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            ELSE 'Other'
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= {{START_DATE}}
      AND p.name IS NOT NULL
      AND ud.domain = {{DOMAIN}}

    UNION ALL

    SELECT t.user_id, t.TIMESTAMP, ud.domain,
        CASE
            WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights'
            WHEN t.event_text LIKE 'governance_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            ELSE 'Other'
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND ud.domain = {{DOMAIN}}
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
),

user_scores AS (
    SELECT
        a.user_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.TIMESTAMP))) AS active_days,
        COUNT(DISTINCT CASE WHEN a.feature_area != 'Other' THEN a.feature_area END) AS feature_breadth
    FROM all_activity a
    GROUP BY a.user_id
)

SELECT
    us.user_id,
    u.email,
    u.username,
    MAX(u.role) AS role,
    MAX(u.job_role) AS job_role,
    us.total_events,
    us.active_days,
    us.feature_breadth,
    -- Composite score: 40% active days, 30% feature breadth, 30% event volume (log-scaled)
    ROUND(
        40.0 * us.active_days / NULLIF(MAX(us.active_days) OVER (), 0)
        + 30.0 * us.feature_breadth / NULLIF(MAX(us.feature_breadth) OVER (), 0)
        + 30.0 * LN(1 + us.total_events) / NULLIF(MAX(LN(1 + us.total_events)) OVER (), 0)
    , 1) AS power_score
FROM user_scores us
LEFT JOIN {{DATABASE}}.{{SCHEMA}}.USERS u ON u.id = us.user_id
GROUP BY us.user_id, u.email, u.username, us.total_events, us.active_days, us.feature_breadth
ORDER BY power_score DESC
LIMIT 25;
