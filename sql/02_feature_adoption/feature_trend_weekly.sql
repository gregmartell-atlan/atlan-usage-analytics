-- feature_trend_weekly.sql
-- Week-over-week feature usage trending for a domain.
-- Shows unique users per feature area per week.
--
-- Pattern: Schema-aware (domain from PAGES directly, user_id as identity key).
-- PAGES provides domain natively; TRACKS gets domain via user_domains CTE.
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
    SELECT
        user_id,
        domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS event_week,
        CASE
            WHEN name = 'discovery' THEN 'Discovery'
            WHEN name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN name IN ('saved_query', 'insights') THEN 'Insights/SQL'
            WHEN name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
            WHEN name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN name = 'monitor' THEN 'Data Quality'
            WHEN name = 'home' THEN 'Home'
            ELSE NULL
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE TIMESTAMP >= {{START_DATE}} AND name IS NOT NULL

    UNION ALL

    SELECT
        t.user_id,
        ud.domain,
        DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_week,
        CASE
            WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
            WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            ELSE NULL
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND t.event_text NOT IN (
          'workflows_run_ended', 'atlan_analaytics_aggregateinfo_fetch',
          'workflow_run_finished', 'workflow_step_finished', 'api_error_emit',
          'api_evaluator_cancelled', 'api_evaluator_succeeded', 'Experiment Started',
          '$experiment_started',
          'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search',
          'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
      )
)

SELECT
    event_week,
    feature_area,
    COUNT(DISTINCT a.user_id) AS unique_users,
    COUNT(*) AS total_events
FROM all_activity a
WHERE a.feature_area IS NOT NULL
  AND a.domain = {{DOMAIN}}
GROUP BY event_week, feature_area
ORDER BY event_week DESC, unique_users DESC;
