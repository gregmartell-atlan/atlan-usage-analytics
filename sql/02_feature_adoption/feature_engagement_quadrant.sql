-- feature_engagement_quadrant.sql
-- Feature engagement quadrant: plots each feature by reach (unique users)
-- vs depth (avg events per user). Inspired by Heap's engagement matrix.
-- Combines PAGES and TRACKS into unified feature categories.
--
-- Pattern: Schema-aware. PAGES has domain natively; TRACKS uses user_domains CTE.
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

page_events AS (
    SELECT p.user_id, CASE
            WHEN p.name = 'discovery' THEN 'Discovery/Search'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name IN ('saved_query', 'insights') THEN 'Insights/SQL'
            WHEN p.name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            WHEN p.name IN ('workflows-home', 'workflows-profile', 'runs', 'playbook') THEN 'Workflows'
            WHEN p.name = 'home' THEN 'Home'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.domain = {{DOMAIN}}
      AND p.TIMESTAMP >= {{START_DATE}}
      AND p.name IS NOT NULL
),

track_events AS (
    SELECT t.user_id, CASE
            WHEN t.event_text LIKE 'discovery_search%' THEN 'Discovery/Search'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
            WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            WHEN t.event_text LIKE 'discovery_metadata_%' THEN 'Metadata Curation'
            WHEN t.event_text = 'main_navigation_button_clicked' THEN 'Navigation'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id AND ud.domain = {{DOMAIN}}
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
),

combined AS (
    SELECT user_id, feature FROM page_events WHERE feature IS NOT NULL
    UNION ALL
    SELECT user_id, feature FROM track_events WHERE feature IS NOT NULL
),

per_user AS (
    SELECT feature, user_id, COUNT(*) AS events
    FROM combined
    GROUP BY feature, user_id
)

SELECT
    feature,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(events) AS total_events,
    ROUND(AVG(events), 1) AS avg_events_per_user,
    ROUND(MEDIAN(events), 1) AS median_events_per_user
FROM per_user
GROUP BY feature
ORDER BY unique_users DESC;
