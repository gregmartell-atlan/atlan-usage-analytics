-- top_events_by_domain.sql
-- Most frequent tracked events per customer domain (noise filtered).
--
-- Pattern: Schema-aware (domain from PAGES via user_domains CTE, user_id as identity key).
-- TRACKS does not have domain natively, so we derive it from PAGES.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com' (or replace filter with 1=1 for all)

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)

SELECT
    ud.domain,
    t.event_text,
    COUNT(*) AS event_count,
    COUNT(DISTINCT t.user_id) AS unique_users,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT t.user_id), 0), 1) AS events_per_user
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
INNER JOIN user_domains ud ON ud.user_id = t.user_id
WHERE t.TIMESTAMP >= {{START_DATE}}
  AND t.event_text IS NOT NULL
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
  AND ud.domain = {{DOMAIN}}
GROUP BY ud.domain, t.event_text
ORDER BY event_count DESC
LIMIT 50;
