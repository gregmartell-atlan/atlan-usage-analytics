-- discover_events.sql
-- Event catalog: lists all tracked events with usage stats.
-- Shows total occurrences, unique users, and domains using each event.
-- Excludes known noise events (workflows, web vitals, etc.).
--
-- Parameters:
--   {{DATABASE}} - Snowflake database name
--   {{SCHEMA}}   - Schema containing PAGES and TRACKS tables
--
-- Optional filters (add before GROUP BY):
--   AND LOWER(t.event_text) LIKE '%<search_term>%'   -- filter by search term
--   AND ud.domain = '<domain>'                        -- filter by customer domain

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)
SELECT
    t.event_text,
    COUNT(*) AS total_occurrences,
    COUNT(DISTINCT t.user_id) AS unique_users,
    COUNT(DISTINCT ud.domain) AS domains_using,
    MIN(DATE(t.TIMESTAMP)) AS first_seen,
    MAX(DATE(t.TIMESTAMP)) AS last_seen
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
AND t.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY t.event_text
ORDER BY total_occurrences DESC
