-- daily_retention_session_to_search.sql
-- Day-N retention: of users who had activity (any event) on a given day,
-- what % performed a search or AI action on day N (N=0..13).
-- Uses daily user activity instead of amplitude session IDs.
--
-- Search/AI events: discovery_search_results, atlan_ai_conversation_prompt_submitted
--
-- Parameters:
--   {{START_DATE}}     - e.g., '2025-01-01'
--   {{DOMAIN}}         - e.g., 'acme.atlan.com'
--   {{RETENTION_DAYS}} - e.g., 14

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- Users with any activity per day (cohort entry)
activity_days AS (
    SELECT DISTINCT
        sub.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS cohort_date
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
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
          AND ud.domain = {{DOMAIN}}
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        WHERE p.TIMESTAMP >= {{START_DATE}}
          AND p.domain = {{DOMAIN}}
    ) sub
),

-- Users with search/AI events per day (return signal)
search_days AS (
    SELECT DISTINCT
        t.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS search_date
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND ud.domain = {{DOMAIN}}
      AND t.event_text IN (
          'discovery_search_results',
          'atlan_ai_conversation_prompt_submitted'
      )
),

day_offsets AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS day_n
    FROM TABLE(GENERATOR(ROWCOUNT => {{RETENTION_DAYS}}))
),

cohort_sizes AS (
    SELECT cohort_date, COUNT(DISTINCT user_id) AS cohort_size
    FROM activity_days
    GROUP BY cohort_date
),

retention AS (
    SELECT
        ad.cohort_date,
        d.day_n,
        COUNT(DISTINCT CASE WHEN sr.user_id IS NOT NULL THEN ad.user_id END) AS retained_users
    FROM activity_days ad
    CROSS JOIN day_offsets d
    LEFT JOIN search_days sr
        ON sr.user_id = ad.user_id
        AND sr.search_date = DATEADD('day', d.day_n, ad.cohort_date)
    GROUP BY ad.cohort_date, d.day_n
)

SELECT
    r.cohort_date,
    cs.cohort_size,
    r.day_n,
    r.retained_users,
    ROUND(100.0 * r.retained_users / NULLIF(cs.cohort_size, 0), 1) AS retention_pct
FROM retention r
INNER JOIN cohort_sizes cs ON cs.cohort_date = r.cohort_date
WHERE r.cohort_date <= DATEADD('day', -{{RETENTION_DAYS}}, CURRENT_DATE())
ORDER BY r.cohort_date DESC, r.day_n;
