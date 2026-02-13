-- funnel_session_to_pageview.sql
-- Multi-step funnel analysis: active user -> pageview -> deeper engagement.
-- Uses "user had any event" instead of amplitude session checks.
--
-- Steps:
--   Step 1: User had any event (track or page)
--   Step 2: User viewed a page
--   Step 3: User viewed 2+ pages (optional)
--
-- Governance split: splits step 2+ by whether the user also
-- performed a governance action in the same period.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{END_DATE}}   - e.g., '2025-12-31'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- Step 1: Users with at least one event (any activity)
active_users AS (
    SELECT DISTINCT sub.user_id
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.TIMESTAMP >= {{START_DATE}} AND t.TIMESTAMP < {{END_DATE}}
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
        WHERE p.TIMESTAMP >= {{START_DATE}} AND p.TIMESTAMP < {{END_DATE}}
          AND p.domain = {{DOMAIN}}
    ) sub
),

-- Step 2: Users with at least one pageview
pageview_counts AS (
    SELECT
        p.user_id,
        COUNT(*) AS pv_count
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}} AND p.TIMESTAMP < {{END_DATE}}
      AND p.domain = {{DOMAIN}}
    GROUP BY p.user_id
),

-- Governance action flag per user
governance_users AS (
    SELECT DISTINCT t.user_id
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}} AND t.TIMESTAMP < {{END_DATE}}
      AND ud.domain = {{DOMAIN}}
      AND (
          t.event_text LIKE 'governance_%'
          OR t.event_text LIKE 'gtc_tree_create_%'
          OR t.event_text LIKE 'asset_update_%'
      )
),

-- Funnel summary
funnel AS (
    SELECT
        'Step 1: Active User' AS step_name,
        1 AS step_order,
        COUNT(DISTINCT au.user_id) AS total_users,
        NULL AS with_governance,
        NULL AS without_governance
    FROM active_users au

    UNION ALL

    SELECT
        'Step 2: Pageview' AS step_name,
        2 AS step_order,
        COUNT(DISTINCT au.user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN gu.user_id IS NOT NULL THEN au.user_id END) AS with_governance,
        COUNT(DISTINCT CASE WHEN gu.user_id IS NULL THEN au.user_id END) AS without_governance
    FROM active_users au
    INNER JOIN pageview_counts pc ON pc.user_id = au.user_id AND pc.pv_count >= 1
    LEFT JOIN governance_users gu ON gu.user_id = au.user_id

    UNION ALL

    SELECT
        'Step 3: 2+ Pageviews' AS step_name,
        3 AS step_order,
        COUNT(DISTINCT au.user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN gu.user_id IS NOT NULL THEN au.user_id END) AS with_governance,
        COUNT(DISTINCT CASE WHEN gu.user_id IS NULL THEN au.user_id END) AS without_governance
    FROM active_users au
    INNER JOIN pageview_counts pc ON pc.user_id = au.user_id AND pc.pv_count >= 2
    LEFT JOIN governance_users gu ON gu.user_id = au.user_id
)

SELECT
    step_name,
    step_order,
    total_users,
    with_governance,
    without_governance,
    ROUND(100.0 * total_users / NULLIF(FIRST_VALUE(total_users) OVER (ORDER BY step_order), 0), 1) AS conversion_from_step1_pct,
    ROUND(100.0 * total_users / NULLIF(LAG(total_users) OVER (ORDER BY step_order), 0), 1) AS conversion_from_prev_pct
FROM funnel
ORDER BY step_order;
