-- engagement_tiers.sql
-- Classify users into Power/Heavy/Light/Dormant tiers per month.
-- Power = top 10% by activity, Heavy = above median, Light = below median, Dormant = zero.
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

-- All domain users (for generating dormant entries)
domain_users AS (
    SELECT user_id
    FROM user_domains
    WHERE domain = {{DOMAIN}}
),

activity_events AS (
    SELECT user_id, DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS event_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE ud.domain = {{DOMAIN}}
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

        UNION ALL

        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        INNER JOIN user_domains ud ON ud.user_id = p.user_id
        WHERE ud.domain = {{DOMAIN}}
    )
    WHERE TIMESTAMP >= {{START_DATE}}
),

-- All months in range
months AS (
    SELECT DISTINCT event_month FROM activity_events
),

-- All domain users x all months
user_months AS (
    SELECT du.user_id, m.event_month
    FROM domain_users du
    CROSS JOIN months m
),

-- Activity counts per user per month
user_activity AS (
    SELECT
        act.user_id,
        act.event_month,
        COUNT(*) AS event_count
    FROM activity_events act
    GROUP BY act.user_id, act.event_month
),

-- Merge and compute percentiles
user_tiered AS (
    SELECT
        um.event_month,
        um.user_id,
        COALESCE(ua.event_count, 0) AS event_count,
        CASE
            WHEN COALESCE(ua.event_count, 0) = 0 THEN 'Dormant'
            WHEN PERCENT_RANK() OVER (PARTITION BY um.event_month ORDER BY COALESCE(ua.event_count, 0)) >= 0.9 THEN 'Power'
            WHEN COALESCE(ua.event_count, 0) >= MEDIAN(CASE WHEN ua.event_count > 0 THEN ua.event_count END)
                OVER (PARTITION BY um.event_month) THEN 'Heavy'
            ELSE 'Light'
        END AS tier
    FROM user_months um
    LEFT JOIN user_activity ua ON ua.user_id = um.user_id AND ua.event_month = um.event_month
)

SELECT
    event_month,
    tier,
    COUNT(*) AS user_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY event_month), 1) AS pct_of_users
FROM user_tiered
GROUP BY event_month, tier
ORDER BY event_month DESC, CASE tier WHEN 'Power' THEN 1 WHEN 'Heavy' THEN 2 WHEN 'Light' THEN 3 ELSE 4 END;
