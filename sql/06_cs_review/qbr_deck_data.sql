-- qbr_deck_data.sql
-- All-in-one QBR data pull for a single customer.
-- Returns sections: MAU trend, top features, top users, user growth, feature breadth.
--
-- Parameters:
--   {{DOMAIN}}      - e.g., 'acme.atlan.com'
--   {{MONTHS_BACK}} - e.g., 6

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity_events AS (
    SELECT p.user_id, p.TIMESTAMP, p.name AS activity_name, 'page' AS source
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= DATEADD('month', -{{MONTHS_BACK}}, CURRENT_TIMESTAMP())
      AND p.name IS NOT NULL
      AND p.domain = {{DOMAIN}}

    UNION ALL

    SELECT t.user_id, t.TIMESTAMP, t.event_text AS activity_name, 'track' AS source
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= DATEADD('month', -{{MONTHS_BACK}}, CURRENT_TIMESTAMP())
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
),

-- Section 1: MAU Trend
mau_trend AS (
    SELECT
        '1_MAU_TREND' AS section,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', a.TIMESTAMP))::VARCHAR AS period,
        COUNT(DISTINCT a.user_id)::VARCHAR AS metric_value,
        'monthly_active_users' AS metric_name
    FROM activity_events a
    GROUP BY period
),

-- Section 2: Top Features (pages)
top_features AS (
    SELECT
        '2_TOP_FEATURES' AS section,
        a.activity_name AS period,
        COUNT(DISTINCT a.user_id)::VARCHAR AS metric_value,
        'unique_users' AS metric_name
    FROM activity_events a
    WHERE a.source = 'page'
    GROUP BY a.activity_name
    ORDER BY COUNT(DISTINCT a.user_id) DESC
    LIMIT 10
),

-- Section 3: Top Users by Activity
top_users AS (
    SELECT
        '3_TOP_USERS' AS section,
        u.email AS period,
        COUNT(*)::VARCHAR AS metric_value,
        u.role AS metric_name
    FROM activity_events a
    LEFT JOIN (
        SELECT id, MAX(email) AS email, MAX(role) AS role
        FROM {{DATABASE}}.{{SCHEMA}}.USERS
        GROUP BY id
    ) u ON u.id = a.user_id
    GROUP BY u.email, u.role
    ORDER BY COUNT(*) DESC
    LIMIT 10
),

-- Section 4: New Users per Month
new_users AS (
    SELECT
        '4_NEW_USERS' AS section,
        DATE_TRUNC('MONTH', u.user_created_at)::VARCHAR AS period,
        COUNT(DISTINCT ud.user_id)::VARCHAR AS metric_value,
        'new_users' AS metric_name
    FROM user_domains ud
    INNER JOIN (
        SELECT id, MIN(created_at) AS user_created_at
        FROM {{DATABASE}}.{{SCHEMA}}.USERS
        GROUP BY id
    ) u ON u.id = ud.user_id
    WHERE ud.domain = {{DOMAIN}}
      AND u.user_created_at >= DATEADD('month', -{{MONTHS_BACK}}, CURRENT_TIMESTAMP())
    GROUP BY period
)

SELECT * FROM mau_trend
UNION ALL SELECT * FROM top_features
UNION ALL SELECT * FROM top_users
UNION ALL SELECT * FROM new_users
ORDER BY section, period;
