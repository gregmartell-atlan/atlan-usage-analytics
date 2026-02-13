-- trending_alert.sql
-- Proactive risk detection. Flags domains with warning signals.
-- Run weekly by CS leadership to catch declining accounts early.
--
-- Alerts:
--   MAU_DROP_20PCT    - MAU dropped >20% month-over-month
--   ZERO_NEW_USERS    - No new users in last 30 days
--   LOW_STICKINESS    - DAU/MAU ratio below 0.05
--   FEATURE_SHRINK    - Fewer features used than prior month
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity AS (
    SELECT sub.user_id, ud.domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_date,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', sub.TIMESTAMP)) AS event_month
    FROM (
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud2 ON ud2.user_id = t.user_id
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
        UNION ALL
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    ) sub
    INNER JOIN user_domains ud ON ud.user_id = sub.user_id
    WHERE sub.TIMESTAMP >= {{START_DATE}}
),

-- MAU current vs prior month
mau_by_month AS (
    SELECT
        domain,
        event_month,
        COUNT(DISTINCT user_id) AS mau,
        COUNT(DISTINCT CONCAT(user_id, event_date)) AS user_days
    FROM activity
    WHERE event_month >= DATEADD('month', -2, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
    GROUP BY domain, event_month
),

mau_comparison AS (
    SELECT
        domain,
        MAX(CASE WHEN event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) THEN mau END) AS curr_mau,
        MAX(CASE WHEN event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())) THEN mau END) AS prev_mau,
        MAX(CASE WHEN event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) THEN user_days END) AS curr_user_days
    FROM mau_by_month
    GROUP BY domain
),

-- New users in last 30 days
new_users AS (
    SELECT ud.domain, COUNT(DISTINCT ud.user_id) AS new_user_count
    FROM user_domains ud
    INNER JOIN (
        SELECT id, MIN(created_at) AS user_created_at
        FROM {{DATABASE}}.{{SCHEMA}}.USERS
        GROUP BY id
    ) u ON u.id = ud.user_id
    WHERE u.user_created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY ud.domain
),

-- All domains
all_domains AS (
    SELECT DISTINCT domain FROM user_domains
),

-- Generate alerts
alerts AS (
    -- MAU drop >20%
    SELECT
        mc.domain,
        'MAU_DROP_20PCT' AS alert_type,
        'MAU dropped from ' || mc.prev_mau || ' to ' || mc.curr_mau
            || ' (' || ROUND(100.0 * (mc.curr_mau - mc.prev_mau) / NULLIF(mc.prev_mau, 0), 1) || '%)' AS detail
    FROM mau_comparison mc
    WHERE mc.prev_mau > 0
      AND mc.curr_mau < mc.prev_mau * 0.8

    UNION ALL

    -- Zero new users in 30 days
    SELECT
        ad.domain,
        'ZERO_NEW_USERS' AS alert_type,
        'No new users created in the last 30 days' AS detail
    FROM all_domains ad
    LEFT JOIN new_users nu ON nu.domain = ad.domain
    WHERE nu.new_user_count IS NULL OR nu.new_user_count = 0

    UNION ALL

    -- Low stickiness
    SELECT
        mc.domain,
        'LOW_STICKINESS' AS alert_type,
        'DAU/MAU ratio: ' || ROUND(mc.curr_user_days::FLOAT / NULLIF(mc.curr_mau * DAY(LAST_DAY(CURRENT_TIMESTAMP())), 0), 3) AS detail
    FROM mau_comparison mc
    WHERE mc.curr_mau > 0
      AND mc.curr_user_days::FLOAT / NULLIF(mc.curr_mau * DAY(LAST_DAY(CURRENT_TIMESTAMP())), 0) < 0.05
)

SELECT
    domain,
    alert_type,
    detail
FROM alerts
ORDER BY domain, alert_type;
