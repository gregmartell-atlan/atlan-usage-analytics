-- customer_health_scorecard.sql
-- Composite 0-100 health score per customer domain.
-- Combines: MAU trend, stickiness, feature breadth, retention, license utilization.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
-- (Runs across all domains - no domain filter needed)

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

activity_events AS (
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

-- Total users per domain
total_users AS (
    SELECT domain, COUNT(DISTINCT user_id) AS total_user_count
    FROM user_domains
    GROUP BY domain
),

-- MAU per domain for last 3 months
mau_monthly AS (
    SELECT
        domain,
        event_month,
        COUNT(DISTINCT user_id) AS mau
    FROM activity_events
    WHERE event_month >= DATEADD('month', -3, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
    GROUP BY domain, event_month
),

-- Current month MAU and trend
mau_summary AS (
    SELECT
        domain,
        MAX(CASE WHEN event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) THEN mau END) AS current_mau,
        MAX(CASE WHEN event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())) THEN mau END) AS prev_mau,
        AVG(mau) AS avg_mau_3m
    FROM mau_monthly
    GROUP BY domain
),

-- Stickiness (DAU/MAU) for current month
stickiness AS (
    SELECT
        domain,
        COUNT(DISTINCT user_id) AS current_month_mau,
        COUNT(DISTINCT CONCAT(user_id, event_date)) AS user_days,
        DATEDIFF('day', DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()), LEAST(CURRENT_TIMESTAMP(), LAST_DAY(CURRENT_TIMESTAMP()))) AS days_in_period
    FROM activity_events
    WHERE event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    GROUP BY domain
),

-- Feature breadth: distinct feature areas used per domain (current month)
feature_breadth AS (
    SELECT
        ud.domain,
        COUNT(DISTINCT
            CASE
                WHEN p.name = 'discovery' THEN 'Discovery'
                WHEN p.name IN ('saved_query', 'insights') THEN 'Insights'
                WHEN p.name IN ('glossary', 'term', 'category') THEN 'Governance'
                WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
                WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
                WHEN p.name = 'monitor' THEN 'Data Quality'
                ELSE NULL
            END
        ) AS features_used
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
      AND p.name IS NOT NULL
    GROUP BY ud.domain
),

-- Month-over-month retention
retention AS (
    SELECT
        curr.domain,
        COUNT(DISTINCT CASE WHEN prev.user_id IS NOT NULL THEN curr.user_id END) AS retained,
        COUNT(DISTINCT curr.user_id) AS prev_active
    FROM (
        SELECT DISTINCT user_id, domain
        FROM activity_events
        WHERE event_month = DATEADD('month', -1, DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()))
    ) curr
    LEFT JOIN (
        SELECT DISTINCT user_id
        FROM activity_events
        WHERE event_month = DATE_TRUNC('MONTH', CURRENT_TIMESTAMP())
    ) prev ON prev.user_id = curr.user_id
    GROUP BY curr.domain
)

SELECT
    ms.domain,
    tu.total_user_count,
    COALESCE(ms.current_mau, 0) AS current_mau,
    COALESCE(ms.prev_mau, 0) AS prev_mau,
    ROUND(COALESCE(ms.avg_mau_3m, 0), 0) AS avg_mau_3m,

    -- License utilization (0-100)
    ROUND(100.0 * COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0), 1) AS license_util_pct,

    -- Stickiness
    ROUND(COALESCE(s.user_days, 0) / NULLIF(COALESCE(s.current_month_mau, 1) * GREATEST(s.days_in_period, 1), 0), 3) AS stickiness,

    -- Feature breadth (out of 6 max)
    COALESCE(fb.features_used, 0) AS features_used,

    -- Retention rate
    ROUND(100.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 0), 1) AS retention_pct,

    -- Composite health score (0-100)
    ROUND(
        -- 25% license utilization
        25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
        -- 25% MAU trend (positive = good)
        + 25.0 * CASE
            WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5
            WHEN ms.current_mau >= ms.prev_mau THEN 1.0
            WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5
            ELSE 0.0
          END
        -- 25% feature breadth (out of 6)
        + 25.0 * COALESCE(fb.features_used, 0) / 6.0
        -- 25% retention
        + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1)
    , 0) AS health_score,

    CASE
        WHEN ROUND(
            25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
            + 25.0 * CASE WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5
                         WHEN ms.current_mau >= ms.prev_mau THEN 1.0
                         WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5
                         ELSE 0.0 END
            + 25.0 * COALESCE(fb.features_used, 0) / 6.0
            + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1)
        , 0) >= 70 THEN 'Healthy'
        WHEN ROUND(
            25.0 * LEAST(1.0, COALESCE(ms.current_mau, 0) / NULLIF(tu.total_user_count, 0))
            + 25.0 * CASE WHEN COALESCE(ms.prev_mau, 0) = 0 THEN 0.5
                         WHEN ms.current_mau >= ms.prev_mau THEN 1.0
                         WHEN ms.current_mau >= ms.prev_mau * 0.8 THEN 0.5
                         ELSE 0.0 END
            + 25.0 * COALESCE(fb.features_used, 0) / 6.0
            + 25.0 * COALESCE(r.retained, 0) / NULLIF(r.prev_active, 1)
        , 0) >= 40 THEN 'At Risk'
        ELSE 'Critical'
    END AS health_status

FROM mau_summary ms
JOIN total_users tu ON tu.domain = ms.domain
LEFT JOIN stickiness s ON s.domain = ms.domain
LEFT JOIN feature_breadth fb ON fb.domain = ms.domain
LEFT JOIN retention r ON r.domain = ms.domain
ORDER BY health_score DESC;
