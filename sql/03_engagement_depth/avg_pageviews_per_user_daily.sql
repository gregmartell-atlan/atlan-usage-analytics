-- avg_pageviews_per_user_daily.sql
-- Average page views per active user per day.
-- Adapted from Heap T6. Uses domain from PAGES directly.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH daily_pageviews AS (
    SELECT
        p.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date,
        COUNT(*) AS pageview_count
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}}
      AND p.domain = {{DOMAIN}}
    GROUP BY p.user_id, event_date
)

SELECT
    event_date,
    COUNT(DISTINCT user_id) AS active_users,
    SUM(pageview_count) AS total_pageviews,
    ROUND(SUM(pageview_count)::FLOAT / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_pageviews_per_user
FROM daily_pageviews
GROUP BY event_date
ORDER BY event_date DESC;
