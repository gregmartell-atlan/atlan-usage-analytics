-- discover_domains.sql
-- Domain catalog: lists all customer domains with activity stats.
-- Shows total users, pageviews, and activity date range per domain.
--
-- Parameters:
--   {{DATABASE}} - Snowflake database name
--   {{SCHEMA}}   - Schema containing PAGES table

SELECT
    p.domain,
    COUNT(DISTINCT p.user_id) AS total_users,
    COUNT(*) AS total_pageviews,
    MIN(DATE(p.TIMESTAMP)) AS first_activity,
    MAX(DATE(p.TIMESTAMP)) AS last_activity
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.domain IS NOT NULL
  AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.domain
ORDER BY total_users DESC
