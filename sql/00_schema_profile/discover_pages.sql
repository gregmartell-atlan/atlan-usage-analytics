-- discover_pages.sql
-- Page catalog: lists all tracked page names with usage stats.
-- Shows total views, unique users, and domains using each page.
--
-- Parameters:
--   {{DATABASE}} - Snowflake database name
--   {{SCHEMA}}   - Schema containing PAGES table

SELECT
    p.name AS page_name,
    COUNT(*) AS total_views,
    COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.domain) AS domains_using,
    MIN(DATE(p.TIMESTAMP)) AS first_seen,
    MAX(DATE(p.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.name IS NOT NULL
  AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.name
ORDER BY total_views DESC
