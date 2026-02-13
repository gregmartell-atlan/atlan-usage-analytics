-- top_pages_by_domain.sql
-- Most visited Atlan pages per customer domain, ranked by usage.
-- Includes tab-level detail for sub-page analysis.
--
-- Pattern: Schema-aware (domain from PAGES directly, user_id as identity key).
-- USERS table is not required since PAGES has domain natively.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com' (or replace filter with 1=1 for all)

SELECT
    p.domain,
    p.name AS page_name,
    p.tab,
    COUNT(*) AS page_views,
    COUNT(DISTINCT p.user_id) AS unique_users,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT p.user_id), 0), 1) AS views_per_user
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.TIMESTAMP >= {{START_DATE}}
  AND p.name IS NOT NULL
  AND p.domain IS NOT NULL
  AND p.domain = {{DOMAIN}}
GROUP BY p.domain, p.name, p.tab
ORDER BY page_views DESC
LIMIT 50;
