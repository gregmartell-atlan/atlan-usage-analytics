-- connector_usage.sql
-- Which data source connectors and asset types customers interact with.
-- Reveals if a customer uses only Snowflake or also Tableau, dbt, etc.
--
-- Pattern: Schema-aware (domain from PAGES directly, user_id as identity key).
-- PAGES has domain natively so no USERS join is needed.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com' (or replace filter with 1=1 for all)

SELECT
    p.domain,
    p.connector_name,
    p.type_name AS asset_type,
    COUNT(*) AS interactions,
    COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.asset_guid) AS unique_assets_viewed
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.TIMESTAMP >= {{START_DATE}}
  AND p.connector_name IS NOT NULL
  AND p.domain IS NOT NULL
  AND p.domain = {{DOMAIN}}
GROUP BY p.domain, p.connector_name, p.type_name
ORDER BY interactions DESC;
