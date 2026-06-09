-- Top users in Atlan and the assets they viewed (MDLH USAGE_ANALYTICS, Snowflake)
-- Replace {{LAKEHOUSE_DB}}, {{DOMAIN}} (e.g. 'acme.atlan.com'), {{START_DATE}}, {{END_DATE}}
-- Note: USAGE_ANALYTICS refreshes once daily (not real-time).
-- Note: ~98% of active user_ids have no USERS row, so user_label is often a UUID, not an email.

WITH user_map AS (
  SELECT id, MAX(email) AS email, MAX(username) AS username, MAX(role) AS role
  FROM {{LAKEHOUSE_DB}}.USAGE_ANALYTICS.USERS
  WHERE email IS NOT NULL
  GROUP BY id
),
asset_views AS (
  SELECT
    COALESCE(u.email, p.user_id) AS user_label,
    u.username,
    u.role,
    p.asset_guid,
    MAX(p.name)           AS asset_name,
    MAX(p.type_name)      AS asset_type,
    MAX(p.connector_name) AS connector_name,
    COUNT(*)              AS view_count,
    MAX(p.timestamp)      AS last_viewed_at
  FROM {{LAKEHOUSE_DB}}.USAGE_ANALYTICS.PAGES p
  LEFT JOIN user_map u ON u.id = p.user_id
  WHERE p.domain = {{DOMAIN}}
    AND p.timestamp >= {{START_DATE}}
    AND p.timestamp <  {{END_DATE}}
    AND p.asset_guid IS NOT NULL
    -- exclude Atlan internal / support / automation accounts
    AND (u.email IS NULL OR (
             u.email NOT ILIKE '%automation%'
         AND u.email NOT IN ('atlansupport@atlan.com','support@atlan.com','hello@atlanhq.com')
    ))
  GROUP BY COALESCE(u.email, p.user_id), u.username, u.role, p.asset_guid
),
top_users AS (
  SELECT
    user_label, username, role,
    SUM(view_count)            AS total_views,
    COUNT(DISTINCT asset_guid) AS distinct_assets_viewed,
    MAX(last_viewed_at)        AS last_activity_at
  FROM asset_views
  GROUP BY user_label, username, role
)
SELECT
  tu.user_label, tu.username, tu.role,
  tu.total_views, tu.distinct_assets_viewed, tu.last_activity_at,
  av.asset_guid, av.asset_name, av.asset_type, av.connector_name,
  av.view_count AS views_on_asset, av.last_viewed_at,
  CONCAT('https://', {{DOMAIN}}, '/assets/', av.asset_guid) AS asset_link
FROM top_users tu
JOIN asset_views av ON tu.user_label = av.user_label
QUALIFY DENSE_RANK() OVER (ORDER BY tu.total_views DESC) <= 25
ORDER BY tu.total_views DESC, av.view_count DESC, av.last_viewed_at DESC;
