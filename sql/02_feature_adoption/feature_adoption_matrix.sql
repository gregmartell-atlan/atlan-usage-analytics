-- feature_adoption_matrix.sql
-- Feature-by-user boolean matrix per month.
-- Maps raw page names and events into logical feature areas.
--
-- Pattern: Schema-aware (domain from PAGES directly, user_id as identity key).
-- LEFT JOINs to USERS only for optional metadata (email, username, role).
--
-- Feature area mapping (derived from data exploration):
--   Discovery/Search: pages=discovery, events=discovery_search_*, discovery_filter_*, discovery_asset_*
--   Chrome Extension:  pages=reverse-metadata-sidebar, events=chrome_reverse_*
--   Insights/SQL:      pages=saved_query, events=insights_*
--   Governance:        pages=glossary/term/category/classifications, events=governance_*, gtc_tree_*
--   AI Copilot:        events=atlan_ai_*
--   Lineage:           events=lineage_*
--   Asset Profile:     pages=asset_profile/overview
--   Admin:             pages=users/personas/config/sso/api-access/api_keys/policyManager
--   Workflows:         pages=workflows-home/workflows-profile/runs
--   Data Quality:      pages=monitor
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

page_features AS (
    SELECT
        p.user_id,
        p.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS activity_month,
        CASE
            WHEN p.name = 'discovery' THEN 'Discovery'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name = 'saved_query' THEN 'Insights/SQL'
            WHEN p.name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name IN ('users', 'personas', 'config', 'sso', 'api-access', 'api_keys', 'policyManager', 'manage') THEN 'Admin'
            WHEN p.name IN ('workflows-home', 'workflows-profile', 'runs', 'playbook') THEN 'Workflows'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            WHEN p.name = 'home' THEN 'Home'
            WHEN p.name = 'marketplace' THEN 'Marketplace'
            WHEN p.name = 'insights' THEN 'Insights/SQL'
            WHEN p.name = 'inbox' THEN 'Notifications'
            WHEN p.name = 'usage' THEN 'Usage Analytics'
            ELSE 'Other'
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.TIMESTAMP >= {{START_DATE}} AND p.name IS NOT NULL
),

event_features AS (
    SELECT
        t.user_id,
        ud.domain,
        DATE_TRUNC('MONTH', CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS activity_month,
        CASE
            WHEN t.event_text LIKE 'discovery_%' THEN 'Discovery'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
            WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            WHEN t.event_text LIKE 'products_home_%' THEN 'Data Products'
            ELSE NULL
        END AS feature_area
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
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
),

all_features AS (
    SELECT user_id, domain, activity_month, feature_area FROM page_features WHERE feature_area != 'Other'
    UNION
    SELECT user_id, domain, activity_month, feature_area FROM event_features WHERE feature_area IS NOT NULL
)

SELECT
    af.domain,
    af.activity_month,
    af.user_id,
    um.email,
    um.username,
    um.role,
    MAX(CASE WHEN af.feature_area = 'Discovery' THEN 1 ELSE 0 END) AS used_discovery,
    MAX(CASE WHEN af.feature_area = 'Chrome Extension' THEN 1 ELSE 0 END) AS used_chrome_ext,
    MAX(CASE WHEN af.feature_area = 'Insights/SQL' THEN 1 ELSE 0 END) AS used_insights,
    MAX(CASE WHEN af.feature_area = 'Governance' THEN 1 ELSE 0 END) AS used_governance,
    MAX(CASE WHEN af.feature_area = 'AI Copilot' THEN 1 ELSE 0 END) AS used_ai_copilot,
    MAX(CASE WHEN af.feature_area = 'Lineage' THEN 1 ELSE 0 END) AS used_lineage,
    MAX(CASE WHEN af.feature_area = 'Asset Profile' THEN 1 ELSE 0 END) AS used_asset_profile,
    MAX(CASE WHEN af.feature_area = 'Admin' THEN 1 ELSE 0 END) AS used_admin,
    MAX(CASE WHEN af.feature_area = 'Workflows' THEN 1 ELSE 0 END) AS used_workflows,
    MAX(CASE WHEN af.feature_area = 'Data Quality' THEN 1 ELSE 0 END) AS used_data_quality,
    MAX(CASE WHEN af.feature_area = 'Data Products' THEN 1 ELSE 0 END) AS used_data_products,
    MAX(CASE WHEN af.feature_area = 'Marketplace' THEN 1 ELSE 0 END) AS used_marketplace
FROM all_features af
LEFT JOIN (
    SELECT id, email, username, role, MAX(job_role) AS job_role
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    WHERE email IS NOT NULL
    GROUP BY id, email, username, role
) um ON um.id = af.user_id
WHERE af.domain = {{DOMAIN}}
GROUP BY af.domain, af.activity_month, af.user_id, um.email, um.username, um.role
ORDER BY af.activity_month DESC, af.user_id;
