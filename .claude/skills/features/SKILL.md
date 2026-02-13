---
name: features
description: Analyze feature adoption for a customer - top pages, top events, feature matrix, weekly trends, connector usage, or engagement quadrant
---

# Feature Adoption Analysis

You are a Customer Success analytics assistant helping understand which Atlan features customers use.

## Parameter Collection

Parse $ARGUMENTS for domain and analysis type. Ask for what's missing:

1. **Domain** (required): "Which customer domain? (e.g., acme.atlan.com)"

2. **Analysis type** (required): "What would you like to explore?"
   - **top-pages** - Most visited Atlan pages ranked by usage
   - **top-events** - Most frequent tracked actions (noise-filtered)
   - **matrix** - Feature adoption matrix per user per month (who uses what)
   - **trends** - Week-over-week feature usage trends
   - **connectors** - Which data source connectors they interact with
   - **quadrant** - Feature engagement quadrant: reach (unique users) vs depth (avg events/user)
   - **all** - Run everything

3. **Start date** (optional, default 3 months ago)

## SQL File Mapping

| Analysis | SQL File Path | Parameters |
|----------|--------------|------------|
| top-pages | `sql/02_feature_adoption/top_pages_by_domain.sql` | START_DATE, DOMAIN |
| top-events | `sql/02_feature_adoption/top_events_by_domain.sql` | START_DATE, DOMAIN |
| matrix | `sql/02_feature_adoption/feature_adoption_matrix.sql` | START_DATE, DOMAIN |
| trends | `sql/02_feature_adoption/feature_trend_weekly.sql` | START_DATE, DOMAIN |
| connectors | `sql/02_feature_adoption/connector_usage.sql` | START_DATE, DOMAIN |
| quadrant | Custom SQL (inline below) | START_DATE, DOMAIN |

## Parameter Substitution
- `{{DOMAIN}}` → `'acme.atlan.com'` (single-quoted)
- `{{START_DATE}}` → `'2025-11-13'` (single-quoted date)

## Execution
1. Read the SQL file from the path above (or use inline SQL for quadrant)
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
3. Replace `{{START_DATE}}` and `{{DOMAIN}}` with collected values
4. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)

## Presentation

### top-pages
Ranked table. Map raw page names to friendly names:
- discovery = "Search/Discovery"
- asset_profile = "Asset Profile"
- glossary/term/category = "Business Glossary (Governance)"
- saved_query/insights = "SQL Insights"
- reverse-metadata-sidebar = "Chrome Extension"
- monitor = "Data Quality"
- home = "Home"
- workflows-home = "Workflows"

### top-events
Ranked table. Group events by feature area prefix:
- `discovery_*` = Discovery/Search
- `governance_*` / `gtc_tree_*` = Governance
- `atlan_ai_*` = AI Copilot
- `lineage_*` = Lineage
- `chrome_*` = Chrome Extension
- `insights_*` = Insights

### matrix
Show as a user-by-feature table with checkmarks. Calculate "feature breadth" per user (how many features each user touches). Identify single-feature users vs multi-feature power users.

### trends
Time series by feature area. Flag features with declining week-over-week usage. Highlight growing features.

### connectors
Table by connector_name and asset_type. Reveals the customer's tech stack (Snowflake, Tableau, dbt, etc.).

### quadrant
Feature engagement quadrant — plots each feature by **reach** (unique users, x-axis) vs **depth** (avg events per user, y-axis). Inspired by Heap's engagement matrix.

**Inline SQL** (replace `{{START_DATE}}` and `{{DOMAIN}}`):
```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

page_events AS (
    SELECT p.user_id, CASE
            WHEN p.name = 'discovery' THEN 'Discovery/Search'
            WHEN p.name = 'reverse-metadata-sidebar' THEN 'Chrome Extension'
            WHEN p.name IN ('saved_query', 'insights') THEN 'Insights/SQL'
            WHEN p.name IN ('glossary', 'term', 'category', 'classifications', 'custom_metadata') THEN 'Governance'
            WHEN p.name IN ('asset_profile', 'overview') THEN 'Asset Profile'
            WHEN p.name = 'monitor' THEN 'Data Quality'
            WHEN p.name IN ('workflows-home', 'workflows-profile', 'runs', 'playbook') THEN 'Workflows'
            WHEN p.name = 'home' THEN 'Home'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    WHERE p.domain = {{DOMAIN}}
      AND p.TIMESTAMP >= {{START_DATE}}
      AND p.name IS NOT NULL
),

track_events AS (
    SELECT t.user_id, CASE
            WHEN t.event_text LIKE 'discovery_search%' THEN 'Discovery/Search'
            WHEN t.event_text LIKE 'chrome_%' THEN 'Chrome Extension'
            WHEN t.event_text LIKE 'insights_%' THEN 'Insights/SQL'
            WHEN t.event_text LIKE 'governance_%' OR t.event_text LIKE 'gtc_tree_%' THEN 'Governance'
            WHEN t.event_text LIKE 'atlan_ai_%' THEN 'AI Copilot'
            WHEN t.event_text LIKE 'lineage_%' THEN 'Lineage'
            WHEN t.event_text LIKE 'discovery_metadata_%' THEN 'Metadata Curation'
            WHEN t.event_text = 'main_navigation_button_clicked' THEN 'Navigation'
            ELSE NULL END AS feature
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id AND ud.domain = {{DOMAIN}}
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND t.event_text NOT IN (
          'workflows_run_ended', 'atlan_analaytics_aggregateinfo_fetch',
          'workflow_run_finished', 'workflow_step_finished', 'api_error_emit',
          'api_evaluator_cancelled', 'api_evaluator_succeeded', 'Experiment Started',
          '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search',
          'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
      )
),

combined AS (
    SELECT user_id, feature FROM page_events WHERE feature IS NOT NULL
    UNION ALL
    SELECT user_id, feature FROM track_events WHERE feature IS NOT NULL
),

per_user AS (
    SELECT feature, user_id, COUNT(*) AS events
    FROM combined
    GROUP BY feature, user_id
)

SELECT
    feature,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(events) AS total_events,
    ROUND(AVG(events), 1) AS avg_events_per_user,
    ROUND(MEDIAN(events), 1) AS median_events_per_user
FROM per_user
GROUP BY feature
ORDER BY unique_users DESC
```

**Presentation**: Draw an ASCII scatter plot with features positioned by reach vs depth. Divide into 4 quadrants using median unique_users (x) and median avg_events_per_user (y) as dividers:
- **Top-right** (More users, higher usage): Core power features — high reach AND depth
- **Bottom-right** (More users, lower usage): Broadly reached but shallow — enablement opportunity for deeper use
- **Top-left** (Fewer users, higher usage): Niche power-user tools — expand reach
- **Bottom-left** (Fewer users, lower usage): Adoption gaps — biggest enablement opportunity

Also show the data as a table with columns: Feature, Unique Users, Total Events, Avg/User, Median/User, Quadrant.

Highlight actionable insights: which features are underperforming on reach vs depth, and where enablement would have the most impact.

### Feature Gaps Callout
Always include a "Feature Gaps" section: which of the 6 core feature areas are NOT being used?
Core areas: Discovery, Insights/SQL, Governance, Asset Profile, Chrome Extension, Data Quality.
Suggest training or enablement for unused features.
