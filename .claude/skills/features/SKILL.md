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

4. **Include workflows?** (optional, default: no): "Include workflow/automation events? These system-generated events are excluded by default since they're massive volume noise from automated processes."
   - If **yes**: Before executing, remove the `AND ... NOT LIKE 'workflow_%'` filter from TRACKS queries in the SQL.
   - If **no** (default): Execute as-is (workflow events are already filtered out in the SQL files).
   - Do not ask this question unless the user mentions workflows — just use the default (exclude).

## SQL File Mapping

| Analysis | SQL File Path | Parameters |
|----------|--------------|------------|
| top-pages | `~/atlan-usage-analytics/sql/02_feature_adoption/top_pages_by_domain.sql` | START_DATE, DOMAIN |
| top-events | `~/atlan-usage-analytics/sql/02_feature_adoption/top_events_by_domain.sql` | START_DATE, DOMAIN |
| matrix | `~/atlan-usage-analytics/sql/02_feature_adoption/feature_adoption_matrix.sql` | START_DATE, DOMAIN |
| trends | `~/atlan-usage-analytics/sql/02_feature_adoption/feature_trend_weekly.sql` | START_DATE, DOMAIN |
| connectors | `~/atlan-usage-analytics/sql/02_feature_adoption/connector_usage.sql` | START_DATE, DOMAIN |
| quadrant | `~/atlan-usage-analytics/sql/02_feature_adoption/feature_engagement_quadrant.sql` | START_DATE, DOMAIN |

## Parameter Substitution
- `{{DOMAIN}}` → `'acme.atlan.com'` (single-quoted)
- `{{START_DATE}}` → `'2025-11-13'` (single-quoted date)

## Execution
1. Read the SQL file from the path above
2. Replace `{{START_DATE}}` and `{{DOMAIN}}` with collected values
3. Execute via `mcp__snowflake__run_snowflake_query`

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
