# CS Usage Analytics - Claude Code Context

## Configuration

**Set these values for your Snowflake environment before using any skills or queries.**

| Variable | Value | Description |
|----------|-------|-------------|
| `DATABASE` | `LANDING` | Snowflake database name |
| `SCHEMA` | `FRONTEND_PROD` | Schema containing PAGES, TRACKS, USERS tables |
| `SNOWFLAKE_MCP_TOOL` | `mcp__snowflake__run_snowflake_query` | MCP tool name for query execution |

**Substitution rule**: All SQL files use `{{DATABASE}}.{{SCHEMA}}.TABLE` placeholders. Before executing any query, replace `{{DATABASE}}` and `{{SCHEMA}}` with the values above.

## Project Purpose
SQL query library for Customer Success usage analytics at Atlan.
Used by CS leadership to assess customer health, engagement, and adoption across tenants.
Queries are executed via Claude Code + Snowflake MCP (see Configuration above).

## Data Source
- **Snowflake Database**: Configured via `DATABASE` in Configuration above
- **Schema**: Configured via `SCHEMA` in Configuration above
- **Limitation**: If using a linked catalog — read-only, no DDL, no SHOW commands, no INFORMATION_SCHEMA

## Tables

### usage_analytics.PAGES
Page view events. ~3-4K rows/month.
- **Identity**: `user_id` (UUID), `domain` (always populated — primary domain source)
- **Timestamps**: `timestamp`, `received_at` (UTC stored, convert to IST)
- **Page context**: `name` (page name), `tab`, `path`, `url`, `category`
- **Asset context**: `asset_guid`, `type_name`, `connector_name`, `asset_type`
- **Empty columns**: `email`, `username`, `session_uuid`, `context_actions_amplitude_session_id` — always NULL

### usage_analytics.TRACKS
Event tracking. ~80K+ rows/month (mostly noise — ~12K real user events after filtering).
- **Identity**: `user_id` (UUID — no domain column, get domain via PAGES join)
- **Events**: `event`, `event_text` (use event_text for filtering)
- **Timestamps**: `timestamp`, `received_at`
- **Empty columns**: `context_actions_amplitude_session_id` — always NULL

### usage_analytics.USERS
Segment identify snapshots. 333 rows (one per identified user). NOT a sessions table.
- **Identity**: `id` (UUID — join key to `user_id`), `email`, `username`, `domain`
- **Metadata**: `role`, `license_type`, `job_role`, `personas`, `plan`
- **Sessions**: `session_uuid` (229/333 populated), `received_at` (last identify time)
- **Coverage**: Only ~2% of active PAGES/TRACKS user_ids match. Use for enrichment only, not filtering.
- **Dates**: `created_at` (only 14 rows populated), `tenant_created_at`

## Important Conventions

### Domain Filtering
**Use PAGES.domain** — it's the only reliable, always-populated domain source.
For TRACKS (which has no domain column), join to a user_domains lookup:
```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)
-- Then: INNER JOIN user_domains ud ON ud.user_id = t.user_id
```

### User Identity
**Use `user_id` as primary key**, not email. Most active users have no USERS record.
LEFT JOIN to USERS for optional enrichment:
```sql
LEFT JOIN (
    SELECT id, email, username, role, MAX(job_role) AS job_role
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    WHERE email IS NOT NULL
    GROUP BY id, email, username, role
) um ON um.id = <user_id>
```

### Noise Event Exclusions
Always filter these from TRACKS.event_text:
```
workflows_run_ended, workflow_run_finished, workflow_step_finished,
atlan_analaytics_aggregateinfo_fetch,
api_error_emit, api_evaluator_cancelled, api_evaluator_succeeded,
Experiment Started, $experiment_started,
web_vital_metric_inp_track, web_vital_metric_ttfb_track,
web_vital_metric_fcp_track, web_vital_metric_lcp_track,
performance_metric_user_timing_discovery_search,
performance_metric_user_timing_app_bootstrap
```

### Session Derivation
No session IDs are populated. Derive sessions from 30-minute inactivity gaps:
```sql
-- See sql/_shared/derived_sessions_cte.sql for full reusable CTE
-- Uses LAG() + DATEDIFF to detect gaps > 1800 seconds, SUM() OVER for session IDs
```

### Key Event Mappings
- **Search**: `event_text = 'discovery_search_results'`
- **AI copilot**: `event_text = 'atlan_ai_conversation_prompt_submitted'`
- **Governance**: `event_text LIKE 'governance_%' OR event_text LIKE 'gtc_tree_create_%'`
- **Navigation**: `event_text = 'main_navigation_button_clicked'`
- **Insights**: `event_text = 'insights_query_run'`

### Timezone
All timestamps stored as UTC. Convert to IST for display:
```sql
CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)
```

## Query Parameters
All queries use `{{PARAMETER}}` placeholders. Substitute before execution:
- `{{DATABASE}}` - Snowflake database (from Configuration above)
- `{{SCHEMA}}` - Schema name (from Configuration above)
- `{{DOMAIN}}` - Customer domain, e.g., `'acme.atlan.com'`
- `{{START_DATE}}` - Date literal, e.g., `'2025-01-01'`
- `{{END_DATE}}` - Date literal, e.g., `'2025-12-31'`
- `{{MONTHS_BACK}}` - Integer for lookback, e.g., `6`
- `{{RETENTION_DAYS}}` - Integer for daily retention window, e.g., `14`

For all-domain queries, replace `AND domain = {{DOMAIN}}` with `AND 1=1`.

## Execution Pattern
1. Read the `.sql` file from `sql/` directory
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from Configuration above
3. Replace other `{{parameters}}` with actual values
4. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in Configuration)
5. Note: Snowflake MCP does not support UNION at top level or SHOW commands

## Directory Layout
```
sql/00_schema_profile/ - Run first: table profiling and data availability checks
sql/_shared/           - Reusable CTE snippets (copy into WITH blocks)
sql/01_active_users/   - MAU, DAU, WAU, stickiness ratio
sql/02_feature_adoption/ - Page/event analytics, feature matrix
sql/03_engagement_depth/ - Sessions, power users, engagement tiers, daily engagement
sql/04_retention/      - Cohorts, churn, activation, reactivation, daily retention, funnels
sql/05_customer_health/ - Composite scores, domain summaries
sql/06_cs_review/      - QBR prep, multi-customer comparison, alerts
```

## Reference
- `models/usage_analytics.malloy` - Semantic model (reference only — check actual data with profiler)
- `sql/00_schema_profile/table_profiler.sql` - Run to verify current data availability
