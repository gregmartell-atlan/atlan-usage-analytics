# Atlan Usage Analytics

SQL query library and Claude Code skills for Atlan usage analytics. Works with any Snowflake instance where the Atlan Lakehouse is configured (PAGES, TRACKS, USERS tables).

## Prerequisites

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed
- Snowflake account with access to your usage analytics data
- Snowflake MCP server configured in Claude Code

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/gregmartell-atlan/atlan-usage-analytics.git
cd atlan-usage-analytics
```

### 2. Configure Snowflake MCP

Add to your Claude Code MCP settings (`~/.claude/mcp.json` or via Claude Code settings):

```json
{
  "mcpServers": {
    "snowflake": {
      "command": "npx",
      "args": ["-y", "@anthropic/snowflake-mcp-server"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "<your-account>",
        "SNOWFLAKE_USER": "<your-user>",
        "SNOWFLAKE_PASSWORD": "<your-password>",
        "SNOWFLAKE_WAREHOUSE": "<your-warehouse>"
      }
    }
  }
}
```

### 3. Configure your database

Edit `CLAUDE.md` and update the **Configuration** table at the top:

| Variable | What to set |
|----------|-------------|
| `DATABASE` | Your Snowflake database name (e.g., `MY_DATABASE`) |
| `SCHEMA` | Your schema containing PAGES, TRACKS, USERS (e.g., `usage_analytics`) |
| `SNOWFLAKE_MCP_TOOL` | Your MCP tool name (default: `mcp__snowflake__run_snowflake_query`) |

Or simply run `/setup` in Claude Code and it will walk you through it.

### 4. Verify data access

Open Claude Code in this directory and run:

```
/analyze table profiler
```

This executes `table_profiler.sql` to verify connectivity and data shape.

## Skills (Slash Commands)

| Skill | Description | Example |
|-------|-------------|---------|
| `/setup` | Configure Snowflake connection — run this first | `/setup` |
| `/analyze` | General analytics — finds the right query or writes custom SQL | `/analyze MAU for acme.atlan.com` |
| `/discover` | Browse available events, pages, domains | `/discover events` |
| `/users` | Active users — MAU/DAU/WAU, stickiness, power users | `/users trends acme.atlan.com` |
| `/features` | Feature adoption — pages, events, matrix, quadrant | `/features quadrant acme.atlan.com` |
| `/engagement` | Session depth, actions per session, daily patterns | `/engagement acme.atlan.com` |
| `/retention` | Cohorts, churn, reactivation, funnels | `/retention cohort acme.atlan.com` |
| `/health` | Customer health score, license, roles, alerts | `/health acme.atlan.com` |
| `/qbr` | QBR data pack, multi-customer compare, risk alerts | `/qbr acme.atlan.com` |

## Data Architecture

| Table | What | Volume | Key columns |
|-------|------|--------|-------------|
| **PAGES** | Page views | ~3-4K/month | `user_id`, `domain`, `name`, `timestamp` |
| **TRACKS** | Events | ~80K/month (12K after noise filter) | `user_id`, `event_text`, `timestamp` |
| **USERS** | Identify snapshots | ~333 rows | `id`, `email`, `role`, `domain` (~2% match rate) |

**Key**: `PAGES.domain` is the primary domain source. `user_id` is the primary identity key. USERS provides optional email/role enrichment via LEFT JOIN.

> **Note**: System-generated workflow events (100K+/month) are excluded by default. Use the "Include workflows?" option in any skill to include them.

## Query Categories

### 00 - Schema Profile
| Query | Description |
|-------|-------------|
| `table_profiler` | Data availability check: row counts, column fill rates, user overlap |
| `discover_events` | Event catalog — all tracked events with usage stats |
| `discover_pages` | Page catalog — all page names with view counts |
| `discover_domains` | Domain catalog — all customer domains with activity levels |

### 01 - Active Users
| Query | Description |
|-------|-------------|
| `mau_by_domain` | Monthly active users per customer domain with MoM delta |
| `dau_by_domain` | Daily active users per domain |
| `wau_by_domain` | Weekly active users per domain |
| `mau_dau_ratio` | DAU/MAU stickiness ratio (>0.3 = strong, <0.1 = episodic) |
| `user_roster_by_domain` | Full user list with last activity, total events, status |

### 02 - Feature Adoption
| Query | Description |
|-------|-------------|
| `top_pages_by_domain` | Most visited Atlan pages per domain |
| `top_events_by_domain` | Most frequent tracked events per domain |
| `feature_adoption_matrix` | Feature-by-user boolean matrix per month |
| `feature_trend_weekly` | Week-over-week feature usage trends |
| `feature_engagement_quadrant` | Feature reach vs depth analysis (engagement matrix) |
| `connector_usage` | Data source/connector interaction patterns |

### 03 - Engagement Depth
| Query | Description |
|-------|-------------|
| `session_duration` | Session length analysis per domain (monthly, time-gap derived) |
| `session_duration_daily` | Daily session duration (avg/median in seconds) |
| `power_users` | Top users by composite activity score |
| `actions_per_session` | Average events/pages per session |
| `engagement_tiers` | Power/Heavy/Light/Dormant user segmentation |
| `daily_engagement_matrix` | Daily event counts bucketed into engagement tiers |
| `avg_pageviews_per_user_daily` | Average page views per active user per day |

### 04 - Retention
| Query | Description |
|-------|-------------|
| `monthly_retention_cohort` | Cohort retention matrix |
| `activation_funnel` | New user activation rates (1d/7d/14d/30d) |
| `churned_users` | Users lost month-over-month with context |
| `reactivated_users` | Users who returned after 30+ day gap |
| `daily_retention_session_to_pageview` | Day-N retention: activity to pageview (14-day window) |
| `daily_retention_session_to_search` | Day-N retention: activity to search/AI action |
| `daily_retention_session_to_session` | Day-N retention: activity to return activity |
| `retention_rate_aggregate` | Aggregate retention rate: activity to pageview within 7 days |
| `funnel_session_to_pageview` | Multi-step funnel with governance split |

### 05 - Customer Health
| Query | Description |
|-------|-------------|
| `customer_health_scorecard` | Composite 0-100 health score per domain |
| `domain_summary_snapshot` | One-row summary per domain, latest month |
| `license_utilization` | Active vs total users by domain/role |
| `role_distribution` | Role and job_role breakdown per domain |

### 06 - CS Review
| Query | Description |
|-------|-------------|
| `qbr_deck_data` | All-in-one QBR data pull for a single customer |
| `multi_customer_comparison` | Side-by-side domain comparison |
| `trending_alert` | Proactive risk flags (declining MAU, low stickiness) |

## Parameters

All SQL files use `{{placeholders}}`. Claude Code substitutes these automatically when using skills. For manual execution:

- `{{DATABASE}}` - Your Snowflake database name
- `{{SCHEMA}}` - Your schema name
- `{{DOMAIN}}` - e.g., `'acme.atlan.com'`
- `{{START_DATE}}` - e.g., `'2025-01-01'`
- `{{END_DATE}}` - e.g., `'2025-12-31'`
- `{{MONTHS_BACK}}` - e.g., `6`
- `{{RETENTION_DAYS}}` - e.g., `14` (daily retention window)

## Directory Layout

```
CLAUDE.md                      - Configuration + context (edit this first)
sql/00_schema_profile/         - Data availability checks
sql/_shared/                   - Reusable CTE snippets
sql/01_active_users/           - MAU, DAU, WAU, stickiness
sql/02_feature_adoption/       - Pages, events, feature matrix
sql/03_engagement_depth/       - Sessions, power users, engagement tiers
sql/04_retention/              - Cohorts, churn, activation, funnels
sql/05_customer_health/        - Composite scores, domain summaries
sql/06_cs_review/              - QBR prep, comparison, alerts
.claude/skills/                - Claude Code skill definitions
models/                        - Semantic model (reference only)
```
