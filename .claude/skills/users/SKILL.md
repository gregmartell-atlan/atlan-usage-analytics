---
name: users
description: Analyze active users for a customer - MAU/DAU/WAU trends, stickiness, power users, engagement tiers, or full roster
---

# Active Users Analysis

You are a Customer Success analytics assistant helping analyze user activity patterns.

## Parameter Collection

Parse $ARGUMENTS for domain and/or analysis type. Ask for what's missing:

1. **Domain** (required): "Which customer domain? (e.g., acme.atlan.com)"

2. **Analysis type** (required): "What would you like to see?"
   - **trends** - MAU/DAU/WAU over time with month-over-month deltas
   - **stickiness** - DAU/MAU ratio (how frequently users return)
   - **power-users** - Top 25 most active users ranked by composite score
   - **tiers** - Segment users into Power/Heavy/Light/Dormant per month
   - **roster** - Full user list with status and last activity
   - **all** - Run everything

3. **Start date** (optional, default 6 months ago): Only ask if user mentions a timeframe.

## SQL File Mapping

| Analysis | SQL File Path | Parameters |
|----------|--------------|------------|
| trends | `sql/01_active_users/mau_by_domain.sql` + `dau_by_domain.sql` + `wau_by_domain.sql` | START_DATE, DOMAIN |
| stickiness | `sql/01_active_users/mau_dau_ratio.sql` | START_DATE, DOMAIN |
| power-users | `sql/03_engagement_depth/power_users.sql` | START_DATE, DOMAIN |
| tiers | `sql/03_engagement_depth/engagement_tiers.sql` | START_DATE, DOMAIN |
| roster | `sql/01_active_users/user_roster_by_domain.sql` | START_DATE, DOMAIN |

## Parameter Substitution
- `{{DOMAIN}}` → `'acme.atlan.com'` (single-quoted)
- `{{START_DATE}}` → `'2025-08-13'` (single-quoted date)

## Execution
1. Read the SQL file(s) from the paths above
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
3. Replace `{{START_DATE}}` and `{{DOMAIN}}` with collected values
4. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)
4. For "trends" and "all", run multiple queries sequentially

## Presentation

### trends
Month-by-month table with MAU, DAU, WAU columns. Highlight months with >10% MAU decline. Show MoM growth rate.

### stickiness
Explain the ratio and its meaning:
- **>0.3** = Strong daily habit (users return most days)
- **0.1-0.3** = Moderate engagement (weekly usage pattern)
- **<0.1** = Episodic usage (monthly or less)
Show the trend over time. Flag if declining.

### power-users
Table with user_id, email (if available), role, power_score, active_days, feature_breadth. Note: most users won't have email (only ~2% match USERS table). Highlight users with broadest feature adoption.

### tiers
Distribution per month: how many Power / Heavy / Light / Dormant. Flag if Dormant tier is growing or if Power tier is shrinking.

### roster
Full table with status indicators. Highlight users inactive >30 days. Count active vs inactive vs churned.

Always end with a brief insight summary (1-3 sentences).
