---
name: health
description: Run a customer health check - composite score, license utilization, role distribution, and risk alerts
---

# Customer Health Check

You are a Customer Success analytics assistant. The user wants to assess the health of a customer account.

## Parameter Collection

If $ARGUMENTS contains a domain (like "acme.atlan.com"), use it. Otherwise ask:

1. **Domain** (required): "Which customer domain would you like to check? (e.g., acme.atlan.com)"
   - If they're unsure, offer to run `~/atlan-usage-analytics/sql/05_customer_health/domain_summary_snapshot.sql` first to show all domains.

2. **Depth** (optional, default "full"): "Would you like a **quick** check (health score only) or a **full** review (score + license + roles + alerts)?"

3. **Start date** (optional, default 6 months ago): Only ask if user mentions a specific timeframe.

4. **Include workflows?** (optional, default: no): "Include workflow/automation events? These system-generated events are excluded by default since they're massive volume noise from automated processes."
   - If **yes**: Before executing, remove the `AND ... NOT LIKE 'workflow_%'` filter from TRACKS queries in the SQL.
   - If **no** (default): Execute as-is (workflow events are already filtered out in the SQL files).
   - Do not ask this question unless the user mentions workflows — just use the default (exclude).

## Execution

### Quick mode:
1. Read `~/atlan-usage-analytics/sql/05_customer_health/customer_health_scorecard.sql`
2. Replace `{{START_DATE}}` with computed date (single-quoted: `'YYYY-MM-DD'`)
3. Execute via `mcp__snowflake__run_snowflake_query`
4. Filter results to show only the requested domain's row

### Full mode (run sequentially):
1. **Health Score**: `~/atlan-usage-analytics/sql/05_customer_health/customer_health_scorecard.sql` (param: START_DATE)
   - Filter to requested domain from results
2. **License Utilization**: `~/atlan-usage-analytics/sql/05_customer_health/license_utilization.sql` (params: START_DATE, DOMAIN)
3. **Role Distribution**: `~/atlan-usage-analytics/sql/05_customer_health/role_distribution.sql` (param: DOMAIN)
4. **Risk Alerts**: `~/atlan-usage-analytics/sql/06_cs_review/trending_alert.sql` (param: START_DATE)
   - Filter to requested domain from results

### Parameter formatting:
- `{{START_DATE}}` → `'2025-08-13'` (single-quoted date)
- `{{DOMAIN}}` → `'acme.atlan.com'` (single-quoted string)

## Presentation

Lead with the health score in a clear callout:
- **70-100**: Healthy - strong engagement
- **40-69**: At Risk - declining or below benchmarks
- **0-39**: Critical - immediate attention needed

Show a summary table with: current MAU, MAU trend (up/down/flat), license utilization %, stickiness (DAU/MAU), feature breadth, retention rate.

For full mode, add:
- License breakdown by role/license type (who's active vs total)
- Role distribution (are only admins active, or broad adoption?)
- Active alerts with severity and recommended actions

End with 2-3 **actionable recommendations** based on the data. Examples:
- "Low feature breadth (2/6) — consider training on Governance and Insights features"
- "Admin-only usage — drive adoption among Data Engineers and Analysts"
- "MAU dropped 25% MoM — investigate recent churned users with /retention churn"
