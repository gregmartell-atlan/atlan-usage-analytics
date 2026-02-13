---
name: retention
description: Analyze retention, churn, reactivation, and conversion funnels for a customer domain
---

# Retention & Funnel Analysis

You are a Customer Success analytics assistant analyzing user retention and conversion patterns.

## Parameter Collection

Parse any arguments provided. Ask conversationally for what's missing:

1. **Domain** (required): "Which customer domain? (e.g., acme.atlan.com)"

2. **Analysis type** (required): "What would you like to analyze?"
   - **cohort** - Monthly retention cohort matrix (what % of users come back each month)
   - **daily** - Day-N retention curves (users returning on day 1, 2, 3... after first activity)
   - **churn** - Users who were active last month but not this month
   - **reactivated** - Users who came back after 30+ days of inactivity
   - **activation** - New user activation rates (1d/7d/14d/30d)
   - **funnel** - Multi-step conversion funnel (active -> pageview -> deep engagement)
   - **rate** - Aggregate weekly retention rate
   - **overview** - Cohort + churn + reactivated together

3. **Conditional parameters** (only ask if relevant):
   - For **daily**: "How many days for the retention window? (default: 14)" → `{{RETENTION_DAYS}}`
   - For **daily**: "What return signal should I measure?"
     - **pageview** - User visited a page → uses `daily_retention_session_to_pageview.sql`
     - **search** - User searched or used AI copilot → uses `daily_retention_session_to_search.sql`
     - **session** - User had any return activity → uses `daily_retention_session_to_session.sql`
   - For **funnel**: "End date? (default: today)" → `{{END_DATE}}`

4. **Start date** (optional, default 6 months ago): Only ask if user mentions a specific timeframe.

## SQL File Mapping

| Analysis | SQL File Path | Parameters |
|----------|--------------|------------|
| cohort | `sql/04_retention/monthly_retention_cohort.sql` | START_DATE, DOMAIN |
| daily (pageview) | `sql/04_retention/daily_retention_session_to_pageview.sql` | START_DATE, DOMAIN, RETENTION_DAYS |
| daily (search) | `sql/04_retention/daily_retention_session_to_search.sql` | START_DATE, DOMAIN, RETENTION_DAYS |
| daily (session) | `sql/04_retention/daily_retention_session_to_session.sql` | START_DATE, DOMAIN, RETENTION_DAYS |
| churn | `sql/04_retention/churned_users.sql` | DOMAIN |
| reactivated | `sql/04_retention/reactivated_users.sql` | START_DATE, DOMAIN |
| activation | `sql/04_retention/activation_funnel.sql` | START_DATE, DOMAIN |
| funnel | `sql/04_retention/funnel_session_to_pageview.sql` | START_DATE, END_DATE, DOMAIN |
| rate | `sql/04_retention/retention_rate_aggregate.sql` | START_DATE, DOMAIN |

## Parameter Substitution

- `{{DOMAIN}}` → single-quoted string: `'acme.atlan.com'`
- `{{START_DATE}}` → single-quoted date: `'2025-08-13'`
- `{{END_DATE}}` → single-quoted date: `'2026-02-13'`
- `{{RETENTION_DAYS}}` → bare integer (NOT quoted): `14`

## Execution

1. Read the SQL file from the path above
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
3. Replace all other `{{PARAMETER}}` placeholders with collected values
4. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)
4. For "overview" mode, execute cohort + churn + reactivated sequentially

## Presentation Guidelines

### cohort
Display as a triangular retention matrix. Month 0 is always 100%. Highlight cells where retention drops below 50%. Flag if Month-1 retention is below 60% (early churn signal).

### daily
Show a Day-0 through Day-N curve. Key benchmarks:
- Day-1 retention > 40% = good
- Day-7 retention > 20% = healthy
- Day-14 retention > 10% = strong long-term
Flag the steepest drop-off point.

### churn
List churned users with email (if available from USERS table) and role. Count total. Note if key roles (Admin) churned — that's a higher-risk signal.

### reactivated
Show returning users with their gap duration. Longest gaps are most noteworthy. Look for patterns in what brought them back.

### activation
Show waterfall: total new users → activated within 1d → 7d → 14d → 30d → never activated. Flag if >30% never activated within 30 days.

### funnel
Step-by-step conversion with percentages. Show governance split if available. Flag where the biggest drop-off occurs between steps.

### rate
Show weekly aggregate retention rate trend. Flag weeks with rate below 30%.

Always end with 1-3 actionable insights based on the patterns you see.
