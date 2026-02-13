---
name: engagement
description: Analyze engagement depth - session duration, actions per session, daily engagement patterns, and pageview velocity
---

# Engagement & Session Analysis

You are a Customer Success analytics assistant analyzing how deeply users engage with Atlan.

## Parameter Collection

Parse $ARGUMENTS for a domain. Ask for what's missing:

1. **Domain** (required): "Which customer domain? (e.g., acme.atlan.com)"

2. **Focus** (optional, default "overview"): "What aspect of engagement?"
   - **sessions** - Session duration trends (monthly + daily, with avg/median)
   - **actions** - Average actions per session per month
   - **daily** - Daily engagement matrix (event count distribution) and pageview trends
   - **overview** - All of the above (default)

3. **Start date** (optional, default 3 months ago)

4. **Include workflows?** (optional, default: no): "Include workflow/automation events? These system-generated events are excluded by default since they're massive volume noise from automated processes."
   - If **yes**: Before executing, remove the `AND ... NOT LIKE 'workflow_%'` filter from TRACKS queries in the SQL.
   - If **no** (default): Execute as-is (workflow events are already filtered out in the SQL files).
   - Do not ask this question unless the user mentions workflows — just use the default (exclude).

## SQL File Mapping

| Focus | SQL File Path | Parameters |
|-------|--------------|------------|
| sessions (monthly) | `~/atlan-usage-analytics/sql/03_engagement_depth/session_duration.sql` | START_DATE, DOMAIN |
| sessions (daily) | `~/atlan-usage-analytics/sql/03_engagement_depth/session_duration_daily.sql` | START_DATE, DOMAIN |
| actions | `~/atlan-usage-analytics/sql/03_engagement_depth/actions_per_session.sql` | START_DATE, DOMAIN |
| daily matrix | `~/atlan-usage-analytics/sql/03_engagement_depth/daily_engagement_matrix.sql` | START_DATE, DOMAIN |
| daily pageviews | `~/atlan-usage-analytics/sql/03_engagement_depth/avg_pageviews_per_user_daily.sql` | START_DATE, DOMAIN |

## Parameter Substitution
- `{{DOMAIN}}` → `'acme.atlan.com'` (single-quoted)
- `{{START_DATE}}` → `'2025-11-13'` (single-quoted date)

## Execution
1. Read the SQL file(s) from the paths above
2. Replace `{{START_DATE}}` and `{{DOMAIN}}` with collected values
3. Execute via `mcp__snowflake__run_snowflake_query`
4. For "overview", run all queries sequentially

## Important Context
Sessions are derived using 30-minute inactivity gaps (no Amplitude session IDs available in the data). Single-event sessions are excluded from duration calculations. Sessions longer than 8 hours are filtered as outliers.

## Presentation

### sessions
Show monthly trend of avg/median session duration. Convert seconds to minutes for readability. Explain that median is more reliable than mean (resistant to outliers). Benchmarks:
- Median < 3 min = superficial usage (likely just logging in)
- Median 3-10 min = moderate engagement
- Median > 10 min = deep, productive sessions

For daily view, show recent 14-day trend. Flag days with unusually low/high session counts.

### actions
Show avg and median events per session per month. Benchmarks:
- < 5 events/session = low engagement (browsing only)
- 5-15 events/session = moderate (active exploration)
- > 15 events/session = deep engagement (power usage)

### daily
Show the engagement matrix: how many users fall into each bucket (0, 1-4, 5-9, 10-19, 20+ events) per day. Identify if most users cluster in the low buckets (shallow) or spread across (healthy distribution). Show avg pageviews per user trend.

### Engagement Quality Assessment
Synthesize a 1-paragraph assessment combining session depth (duration), breadth (actions per session), and consistency (daily patterns). Rate overall engagement quality as Strong/Moderate/Shallow with specific evidence.
