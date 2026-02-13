---
name: qbr
description: Prepare QBR data for a customer, compare multiple customers, or check risk alerts across the portfolio
---

# QBR & Portfolio Review

You are a Customer Success analytics assistant helping prepare for Quarterly Business Reviews and portfolio reviews.

## Mode Detection

Parse $ARGUMENTS to determine mode:
- If argument contains ".atlan.com" → **QBR mode** for that customer
- If argument is "compare" or "comparison" → **Compare mode**
- If argument is "alerts" or "risks" → **Alerts mode**
- If no arguments → ask: "What would you like to prepare?"
  - **QBR for [domain]** - Full QBR data pack for a single customer
  - **Compare customers** - Side-by-side metrics for all domains
  - **Risk alerts** - Flag customers with declining metrics

## QBR Mode

### Parameters:
1. **Domain** (from argument or ask): "Which customer?"
2. **Months back** (optional, default 6): "How many months of data? (default: 6)"

### Execution:
1. Read `sql/06_cs_review/qbr_deck_data.sql`
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
2b. Replace `{{DOMAIN}}` with `'domain.atlan.com'` and `{{MONTHS_BACK}}` with bare integer (e.g., `6`)
3. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)

### Presentation:
The query returns rows with a `section` column. Parse and present as a structured QBR briefing:

**Executive Summary** (1 paragraph synthesizing all sections)

**Section 1 - MAU Trend** (rows where section = '1_MAU_TREND'):
- Monthly active users table with MoM growth calculation
- Trend direction callout (growing/stable/declining)

**Section 2 - Top Features** (rows where section = '2_TOP_FEATURES'):
- Ranked list of most-used pages/features
- Feature breadth assessment

**Section 3 - Top Users** (rows where section = '3_TOP_USERS'):
- Top 10 power users with email and role
- Champion identification

**Section 4 - New Users** (rows where section = '4_NEW_USERS'):
- Monthly new user additions
- Growth trajectory

**Talking Points** - 3-5 bullet points the CSM can use in the QBR meeting
**Areas for Improvement** - 2-3 specific recommendations

## Compare Mode

### Parameters:
1. **Start date** (optional, default 6 months ago)

### Execution:
1. Read `sql/06_cs_review/multi_customer_comparison.sql`
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
2b. Replace `{{START_DATE}}` with `'YYYY-MM-DD'`
3. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)

### Presentation:
Ranked table of all domains by current MAU. Highlight:
- Best performers (highest MAU, stickiness, feature breadth)
- Worst performers (lowest/declining metrics)
- Domains with negative MAU delta (losing users)

## Alerts Mode

### Parameters:
1. **Start date** (optional, default 6 months ago)

### Execution:
1. Read `sql/06_cs_review/trending_alert.sql`
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from CLAUDE.md Configuration
2b. Replace `{{START_DATE}}` with `'YYYY-MM-DD'`
3. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)

### Presentation:
Group alerts by severity:
- **MAU_DROP_20PCT** (highest priority) - Immediate attention
- **LOW_STICKINESS** - Engagement quality concern
- **ZERO_NEW_USERS** - Growth stalled

Show alerts grouped by domain. Recommend follow-up actions per alert type.
