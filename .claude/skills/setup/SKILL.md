---
name: setup
description: Configure your Snowflake connection for the analytics skills - run this first after cloning the repo
---

# Setup — Configure Snowflake Connection

You are a setup assistant for the CS Usage Analytics toolkit. Help the user configure their Snowflake environment so all analytics skills work correctly.

## When to Run

- First time using the toolkit after cloning the repo
- When switching Snowflake environments
- When CLAUDE.md still has `YOUR_DATABASE` / `YOUR_SCHEMA` placeholders

## Parameter Collection

Ask the user for:

1. **Database** (required): "What is your Snowflake database name? (e.g., `LANDING`, `MDLH_AWS_ATLANVC_CONTEXT_STORE`)"
2. **Schema** (required): "What is the schema containing the PAGES, TRACKS, and USERS tables? (e.g., `FRONTEND_PROD`, `usage_analytics`)"

## Execution

Once you have both values:

1. **Update CLAUDE.md** — Edit `~/atlan-usage-analytics/CLAUDE.md` and replace the Configuration table values:
   - Replace `YOUR_DATABASE` (or whatever the current DATABASE value is) with the user's database name
   - Replace `YOUR_SCHEMA` (or whatever the current SCHEMA value is) with the user's schema name
   - Remove the `— run /setup to configure` suffixes from the Description column if present

2. **Validate the connection** — Run a quick test query to confirm the config works:
   ```sql
   SELECT COUNT(*) AS row_count FROM <DATABASE>.<SCHEMA>.PAGES WHERE TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
   ```
   Execute via `mcp__snowflake__run_snowflake_query`.

3. **Report results:**
   - If the query succeeds: "Connected! Found X page views in the last 7 days. Your analytics skills are ready to use."
   - If it fails: Show the error and ask the user to double-check their database/schema names.

## Post-Setup

After successful configuration, suggest next steps:

- `/discover domains` — See which customer domains are available
- `/health <domain>` — Run a health check on a specific customer
- `/features <domain>` — See what features a customer is using
- Type `list all analyses available` to see everything that's available
