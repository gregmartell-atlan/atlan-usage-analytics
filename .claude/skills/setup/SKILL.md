---
name: setup
description: Configure your Snowflake connection for the analytics skills - run this first after cloning the repo
---

# Setup — Full Snowflake + Analytics Configuration

You are a setup assistant for the CS Usage Analytics toolkit. Walk the user through the complete setup from zero to running queries — including MCP server installation, Snowflake connection, and query library configuration.

## Step 1: Check Current State

Before asking questions, check what's already configured:

1. Run `claude mcp list` to see if the `snowflake` MCP server is already installed
2. Check if `~/.snowflake/connections.toml` exists (Read tool)
3. Check if `~/.snowflake/service_config.yaml` exists (Read tool)
4. Check if `~/atlan-usage-analytics/CLAUDE.md` exists and whether it has placeholder values

Report what's already done and skip those steps. For example: "Looks like you already have the Snowflake MCP server and connection configured. Just need to wire up the query library."

## Step 2: Install Snowflake MCP Server (if needed)

If the `snowflake` MCP server is not listed:

1. Tell the user: "I need to install the Snowflake MCP server. This gives Claude Code the ability to run SQL queries directly."
2. Run:
   ```bash
   claude mcp add snowflake -s user -- uvx snowflake-labs-mcp
   ```
3. Confirm it was added: `claude mcp list`

**Note:** After adding a new MCP server, Claude Code needs to be restarted for it to take effect. If this is a fresh install, tell the user: "The Snowflake MCP server is installed, but you'll need to restart Claude Code (exit and reopen) for it to become available. After restarting, run `/setup` again and we'll pick up from where we left off."

## Step 3: Configure Snowflake Connection (if needed)

If `~/.snowflake/connections.toml` does not exist, collect these values from the user:

1. **Account ID** (required): "What is your Snowflake account identifier? (e.g., `QMVBCPT-ZIB00671` — find it in Snowflake under Admin > Accounts)"
2. **User email** (required): "What email do you use to log into Snowflake? (e.g., `jane.doe@atlan.com`)"
3. **Role** (optional, default `ACCOUNTADMIN`): "What Snowflake role should we use? Press enter for `ACCOUNTADMIN`."
4. **Warehouse** (optional, default `COMPUTE_WH`): "Which warehouse? Press enter for `COMPUTE_WH`."

Then create the file:

```bash
mkdir -p ~/.snowflake
```

Write `~/.snowflake/connections.toml`:
```toml
[my_example_connection]
account = "<ACCOUNT_ID>"
user = "<USER_EMAIL>"
authenticator = "externalbrowser"
role = "<ROLE>"
warehouse = "<WAREHOUSE>"
database = "LANDING"
schema = "FRONTEND_PROD"
```

**Important:** The connection name MUST be `my_example_connection` — that's what the MCP server looks for by default.

## Step 4: Configure Service Permissions (if needed)

If `~/.snowflake/service_config.yaml` does not exist, create it:

```yaml
search_services: []
analyst_services: []

other_services:
  object_manager: true
  query_manager: true

sql_statement_permissions:
  - Select: true
```

This enables read-only query access. Explain: "This config enables the query and object listing tools but restricts to SELECT-only for safety."

## Step 5: Configure Query Library

Ask the user for:

1. **Database** (required): "What is your Snowflake database name? (e.g., `LANDING`)"
2. **Schema** (required): "What is the schema containing the PAGES, TRACKS, and USERS tables? (e.g., `FRONTEND_PROD`)"

If the database/schema were already set in the connections.toml, offer those as defaults.

Then update `~/atlan-usage-analytics/CLAUDE.md`:
- Replace `YOUR_DATABASE` (or the current DATABASE value) with the user's database name
- Replace `YOUR_SCHEMA` (or the current SCHEMA value) with the user's schema name
- Remove any `— run /setup to configure` suffixes from the Description column if present

## Step 6: Validate Connection

Run a test query:
```sql
SELECT COUNT(*) AS row_count FROM <DATABASE>.<SCHEMA>.PAGES WHERE TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
```

Execute via `mcp__snowflake__run_snowflake_query`.

**If it succeeds:** "Connected! Found X page views in the last 7 days. Your analytics toolkit is ready."

**If it fails with auth error:** "Your browser should have opened for Snowflake SSO login. Please authenticate there, then I'll retry the query."

**If it fails with object not found:** "The database or schema might be wrong. Let me check what's available..." Then run `mcp__snowflake__list_objects` to help the user find the right names.

## Step 7: Post-Setup

After successful validation, display:

```
Setup complete! Here's what you can do:

  /discover domains     — See which customer domains are available
  /health <domain>      — Run a customer health check
  /features <domain>    — Feature adoption analysis
  /engagement <domain>  — Engagement depth analysis
  /retention <domain>   — Retention & churn analysis
  /users <domain>       — Active user trends
  /qbr <domain>         — QBR data pack
  /analyze <question>   — Ask anything in plain language

Tip: Start with /discover domains to see what data is available.
```

## Key Gotchas to Mention

When setup is complete, briefly warn about these known limitations:
- **No top-level UNION** — the MCP server rejects bare `UNION` statements; all the pre-built queries already handle this
- **SSO token expiry** — if queries start failing with auth errors later, just retry and the browser will re-prompt
- **Read-only** — the config is SELECT-only; if they need to create tables, they'll need to edit `service_config.yaml`
