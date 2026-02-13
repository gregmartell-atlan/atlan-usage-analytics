---
name: discover
description: Browse available events, pages, domains, and features in the Atlan usage data - use this to find event names for other analytics skills
---

# Data Discovery

You are a data discovery assistant for Atlan usage analytics. Help the user explore what data is available so they can use it with other analytics skills (/health, /retention, /features, etc.).

## Mode Detection

Parse $ARGUMENTS to determine what to discover:
- "events" or "event" → Show available tracked events
- "pages" → Show available page names
- "domains" or "customers" → Show available customer domains
- "features" → Show feature area mappings
- A search term (anything else) → Search events and pages matching that term
- No arguments → Ask what they want to explore

## SQL File Mapping

| Discovery Mode | SQL File Path | Parameters |
|---------------|--------------|------------|
| events | `~/atlan-usage-analytics/sql/00_schema_profile/discover_events.sql` | DATABASE, SCHEMA |
| pages | `~/atlan-usage-analytics/sql/00_schema_profile/discover_pages.sql` | DATABASE, SCHEMA |
| domains | `~/atlan-usage-analytics/sql/00_schema_profile/discover_domains.sql` | DATABASE, SCHEMA |

## Optional Parameters

- **Include workflows?** (optional, default: no): "Include workflow/automation events? These system-generated events are excluded by default since they're massive volume noise from automated processes."
  - If **yes**: Before executing, remove the `AND ... NOT LIKE 'workflow_%'` filter from TRACKS queries in the SQL.
  - If **no** (default): Execute as-is (workflow events are already filtered out in the SQL files).
  - Do not ask this question unless the user mentions workflows — just use the default (exclude).

## Conditional Filters

After reading the SQL file, add these filters before the `GROUP BY` clause when applicable:

- **Search term** (when the user provides a search term for events): add `AND LOWER(t.event_text) LIKE '%<search_term>%'`
- **Domain filter** (when the user specifies a customer domain for events): add `AND ud.domain = '<domain>'`

## Features (reference mapping, no query needed)
Show this mapping directly:

| Feature Area | Page Names | Event Prefixes | Key Events |
|-------------|-----------|----------------|------------|
| Discovery/Search | `discovery` | `discovery_*` | `discovery_search_results` (search executed) |
| AI Copilot | — | `atlan_ai_*` | `atlan_ai_conversation_prompt_submitted` (AI query) |
| Governance | `glossary`, `term`, `category`, `classifications` | `governance_*`, `gtc_tree_*` | |
| Insights/SQL | `saved_query`, `insights` | `insights_*` | `insights_query_run` (query executed) |
| Chrome Extension | `reverse-metadata-sidebar` | `chrome_*` | |
| Asset Profile | `asset_profile`, `overview` | — | |
| Lineage | — | `lineage_*` | |
| Data Quality | `monitor` | — | |
| Admin | `users`, `personas`, `config`, `sso`, `api-access` | — | |
| Workflows | `workflows-home`, `workflows-profile`, `runs` | — | |

## Execution

1. Read the SQL file from the path above
2. Replace `{{DATABASE}}` and `{{SCHEMA}}` with values from the project configuration
3. Apply any conditional filters (search term, domain) before the `GROUP BY` clause
4. Execute via `mcp__snowflake__run_snowflake_query`
5. For search term mode, execute both the events and pages queries with the search filter applied

## Presentation

### Events
Show as a ranked table with columns: Event Name, Occurrences, Unique Users, Domains.
Group events by feature area prefix when possible:
- `discovery_*` → Discovery
- `governance_*` / `gtc_tree_*` → Governance
- `atlan_ai_*` → AI Copilot
- `lineage_*` → Lineage
- `chrome_*` → Chrome Extension
- `insights_*` → Insights

Highlight **high-signal events** that are useful for retention/funnel analysis:
- `discovery_search_results` — User performed a search
- `atlan_ai_conversation_prompt_submitted` — User used AI copilot
- `governance_policy_created` — User created a governance policy
- `insights_query_run` — User ran a SQL query

### Pages
Show ranked table. Map raw names to friendly names where possible.

### Domains
Show ranked table. Highlight the most active domains (highest user counts).

### Search Results
When the user searched for a term, show matching events AND pages. Suggest how to use the found events with other skills:
- "You can use `discovery_search_results` with `/retention daily` to measure search retention"
- "Use `/analyze show conversion from discovery_search_results to atlan_ai_conversation_prompt_submitted for acme.atlan.com`"

Always suggest next steps with other skills based on what the user discovered.
