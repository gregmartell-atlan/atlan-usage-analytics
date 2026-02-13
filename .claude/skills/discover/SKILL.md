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

## Discovery Queries

### Events (TRACKS table)
Run this query to show the event catalog:

```sql
WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
)
SELECT
    t.event_text,
    COUNT(*) AS total_occurrences,
    COUNT(DISTINCT t.user_id) AS unique_users,
    COUNT(DISTINCT ud.domain) AS domains_using,
    MIN(DATE(t.TIMESTAMP)) AS first_seen,
    MAX(DATE(t.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
INNER JOIN user_domains ud ON ud.user_id = t.user_id
WHERE t.event_text NOT IN (
    'workflows_run_ended', 'atlan_analaytics_aggregateinfo_fetch',
    'workflow_run_finished', 'workflow_step_finished', 'api_error_emit',
    'api_evaluator_cancelled', 'api_evaluator_succeeded', 'Experiment Started',
    '$experiment_started', 'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
    'performance_metric_user_timing_discovery_search',
    'performance_metric_user_timing_app_bootstrap',
    'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
)
AND t.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY t.event_text
ORDER BY total_occurrences DESC
```

If the user provided a **search term**, add this filter:
```sql
AND LOWER(t.event_text) LIKE '%<search_term>%'
```

If the user specified a **domain**, add:
```sql
AND ud.domain = '<domain>'
```

### Pages (PAGES table)
```sql
SELECT
    p.name AS page_name,
    COUNT(*) AS total_views,
    COUNT(DISTINCT p.user_id) AS unique_users,
    COUNT(DISTINCT p.domain) AS domains_using,
    MIN(DATE(p.TIMESTAMP)) AS first_seen,
    MAX(DATE(p.TIMESTAMP)) AS last_seen
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.name IS NOT NULL
  AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.name
ORDER BY total_views DESC
```

### Domains
```sql
SELECT
    p.domain,
    COUNT(DISTINCT p.user_id) AS total_users,
    COUNT(*) AS total_pageviews,
    MIN(DATE(p.TIMESTAMP)) AS first_activity,
    MAX(DATE(p.TIMESTAMP)) AS last_activity
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
WHERE p.domain IS NOT NULL
  AND p.TIMESTAMP >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY p.domain
ORDER BY total_users DESC
```

### Features (reference mapping, no query needed)
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

1. Replace `{{DATABASE}}` and `{{SCHEMA}}` in the inline SQL with values from CLAUDE.md Configuration
2. Construct the appropriate query based on the mode
3. Execute via the Snowflake MCP tool (see `SNOWFLAKE_MCP_TOOL` in CLAUDE.md Configuration)

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
