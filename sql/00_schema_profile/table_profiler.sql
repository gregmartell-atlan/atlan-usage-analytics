-- table_profiler.sql
-- Run these queries to understand what data is available.
-- Each section is a standalone query. Run them in order.
--
-- No parameters required.


-- ============================================================
-- SECTION 1: TRACKS - shape, date range, key column fill rates
-- ============================================================
SELECT
    'TRACKS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    MIN(TIMESTAMP) AS earliest_event,
    MAX(TIMESTAMP) AS latest_event,
    COUNT(DISTINCT DATE(TIMESTAMP)) AS days_with_data,
    COUNT(context_actions_amplitude_session_id) AS has_amplitude_session_id,
    COUNT(DISTINCT event_text) AS distinct_event_types
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS;


-- ============================================================
-- SECTION 2: PAGES - shape, date range, key column fill rates
-- ============================================================
SELECT
    'PAGES' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    MIN(TIMESTAMP) AS earliest_event,
    MAX(TIMESTAMP) AS latest_event,
    COUNT(DISTINCT DATE(TIMESTAMP)) AS days_with_data,
    COUNT(domain) AS has_domain,
    COUNT(email) AS has_email,
    COUNT(session_uuid) AS has_session_uuid,
    COUNT(context_actions_amplitude_session_id) AS has_amplitude_session_id,
    COUNT(DISTINCT name) AS distinct_page_names
FROM {{DATABASE}}.{{SCHEMA}}.PAGES;


-- ============================================================
-- SECTION 3: USERS - shape, date range, key column fill rates
-- ============================================================
SELECT
    'USERS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT id) AS distinct_users,
    MIN(received_at) AS earliest_received,
    MAX(received_at) AS latest_received,
    COUNT(email) AS has_email,
    COUNT(role) AS has_role,
    COUNT(session_uuid) AS has_session_uuid,
    COUNT(domain) AS has_domain,
    COUNT(job_role) AS has_job_role,
    COUNT(license_type) AS has_license_type,
    COUNT(created_at) AS has_created_at
FROM {{DATABASE}}.{{SCHEMA}}.USERS;


-- ============================================================
-- SECTION 4: User ID overlap between tables
-- ============================================================
SELECT
    COUNT(DISTINCT p.user_id) AS pages_users,
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN p.user_id END) AS pages_matched_to_users,
    COUNT(DISTINCT CASE WHEN u.id IS NULL THEN p.user_id END) AS pages_unmatched
FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
LEFT JOIN (SELECT DISTINCT id FROM {{DATABASE}}.{{SCHEMA}}.USERS) u
    ON u.id = p.user_id;


-- ============================================================
-- SECTION 5: Domain coverage from PAGES
-- ============================================================
SELECT
    domain,
    COUNT(DISTINCT user_id) AS distinct_users,
    COUNT(*) AS total_events,
    MIN(TIMESTAMP) AS earliest,
    MAX(TIMESTAMP) AS latest
FROM {{DATABASE}}.{{SCHEMA}}.PAGES
WHERE domain IS NOT NULL
GROUP BY domain
ORDER BY total_events DESC;


-- ============================================================
-- SECTION 6: Top TRACKS event types (excluding known noise)
-- ============================================================
SELECT event_text, COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS unique_users
FROM {{DATABASE}}.{{SCHEMA}}.TRACKS
WHERE event_text NOT IN (
    'atlan_analaytics_aggregateinfo_fetch',
    'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
    'Experiment Started', '$experiment_started',
    'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
    'performance_metric_user_timing_discovery_search',
    'performance_metric_user_timing_app_bootstrap',
    'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
)
AND event_text NOT LIKE 'workflow_%'
GROUP BY event_text
ORDER BY cnt DESC
LIMIT 30;


-- ============================================================
-- SECTION 7: USERS sessions per day by role (activity proxy)
-- ============================================================
SELECT
    TO_DATE(received_at) AS day,
    role,
    COUNT(DISTINCT session_uuid) AS unique_sessions,
    COUNT(DISTINCT id) AS unique_users
FROM {{DATABASE}}.{{SCHEMA}}.USERS
WHERE session_uuid IS NOT NULL
GROUP BY TO_DATE(received_at), role
ORDER BY day DESC
LIMIT 30;
