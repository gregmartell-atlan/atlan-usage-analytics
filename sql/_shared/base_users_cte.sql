-- base_users_cte.sql
-- Reusable CTE: User metadata from USERS table (Segment identify snapshots).
-- LEFT JOIN to activity data for optional enrichment (email, role, job_role).
-- Only ~2% of active user_ids match USERS — use user_id as primary key.
--
-- Usage: Add after activity_events CTE. Join: ON user_meta.id = activity_events.user_id
-- Note: Many users will have NULL email/role. Always use COALESCE for display.

, user_meta AS (
    SELECT
        id,
        email,
        username,
        domain,
        role,
        MAX(license_type) AS license_type,
        MAX(job_role) AS job_role,
        MAX(personas) AS personas,
        MAX(plan) AS plan,
        MAX(salesforce_account_id) AS salesforce_account_id,
        MIN(created_at) AS user_created_at,
        MIN(tenant_created_at) AS tenant_created_at,
        MAX(received_at) AS last_identified_at,
        MAX(session_uuid) AS last_session_uuid
    FROM {{DATABASE}}.{{SCHEMA}}.USERS
    WHERE email IS NOT NULL
    GROUP BY id, email, username, domain, role
)
