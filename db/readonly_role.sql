-- Run once in the Supabase SQL editor AFTER the pipeline has created the tables.
-- The public Streamlit dashboard connects with this role, so the deployed app can only read.
-- Choose your own password in place of the placeholder (do not commit it anywhere).
CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'CHOOSE-A-STRONG-PASSWORD';
GRANT USAGE ON SCHEMA public TO dashboard_reader;
GRANT SELECT ON wiki_edits, edit_stats_minute, edit_alerts TO dashboard_reader;
