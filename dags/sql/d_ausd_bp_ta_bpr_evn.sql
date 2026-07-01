-- File: dags/sql/d_ausd_bp_ta_bpr_evn.sql
-- Target: BigQuery Standard SQL
-- Purpose: Truncate and reload EVN basis products into target table.

-- Retrieve parameters dynamically from Airflow Variables or DAG execution context.
-- Using var.json.get for safe access with defaults to prevent Jinja rendering errors.
DECLARE p_stichtag STRING DEFAULT '{{ dag_run.conf.get("stichtag", ds_nodash) }}';
DECLARE p_wiederanlaufwert INT64 DEFAULT CAST('{{ dag_run.conf.get("wiederanlaufwert", "0") }}' AS INT64);

-- Step 1: Clear the target table so the DAG can be safely rerun on the same day.
TRUNCATE TABLE `{{ var.json.get("gcp_project", "gcp-project-placeholder") }}.{{ var.json.get("gcp_dataset", "isbert_schema") }}.sof_ta_bpr_evn`;

-- Step 2: Insert the EVN basis product instances into the target table.
INSERT INTO `{{ var.json.get("gcp_project", "gcp-project-placeholder") }}.{{ var.json.get("gcp_dataset", "isbert_schema") }}.sof_ta_bpr_evn` (cntrct_id, bpr_id)
SELECT
    bp.cntrct_id,
    bp.bpr_id
FROM `{{ var.json.get("gcp_project", "gcp-project-placeholder") }}.{{ var.json.get("gcp_dataset", "isbert_schema") }}.sof_ta_bpr_instance` AS bp
WHERE bp.bpr_id IN (
    32,    -- standard-evn
    2506,  -- komfort-evn
    2839,  -- standard-evn separat
    2840,  -- komfort-evn separat
    3055,  -- komfort-plus-evn
    3056,  -- komfort-plus-evn separat
    3821   -- standard-plus-evn
);