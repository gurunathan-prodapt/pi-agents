# Migration Notes: DW.BERT_P_ADRESSEN

This document details the migration of the legacy Oracle-based job `DW.BERT_P_ADRESSEN` to Google Cloud BigQuery and Apache Airflow.

---

## 1. Summary

The legacy `DW.BERT_P_ADRESSEN` job was orchestrated by Automic UC4, using KornShell (`.ksh`) wrapper scripts to execute database-level PL/SQL processing in an Oracle environment. 

This job has been migrated to:
*   **Target Query Engine:** Google Cloud BigQuery (Standard SQL Scripting)
*   **Target Orchestrator:** Google Cloud Composer / Apache Airflow (Python DAG)

The migration preserves the business logic that extracts, filters, and transforms address, reachability, and business partner configurations based on historical validity dates (`d_datum`).

---

## 2. Generated Artifacts

The migration process generated the following files, which must be deployed to your Airflow environment:

| Target File Path | Target Technology | Role / Description |
| :--- | :--- | :--- |
| `sql/d_ausd_adressen.sql` | BigQuery SQL | Procedural SQL script containing the table truncations (`TRUNCATE`) and step-by-step data transformations (`INSERT INTO ... SELECT`). |
| `dags/dw_bert_p_adressen_dag.py` | Python (Airflow DAG) | Orchestrates the execution of the BigQuery SQL script. Replaces the legacy Automic UC4 XML configuration and KornShell wrappers. |

---

## 3. Key Design Decisions

### 3.1 Procedural Scripting over Multiple Tasks
Instead of breaking each step into individual Airflow tasks, the entire sequence of `TRUNCATE` and `INSERT` statements is kept within a single BigQuery SQL script (`d_ausd_adressen.sql`). 
*   **Why:** This minimizes Airflow task scheduling overhead, maintains transactional consistency within the session, and allows BigQuery to optimize execution paths.
*   **Trade-off:** If a step fails halfway through, the script stops. However, because all target tables are truncated at the start of the script (Step 01), the process is fully idempotent and can be safely re-run from the beginning.

### 3.2 Dynamic Date Parameterization (`v_datum`)
The legacy script dynamically determined its execution key date (`v_datum`) by querying `dwtk_meldungen` for the last execution of `BERT_DROP_TEMP_TABLE`.
*   **Why:** This logic was preserved verbatim in the SQL script using a `DECLARE` statement to ensure exact behavioral parity with the legacy system.
*   **Alternative Considered:** Passing the Airflow execution date (`{{ ds_nodash }}`) directly. This was deferred to avoid breaking dependencies on upstream legacy jobs that still write to `dwtk_meldungen`.

### 3.3 Elimination of Database Links
The legacy Oracle database link reference (`@pcrs1`) has been removed. In the target architecture, upstream data from the CARMEN system is assumed to be replicated directly into the `cds` dataset within the same Google Cloud project.

---

## 4. Manual Steps Before Go-Live

Before enabling and running the Airflow DAG, the following infrastructure and configuration steps must be completed:

### 4.1 Schema and Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project (`gcp_project_id`):
*   `isbert_schema`
*   `cds`
*   `glv`
*   `bpd`
*   `sof`

### 4.2 Table DDL Deployment
All target tables in the `sof` dataset must be pre-created with schemas matching their legacy Oracle counterparts. Ensure the following tables exist:
*   `sof.ta_bp_ref_gp`, `sof.ta_bp_ref_re`, `sof.ta_bp_ref_ev`, `sof.ta_bp_ref_dn`
*   `sof.ta_bp_ref_gp_nodp`, `sof.ta_bp_ref_re_nodp`, `sof.ta_bp_ref_ev_nodp`, `sof.ta_bp_ref_dn_nodp`
*   `sof.ta_reachability`, `sof.ta_business_pt`, `sof.ta_country`, `sof.ta_country_desc`, `sof.ta_laender_kng`
*   `sof.ta_e_reach_gp`, `sof.ta_e_reach_re`, `sof.ta_e_reach_dn`, `sof.ta_e_reach_ev`
*   `sof.ta_e_business_gp`, `sof.ta_e_business_re`, `sof.ta_e_business_dn`, `sof.ta_e_business_ev`
*   `sof.ta_e_regulierer`

### 4.3 IAM & Permissions
The service account running the Airflow worker/Composer environment must have the following IAM roles:
*   `roles/bigquery.jobUser` (to run BigQuery jobs)
*   `roles/bigquery.dataEditor` on the `sof` dataset (to truncate and insert data)
*   `roles/bigquery.dataViewer` on the `isbert_schema`, `cds`, `glv`, and `bpd` datasets (to read source data)

### 4.4 Airflow Connection
Ensure that an Airflow connection named `google_cloud_default` is configured and points to the correct GCP project containing your BigQuery datasets.

### 4.5 Scheduling & Upstream Dependencies
This DAG is currently configured with `schedule_interval=None`. It should be scheduled to run only after the upstream jobs `DW.BERT_P_GESCHAEFTSP` and `DW.BERT_P_RECHEMPF` have successfully completed.

---

## 5. Known Gaps & Unresolved References

### 5.1 Hardcoded Project ID Placeholder
The SQL script and DAG use the placeholder `gcp_project_id`. 
*   **Action Required:** Replace all occurrences of `gcp_project_id` with your actual GCP Project ID (e.g., `prod-data-warehouse-123`) before deploying to production.

### 5.2 Dependency on `dwtk_meldungen` (Redesign B4 Item)
The script relies on the table `gcp_project_id.isbert_schema.dwtk_meldungen` to determine the execution date (`v_datum`). 
*   **Follow-up:** If `dwtk_meldungen` is not migrated or maintained in GCP, this logic must be refactored. The DAG should be modified to pass Airflow's logical execution date (`{{ ds }}`) directly into the SQL script as a query parameter:
    ```python
    # Example Refactoring
    "queryParameters": [
        {
            "name": "v_datum",
            "parameterType": {"type": "STRING"},
            "parameterValue": {"value": "{{ ds_nodash }}"}
        }
    ]
    ```

---

## 6. Validation

To validate the migration, perform a parallel run and compare the outputs:

1.  **Dry Run:** Execute the SQL script in the BigQuery console using a test project ID to verify syntax and schema compatibility.
2.  **Data Comparison (Row Counts):** 
    *   Run the legacy Oracle job for a specific date.
    *   Run the migrated BigQuery script for the same date.
    *   Compare row counts for all target tables in the `sof` schema. They must match exactly.
3.  **Data Integrity Check:** Run a checksum or hash comparison on a sample of records from high-volume tables (e.g., `sof.ta_e_reach_gp`) between Oracle and BigQuery to ensure character encoding and date casting (e.g., `PARSE_DATE`) behaved identically.

---

## 7. Rollback Procedure

If issues are detected in production after go-live, follow these steps to roll back:

1.  **Pause the Airflow DAG:** Turn off the toggle for `dw_bert_p_adressen_dag` in the Airflow UI to prevent further automated executions.
2.  **Re-enable Legacy Orchestration:** Reactivate the `DW.BERT_P_ADRESSEN` job in Automic UC4.
3.  **Verify Legacy Source Sync:** Ensure that any data modified during the BigQuery run has not caused desynchronization. Since the job is a truncate-and-reload operation, running the legacy Oracle job will naturally overwrite and correct the state of the target tables in the Oracle database.