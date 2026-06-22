# MIGRATION_NOTES.md

## 1. Summary

The ETL job `DW.BERT_AUSD_BP_TA_P_BASISPROD`, responsible for preparing "instantiated base products" for the BERT system, has been migrated.

**Original Platform:**
*   **Orchestration:** UC4 Job Scheduler (`DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` as a sub-component of `DW.BERT_P_BASISPRODUKT_JP`).
*   **Execution Environment:** UNIX KornShell scripts (`r_ausd_bp_ta_p_basisprod.ksh`, `k_ausd_bp_ta_p_basisprod.ksh`).
*   **Data Transformation:** Oracle SQLPlus script (`d_ausd_bp_ta_p_basisprod.sql`) interacting with an Oracle database.

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow).
*   **Execution Environment:** Google Cloud Dataproc (for PySpark job execution).
*   **Data Transformation:** Google BigQuery (Standard SQL).
*   **Data Storage:** Google BigQuery for all source and target tables.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dw_bert_ausd_bp_ta_p_basisprod.py`**
    *   **Role:** Airflow DAG (Directed Acyclic Graph). This Python script defines the workflow for the migrated job. It orchestrates the execution of the PySpark transformation script on a Dataproc cluster.
    *   **Origin:** Converted from `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`.

*   **`r_ausd_bp_ta_p_basisprod.py`**
    *   **Role:** PySpark Transformation Script. This Python script encapsulates the wrapper logic previously handled by `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh`. It handles parameter parsing, environment setup, date validation, and executes the core BigQuery SQL transformation.
    *   **Origin:** Manually implemented based on the logic of the original KornShell scripts.

*   **`d_ausd_bp_ta_p_basisprod.bqsql`**
    *   **Role:** BigQuery Standard SQL Script. This script contains the core data extraction, transformation, and loading logic. It populates the `bert_dwh.SOF_TA_P_BASISPROD` table by joining various source tables in BigQuery.
    *   **Origin:** Converted from `d_ausd_bp_ta_p_basisprod.sql` (Oracle SQLPlus).

## 3. Key Design Decisions

*   **Cloud-Native Architecture:** The decision to migrate to Google Cloud Platform (GCP) with Airflow, BigQuery, and Dataproc aligns with the strategic goal of leveraging scalable, managed, and cost-effective cloud services for ETL workloads.
*   **Consolidated Wrapper Logic:** The two original KornShell scripts (`r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh`) were combined into a single Python (PySpark) script (`r_ausd_bp_ta_p_basisprod.py`).
    *   **Rationale:** This simplifies the execution flow, centralizes parameter handling and environment setup, and allows for more robust error handling and logging using Python's capabilities. Running it as a PySpark job on Dataproc provides a managed, scalable execution environment.
    *   **Trade-off:** Requires manual re-implementation of shell utility functions (e.g., date handling, logging) in Python.
*   **Direct SQL Translation to BigQuery:** The core data transformation logic from `d_ausd_bp_ta_p_basisprod.sql` was directly translated into BigQuery Standard SQL.
    *   **Rationale:** BigQuery's powerful SQL engine is highly optimized for analytical workloads, making it suitable for complex joins and transformations. This approach minimizes re-engineering of the core business logic.
    *   **Trade-off:** Required careful conversion of Oracle-specific syntax (e.g., `DECODE`, `NVL`, `(+)` outer joins, `DEFINE` variables, `TRUNCATE` behavior, `COLUMN new_value`) to BigQuery equivalents. Oracle procedural calls (`isbert_schema.dwpa_util_skript.runstatement`) needed specific re-evaluation.
*   **Airflow for Orchestration:** Airflow on Cloud Composer was chosen as the orchestration tool.
    *   **Rationale:** Provides a robust, scalable, and widely adopted platform for managing complex data pipelines, offering features like scheduling, monitoring, and dependency management.
    *   **Trade-off:** Requires defining DAGs and configuring `DataprocSubmitJobOperator` tasks, which is a different paradigm than UC4 job definitions.
*   **BigQuery for Data Storage:** All Oracle source and target tables are migrated to BigQuery.
    *   **Rationale:** Centralizes data in a single, highly scalable data warehouse, simplifying data access and reducing data movement.
    *   **Trade-off:** Requires a separate data migration effort from Oracle to BigQuery as a prerequisite.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the GCP environment for the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `bert_dwh` BigQuery dataset exists.
    *   Ensure the `isbert_schema` BigQuery dataset exists (if `dwtk_meldungen` is in a separate dataset).

2.  **BigQuery Table Creation & Data Ingestion:**
    *   Create the target table: `bert_dwh.SOF_TA_P_BASISPROD`.
    *   Create all required source tables: `bert_dwh.SOF_TA_CNTRCT_DIST`, `bert_dwh.SOF_TA_CNTRCT_EVN`, `bert_dwh.SOF_TA_ICCID_VERTRAG`, `bert_dwh.SOF_TA_RN_VERTRAG`, `bert_dwh.SOF_TA_RN_DA_VDA_TK`, `bert_dwh.SOF_TA_TARIFOPTION`, `bert_dwh.SOF_TA_APN_VERTRAG`, `bert_dwh.SOF_TA_BCP_ICCID`, `bert_dwh.SOF_TA_BCP_MSISDN`, and `isbert_schema.dwtk_meldungen`.
    *   Ingest historical and ongoing data from the Oracle source database into these BigQuery tables. This is a critical prerequisite and typically involves a separate data migration project (e.g., using Datastream, DMS, or batch loading tools).

3.  **IAM & Permissions:**
    *   Ensure the Cloud Composer service account has permissions to:
        *   Submit jobs to Dataproc.
        *   Access GCS buckets (for DAGs, PySpark scripts, and temporary files).
    *   Ensure the Dataproc cluster's service account (or the user-managed service account for serverless Dataproc) has permissions to:
        *   Read from and write to the relevant BigQuery datasets (`bert_dwh`, `isbert_schema`).
        *   Access GCS buckets (for PySpark script, BigQuery SQL script, and any temporary data).

4.  **Dataproc Cluster Setup:**
    *   Provision a Dataproc cluster (or configure serverless Dataproc) in the target GCP project and region. The Airflow DAG will submit jobs to this cluster. Ensure the cluster has the necessary PySpark components.

5.  **GCS Bucket for Scripts:**
    *   Create a dedicated GCS bucket (e.g., `gs://your-project-dataproc-scripts`) to store the `r_ausd_bp_ta_p_basisprod.py` and `d_ausd_bp_ta_p_basisprod.bqsql` files. The Airflow DAG will reference these paths.

6.  **Airflow Configuration:**
    *   **DAG Deployment:** Upload `dw_bert_ausd_bp_ta_p_basisprod.py` to the Cloud Composer DAGs folder.
    *   **Airflow Variables/Connections:** If any parameters (e.g., `JobKennung`, `Stichtag`, `EintragsNr`, `Wiederanlaufwert`) are to be passed via Airflow, configure them as Airflow Variables. Ensure the `DataprocSubmitJobOperator` is correctly configured with the cluster name and GCS paths.
    *   **Scheduling:** Define the appropriate schedule for the `dw_bert_ausd_bp_ta_p_basisprod` DAG based on the original UC4 schedule (no `EVNT_TIME` was available, so this needs to be determined).

## 5. Known Gaps & Unresolved References

The following items were flagged during the migration design and require further attention or are considered out of scope for this specific job's migration:

*   **Parent Job Plan Redesign (B4 Item):** The parent UC4 Job Plan `DW.BERT_P_BASISPRODUKT_JP.xml` is marked as `very_complex` and `redesign`. This migration focuses only on the sub-component `DW.BERT_AUSD_BP_TA_P_BASISPROD`. A comprehensive redesign of the entire `DW.BERT_P_BASISPRODUKT_JP` hierarchy and its dependencies is a separate, larger effort. The current DAG assumes it will be invoked as a standalone Airflow DAG or integrated into a higher-level Airflow workflow later.
*   **KornShell Utility Scripts Re-implementation:** The original KornShell scripts relied on several shared utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While core functionalities (date handling, parameter parsing) have been re-implemented in `r_ausd_bp_ta_p_basisprod.py`, a full, standardized Python library for all common utility functions might be beneficial for future migrations.
*   **Oracle Stored Procedure `isbert_schema.dwpa_util_skript.runstatement`:** The exact business logic within this Oracle stored procedure, called by the original SQL, needs to be fully understood. If it contains critical business logic beyond simple DDL execution (e.g., `TRUNCATE`), that logic must be migrated to a BigQuery Stored Procedure or implemented in the Python wrapper. For now, it's assumed to be part of the truncation process and handled by the overall script.
*   **Parameter Defaults and Validation:** The logic for default values (e.g., `p_wiederanlaufWert`, `p_stichtag`) and parameter validation from the KornShell scripts has been replicated in the Python wrapper. Thorough testing is required to ensure this replication is accurate and robust.
*   **Data Type Mismatches:** While the SQL conversion tool attempts to map data types, a careful manual review of the `d_ausd_bp_ta_p_basisprod.bqsql` script is necessary to ensure no precision loss or unexpected behavior, especially for fields like `ICCID`, `MSISDN`, status codes, validity dates, and the newly added `MS3` through `MS10` MultiSIM fields.

## 6. Validation

To validate the successful migration and execution of the `DW.BERT_AUSD_BP_TA_P_BASISPROD` job:

1.  **Trigger the Airflow DAG:** Manually trigger the `dw_bert_ausd_bp_ta_p_basisprod` DAG from the Airflow UI.
2.  **Monitor Airflow Task Status:** Observe the DAG run in the Airflow UI. All tasks (e.g., `run_bert_ausd_bp_ta_p_basisprod`) should complete successfully (green status).
3.  **Check Dataproc Job Logs:** Access the Dataproc job logs in the GCP Console. Verify that the PySpark job (`r_ausd_bp_ta_p_basisprod.py`) started, executed the BigQuery SQL, and completed without errors. Look for any Python tracebacks or BigQuery error messages.
4.  **Verify BigQuery Execution:** Check the BigQuery job history in the GCP Console. Confirm that the SQL query from `d_ausd_bp_ta_p_basisprod.bqsql` executed successfully.
5.  **Data Validation in BigQuery:**
    *   **Row Count:** Compare the number of rows in `bert_dwh.SOF_TA_P_BASISPROD` after the job run with the expected row count from the original Oracle execution for the same date.
    *   **Data Integrity:** Sample data from `bert_dwh.SOF_TA_P_BASISPROD` and compare it against the corresponding data in the original Oracle `SOF$TA_P_BASISPROD` table. Focus on key fields, calculated values, and date formats.
    *   **Parameter Handling:** Ensure that parameters passed to the PySpark script (e.g., `Stichtag`) are correctly interpreted and used in the BigQuery SQL.
    *   **Logging:** Verify that the logging output from the PySpark script provides sufficient information about the job's progress and any potential issues.

**"Passing" Criteria:**
*   The Airflow DAG completes successfully without any failed tasks.
*   The Dataproc job completes successfully with exit code 0.
*   The BigQuery SQL query executes without errors.
*   The data in `bert_dwh.SOF_TA_P_BASISPROD` is accurate and consistent with the expected output from the original Oracle job, both in terms of row count and data values.
*   All parameters are correctly processed, and the job's behavior matches the original logic.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow these steps to roll back to the original Oracle-based job:

1.  **Stop New Job Execution:**
    *   Immediately **pause** or **delete** the `dw_bert_ausd_bp_ta_p_basisprod` DAG in the Airflow UI to prevent further runs.

2.  **Revert Data (if necessary):**
    *   If the migrated job has written incorrect or incomplete data to `bert_dwh.SOF_TA_P_BASISPROD`, either:
        *   **Truncate/Delete:** Execute a `TRUNCATE TABLE bert_dwh.SOF_TA_P_BASISPROD` statement in BigQuery.
        *   **Restore from Snapshot/Backup:** If BigQuery table snapshots or backups are in place, restore the table to its state before the problematic run.
        *   **Re-run Original Job:** If the original Oracle job can safely overwrite the target table, it might be sufficient to re-enable and re-run it.

3.  **Re-enable Original Job:**
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` job in UC4. Ensure its schedule and dependencies are correctly restored.

4.  **Clean Up Migrated Artifacts (Optional, post-rollback confirmation):**
    *   Once the original job is confirmed to be running correctly, you may delete the `dw_bert_ausd_bp_ta_p_basisprod.py` DAG from Cloud Composer.
    *   Remove `r_ausd_bp_ta_p_basisprod.py` and `d_ausd_bp_ta_p_basisprod.bqsql` from the GCS bucket.
    *   (Optional) Delete the `bert_dwh.SOF_TA_P_BASISPROD` table in BigQuery if it's no longer needed or if a clean slate is desired for a future migration attempt.