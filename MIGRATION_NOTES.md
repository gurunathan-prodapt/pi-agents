# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `DW.BERT_AUSD_BP_TA_P_BASISPROD` job. This job is responsible for provisioning and preparing "Basisprodukte" (base products) data for the BERT system, generating a daily snapshot of contract cache for "Forderungsscoring".

The original job, consisting of UC4 orchestration, KornShell wrapper scripts, and a core Oracle SQLPlus script, has been migrated to Google Cloud Platform (GCP). The target platform utilizes:
*   **Apache Airflow** for workflow orchestration.
*   **PySpark on Dataproc** for handling the wrapper logic, parameter passing, and execution flow.
*   **BigQuery** for data storage and the core SQL transformation logic.

The migration aims to achieve a full refresh of the target table `sof$ta_p_basisprod` (now `dw_dwh_prod.sof_ta_p_basisprod` in BigQuery) daily, overwriting it with the latest snapshot derived from various source tables.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`python/k_ausd_bp_ta_p_basisprod.py`**
    *   **Role:** This PySpark application replaces the original `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh` KornShell scripts. It acts as the orchestrator for the core BigQuery SQL logic.
    *   **Functionality:** It parses command-line arguments (`stichtag`, `wiederanlaufwert`, `sql_file_gcs_path`, `project_id`), downloads the BigQuery SQL script from a specified GCS path, substitutes project IDs, and executes the SQL statements sequentially against BigQuery. It also handles logging and error reporting.
    *   **Location:** Intended to be uploaded to a GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/k_ausd_bp_ta_p_basisprod.py`).

*   **`airflow/dw_bert_ausd_bp_ta_p_basisprod_dag.py`**
    *   **Role:** This is the Airflow DAG definition that orchestrates the entire job. It replaces the UC4 job definition (`DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`).
    *   **Functionality:** It defines a single task using `DataprocSubmitJobOperator` to launch the `k_ausd_bp_ta_p_basisprod.py` PySpark application on a Dataproc cluster. It passes necessary parameters like the execution date (`stichtag`), `wiederanlaufwert`, GCS paths for the PySpark script and BigQuery SQL, and the GCP project ID. It also configures Spark properties and includes the BigQuery connector JAR.
    *   **Location:** To be deployed to the Airflow environment's DAGs folder.

*   **`sql/d_ausd_bp_ta_p_basisprod_bq.sql` (Manual Creation Required)**
    *   **Role:** This file will contain the translated BigQuery SQL equivalent of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql`.
    *   **Functionality:** It will perform the core data transformation: determining the reference date, truncating the target table, and inserting data by joining multiple source tables with extensive column remapping and transformations.
    *   **Location:** Intended to be uploaded to a GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/sql/d_ausd_bp_ta_p_basisprod_bq.sql`) for the PySpark script to download and execute. **Note: This file is NOT automatically generated and requires manual translation and creation.**

## 3. Key Design Decisions

*   **Airflow for Orchestration:** Airflow was chosen to replace UC4 due to its native integration with GCP services, robust scheduling capabilities, and ability to define complex DAGs for workflow management.
*   **PySpark on Dataproc for Wrapper Logic:** Instead of directly translating KornShell to a simple Python script or using `BashOperator` in Airflow, PySpark on Dataproc was selected. This provides a scalable and robust environment for handling environment setup, parameter parsing, and logging, mirroring the original shell script's role. It also offers flexibility for future data processing needs if the logic evolves to require distributed computing.
*   **BigQuery SQL for Core Transformation:** The complex Oracle SQL logic was translated directly to BigQuery SQL. This leverages BigQuery's powerful analytical capabilities, serverless execution, and cost-effectiveness for large-scale data transformations.
*   **Externalization of BigQuery SQL:** The BigQuery SQL script (`d_ausd_bp_ta_p_basisprod_bq.sql`) is stored separately in GCS and downloaded by the PySpark application. This promotes separation of concerns, allows for easier SQL updates without redeploying the PySpark code, and improves readability.
*   **Handling Oracle-Specific SQL Features:**
    *   Oracle's `(+)` outer join syntax is translated to explicit `LEFT OUTER JOIN`.
    *   Functions like `NVL`, `TO_CHAR`, `decode` are converted to BigQuery equivalents (`IFNULL`/`COALESCE`, `FORMAT_DATE`, `CASE` statements).
    *   Oracle-specific hints (e.g., `/*+ APPEND */`, `PARALLEL`) are removed as BigQuery's optimizer manages query execution automatically.
    *   The `TRUNCATE TABLE` operation is handled directly in BigQuery SQL.
*   **Parameter Passing:** Key parameters like `stichtag` (execution date) are dynamically passed from Airflow's execution context (`{{ ds_nodash }}`) to the PySpark job, ensuring daily processing uses the correct date.
*   **Logging and Error Handling:** Python's standard `logging` module is used within the PySpark script to replace custom KornShell logging functions, integrating seamlessly with Dataproc and Airflow logs.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment and data for go-live:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `dw_dwh_prod`) and source datasets (e.g., `isbert_schema`) exist in your GCP project.
    *   Create the target table `dw_dwh_prod.sof_ta_p_basisprod` in BigQuery with the appropriate schema, matching the output of the translated SQL.

2.  **Source Data Migration to BigQuery:**
    *   All Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_dist`, `sof$ta_bcp_iccid`, etc.) must be migrated and loaded into their corresponding BigQuery tables (e.g., `isbert_schema.dwtk_meldungen_bq`, `dw_dwh_prod.sof_ta_cntrct_dist`, etc.). Ensure data types and column names are correctly mapped.

3.  **IAM / Permissions Configuration:**
    *   **Dataproc Service Account:** The service account used by the Dataproc cluster must have:
        *   `BigQuery Data Editor` role on the target dataset (`dw_dwh_prod`) and `BigQuery Data Viewer` on source datasets.
        *   `BigQuery Job User` role to run BigQuery queries.
        *   `Storage Object Viewer` role on the GCS bucket containing the PySpark script and BigQuery SQL file.
        *   `Storage Object Creator` role if the PySpark script were to write temporary files to GCS (not currently the case, but good practice).
    *   **Airflow Service Account:** The service account running the Airflow worker (or Composer environment) must have permissions to submit jobs to Dataproc (`Dataproc Editor` or `Dataproc Worker` roles).

4.  **GCS Bucket and File Uploads:**
    *   Create a GCS bucket (e.g., `gs://YOUR_BUCKET_NAME`) to store the job artifacts.
    *   Upload the generated PySpark script (`python/k_ausd_bp_ta_p_basisprod.py`) to the specified GCS path (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/k_ausd_bp_ta_p_basisprod.py`).
    *   **Manually create and upload the translated BigQuery SQL script (`sql/d_ausd_bp_ta_p_basisprod_bq.sql`)** to its specified GCS path (e.g., `gs://YOUR_BUCKET_NAME/sql/d_ausd_bp_ta_p_basisprod_bq.sql`). This is a critical step requiring careful translation of the Oracle SQL.

5.  **Dataproc Cluster Setup:**
    *   Ensure a Dataproc cluster (e.g., `your-dataproc-cluster-name`) is provisioned and running in the specified GCP region (`your-gcp-region`). The cluster should be configured with appropriate machine types and scaling policies for the workload.

6.  **Airflow DAG Configuration and Deployment:**
    *   Update the `GCP_PROJECT_ID`, `DATAPROC_CLUSTER_NAME`, `GCP_REGION`, and `GCS_BUCKET_NAME` variables in `airflow/dw_bert_ausd_bp_ta_p_basisprod_dag.py` to match your environment.
    *   **Define the DAG schedule:** The `schedule=None` in the DAG needs to be updated to the actual business requirement (e.g., `schedule="@daily"` or a specific cron expression).
    *   Deploy the `airflow/dw_bert_ausd_bp_ta_p_basisprod_dag.py` file to your Airflow environment's DAGs folder.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps, risks, or require further investigation:

*   **BigQuery SQL Translation (`d_ausd_bp_ta_p_basisprod_bq.sql`):** This is the most complex part of the migration and requires careful, manual translation from Oracle SQL to BigQuery SQL. While the design document outlines the general approach (e.g., `(+)` to `LEFT JOIN`, `NVL` to `COALESCE`), specific nuances, complex `decode` statements, and potential data type mismatches need thorough review and testing.
*   **Scheduling and Dependencies:** The original UC4 scheduling details were not available. The Airflow DAG is currently set to `schedule=None`. The correct daily schedule and any upstream/downstream dependencies must be identified and configured in Airflow.
*   **`isbert_schema.dwpa_util_skript.runstatement` Logic:** The full logic of this Oracle stored procedure, beyond the `TRUNCATE TABLE` call, is unknown. It's assumed that its only relevant function for this job is truncation. If it performs other critical operations, these need to be identified and replicated in BigQuery or PySpark.
*   **Parameter `v_carmen = "@pcrs1"`:** The purpose and usage of this parameter in the original Oracle SQL script are unclear. Its relevance in the BigQuery environment needs to be assessed. If it's a critical configuration, it might need to be passed as an Airflow variable or environment variable to the PySpark job.
*   **`wiederanlaufwert` Parameterization:** The `wiederanlaufwert` is currently hardcoded to `0` in the Airflow DAG. If this value needs to be dynamic or configurable, it should be managed via Airflow Variables.
*   **Commented-out Shell Script Logic:** The original `k_ausd_bp_ta_p_basisprod.ksh` contained commented-out sections for file-based processing (`sed`, `sort`, `join`). It's assumed this logic is obsolete and was not migrated. Confirmation is needed if this assumption is incorrect.
*   **Data Quality Checks:** While the migration focuses on functional equivalence, a comprehensive data quality validation plan (e.g., row counts, checksums, specific business rule checks) is crucial post-migration.

## 6. Validation

Validation ensures the migrated job performs as expected and produces correct results.

1.  **Triggering the Job:**
    *   Once all manual steps are completed and the DAG is deployed, trigger the `dw_bert_ausd_bp_ta_p_basisprod` DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for task success/failure.
    *   Check the logs of the `run_bert_ausd_bp_ta_p_basisprod` task for PySpark execution details, including the BigQuery job ID.

2.  **Monitoring BigQuery Job:**
    *   Use the BigQuery UI or `bq` command-line tool to monitor the BigQuery job initiated by the PySpark script.
    *   Verify the job status (should be `SUCCESS`).
    *   Check job statistics, including bytes processed and rows affected.

3.  **What "Passing" Means:**
    *   **Airflow DAG Success:** The `dw_bert_ausd_bp_ta_p_basisprod` DAG completes successfully without any failed tasks.
    *   **PySpark Log Verification:** The PySpark application logs indicate successful execution, including messages like "PySpark job completed successfully" and details about rows affected. No errors or warnings should be present in the logs.
    *   **BigQuery Job Success:** The underlying BigQuery query job completes successfully.
    *   **Target Table Verification:**
        *   **Existence:** The target table `dw_dwh_prod.sof_ta_p_basisprod` exists in BigQuery.
        *   **Schema:** The schema of the target table matches the expected schema.
        *   **Row Count:** Compare the row count of the `dw_dwh_prod.sof_ta_p_basisprod` table in BigQuery with the row count of the original `sof$ta_p_basisprod` table in Oracle for the same `stichtag`. They should be identical.
        *   **Data Quality:** Perform sample data checks to ensure data integrity and correctness. Select a few records from the BigQuery target table and compare them against the corresponding records in the Oracle source, verifying column values, transformations, and joins.
        *   **Timeliness:** Verify that the data reflects the `stichtag` passed to the job.

## 7. Rollback Procedure

In case of issues or failures during or after go-live, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_bert_ausd_bp_ta_p_basisprod` DAG to prevent further runs.

2.  **Revert Airflow DAG Deployment:**
    *   Remove or revert the `airflow/dw_bert_ausd_bp_ta_p_basisprod_dag.py` file from the Airflow DAGs folder.

3.  **Revert GCS Files (Optional but Recommended):**
    *   If any changes were made to the PySpark script or BigQuery SQL in GCS, revert them to their previous, stable versions or remove them if they were specifically for the migration.

4.  **Re-enable Original UC4 Job:**
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` job in UC4/Automic. Ensure its schedule and dependencies are correctly restored.

5.  **Data Rollback (If Necessary):**
    *   Since the BigQuery job performs a `TRUNCATE TABLE` and full refresh, the target table `dw_dwh_prod.sof_ta_p_basisprod` would have been overwritten. If the data in this table is critical and the migration failed, you might need to:
        *   Restore the BigQuery table from a previous snapshot or backup if available.
        *   Alternatively, if the original Oracle job is re-enabled and successfully runs, it will populate the Oracle target table, and a subsequent re-migration (after fixing issues) would repopulate the BigQuery table.
    *   **Note:** For a full refresh strategy, data rollback is often simpler as the next successful run will correct the state. However, if downstream systems depend on the BigQuery table immediately, a restoration might be necessary.