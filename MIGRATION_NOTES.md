# MIGRATION_NOTES.md: r_ausd_geschaeftspartner.ksh

## 1. Summary

The legacy KornShell script `r_ausd_geschaeftspartner.ksh`, responsible for orchestrating the initial provisioning of contract caches for the demand scoring system (FOS), has been migrated.

The migration involved:
*   **Orchestration:** The KornShell script's orchestration logic has been translated into an **Airflow DAG** (`r_ausd_geschaeftspartner_dag.py`) running on Google Cloud Composer.
*   **Transformation:** The core data generation and transformation logic, originally within `k_ausd_geschaeftspartner.ksh` (which invoked Oracle SQL), has been re-implemented as a **PySpark application** (`k_ausd_geschaeftspartner.py`) that executes **BigQuery SQL** (`d_ausd_geschaeftspartner_bq.sql`). This PySpark job runs on Google Cloud Dataproc.
*   **Data Platform:** The target data platform for all source and target tables, including the `FOS-Tabelle`, is **Google BigQuery**.

This migration moves the job from a legacy on-premise KornShell/Oracle environment to a fully managed, cloud-native GCP stack.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/d_ausd_geschaeftspartner_bq.sql`**
    *   **Role:** Contains the translated SQL logic from the original `d_ausd_geschaeftspartner.sql` (invoked by `k_ausd_geschaeftspartner.ksh`) to Google BigQuery Standard SQL. This script performs the core data transformations, truncating and inserting data into various intermediate and final target tables in the `isbert_target_ds` BigQuery dataset. It accepts a `stichtag` parameter (`@p_stichtag_yyyymmdd`).
*   **`pyspark/k_ausd_geschaeftspartner.py`**
    *   **Role:** This PySpark application serves as the execution wrapper for the BigQuery SQL script. It is the direct replacement for the legacy `k_ausd_geschaeftspartner.ksh`. It takes the path to the SQL file and the `stichtag` as arguments, reads the SQL, substitutes the date parameter, and executes the SQL statements sequentially against BigQuery using the `google.cloud.bigquery` client library.
*   **`dags/r_ausd_geschaeftspartner_dag.py`**
    *   **Role:** The Airflow DAG definition. This Python script orchestrates the entire job. It defines a single task, `run_contract_cache_initial_load`, which uses `DataprocSubmitPySparkJobOperator` to launch the `k_ausd_geschaeftspartner.py` PySpark application on a Dataproc cluster. It handles parameter passing for the `stichtag` from the Airflow DAG run configuration.
*   **`ddl/isbert_target_ds.sof_ta_segm_prem.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_segm_prem` table in the `isbert_target_ds` dataset. This is an intermediate target table.
*   **`ddl/isbert_target_ds.sof_ta_p_gesch_part.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_p_gesch_part` table in the `isbert_target_ds` dataset. This is a primary target table for business partner data.
*   **`ddl/isbert_target_ds.sof_ta_bpr_dn_evn_his.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_bpr_dn_evn_his` table in the `isbert_target_ds` dataset. This is an intermediate history table.
*   **`ddl/isbert_target_ds.sof_ta_bpr_dn_evn.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_bpr_dn_evn` table in the `isbert_target_ds` dataset. This is an intermediate table.
*   **`ddl/isbert_target_ds.sof_ta_p_dn_nutzer.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_p_dn_nutzer` table in the `isbert_target_ds` dataset. This is a primary target table for user data.
*   **`ddl/isbert_target_ds.sof_ta_p_evn_empf.sql`**
    *   **Role:** BigQuery DDL for creating the `sof_ta_p_evn_empf` table in the `isbert_target_ds` dataset. This is a primary target table for recipient data.
*   **`ddl/fos_tabelle.sql`**
    *   **Role:** BigQuery DDL for creating a placeholder `FOS_Tabelle` in the `isbert_target_ds` dataset. This is the ultimate output table for the demand scoring system.

## 3. Key Design Decisions

*   **Orchestration Shift to Airflow:** The original KornShell script's primary role was orchestration (parameter parsing, error handling, invoking core logic). Airflow is the natural fit for this in GCP, providing robust scheduling, monitoring, and dependency management. This centralizes job control and leverages cloud-native features.
*   **PySpark for SQL Execution:** While the core logic is SQL, using a PySpark application (`k_ausd_geschaeftspartner.py`) to execute the BigQuery SQL (`d_ausd_geschaeftspartner_bq.sql`) was chosen to maintain a consistent pattern for "semi-automatic" migrations where the core logic is often complex SQL or involves data manipulation beyond simple SQL. This approach allows for:
    *   **Parameterization:** Easily passing dynamic parameters (like `stichtag`) from Airflow to the SQL script.
    *   **Error Handling:** Centralized Python-based error handling and logging, integrating with Cloud Logging.
    *   **Extensibility:** If future requirements demand more complex data processing (e.g., UDFs, DataFrame operations) that are difficult in pure SQL, the PySpark framework is already in place.
    *   **Trade-off:** This introduces the overhead of spinning up a Dataproc cluster for what is essentially a BigQuery SQL execution. For very simple, single-statement SQL, a `BigQueryOperator` might be considered, but the current SQL script has multiple statements and dynamic parameter substitution, making the PySpark wrapper a more flexible choice.
*   **BigQuery as Target Data Warehouse:** BigQuery provides a scalable, serverless, and cost-effective solution for data warehousing, aligning with GCP best practices. All source and target tables are mapped to BigQuery datasets and tables.
*   **Parameter Handling:** Legacy shell script parameter parsing (`-s`, `-l`) is replaced by Airflow DAG run configuration and PySpark's `argparse`. The `stichtag` is passed dynamically, defaulting to the execution date. The `wiederanlaufwert` (restart value) was noted in the legacy script but not explicitly used in the generated SQL; it's commented out in the DAG but can be easily re-enabled if needed.
*   **Utility Script Refactoring (Implicit):** The legacy KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are not directly translated. Their functionalities are either absorbed by Airflow's native capabilities (logging, parameter handling) or are no longer necessary in the new Python/BigQuery context. Error handling is now managed by Python exceptions and Airflow's retry/alerting mechanisms.
*   **SQL Translation:** Oracle-specific SQL constructs (e.g., `COALESCE`, `CASE` statements, date functions) have been translated to their BigQuery Standard SQL equivalents. `TRUNCATE TABLE` statements are used for restartability, mirroring the likely intent of the original script.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project Setup:**
    *   Ensure a GCP project is selected and billing is enabled.
    *   Enable the following APIs: `Cloud Composer API`, `Dataproc API`, `BigQuery API`, `Cloud Storage API`.
2.  **BigQuery Environment Setup:**
    *   **Create Datasets:** Create the necessary BigQuery datasets:
        *   `isbert_source_ds` (if not already existing, to house the source tables)
        *   `isbert_target_ds`
    *   **Create Target Tables:** Execute the DDL scripts provided in the `ddl/` directory to create the target tables in `isbert_target_ds`:
        *   `ddl/isbert_target_ds.sof_ta_segm_prem.sql`
        *   `ddl/isbert_target_ds.sof_ta_p_gesch_part.sql`
        *   `ddl/isbert_target_ds.sof_ta_bpr_dn_evn_his.sql`
        *   `ddl/isbert_target_ds.sof_ta_bpr_dn_evn.sql`
        *   `ddl/isbert_target_ds.sof_ta_p_dn_nutzer.sql`
        *   `ddl/isbert_target_ds.sof_ta_p_evn_empf.sql`
        *   `ddl/fos_tabelle.sql`
    *   **Verify Source Tables:** Ensure that all source tables referenced in `d_ausd_geschaeftspartner_bq.sql` (e.g., `isbert_source_ds.bpd_ta_bp_valueseg_assoc`, `isbert_source_ds.sof_ta_e_reach_gp`, etc.) exist in the `isbert_source_ds` dataset and are populated with the necessary data.
3.  **Cloud Storage (GCS) Setup:**
    *   **Create GCS Bucket:** Create a GCS bucket (e.g., `gs://your-gcs-bucket`) to store the PySpark application and SQL scripts.
    *   **Upload Files:** Upload the following files to a designated path within the bucket (e.g., `gs://your-gcs-bucket/dataproc_jobs/`):
        *   `pyspark/k_ausd_geschaeftspartner.py`
        *   `sql/d_ausd_geschaeftspartner_bq.sql`
4.  **Dataproc Cluster Setup:**
    *   **Create Dataproc Cluster:** Create a Dataproc cluster in the specified GCP region (e.g., `your-gcp-region`) with the name `your-dataproc-cluster`. Ensure it has sufficient resources and is configured with the necessary component gateway for Airflow to interact with it.
5.  **Cloud Composer (Airflow) Setup:**
    *   **Upload DAG:** Upload the `dags/r_ausd_geschaeftspartner_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Update DAG Placeholders:** Edit `r_ausd_geschaeftspartner_dag.py` to replace placeholders:
        *   `project_id="your-gcp-project-id"` with your actual GCP Project ID.
        *   `region="your-gcp-region"` with your actual GCP Region.
        *   `cluster_name="your-dataproc-cluster"` with the name of your Dataproc cluster.
        *   `main_python_file="gs://your-gcs-bucket/dataproc_jobs/pyspark/k_ausd_geschaeftspartner.py"` with the correct GCS path to your PySpark script.
        *   `--sql_file_path", "gs://your-gcs-bucket/dataproc_jobs/sql/d_ausd_geschaeftspartner_bq.sql"` with the correct GCS path to your BigQuery SQL script.
6.  **IAM/Permissions:**
    *   Ensure the Service Account used by your Cloud Composer environment and the Dataproc cluster's Service Account have the necessary IAM roles:
        *   `BigQuery Data Editor` (for `isbert_target_ds`)
        *   `BigQuery Data Viewer` (for `isbert_source_ds`)
        *   `Storage Object Viewer` (for reading scripts from GCS)
        *   `Dataproc Worker` (for the Dataproc cluster service account)
        *   `Composer Worker` (for the Composer environment service account)
7.  **Secrets/Connections:**
    *   If the legacy `. $HOME/.dw_init` contained any sensitive information (e.g., database credentials, API keys), these must be securely configured in GCP, preferably using Secret Manager and accessed via Airflow Connections or directly in the PySpark application. (No explicit secrets were identified in the provided context, but this is a general best practice).
8.  **Scheduling:**
    *   The DAG is currently configured with `schedule=None` (manual trigger). If a specific schedule is required, update the `schedule` parameter in `r_ausd_geschaeftspartner_dag.py` (e.g., `schedule="@daily"` or a cron expression).

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and remain as known gaps or require further follow-up:

*   **Core Script (`k_ausd_geschaeftspartner.ksh`) Logic:** While the SQL portion was translated, the full extent of the original `k_ausd_geschaeftspartner.ksh`'s shell logic (beyond invoking SQL) was not fully detailed. Any non-SQL logic (e.g., file operations, specific shell commands) would need to be re-evaluated and implemented in the PySpark script if critical.
*   **Legacy Scheduling:** No explicit scheduling information was provided for the `r_ausd_geschaeftspartner.ksh` job in the legacy environment. The Airflow DAG is currently designed for manual triggering. A schedule should be determined and configured.
*   **Environment Variables & Secrets:** The exact content and implications of `. $HOME/.dw_init` and `BERT_DIR_ROOT` need to be fully understood. Any critical environment variables or secrets must be securely configured in the GCP environment (e.g., via Airflow Variables, Connections, or Secret Manager).
*   **Dataproc Operator Choice Justification:** The use of `DataprocSubmitPySparkJobOperator` implies a Dataproc cluster will be active. While flexible, for purely BigQuery SQL execution, a `BigQueryOperator` might be more cost-effective if the SQL can be executed as a single query or if the multi-statement execution can be handled directly by BigQuery's scripting capabilities without the need for Python logic. This decision should be re-evaluated if Dataproc costs become a concern.
*   **Error Code Translation:** The legacy script used specific error codes. These have been replaced by Python exceptions and Airflow's native error handling. Ensure that the new error handling and alerting mechanisms meet the original requirements.
*   **`FOS-Tabelle` Schema:** The `FOS-Tabelle` was identified as the ultimate output. A placeholder schema was created. The exact final schema for `FOS-Tabelle` should be confirmed based on the business requirements and the output of the intermediate tables.

## 6. Validation

To validate the successful migration and execution of the `r_ausd_geschaeftspartner.ksh` job:

1.  **Trigger the Airflow DAG:**
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `r_ausd_geschaeftspartner_dag` DAG.
    *   Click the "Trigger DAG" button.
    *   In the trigger dialog, you can optionally provide a `stichtag` in the `dag_run.conf` JSON (e.g., `{"stichtag": "20231026"}`). If not provided, it will default to the execution date.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. The `run_contract_cache_initial_load` task should transition through "running" to "success".
    *   Check the task logs for any errors or warnings. The PySpark script logs its progress, including SQL statement execution, to standard output, which will be captured in Airflow logs.
3.  **Verify Dataproc Job:**
    *   In the GCP Console, navigate to Dataproc -> Jobs.
    *   Confirm that a Dataproc job was submitted and completed successfully by the Airflow task.
4.  **Verify BigQuery Data:**
    *   In the GCP Console, navigate to BigQuery.
    *   Query the target tables in the `isbert_target_ds` dataset (e.g., `sof_ta_segm_prem`, `sof_ta_p_gesch_part`, `FOS_Tabelle`).
    *   **"Passing" Criteria:**
        *   The Airflow DAG run completes successfully without any task failures.
        *   The Dataproc job completes successfully.
        *   The target tables in BigQuery (`isbert_target_ds.sof_ta_segm_prem`, `isbert_target_ds.sof_ta_p_gesch_part`, `isbert_target_ds.sof_ta_bpr_dn_evn_his`, `isbert_target_ds.sof_ta_bpr_dn_evn`, `isbert_target_ds.sof_ta_p_dn_nutzer`, `isbert_target_ds.sof_ta_p_evn_empf`, `isbert_target_ds.FOS_Tabelle`) are populated with data.
        *   Perform data validation checks:
            *   **Row Counts:** Compare row counts of key target tables with expected counts (if available from legacy runs).
            *   **Data Integrity:** Spot-check a sample of records for accuracy and consistency against the source data and legacy output.
            *   **Date Filtering:** Ensure the `stichtag` parameter correctly filters data as expected.

## 7. Rollback Procedure

In case of critical issues or failure of the migrated job, the following rollback procedure can be followed to revert to the legacy system:

1.  **Disable New DAG:**
    *   In the Airflow UI, toggle off the `r_ausd_geschaeftspartner_dag` to prevent any further runs.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh` script in the legacy environment.
    *   Ensure its original scheduling mechanism is reactivated.
3.  **Data Restoration (if necessary):**
    *   The `d_ausd_geschaeftspartner_bq.sql` script uses `TRUNCATE TABLE` before inserting. If the migrated job ran and produced incorrect data in BigQuery, and this data has downstream dependencies, you may need to:
        *   Restore the affected BigQuery target tables (`isbert_target_ds.sof_ta_segm_prem`, `isbert_target_ds.sof_ta_p_gesch_part`, etc., and `isbert_target_ds.FOS_Tabelle`) from a previous backup or a point-in-time snapshot, if available.
        *   Alternatively, if the legacy job can safely re-run and overwrite the target data, execute the legacy job for the affected `stichtag` to correct the data.
4.  **Monitor Legacy Job:**
    *   Verify that the legacy job runs successfully and produces the expected output.
5.  **Post-Rollback Analysis:**
    *   Investigate the root cause of the failure in the migrated job. This may involve reviewing Airflow logs, Dataproc job logs, and BigQuery query history.
    *   Address the identified issues before attempting another migration or re-deployment.