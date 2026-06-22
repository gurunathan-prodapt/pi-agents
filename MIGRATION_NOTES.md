# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_C_BFC

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_C_BFC`, responsible for calculating and maintaining contract binding dates within the `sof$ta_c_bfc` table (contract extension period caching), has been migrated.

**Original Platform:**
*   **Orchestration:** UC4/Automic
*   **Scripting:** KornShell
*   **Database:** Oracle SQL

**Target Platform:**
*   **Orchestration:** Apache Airflow (Google Cloud Composer)
*   **Execution Environment:** Google Cloud Dataproc (for Python script execution)
*   **Data Transformation & Storage:** Google BigQuery

The migration involved re-platforming the entire workflow, converting Oracle SQL to BigQuery SQL, re-implementing KornShell logic in Python, and orchestrating the new components with Airflow.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`sof_ta_c_bfc_ddl.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the target table `sof$ta_c_bfc` in BigQuery. This defines the schema, column types, and partitioning/clustering (if applicable) for the final contract binding date cache table.
*   **`sof_ta_c_bfc_akt_ddl.sql`**
    *   **Role:** BigQuery DDL script to create the temporary table `sof$ta_c_bfc_akt` in BigQuery. This table serves as an intermediate staging area for calculated binding dates before the final merge into `sof$ta_c_bfc`. (Note: This might also be handled as a Common Table Expression (CTE) or a temporary table within the main BigQuery SQL, in which case this DDL would be integrated).
*   **`bfc_get_bindefrist_udf.sql` / `bfc_get_bindefrist_sp.sql`**
    *   **Role:** BigQuery User-Defined Function (UDF) or Stored Procedure that re-implements the business logic of the original Oracle PL/SQL function `Cds$vr_Bindefrist.GetBindeFrist`. This function is critical for calculating the `bindefrist` and is called during the data transformation process.
*   **`d_ausd_v_ta_c_bfc.bqsql`**
    *   **Role:** The core transformation logic, converted from Oracle SQL (`d_ausd_v_ta_c_bfc.sql`) to BigQuery SQL. This script performs the data aggregation, calculations, and the incremental `MERGE` operation into `sof$ta_c_bfc`, utilizing the `bfc_get_bindefrist` UDF/SP.
*   **`r_ausd_v_ta_c_bfc.py`**
    *   **Role:** A Python wrapper script that replaces the original KornShell wrapper (`r_ausd_v_ta_c_bfc.ksh`) and control (`k_ausd_v_ta_c_bfc.ksh`) scripts. This script handles environment setup, parameter parsing, logging, error handling, and orchestrates the execution of the `d_ausd_v_ta_c_bfc.bqsql` against BigQuery using the BigQuery Python client library.
*   **`dw_bert_ausd_v_ta_c_bfc.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) definition. This DAG orchestrates the entire workflow, replacing the UC4 job. It defines a `DataprocSubmitJobOperator` task to execute the `r_ausd_v_ta_c_bfc.py` script on a Dataproc cluster, along with any other necessary Airflow tasks (e.g., start/end tasks, monitoring).

## 3. Key Design Decisions

The migration approach for `DW.BERT_AUSD_V_TA_C_BFC` was guided by the following key design decisions:

*   **Cloud-Native Orchestration with Airflow:** Airflow (Cloud Composer) was chosen to replace UC4 due to its robust scheduling capabilities, extensibility, and native integration with GCP services. This provides a modern, scalable, and observable orchestration layer.
*   **BigQuery for Data Processing and Storage:** BigQuery was selected as the primary data warehouse and transformation engine, replacing Oracle. This leverages BigQuery's serverless architecture, columnar storage, and high-performance query capabilities for large datasets, reducing operational overhead.
*   **Python for Scripting Logic:** The original KornShell scripts were re-implemented in Python. Python offers better maintainability, error handling, and a richer ecosystem for interacting with GCP services, making the job more robust and easier to develop/debug.
*   **Dataproc for Python Execution:** A `DataprocSubmitJobOperator` was chosen to execute the Python wrapper script. This provides a managed environment for running Python jobs, offering flexibility for more complex logic or dependencies that might not be suitable for direct Airflow operators.
*   **Re-implementation of Oracle PL/SQL Function:** The critical `Cds$vr_Bindefrist.GetBindeFrist` PL/SQL function was re-implemented as a BigQuery UDF or Stored Procedure. This was necessary to fully port the business logic to BigQuery, ensuring functional parity without relying on external Oracle systems.

**Notable Trade-offs:**

*   **PL/SQL Re-implementation Complexity:** Re-implementing the Oracle PL/SQL function required a deep understanding of its original business logic, which was not immediately available. This introduced a significant development effort and potential for subtle behavioral differences if the original logic was not fully captured.
*   **Dataproc Overhead:** While flexible, using Dataproc for a job that primarily executes BigQuery SQL introduces an additional layer of infrastructure and associated costs compared to directly using Airflow's `BigQueryOperator`. This decision was made to accommodate the Python wrapper's logic and potential future extensibility.
*   **Loss of Oracle-Specific Optimizations:** Oracle-specific performance hints and constructs (e.g., `ROWNUM`, `(+)` joins) were removed. While BigQuery's optimizer is highly capable, direct translation of such hints is not possible, and performance characteristics may differ, requiring thorough validation.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and prerequisites must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery datasets corresponding to the original Oracle schemas (e.g., `isbert_schema`, `sof_ta_schema`) are created in the target GCP project.
2.  **Source Data Migration:**
    *   All source Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) must be fully migrated to BigQuery. This includes:
        *   Defining their BigQuery schemas.
        *   Performing an initial historical data load.
        *   Establishing an ongoing data synchronization mechanism (e.g., CDC, batch exports) to keep these BigQuery tables up-to-date.
3.  **Target Table DDL Deployment:**
    *   Execute `sof_ta_c_bfc_ddl.sql` to create the `sof$ta_c_bfc` table in BigQuery.
    *   Execute `sof_ta_c_bfc_akt_ddl.sql` to create the `sof$ta_c_bfc_akt` temporary table (if not handled as a CTE or implicit temp table).
4.  **PL/SQL Function Logic Extraction & Deployment:**
    *   **Crucially**, the exact business logic of `spr_schema.Cds$vr_Bindefrist.GetBindeFrist` must be extracted from the original Oracle environment.
    *   Deploy the `bfc_get_bindefrist_udf.sql` or `bfc_get_bindefrist_sp.sql` (BigQuery UDF/Stored Procedure) to the target BigQuery dataset.
5.  **IAM Permissions Configuration:**
    *   **Cloud Composer Service Account:** Grant the service account used by Cloud Composer (Airflow) the necessary IAM roles:
        *   `BigQuery Data Editor` (or more granular roles for specific datasets/tables) to read from source tables and write to target tables.
        *   `Dataproc Worker` and `Dataproc Editor` (or custom roles) to submit and manage Dataproc jobs.
        *   `Storage Object Admin` (or `Storage Object Viewer`/`Creator`) to access GCS buckets for scripts and logs.
    *   **Dataproc Cluster Service Account:** Ensure the service account used by the Dataproc cluster has similar BigQuery and GCS permissions.
6.  **Dataproc Cluster Availability:**
    *   Verify that a Dataproc cluster is provisioned and available in the target GCP project and region, configured with the necessary components (e.g., Python 3).
7.  **Airflow DAG Deployment & Scheduling:**
    *   Upload the `dw_bert_ausd_v_ta_c_bfc.py` DAG file to the Airflow DAGs folder in Cloud Composer.
    *   Determine the original UC4 job's schedule and configure the Airflow DAG's `schedule_interval` accordingly.
8.  **GCS Script Upload:**
    *   Upload the `r_ausd_v_ta_c_bfc.py` Python script and `d_ausd_v_ta_c_bfc.bqsql` BigQuery SQL script to a designated Google Cloud Storage (GCS) bucket, accessible by the Dataproc cluster.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or risks during the design phase and require follow-up:

*   **Missing PL/SQL Source Code for `Cds$vr_Bindefrist.GetBindeFrist`:** The exact business logic of this critical Oracle PL/SQL function was not available in the provided artifacts. Its re-implementation in BigQuery is crucial, and any discrepancies could lead to incorrect binding date calculations. **Action:** Obtain the source code for this function from the original Oracle environment and thoroughly review it for accurate BigQuery re-implementation.
*   **Source Data Migration Status:** The `sof$ta_*` tables and `isbert_schema.dwtk_meldungen` are assumed to be Oracle tables. Their complete migration (schema, initial load, and ongoing synchronization) to BigQuery is a prerequisite for this job and needs to be confirmed as completed and stable.
*   **`v_carmen` and `@PCRS1` DB Link Usage:** The Oracle SQL uses `v_carmen` (defined as `\"@pcrs1\"`) in conjunction with `all_objects`. The exact purpose of querying `all_objects` over a DB link needs clarification. If it's for data, that data source needs to be migrated. If it's for metadata (e.g., package creation date), an alternative BigQuery-native approach or static value might be needed.
*   **`DWPA_UTIL_SKRIPT.runstatement` Functionality:** The Oracle PL/SQL block calls `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_c_bfc_akt')`. The full functionality of this custom utility (beyond just executing DDL) needs to be understood to ensure it's correctly replicated or replaced with standard BigQuery operations.
*   **Error Handling Re-implementation:** The original KornShell scripts had custom error handling (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `DWMSG_*` functions). While the Python wrapper includes error handling, a comprehensive review is needed to ensure all original error conditions and reporting mechanisms are adequately covered and integrated with GCP Cloud Logging and Airflow alerting.
*   **UC4 Scheduler Information:** The precise schedule of the original UC4 job was not available. This needs to be determined to configure the Airflow DAG's `schedule_interval` correctly to match business requirements.

## 6. Validation

Validation of the migrated job involves functional, data, and performance testing.

**How to Run Tests:**

1.  **Deploy to Staging:** Deploy the Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`), Python wrapper (`r_ausd_v_ta_c_bfc.py`), BigQuery SQL (`d_ausd_v_ta_c_bfc.bqsql`), and BigQuery UDF/SP (`bfc_get_bindefrist_udf.sql`/`bfc_get_bindefrist_sp.sql`) to a dedicated staging or development GCP environment.
2.  **Trigger Execution:**
    *   Manually trigger the Airflow DAG from the Cloud Composer UI.
    *   Alternatively, configure a test schedule and allow the DAG to run automatically.
3.  **Monitor Execution:**
    *   Monitor the Airflow UI for task status and logs.
    *   Check Dataproc job logs for the Python script execution.
    *   Review BigQuery job history for the executed SQL queries.
4.  **Data Inspection:**
    *   Query the target BigQuery table `sof$ta_c_bfc` after the job completes.
    *   Compare the results with the corresponding Oracle table (if still available) or with expected output based on source data.

**What "Passing" Means:**

*   **Successful Execution:** The Airflow DAG completes successfully without any failed tasks. All BigQuery jobs initiated by the Python script complete without errors.
*   **Data Accuracy:**
    *   **Row Counts:** The number of records in `sof$ta_c_bfc` (or the number of updated/inserted records) matches expectations and, where possible, aligns with the original Oracle job's output for the same period.
    *   **Key Metrics:** Aggregated values (e.g., `bfc_age`, `bfc_count`) for specific `cntrct_id`s are consistent with the original system.
    *   **Data Integrity:** Spot-check a representative sample of `bindefrist` dates for various contract scenarios, especially those impacted by the `bfc_get_bindefrist` logic, to ensure they are calculated correctly.
    *   **Incremental Logic:** Verify that the `MERGE` statement correctly identifies and updates changed records and inserts new ones, without unintended side effects.
*   **Performance:** The job completes within an acceptable time frame, ideally comparable to or faster than the original Oracle execution, and within defined BigQuery cost limits.
*   **Logging & Alerting:** Relevant logs are generated in Cloud Logging, and any configured alerts (e.g., for failures) are triggered correctly.

## 7. Rollback Procedure

In the event of critical failure or data corruption after go-live, the following rollback procedure should be followed:

1.  **Immediate Action:**
    *   **Pause Airflow DAG:** Immediately pause or disable the `dw_bert_ausd_v_ta_c_bfc` DAG in the Cloud Composer UI to prevent further execution.
    *   **Re-enable Original UC4 Job:** Re-enable the original `DW.BERT_AUSD_V_TA_C_BFC` job in UC4 to ensure business continuity and data updates continue via the legacy system.
2.  **Data Rollback (if necessary):**
    *   **BigQuery Time Travel:** Utilize BigQuery's time travel feature to restore the `sof$ta_c_bfc` table to a state before the problematic run. This can be done by running a `CREATE TABLE ... AS SELECT * FROM sof_ta_c_bfc FOR SYSTEM_TIME AS OF TIMESTAMP 'YYYY-MM-DD HH:MM:SS UTC'` or `CREATE OR REPLACE TABLE ...` statement.
    *   **Backup Restore:** If time travel is insufficient (e.g., for very old states or if the table was completely dropped), restore the table from the most recent BigQuery snapshot or backup.
    *   **Targeted Correction:** If the issue is localized (e.g., specific records were incorrectly updated), a targeted `DELETE` and re-`INSERT` or `UPDATE` might be performed based on the specific error.
3.  **Code Rollback:**
    *   **Remove Airflow DAG:** Delete the `dw_bert_ausd_v_ta_c_bfc.py` DAG file from the Cloud Composer DAGs folder.
    *   **Remove Scripts:** Delete the `r_ausd_v_ta_c_bfc.py` and `d_ausd_v_ta_c_bfc.bqsql` files from the GCS bucket.
    *   **Remove BigQuery UDF/SP:** Drop the `bfc_get_bindefrist` UDF/Stored Procedure from BigQuery.
4.  **Root Cause Analysis:**
    *   Thoroughly investigate the cause of the failure in the GCP environment using Airflow logs, Dataproc logs, and BigQuery job history. Address the identified issues before attempting re-deployment.