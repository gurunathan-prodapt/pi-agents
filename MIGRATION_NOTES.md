# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`.

The original job was a KornShell (ksh) script responsible for orchestrating the execution of an Oracle SQL script (`d_ausd_v_ta_p_discount_rr.sql`). This SQL script performed data transformation and loading into the `sof$ta_p_discount_rr` table, related to discount rate processing, by joining data from `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`. The ksh script also handled parameter parsing, environment setup, error logging, and captured record counts.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **Apache Airflow on Cloud Composer** for orchestration.
*   **Google BigQuery** for data storage and processing.

The KornShell logic has been translated into a Python-based Airflow DAG, and the Oracle SQL logic has been converted to BigQuery SQL.

## 2. Generated artifacts

The migration process generated the following file:

*   **`k_ausd_v_ta_p_discount_rr.py`**:
    *   **Role**: This is the Apache Airflow DAG (Directed Acyclic Graph) responsible for orchestrating the data processing. It defines the workflow, including the BigQuery SQL execution.
    *   **Description**: This Python script contains the Airflow DAG definition, including default arguments, task definitions (start, BigQuery processing, end), and task dependencies. The core data transformation logic is embedded as BigQuery SQL within a `BigQueryExecuteQueryOperator`.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Orchestration from ksh to Airflow**: The KornShell script's role in setting up the environment, handling parameters, executing SQL, and logging was replaced by Airflow's native capabilities. Airflow provides robust scheduling, monitoring, logging, and error handling features, eliminating the need for custom shell utilities.
*   **Data Processing from Oracle SQL to BigQuery SQL**: The Oracle SQL script (`d_ausd_v_ta_p_discount_rr.sql`) was directly translated into BigQuery-compliant SQL. This involved:
    *   Replacing Oracle-specific syntax (e.g., `NVL`, `TO_CHAR` date formats) with BigQuery equivalents (e.g., `IFNULL`, `FORMAT_DATE`).
    *   Removing Oracle-specific hints (e.g., `/*+ parallel(...) */`) as BigQuery automatically handles query parallelism.
    *   Mapping Oracle table names (e.g., `sof$ta_p_discount_rr`) to BigQuery table names (e.g., `your_project.your_dataset.sof_ta_p_discount_rr`).
*   **Consolidated BigQuery Operation**: The entire data transformation (table creation if needed, and data insertion) is encapsulated within a single `BigQueryExecuteQueryOperator`. This simplifies the DAG structure and leverages BigQuery's ability to execute complex multi-statement queries efficiently.
*   **Handling of `TRUNCATE TABLE`**: The original Oracle script used `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount_rr');`. In the migrated DAG, the `write_disposition` parameter of the `BigQueryExecuteQueryOperator` is set to `WRITE_APPEND`. If a full truncate-and-load behavior is required, this should be changed to `WRITE_TRUNCATE` or an explicit `TRUNCATE TABLE` statement should precede the `INSERT` within the SQL. The current generated SQL includes `CREATE TABLE IF NOT EXISTS` and then `INSERT`, implying an append or initial load.
*   **Elimination of Temporary Files**: The `tmpFile` used in the ksh script to store record counts has been eliminated. BigQuery's query history and logging provide sufficient auditing, and record counts can be obtained directly via SQL if needed for specific downstream processes.
*   **Parameter Handling**: The original ksh script used `getopts` for `p_JobKennung` and `p_EintragsNr`. In the current Airflow DAG, these parameters are not explicitly passed into the BigQuery SQL. If these parameters are dynamic and influence the SQL logic, they would need to be passed as templated fields to the `BigQueryExecuteQueryOperator` or handled via Airflow Variables/XComs. For this migration, it's assumed the core SQL logic is static or parameters are derived differently.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (e.g., `your_project.your_dataset`) exists in your GCP project. If not, create it.
2.  **BigQuery Table Schema Definition and Data Loading**:
    *   **Source Tables**: The tables `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, and `dwtk_meldungen` (if used for `v_datum` logic) must be created in BigQuery with appropriate schemas and populated with data migrated from their Oracle counterparts.
    *   **Target Table**: The `sof_ta_p_discount_rr` table will be created by the DAG if it doesn't exist (`CREATE TABLE IF NOT EXISTS`). However, its schema should be reviewed and potentially pre-defined for consistency and to ensure correct data types.
    *   **Replace Placeholders**: Update all instances of `` `your_project.your_dataset` `` in the `k_ausd_v_ta_p_discount_rr.py` DAG with your actual GCP project ID and BigQuery dataset name.
3.  **IAM Permissions**:
    *   Ensure the service account associated with your Cloud Composer environment (or the Airflow worker running the DAG) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (to write to `sof_ta_p_discount_rr`).
        *   `BigQuery Data Viewer` (to read from `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, `dwtk_meldungen`).
        *   `BigQuery Job User` (to run BigQuery queries).
4.  **Airflow Connection**:
    *   The `BigQueryExecuteQueryOperator` typically uses the default GCP connection. Verify this connection is correctly configured in your Airflow environment and has the necessary scope.
5.  **Scheduling Configuration**:
    *   The `schedule_interval` in the DAG is currently set to `None`. Update this to the desired cron expression or timedelta (e.g., `'0 0 * * *'` for daily at midnight) to match the original job's execution frequency.
6.  **BigQuery Region**:
    *   Update the `location="US"` parameter in the `BigQueryExecuteQueryOperator` to match your BigQuery dataset's region (e.g., `"EU"`, `"us-central1"`).

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent known gaps in the current migration:

*   **Dynamic Parameter Handling (`p_JobKennung`, `p_EintragsNr`)**: The original ksh script accepted `p_JobKennung` and `p_EintragsNr` as command-line parameters. The current Airflow DAG does not explicitly pass these into the BigQuery SQL.
    *   **Action Required**: Determine if these parameters are critical for the BigQuery SQL logic. If so, they need to be integrated into the DAG, potentially as Airflow DAG run configurations, Airflow Variables, or passed as templated fields to the `BigQueryExecuteQueryOperator`.
*   **`v_datum` (Stichtag) Logic**: The original Oracle SQL derived `v_datum` from `isbert_schema.dwtk_meldungen`. This logic is mentioned in the design document but not explicitly included in the generated BigQuery SQL.
    *   **Action Required**: If `v_datum` is a critical filter or parameter for the `INSERT` statement, the BigQuery SQL needs to be updated to incorporate this date derivation from `your_project.your_dataset.dwtk_meldungen`.
*   **Record Count (`tmpFile`)**: The original ksh script captured a record count into `tmpFile`. This functionality is not replicated in the current DAG.
    *   **Action Required**: If this record count is required for auditing or downstream processes, a separate `BigQueryExecuteQueryOperator` can be added to count rows and store the result in XComs or a dedicated logging/metadata table.
*   **Specific Error Handling (`f_alis_msgerr.ksh`)**: The original ksh script used `f_alis_msgerr.ksh` for specific error logging and reporting. Airflow provides general logging, but any custom error codes or detailed reporting requirements from `f_alis_msgerr.ksh` are not replicated.
    *   **Action Required**: Review the specific error handling requirements and implement custom Python logic or Airflow callbacks if the default Airflow logging is insufficient.
*   **`VIA` Table / `MERGE` Discrepancy**: The migration design document noted a discrepancy regarding a `MERGE` statement to a `VIA` table that was not present in the provided SQL snippet.
    *   **Action Required**: Investigate if such a `MERGE` operation exists in the complete original SQL. If it does, the BigQuery SQL needs to be updated to include a `MERGE` statement, and the target `VIA` table must be migrated to BigQuery.
*   **`write_disposition` for `TRUNCATE`**: The generated DAG uses `write_disposition="WRITE_APPEND"`. If the job's intent is to always clear the target table before inserting new data (i.e., a full truncate-and-load), this should be changed to `write_disposition="WRITE_TRUNCATE"`.
*   **`schedule_interval`**: Currently set to `None`. This needs to be configured based on the original job's schedule.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data**:
    *   Ensure the BigQuery source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`, `dwtk_meldungen`) contain representative test data that mirrors the production Oracle environment.
2.  **Deploy DAG**:
    *   Upload the `k_ausd_v_ta_p_discount_rr.py` file to your Cloud Composer environment's DAGs folder.
3.  **Trigger DAG**:
    *   Manually trigger the `k_ausd_v_ta_p_discount_rr` DAG from the Airflow UI.
4.  **Monitor Execution**:
    *   Observe the DAG run in the Airflow UI, ensuring all tasks (`start`, `process_discount_rr`, `end`) complete successfully without errors. Check task logs for any warnings or errors.
5.  **Verify Data in BigQuery**:
    *   Query the target table `your_project.your_dataset.sof_ta_p_discount_rr` in BigQuery.
    *   **"Passing" Criteria**:
        *   **DAG Success**: The Airflow DAG completes with a "success" status.
        *   **Table Population**: The `sof_ta_p_discount_rr` table is populated with data.
        *   **Record Count**: The number of rows in `sof_ta_p_discount_rr` matches the expected count based on the source data and original job's logic. If the `tmpFile` record count logic was re-implemented, verify that count.
        *   **Data Integrity**: Perform spot checks on the data in `sof_ta_p_discount_rr` to ensure values, joins, and transformations are correct and match the expected output from the original Oracle job.
        *   **Performance**: The BigQuery query completes within an acceptable timeframe.

## 7. Rollback procedure

In case of issues or failure during the migration or post-go-live, the following rollback procedure can be executed:

1.  **Disable Airflow DAG**:
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_p_discount_rr` DAG to prevent further executions.
    *   Optionally, remove the `k_ausd_v_ta_p_discount_rr.py` file from the DAGs folder.
2.  **Re-enable Original Job**:
    *   Re-enable the original `k_ausd_v_ta_p_discount_rr.ksh` job in the legacy environment.
3.  **Data Remediation (if necessary)**:
    *   If the migrated job introduced incorrect data into `your_project.your_dataset.sof_ta_p_discount_rr`, you may need to:
        *   Truncate the `sof_ta_p_discount_rr` table in BigQuery.
        *   Reload the `sof_ta_p_discount_rr` table with correct data, potentially by re-running the original Oracle job or restoring from a backup.
    *   Assess the impact on any downstream systems that might have consumed data from the BigQuery target table.
4.  **Investigate and Rectify**:
    *   Analyze the logs from the failed Airflow DAG run and BigQuery job history to identify the root cause of the issue.
    *   Correct the `k_ausd_v_ta_p_discount_rr.py` DAG or BigQuery SQL as needed.