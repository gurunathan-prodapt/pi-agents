# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh`, responsible for orchestrating the reconciliation of contract data for the `ta_discount` table, has been migrated.

The migration re-platforms this orchestration and data processing workflow from its legacy KornShell environment to Google Cloud Platform. The target architecture leverages **Cloud Composer (Apache Airflow)** for workflow orchestration and **BigQuery** for data storage and transformation.

## 2. Generated Artifacts

The following artifact was generated as part of this migration:

*   **`dags/r_ausd_v_ta_discount_migration_dag.py`**
    *   **Role:** This Python file defines an Apache Airflow DAG (`r_ausd_v_ta_discount_migration_dag`). It replaces the original KornShell wrapper script, handling environment setup, parameter parsing, and orchestrating the core data processing logic. It integrates with Cloud Composer for scheduling and execution, and uses a `BigQueryOperator` to execute the translated data reconciliation logic.

## 3. Key Design Decisions

The following key design decisions were made for this migration:

*   **Orchestration Re-platforming:** The legacy KornShell wrapper (`r_ausd_v_ta_discount.ksh`) was replaced by an Apache Airflow DAG running on Cloud Composer. This provides robust scheduling, monitoring, error handling, and dependency management capabilities native to GCP.
*   **Core Logic Translation:** The core data reconciliation logic, originally encapsulated within `k_ausd_v_ta_discount.ksh` (assumed to be SQL-based), is to be translated into BigQuery-compatible SQL. This SQL will be executed directly within the Airflow DAG using a `BigQueryOperator`, leveraging BigQuery's scalability and performance.
*   **Data Storage Migration:** The `ta_discount` table, central to the reconciliation process, will be migrated from its legacy database to BigQuery.
*   **Parameter Handling:** Command-line parameters (`-j`, `-f`) from the original script are replaced by Airflow DAG parameters (`job_kennung`, `dw_eintrags_nr`), allowing for flexible input during manual triggers or scheduled runs.
*   **Logging and Error Handling:** Custom KornShell logging (`DWMSG_` functions) and error trapping mechanisms are replaced by Airflow's native logging, which integrates seamlessly with Google Cloud Logging. Airflow's built-in retry mechanisms and alerting capabilities will handle error management.
*   **External Dependency Absorption:** Functionalities provided by legacy shell utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are absorbed by standard Python libraries, Airflow's native features, or Cloud Composer's environment management.

**Notable Trade-offs:**
*   The detailed analysis and translation of `k_ausd_v_ta_discount.ksh` into BigQuery SQL is a significant remaining task, currently represented by a placeholder in the generated DAG. This introduces a dependency on further manual effort.
*   While Airflow provides powerful orchestration, replicating highly specific custom logging formats or external system interactions from `DWMSG_` functions might require additional Python scripting within the DAG.

## 4. Manual Steps Before Go-Live

Before the migrated DAG can be put into production, the following manual steps are required:

1.  **BigQuery Dataset and Table Creation:**
    *   Create the target BigQuery dataset (e.g., `your_bigquery_dataset`) if it doesn't already exist.
    *   Create the `ta_discount` table in BigQuery with a schema that accurately reflects the original table. This DDL should be derived from the legacy system's schema.
2.  **Data Migration:**
    *   Migrate all historical data from the legacy `ta_discount` table to the newly created BigQuery `ta_discount` table. This can be done using tools like `bq load`, Cloud Storage transfers, or Dataflow jobs.
3.  **IAM Permissions:**
    *   Ensure the Cloud Composer service account has the necessary IAM roles to:
        *   Read/Write data in the target BigQuery dataset (`BigQuery Data Editor` or more granular permissions).
        *   Write logs to Cloud Logging (`Logs Writer`).
        *   Access any other GCP resources if the translated `k_ausd_v_ta_discount.ksh` logic requires them (e.g., Cloud Storage for temporary files).
4.  **Translate `k_ausd_v_ta_discount.ksh` to BigQuery SQL:**
    *   **Crucial Step:** Analyze the source code of `k_ausd_v_ta_discount.ksh`.
    *   Translate its core data reconciliation logic into BigQuery-compatible SQL queries.
    *   Replace the placeholder SQL within the `process_ta_discount_data` task in `dags/r_ausd_v_ta_discount_migration_dag.py` with the actual translated SQL. Ensure the SQL references the correct BigQuery project and dataset (e.g., `your-gcp-project.your_bigquery_dataset.ta_discount`).
5.  **Airflow Connections (if applicable):**
    *   If the BigQuery operations require specific credentials or non-default project/dataset configurations, ensure an appropriate Airflow connection (e.g., `google_cloud_default`) is configured in Cloud Composer.
6.  **Deployment to Cloud Composer:**
    *   Upload the `dags/r_ausd_v_ta_discount_migration_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Scheduling Configuration:**
    *   The DAG is currently set to `schedule=None`. If automated scheduling is required, update the `schedule` parameter in the DAG definition (e.g., `schedule="@daily"` or a cron expression) or configure an external trigger.

## 5. Known Gaps & Unresolved References

The following items have been identified as known gaps or require further follow-up:

*   **Detailed Analysis of `k_ausd_v_ta_discount.ksh` (B4 Item):** The most significant unresolved item is the exact content and complexity of `k_ausd_v_ta_discount.ksh`. Without its source, the precise translation to BigQuery SQL or other GCP services remains an assumption. This script needs to be thoroughly analyzed to accurately implement its logic.
*   **Custom `DWMSG_` Functions:** The exact mapping and replication of all `DWMSG_` function functionalities (e.g., specific log formats, external system interactions for alerts) need to be determined and implemented in Airflow. While basic logging is covered, any advanced features require explicit handling.
*   **Legacy Environment Variables:** Any critical environment variables sourced by `.dw_init` that are not implicitly handled by GCP services or Airflow need explicit identification and configuration within the Airflow DAG or Cloud Composer environment variables.
*   **Parameter Equivalence:** A thorough validation is needed to ensure that Airflow DAG parameters provide the same flexibility, validation, and default behavior as the original `getopts` implementation in the KornShell script.
*   **Absence of Lineage:** The lack of detailed lineage in the metadata implies that deeper analysis of the `k_ausd_v_ta_discount.ksh` script and any other potential implicit dependencies (e.g., files or databases accessed indirectly) is crucial to avoid missing components in the migration.

## 6. Validation

To validate the migrated workflow, follow these steps:

1.  **Unit Testing:**
    *   **BigQuery SQL:** Manually execute the translated BigQuery SQL (from `k_ausd_v_ta_discount.ksh`) against a test dataset in BigQuery. Verify that the output data matches the expected transformation logic.
    *   **Airflow Tasks:** If any Python functions are used (e.g., for custom logic or logging), write unit tests for these functions.
2.  **Integration Testing:**
    *   Trigger the `r_ausd_v_ta_discount_migration_dag` in a Cloud Composer development/staging environment.
    *   Monitor the Airflow UI to ensure all tasks complete successfully without errors.
    *   Check Cloud Logging for the DAG's execution logs to confirm expected messages and no unexpected errors.
3.  **Functional Testing (Data Validation):**
    *   Run the legacy `r_ausd_v_ta_discount.ksh` script in the original environment with a specific set of input parameters.
    *   Run the `r_ausd_v_ta_discount_migration_dag` in the Cloud Composer environment with the *same* input parameters.
    *   Compare the state of the `ta_discount` table (or relevant output) in the legacy system with the `ta_discount` table in BigQuery after the respective runs. This comparison should be done on a representative sample of data, or ideally, the entire dataset.

**What "passing" means:**

*   The Airflow DAG completes successfully (all tasks turn green in the Airflow UI).
*   No critical errors are reported in Cloud Logging for the DAG run.
*   The data in the BigQuery `ta_discount` table (or any other output generated by the core logic) is functionally identical or equivalent to the output produced by the legacy `r_ausd_v_ta_discount.ksh` script, given the same input.
*   Performance metrics (e.g., execution time) are acceptable or improved compared to the legacy system.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Runs:** Immediately pause or unschedule the `r_ausd_v_ta_discount_migration_dag` in Cloud Composer to prevent further execution of the migrated workflow.
2.  **Revert to Legacy System:** Re-enable or resume the scheduling/triggering of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh` script in the legacy environment.
3.  **Data Restoration (if necessary):**
    *   If the migrated workflow made irreversible changes to the BigQuery `ta_discount` table and these changes are incorrect, restore the `ta_discount` table in BigQuery from a recent backup taken before the problematic run.
    *   Alternatively, if the BigQuery operations are idempotent or reversible (e.g., `MERGE` statements with proper keys), consider running a corrective BigQuery job to revert the data to its previous state.
4.  **Investigate and Remediate:** Analyze the root cause of the failure in the migrated DAG, make necessary corrections, and re-test in a staging environment before attempting another go-live.