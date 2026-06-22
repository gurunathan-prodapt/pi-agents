# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_v_ta_inv_def.ksh` workflow, originally residing at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`. This job is responsible for synchronizing contract data into the `SOF$TA_INV_DEF` table.

The migration re-platforms the entire workflow from its legacy KornShell and Oracle SQL*Plus environment to Google Cloud Platform (GCP). The target architecture leverages:
*   **Google Cloud Composer (Apache Airflow)** for workflow orchestration.
*   **Google BigQuery** for data warehousing and storage of both source staging tables and the final `SOF$TA_INV_DEF` table.
*   **Google Dataform** for managing, versioning, and executing the SQL transformations.

The core transformation logic, previously in `d_ausd_v_ta_inv_def.sql`, has been refactored into BigQuery SQL within Dataform. The KornShell wrapper scripts (`r_ausd_v_ta_inv_def.ksh`, `k_ausd_v_ta_inv_def.ksh`) have been replaced by a Python-based Airflow DAG. A critical external dependency on the Carmen database via an Oracle DB link will now be handled by a dedicated data ingestion service into BigQuery staging tables.

## 2. Generated artifacts

The migration process has generated the following files, which constitute the new solution on GCP:

*   **`dataform.json`**
    *   **Role**: Dataform project configuration file. It defines the default database, location, and schema settings for the Dataform project, ensuring that models are built within the specified GCP project and BigQuery datasets.

*   **`definitions/stg_carmen/dwtk_meldungen.sqlx`**
    *   **Role**: Dataform declaration for the `dwtk_meldungen` staging table. This file informs Dataform about the existence of this table in the `stg_carmen` BigQuery dataset, which is populated by an external ingestion process. It does not generate any SQL itself but allows other Dataform models to reference it.

*   **`definitions/stg_carmen/cds_ta_inv_definition.sqlx`**
    *   **Role**: Dataform declaration for the `cds_ta_inv_definition` staging table. Similar to `dwtk_meldungen.sqlx`, it declares an externally ingested table from the Carmen database, making it available for use in Dataform transformations.

*   **`definitions/stg_carmen/cds_ta_inv_cont_config.sqlx`**
    *   **Role**: Dataform declaration for the `cds_ta_inv_cont_config` staging table. Declares another externally ingested table from the Carmen database.

*   **`definitions/stg_carmen/cds_ta_care_description.sqlx`**
    *   **Role**: Dataform declaration for the `cds_ta_care_description` staging table. Declares the final externally ingested table from the Carmen database.

*   **`definitions/dwh/sof_ta_inv_def.sqlx`**
    *   **Role**: The core Dataform model responsible for the data transformation. This file contains the BigQuery SQL logic derived from the original `d_ausd_v_ta_inv_def.sql`. It defines how the `sof_ta_inv_def` table in the `dwh` BigQuery dataset is created and populated, including joins, filters, and function conversions. It is configured as a `type: table`, implying a full refresh strategy.

*   **`assertions/sof_ta_inv_def_not_empty.sqlx`**
    *   **Role**: A Dataform assertion to ensure basic data quality. This assertion checks that the `sof_ta_inv_def` table is not empty after the transformation, providing a quick validation step.

*   **`dags/r_ausd_v_ta_inv_def_dag.py`**
    *   **Role**: The Apache Airflow DAG for Cloud Composer. This Python script orchestrates the entire workflow. It includes tasks to:
        *   Fetch the `v_datum` parameter from the `dwtk_meldungen` table in BigQuery.
        *   Trigger the Dataform job to execute the `sof_ta_inv_def` transformation, passing `v_datum` as a compilation variable.
        *   Provide basic logging and success notifications.
    *   This DAG replaces the functionality of `r_ausd_v_ta_inv_def.ksh` and `k_ausd_v_ta_inv_def.ksh`.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Cloud Composer for Orchestration**: The original KornShell scripts (`r_ausd_v_ta_inv_def.ksh`, `k_ausd_v_ta_inv_def.ksh`) were re-platformed to a Python-based Apache Airflow DAG running on Cloud Composer. This decision was driven by the need for a managed, scalable, and cloud-native orchestration service with robust scheduling, monitoring, and error handling capabilities, leveraging the rich Airflow ecosystem and native GCP integrations.
*   **Dataform for SQL Transformation Management**: The Oracle SQL*Plus script (`d_ausd_v_ta_inv_def.sql`) was refactored into BigQuery SQL and managed by Dataform. Dataform provides version control, testing, dependency management, and deployment capabilities for SQL assets, promoting collaborative development and data quality. This decouples the transformation logic from the orchestration layer.
*   **BigQuery as Target Data Warehouse**: BigQuery was chosen as the target data warehouse for `SOF$TA_INV_DEF` and all source tables. Its serverless architecture, columnar storage, and petabyte-scale analytics capabilities make it ideal for modern data warehousing.
*   **Dedicated Data Ingestion for Carmen Database**: The direct Oracle DB link dependency was replaced by a requirement for a dedicated data ingestion pipeline (e.g., Cloud Data Fusion, custom Dataflow) to extract data from the Carmen database into BigQuery staging tables (`stg_carmen` schema). This decision improves reliability, performance, and security by decoupling the transformation from direct external database access and centralizing ingestion.
*   **Parameter Handling via Airflow XComs and Dataform Compilation Variables**: The dynamic `v_datum` parameter, previously handled by Oracle SQL*Plus `DEFINE` variables, is now fetched by an Airflow task using BigQuery and passed to Dataform as a compilation variable. This ensures dynamic parameterization of the SQL transformation based on data-driven logic.
*   **Full Refresh Strategy for `SOF$TA_INV_DEF`**: Based on the original `TRUNCATE TABLE` followed by `INSERT INTO`, the Dataform model for `sof_ta_inv_def` is configured as a `type: table`, which performs a full refresh by default. This maintains semantic equivalence with the legacy process.
*   **Oracle SQL to BigQuery SQL Translation**: Oracle-specific syntax (e.g., `NVL`, `TO_CHAR`, `TO_DATE`, `(+)` outer join operator, `/*+ hints */`) was translated to their BigQuery equivalents (e.g., `COALESCE`, `FORMAT_DATE`, `PARSE_DATE`, standard `LEFT JOIN`). This leverages BigQuery's native functions and optimizer.

**Notable Trade-offs**:
*   **Increased Infrastructure Complexity**: While managed, introducing Cloud Composer and Dataform adds new services to manage compared to a simple KornShell script.
*   **External Ingestion Dependency**: The success of this migrated job is entirely dependent on the separate implementation and reliability of the Carmen data ingestion pipeline, which is outside the scope of this specific migration.
*   **Oracle `(+)` Join Semantics**: Translating complex Oracle `(+)` outer join conditions, especially those involving `WHERE` clauses on the outer-joined table, to standard SQL `LEFT JOIN` can be semantically challenging. The current translation uses `COALESCE` with date conditions and `is_production` to approximate the behavior, but thorough validation with real data is crucial.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Configuration**:
    *   Ensure the `GCP_PROJECT_ID` placeholder in `dags/r_ausd_v_ta_inv_def_dag.py` and all Dataform SQLX files (`dataform.json`, `*.sqlx`) is replaced with your actual Google Cloud Project ID.
    *   Replace `REGION` and `DATAFORM_REPOSITORY_ID` placeholders in `dags/r_ausd_v_ta_inv_def_dag.py` with your Dataform repository's region and ID.

2.  **BigQuery Dataset Creation**:
    *   Manually create the following BigQuery datasets in your GCP project:
        *   `stg_carmen`: To hold the staging tables ingested from the Carmen database.
        *   `dwh`: To hold the final `sof_ta_inv_def` table.
        *   `dataform`: Default schema for Dataform's internal operations (if not already existing).
        *   `dataform_assertions`: Default schema for Dataform's assertion results (if not already existing).

3.  **Carmen Data Ingestion Pipeline Setup**:
    *   **Crucial Step**: Implement and configure the external data ingestion pipeline to extract data from the Carmen database and load it into the BigQuery `stg_carmen` dataset. This pipeline must populate the following tables *before* the Airflow DAG runs:
        *   `project_id.stg_carmen.dwtk_meldungen`
        *   `project_id.stg_carmen.cds_ta_inv_definition`
        *   `project_id.stg_carmen.cds_ta_inv_cont_config`
        *   `project_id.stg_carmen.cds_ta_care_description`
    *   Ensure this ingestion process is scheduled and reliable.

4.  **Cloud Composer Environment**:
    *   Verify that a Cloud Composer environment is provisioned and running in your GCP project.

5.  **Dataform Repository Setup**:
    *   Create a Dataform repository in your GCP project (if not already done).
    *   Initialize the Dataform project by uploading the `dataform.json` and all `definitions/*.sqlx` and `assertions/*.sqlx` files to the Dataform repository. This can typically be done via Git integration or Dataform CLI.

6.  **IAM Permissions**:
    *   Grant the Cloud Composer Service Account (e.g., `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary IAM roles:
        *   `BigQuery Data Editor` (or more granular roles) on the `stg_carmen` and `dwh` datasets to read from staging and write to target tables.
        *   `Dataform Editor` (or `Dataform Developer` and `Dataform Viewer`) to trigger and monitor Dataform jobs.
        *   `BigQuery Job User` to run BigQuery queries (e.g., for `get_v_datum` task).
        *   `Storage Object Viewer` and `Storage Object Creator` for accessing DAGs in the Composer bucket.

7.  **Airflow Connections**:
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment. This connection is used by the `BigQueryExecuteQueryOperator` and `DataformRunOperator`.

8.  **DAG Deployment**:
    *   Upload the `dags/r_ausd_v_ta_inv_def_dag.py` file to the DAGs folder of your Cloud Composer environment (typically a Cloud Storage bucket). Airflow will automatically detect and parse the DAG.

9.  **Scheduling**:
    *   Configure the desired schedule for the `r_ausd_v_ta_inv_def_dag` in the Airflow UI. The `schedule=None` placeholder in the DAG needs to be updated (e.g., to `@daily`, `0 0 * * *`).

## 5. Known gaps & unresolved references

The following items are known gaps, require further follow-up, or are flagged as redesign (B4) items:

*   **Carmen Data Ingestion Pipeline (B4 Item)**: The most significant unresolved reference is the actual implementation of the data ingestion pipeline from the external Carmen database into BigQuery staging tables. This migration assumes such a pipeline exists and reliably populates `stg_carmen.dwtk_meldungen`, `stg_carmen.cds_ta_inv_definition`, `stg_carmen.cds_ta_inv_cont_config`, and `stg_carmen.cds_ta_care_description`. Without this, the migrated job cannot run.
*   **KornShell Utility Script Re-implementation**: The original KornShell scripts sourced several utility files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`). While core orchestration and parameter passing are handled by Airflow, a detailed analysis of all functionalities within these utilities (e.g., specific logging formats, custom error handling, date calculations, environment setups) is required. Any critical logic not covered by standard Airflow features or the current DAG needs to be explicitly reimplemented in Python or as custom Airflow operators/hooks.
*   **Oracle `(+)` Join Semantics with `WHERE` Clauses**: The translation of Oracle's proprietary `(+)` outer join operator, especially when combined with `WHERE` clause conditions on the outer-joined table, can have subtle semantic differences compared to standard SQL `LEFT JOIN`. While `COALESCE` has been used to approximate the behavior for date and `is_production` filters, thorough testing with diverse datasets is required to confirm exact equivalence. This might require fine-tuning the BigQuery SQL.
*   **`DWPA_UTIL_SKRIPT.runstatement` Functionality**: The original Oracle script used `isbert_schema.DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE sof$ta_inv_def')`. This migration assumes `runstatement` primarily executes the provided DDL. If this utility performs additional complex logic (e.g., logging, auditing, conditional execution, or other DML/DDL operations), that functionality is not explicitly translated and needs to be investigated and potentially reimplemented.
*   **Missing `file_complexity` and `automation_rate`**: The absence of these metrics from the source inventory means the initial effort estimation and automation strategy might have been based on incomplete data. A manual assessment of these factors might still be beneficial for future planning.
*   **Error Handling and Alerting**: While Airflow provides robust error handling, the specific error reporting and alerting mechanisms from the original `f_alis_msgerr.ksh` need to be explicitly configured in the Airflow DAG (e.g., email alerts, PagerDuty, Slack notifications).

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **Ensure Prerequisites**: Complete all "Manual steps before go-live" (Section 4), especially the Carmen data ingestion.
2.  **Trigger Airflow DAG**:
    *   Navigate to the Cloud Composer UI.
    *   Find the `r_ausd_v_ta_inv_def_dag`.
    *   Manually trigger the DAG.
3.  **Monitor Airflow Logs**:
    *   Observe the progress of tasks in the Airflow UI (Graph View, Gantt Chart).
    *   Check task logs for `get_v_datum` to confirm the correct date is being fetched.
    *   Monitor the `run_dataform_job` task for any Dataform-related errors or warnings.
4.  **Check Dataform Execution**:
    *   If `run_dataform_job` fails, navigate to the Dataform UI to inspect the specific job run, compilation errors, or assertion failures.
5.  **Query BigQuery Target Table**:
    *   Once the Airflow DAG completes successfully, use the BigQuery UI or `bq` command-line tool to query `project_id.dwh.sof_ta_inv_def`.

**What "passing" means:**

*   **Airflow DAG Completion**: The `r_ausd_v_ta_inv_def_dag` completes successfully without any failed tasks.
*   **Dataform Job Success**: The Dataform job triggered by Airflow completes successfully, indicating that all models (including `sof_ta_inv_def`) were built without compilation or runtime errors.
*   **Assertion Pass**: The `sof_ta_inv_def_not_empty` assertion (and any other custom assertions) passes, confirming basic data integrity.
*   **Target Table Existence and Data**: The `project_id.dwh.sof_ta_inv_def` table exists in BigQuery and contains data.
*   **Row Count Verification**: The number of rows in `project_id.dwh.sof_ta_inv_def` matches the expected count from the source system after transformation. This is a critical check.
*   **Data Quality Spot Checks**: Perform targeted queries on `project_id.dwh.sof_ta_inv_def` to verify:
    *   Correctness of `v_datum` filtering.
    *   Accuracy of `COALESCE` (formerly `NVL`) function translations.
    *   Correctness of join logic and column mappings.
    *   Absence of unexpected NULLs or incorrect values.
*   **Performance**: The overall execution time of the Airflow DAG and the Dataform job is within acceptable limits.

## 7. Rollback procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action**:
    *   **Pause/Delete Airflow DAG**: Immediately pause or delete the `r_ausd_v_ta_inv_def_dag` in the Cloud Composer UI to prevent further execution of the migrated job.
    *   **Re-enable Legacy Job**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh` job in the legacy environment.

2.  **Data Reversion (if necessary)**:
    *   **Target Table**: If the `project_id.dwh.sof_ta_inv_def` table was corrupted or contains incorrect data, and if a backup strategy was in place (e.g., BigQuery table snapshots, time travel), revert the table to a known good state. If the table is always a full refresh, simply stopping the new job and restarting the old one will eventually overwrite the target.
    *   **Downstream Impact**: Assess and mitigate any immediate impact on downstream systems that might have consumed data from the potentially corrupted `dwh.sof_ta_inv_def` table.

3.  **Code Removal**:
    *   **Airflow DAG**: Remove the `dags/r_ausd_v_ta_inv_def_dag.py` file from the Cloud Composer DAGs folder.
    *   **Dataform Project**: Delete the Dataform project and its associated files from the Dataform repository.

4.  **Data Ingestion (if applicable)**:
    *   If the Carmen data ingestion pipeline was specifically set up for this migration and is causing issues, pause or disable it.

5.  **Communication**:
    *   Notify relevant stakeholders about the rollback and the status of the data synchronization process.

**Considerations for Rollback**:
*   **Data Consistency**: Ensure that the rollback does not leave data in an inconsistent state between the legacy and new systems, especially if the target table is shared.
*   **Downtime**: Rollback procedures should aim to minimize downtime for critical data processes.
*   **Root Cause Analysis**: After a rollback, a thorough root cause analysis must be performed to identify and resolve the underlying issues before attempting re-migration.