# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh` has been migrated. This script, responsible for orchestrating the date-based extraction of address data, has been re-platformed to **Google Cloud Platform (GCP)**.

The new architecture leverages:
*   **Cloud Composer (Apache Airflow)** for job orchestration, parameter handling, and logging.
*   **BigQuery** for the core data extraction and transformation logic, which was previously handled by the invoked `k_ausd_adressen.ksh` script.

This migration aims to modernize the data pipeline, improve scalability, enhance observability, and integrate with the broader GCP data ecosystem.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dwh_util/utils.py`**
    *   **Role:** This Python module serves as a utility library, refactoring common functions from the original shell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). It provides standardized logging, parameter validation, job entry number generation, and date utility functions for use within the Airflow DAG.

*   **`dags/r_ausd_adressen_ksh_migration.py`**
    *   **Role:** This is the main Airflow Directed Acyclic Graph (DAG) that replaces the `r_ausd_adressen.ksh` KornShell script. It defines the orchestration workflow, including:
        *   Parsing and defaulting command-line parameters (`stichtag`, `wiederanlaufwert`).
        *   Validating input parameters.
        *   Initializing job-specific logging and tracking.
        *   Invoking the core data processing logic (now in BigQuery SQL).
        *   Updating the final job status.

*   **`sql/k_ausd_adressen_logic.sql`**
    *   **Role:** This SQL script contains the core data extraction and transformation logic, re-implemented in BigQuery Standard SQL. It replaces the functionality previously encapsulated within `k_ausd_adressen.ksh`. This script is executed by the `BigQueryOperator` within the Airflow DAG, receiving dynamic parameters (e.g., `stichtag`, `wiederanlaufwert`) from the orchestrator. It is responsible for selecting, transforming, and inserting address data into the target BigQuery table.

## 3. Key Design Decisions

*   **Orchestration to Airflow (Cloud Composer):** The `r_ausd_adressen.ksh` script's primary role was orchestration, parameter handling, and error management. Airflow is a natural fit for this, offering robust scheduling, dependency management, native logging, and a rich ecosystem of operators. This replaces custom shell scripting for these concerns.
*   **Core Logic to BigQuery SQL:** The invoked `k_ausd_adressen.ksh` script, which performs the actual data processing, was migrated to BigQuery SQL. This decision leverages BigQuery's serverless, scalable, and cost-effective data warehousing capabilities, aligning with the target GCP architecture. It eliminates the need for a separate execution environment for the shell script and allows for direct data manipulation within BigQuery.
*   **Refactoring Utilities to Python:** The various shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were refactored into a single Python module (`dwh_util/utils.py`). This promotes code reusability, maintainability, and seamless integration with the Python-based Airflow DAG.
*   **Airflow Native Logging and Error Handling:** Custom shell `DWMSG_` functions and `trap` commands for logging and error handling were replaced by Airflow's native logging mechanisms and Python's exception handling. This provides a standardized and observable way to monitor job execution and failures within the Cloud Composer environment.
*   **Parameterization via Airflow DAG `params` and XComs:** Command-line arguments (`-s`, `-l`) from the original script are now handled by Airflow DAG `params`, allowing for easy configuration via the Airflow UI or API. Intermediate values are passed between tasks using Airflow's XComs, replacing shell environment variables.

**Notable Trade-offs:**
*   **Increased Initial Setup Complexity:** Migrating from a simple shell script to an Airflow DAG on Cloud Composer involves a higher initial setup and development overhead (e.g., Python development, Cloud Composer environment setup) compared to maintaining the original shell script.
*   **Dependency on `k_ausd_adressen.ksh` Migration:** The success of this orchestrator migration is heavily dependent on the complete and accurate migration of the core `k_ausd_adressen.ksh` logic to BigQuery SQL. Any delays or issues with the core logic directly impact this DAG.
*   **Re-implementation of Custom Logic:** Custom shell functions (e.g., `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`) required re-implementation in Python, which necessitated careful analysis of their original behavior.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in a production environment, the following manual steps are required:

1.  **BigQuery Schema/Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_target_dataset`) exists.
    *   Create the target table (`dwh_target_addresses`) in BigQuery with the appropriate schema as defined by the `sql/k_ausd_adressen_logic.sql` script.
    *   Verify that source tables (e.g., `crs_source_addresses`) are available and accessible in BigQuery within `your_source_dataset`.

2.  **IAM/Permissions:**
    *   The Google Cloud service account used by Cloud Composer (Airflow) must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Data Editor` for specific datasets) to write to `your_target_dataset`.
        *   `BigQuery Job User` to run BigQuery queries.
        *   `Storage Object Viewer` to read DAG files and SQL scripts from the Cloud Composer bucket.
        *   Permissions for Cloud Logging to write logs.

3.  **Connection Strings / Secrets:**
    *   The generated code assumes BigQuery access via the default GCP connection. No explicit connection strings are required for BigQuery itself.
    *   **Critical:** If the original `k_ausd_adressen.ksh` or any of its dependencies accessed external systems requiring credentials (e.g., other databases, APIs), these credentials must be securely stored in **Google Secret Manager** and accessed via Airflow connections or Python code. The current generated code does not include this, as the original script did not explicitly show credential handling.

4.  **Scheduling:**
    *   The `schedule_interval` for the `r_ausd_adressen_ksh_migration` DAG is currently set to `None`. For automated execution, update this to a suitable cron expression (e.g., `'0 0 * * *'` for daily at midnight) or configure external triggers.

5.  **Deployment:**
    *   Upload `dags/r_ausd_adressen_ksh_migration.py` to the DAGs folder of your Cloud Composer environment.
    *   Upload `sql/k_ausd_adressen_logic.sql` to a location accessible by the DAG, typically within a `sql/` subdirectory inside the DAGs folder (e.g., `dags/sql/k_ausd_adressen_logic.sql`).
    *   Deploy `dwh_util/utils.py` as a custom plugin or a Python package within your Cloud Composer environment so it's available on the Python path for the DAG.

6.  **Placeholder Replacement:**
    *   Edit `sql/k_ausd_adressen_logic.sql` and replace all instances of `'your-gcp-project'`, `'your_source_dataset'`, and `'your_target_dataset'` with your actual GCP project ID and BigQuery dataset names.

## 5. Known Gaps & Unresolved References

*   **`k_ausd_adressen.ksh` Full Migration Status:** The `BigQueryOperator` in the DAG assumes that the entire logic of `k_ausd_adressen.ksh` has been accurately and completely translated into `sql/k_ausd_adressen_logic.sql`. If `k_ausd_adressen.ksh` has not been fully migrated or contains complex logic not yet captured in SQL, the DAG will not function as expected. This is the most significant dependency and potential gap.
*   **`dwh_util/utils.py` Completeness:**
    *   The `get_zeitraum_dates` function is a simplified placeholder. Its full business logic, as derived from `h_alis_date.ksh`'s `DWDate_Gib_Zeitraum`, needs to be thoroughly reviewed and implemented if it involves complex date range calculations beyond just the `stichtag`.
    *   `generate_new_entry_number` and `log_job_entry` are basic implementations. If the original `DW_EintragsNr` generation or `DWMSG_` functions involved interaction with a central metadata repository or specific formatting, these need further refinement.
*   **`wiederanlaufwert` Logic in SQL:** The `wiederanlaufwert` logic in `sql/k_ausd_adressen_logic.sql` is currently commented out or a placeholder. The exact impact of this parameter (e.g., conditional deletion, specific contract reprocessing) needs to be fully translated from `k_ausd_adressen.ksh` into BigQuery SQL.
*   **`$HOME/.dw_init` Dependencies:** The contents and implications of `$HOME/.dw_init` (e.g., environment variables, sourced configurations) need to be fully analyzed and translated into Airflow environment variables, Airflow connections, or Python configuration files.
*   **CRS System Ingestion Pipeline:** The migration assumes that the source data from the CRS system is already available in BigQuery (e.g., in `crs_source_addresses`). The pipeline responsible for ingesting data from CRS into BigQuery is a prerequisite and is outside the scope of this migration.
*   **Comprehensive Error Reporting (`DWMSG_`):** While basic error logging is implemented, the full suite of `DWMSG_` functions, including specific error codes, detailed reporting, and potential integration with legacy monitoring systems, might require further mapping to Airflow's alerting mechanisms (e.g., email, Slack, PagerDuty) or a custom BigQuery logging table.
*   **Dynamic Lineage:** The original `lineage_edges` were incomplete. While the migration explicitly defines dependencies, ensuring comprehensive data lineage tracking in the new GCP environment (e.g., via Data Catalog) might require additional configuration.

## 6. Validation

To ensure the successful migration and correct functioning of the `r_ausd_adressen_ksh_migration` DAG, follow these validation steps:

1.  **Code Review & Unit Tests:**
    *   Thoroughly review `dags/r_ausd_adressen_ksh_migration.py`, `dwh_util/utils.py`, and `sql/k_ausd_adressen_logic.sql` for correctness, adherence to BigQuery best practices, and accurate translation of business logic.
    *   Implement unit tests for `dwh_util/utils.py` functions to verify their behavior independently.

2.  **Airflow DAG Syntax Check:**
    *   Run `airflow dags test dags/r_ausd_adressen_ksh_migration.py` (or equivalent in your Composer environment) to check for syntax errors and basic DAG structure validity.

3.  **Local Airflow Testing (if applicable):**
    *   If a local Airflow environment is available, run the DAG with `airflow tasks test r_ausd_adressen_ksh_migration <task_id> <date>` for individual tasks to verify their execution logic.

4.  **Cloud Composer Development Environment Testing:**
    *   Deploy the DAG to a non-production Cloud Composer environment.
    *   Trigger the DAG manually with various `stichtag` and `wiederanlaufwert` parameters:
        *   **Default `stichtag`:** Run without providing `stichtag` to verify it defaults to the current date.
        *   **Specific `stichtag`:** Run with a valid `DDMMYYYY` date.
        *   **Invalid `stichtag`:** Run with an invalid date format to test validation.
        *   **`wiederanlaufwert`:** Test with `0` and a non-zero value (once its logic is implemented in SQL).
    *   Monitor the Airflow UI for task success/failure, logs, and XCom values.

5.  **Data Validation:**
    *   **Row Counts:** Compare the number of rows inserted into `your-gcp-project.your_target_dataset.dwh_target_addresses` with the expected count from the source system for the given `stichtag`.
    *   **Data Accuracy:**
        *   Perform spot checks on a representative sample of address records. Compare values in the target BigQuery table with the original source data (if accessible).
        *   Verify that transformations (e.g., `address_key` generation, date parsing) are correct.
    *   **Metadata Columns:** Confirm that `extraction_stichtag`, `load_timestamp`, `job_identifier`, and `job_entry_number` are correctly populated in the target table.
    *   **Idempotency:** If `wiederanlaufwert` implies idempotent behavior (e.g., full refresh for a `stichtag`), run the DAG multiple times with the same parameters and ensure the final data state is consistent.

6.  **Logging and Monitoring Validation:**
    *   Verify that detailed logs appear in Cloud Logging for each task.
    *   If a custom BigQuery logging table is used for job status, ensure entries are correctly recorded for job initiation, `stichtag` info, and final status.
    *   Confirm that error conditions trigger appropriate alerts (if configured).

**"Passing" means:**
*   The `r_ausd_adressen_ksh_migration` DAG completes successfully without any failed tasks.
*   The `your-gcp-project.your_target_dataset.dwh_target_addresses` table is populated with the correct and accurate address data for the specified `stichtag`.
*   All expected logs are generated in Cloud Logging, and any custom job status tracking is updated correctly.
*   No unexpected errors or warnings are observed in the Airflow UI or Cloud Logging.
*   The data in the target table matches the expected output from the original `r_ausd_adressen.ksh` and `k_ausd_adressen.ksh` scripts for the same input parameters.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow this rollback procedure:

1.  **Pause New DAG:** Immediately pause the `r_ausd_adressen_ksh_migration` DAG in the Cloud Composer Airflow UI to prevent further executions.

2.  **Revert BigQuery Data (if necessary):**
    *   If the `sql/k_ausd_adressen_logic.sql` script performed destructive operations (e.g., `DELETE` or `TRUNCATE` before `INSERT`) or introduced incorrect data, use BigQuery's [time travel feature](https://cloud.google.com/bigquery/docs/data-manipulation-language#time-travel) to restore the `dwh_target_addresses` table to a state prior to the problematic DAG run.
    *   Alternatively, if backups were taken, restore the table from the most recent valid backup.

3.  **Re-enable Original Script:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh` script in the legacy environment. Ensure it has access to its original dependencies and data sources.

4.  **Communicate:**
    *   Inform all relevant stakeholders (data consumers, business users, operations team) about the rollback and the status of the data pipeline.

5.  **Investigate and Rectify:**
    *   Analyze the root cause of the failure using Airflow logs, BigQuery job history, and any monitoring tools.
    *   Address the identified issues in the Airflow DAG, Python utilities, or BigQuery SQL.
    *   Perform thorough testing in development environments before attempting another deployment.