# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_ausd_bp_ta_bcp_msisdn.ksh` job. This KornShell script, originally responsible for orchestrating the initial provisioning of selected basic products for BERT by extracting contract cache data from the Data Warehouse (DWH) for Forderungsscoring, has been migrated.

The migration target platform is **Google Cloud Platform (GCP)**, specifically utilizing:
*   **Google Cloud Composer (Airflow)** for job orchestration, scheduling, and monitoring.
*   **Python** for re-implementing helper functions and parameter handling.
*   **Google Cloud Storage (GCS)** for storing logs and potentially intermediate data or PySpark scripts.
*   **BigQuery** or **PySpark on Dataproc/Spark on GKE** for the core data transformation logic (pending further analysis of the kernel script).

The migrated solution aims to leverage GCP's managed services for enhanced scalability, reliability, observability, and maintainability compared to the legacy KornShell environment.

## 2. Generated Artifacts

The migration process has generated the following primary artifacts:

*   **`bert_utils.py`**:
    *   **Role**: This Python module consolidates and re-implements the core functionalities of the original KornShell helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). It provides Pythonic implementations for logging, date parsing/formatting, and parameter validation, making these utilities available to the Airflow DAG.
*   **`r_ausd_bp_ta_bcp_msisdn.py`**:
    *   **Role**: This is the main Airflow Directed Acyclic Graph (DAG) file. It replaces the orchestration logic of the original `r_ausd_bp_ta_bcp_msisdn.ksh` script. It defines the workflow, handles parameter input (Stichtag, Wiederanlaufwert), initializes logging, and orchestrates the execution of the core data transformation logic (which will be implemented in a separate artifact).
*   **`k_ausd_bp_ta_bcp_msisdn.sql` (or `k_ausd_bp_ta_bcp_msisdn.py`)**:
    *   **Role**: (Expected, pending further analysis) This artifact will contain the core data transformation logic migrated from the original `k_ausd_bp_ta_bcp_msisdn.ksh` kernel script. Depending on the complexity and nature of the original logic, this will either be a BigQuery SQL script/stored procedure or a PySpark script. This artifact will be invoked by the `r_ausd_bp_ta_bcp_msisdn.py` DAG.

## 3. Key Design Decisions

The following key design decisions guided the migration:

*   **Orchestration Layer Migration to Airflow**: The original KornShell script primarily served as an orchestrator. Migrating this to Google Cloud Composer (Airflow) provides a robust, managed, and scalable platform for workflow definition, scheduling, monitoring, and error handling, significantly improving operational efficiency and visibility.
*   **Python for Helper Function Re-implementation**: Instead of attempting direct shell script emulation, common utility functions (logging, date handling, parameter parsing) from the original helper scripts were re-implemented in Python. This ensures native compatibility with the Airflow DAG and leverages Python's extensive libraries and readability.
*   **Decoupling Orchestration from Core Logic**: The migration explicitly separates the orchestration logic (now in the Airflow DAG) from the core data transformation logic (to be migrated from `k_ausd_bp_ta_bcp_msisdn.ksh`). This modular approach allows for independent development, testing, and scaling of each component.
*   **Leveraging GCP Native Services for Data Processing**: The core data transformation logic is planned for migration to either BigQuery (for SQL-centric transformations) or PySpark on Dataproc/Spark on GKE (for more complex, procedural, or large-scale data manipulations). This decision aligns with GCP best practices for data warehousing and processing, offering high performance and scalability.
*   **Standardized Parameter Handling**: The custom command-line parameter parsing of the KornShell script is replaced by Airflow's native DAG parameters and Python-based validation, providing a more structured and user-friendly interface for job configuration.
*   **Enhanced Logging and Error Handling**: The custom shell-based logging and `trap` mechanisms are superseded by Airflow's integrated logging (to Cloud Logging), Python's standard `logging` module, and Airflow's robust task failure and alerting capabilities, improving observability and incident response.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **Google Cloud Project Setup**:
    *   Ensure a dedicated GCP project is established and billing is enabled.
2.  **Cloud Composer Environment Provisioning**:
    *   Provision a Google Cloud Composer environment (Airflow instance) in the target GCP region.
3.  **BigQuery Dataset Creation**:
    *   Create the necessary BigQuery datasets to host the output tables, any intermediate tables, and potentially a custom logging table if structured logging to BigQuery is desired.
4.  **IAM Permissions Configuration**:
    *   The **service account** associated with the Cloud Composer environment must be granted the following roles:
        *   `BigQuery Data Editor` and `BigQuery Job User` for executing BigQuery queries and stored procedures.
        *   `Storage Object Admin` or `Storage Object Creator` for reading/writing files to GCS (e.g., PySpark scripts, intermediate data, logs).
        *   If PySpark on Dataproc is chosen for core logic: `Dataproc Worker` and `Dataproc Editor` roles.
        *   `Secret Manager Secret Accessor` if any sensitive configuration or credentials are to be stored in Google Secret Manager.
5.  **Airflow Connections Setup**:
    *   Configure a `google_cloud_default` connection in Airflow, or any other specific GCP connections required for BigQuery, Dataproc, etc.
6.  **Secrets Management**:
    *   Identify any sensitive information (e.g., database credentials, API keys) that were previously sourced via `$HOME/.dw_init` or hardcoded. Migrate these to Google Secret Manager and update the Airflow DAG or Python helper modules to retrieve them securely.
7.  **GCS Bucket for PySpark (if applicable)**:
    *   If the core logic is migrated to PySpark, create a dedicated GCS bucket to store the `k_ausd_bp_ta_bcp_msisdn.py` script and any other PySpark dependencies.
8.  **Deployment of `bert_utils.py`**:
    *   Upload the `bert_utils.py` file to the Cloud Composer environment's `dags/plugins` folder (or a similar location configured for custom plugins) to ensure it's accessible by the Airflow DAG.
9.  **Deployment of Core Transformation Logic**:
    *   Deploy the migrated `k_ausd_bp_ta_bcp_msisdn.sql` (as a BigQuery stored procedure or script) or `k_ausd_bp_ta_bcp_msisdn.py` (to the designated GCS bucket for Dataproc execution).
10. **Scheduling Configuration**:
    *   Configure the Airflow DAG's schedule (e.g., daily, hourly) or set up external triggers as per the business requirements.

## 5. Known Gaps & Unresolved References

The following items have been identified as known gaps or require further resolution:

*   **Core Logic Analysis for `k_ausd_bp_ta_bcp_msisdn.ksh` (B4 Item)**: The most significant unresolved item. The detailed data transformation logic within the `k_ausd_bp_ta_bcp_msisdn.ksh` kernel script has not yet been fully analyzed or migrated. A separate, in-depth analysis is required to determine the optimal migration path (BigQuery SQL/Stored Procedures vs. PySpark) and to implement this logic. This is a **blocker** for the complete end-to-end migration of the job's functionality.
*   **Complex Shell Logic Translation**: While the `r_ausd_bp_ta_bcp_msisdn.ksh` script is primarily an orchestrator, the possibility of complex, shell-specific operations (e.g., advanced text processing, specific system calls, file manipulations) within `k_ausd_bp_ta_bcp_msisdn.ksh` or its other dependencies remains. These may not have direct Python/BigQuery equivalents and could require significant redesign.
*   **Environment Variable Mapping**: A comprehensive mapping of all environment variables sourced from `$HOME/.dw_init` and their usage throughout the original scripts needs to be completed. These variables must be securely translated into Airflow variables, GCP Secret Manager secrets, or other appropriate GCP configuration mechanisms.
*   **Error Code Mapping**: The original script uses specific numerical error codes (e.g., 192, 193, 195). A clear mapping of these legacy error codes to Airflow's exception handling and logging best practices, or a custom error reporting mechanism, needs to be defined.
*   **Performance Benchmarking**: No performance benchmarks have been conducted for the migrated solution against the legacy system. Thorough performance testing will be crucial post-migration of the core logic.
*   **Data Volume and Scalability Testing**: While BigQuery and PySpark are designed for scale, specific testing with production-like data volumes is needed to ensure the migrated solution meets performance and scalability requirements.

## 6. Validation

Validation of the migrated job involves several stages:

*   **Unit Testing (`bert_utils.py`)**:
    *   **How to run**: Execute Python unit tests (e.g., using `pytest`) against the `bert_utils.py` module.
    *   **Passing means**: All tests pass, confirming that helper functions like `parse_date_ddmmyyyy_to_yyyymmdd`, `parse_and_validate_parameters`, and `get_current_date_yyyymmdd` behave as expected for both valid and invalid inputs.
*   **Airflow DAG Functional Testing (`r_ausd_bp_ta_bcp_msisdn.py`)**:
    *   **How to run**:
        1.  Upload `r_ausd_bp_ta_bcp_msisdn.py` and `bert_utils.py` to the Composer DAGs folder.
        2.  Access the Airflow UI for the Composer environment.
        3.  Manually trigger the `r_ausd_bp_ta_bcp_msisdn_migration` DAG with various parameter combinations:
            *   No parameters (relying on defaults).
            *   Specific `stichtag` (e.g., `01012023`).
            *   Specific `wiederanlaufwert` (e.g., `12345`).
            *   Both `stichtag` and `wiederanlaufwert`.
            *   Invalid `stichtag` format (e.g., `2023-01-01`) or invalid `wiederanlaufwert` (e.g., `abc`) to test error handling.
        4.  Monitor the DAG run in the Airflow UI.
        5.  Review the logs for each task (`parse_and_validate_parameters`, `initialize_job_logging`, `execute_core_transformation_logic`).
        6.  Inspect XCom values for tasks to ensure parameters are correctly parsed and propagated.
    *   **Passing means**:
        *   All tasks within the DAG complete successfully (indicated by green status in the Airflow UI) for valid parameter inputs.
        *   For invalid inputs, the `parse_and_validate_parameters` task fails gracefully with an `AirflowException`, and appropriate error messages are present in the logs.
        *   XComs for `stichtag_yyyymmdd`, `wiederanlaufwert`, `job_kennung`, and `job_nr` contain the expected values.
        *   Log messages from `bert_utils.py` (e.g., "Stichtag provided", "Stichtag not provided, defaulting") are correctly displayed.
        *   The `execute_core_transformation_logic` task is invoked with the correct parameters (as seen in its logs).
*   **End-to-End Data Validation (Post-Core Logic Migration)**:
    *   **How to run**: Once the `k_ausd_bp_ta_bcp_msisdn.ksh` logic is migrated and integrated, execute the full Airflow DAG.
    *   **Passing means**:
        *   The data generated in BigQuery (or other target systems) by the migrated job for a given `Stichtag` and `Wiederanlaufwert` is identical in structure, count, and content to the output produced by the legacy KornShell script for the same inputs.
        *   Data quality checks (e.g., null checks, referential integrity) pass on the output data.
        *   The job completes within acceptable performance thresholds.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after the migration, the following rollback procedure should be followed to revert to the legacy system:

1.  **Disable New Airflow DAG**:
    *   Navigate to the Google Cloud Composer Airflow UI.
    *   Locate the `r_ausd_bp_ta_bcp_msisdn_migration` DAG.
    *   Toggle the DAG to "Off" (unpause) to prevent any further runs.
2.  **Re-enable Legacy Job**:
    *   Access the legacy scheduling system (e.g., cron, enterprise scheduler) where the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh` job is configured.
    *   Re-enable and re-schedule the original KornShell script to resume its normal operation.
3.  **Verify Legacy Execution**:
    *   Monitor the re-enabled legacy job closely to ensure it runs successfully and produces the expected output without issues.
    *   Check legacy logs and output data to confirm proper functioning.
4.  **Data State Assessment and Cleanup**:
    *   If the migrated Airflow DAG had already written data to BigQuery or other target systems, assess the impact. Depending on the severity of the issue and the data written, a data rollback or cleanup might be necessary. This could involve:
        *   Restoring BigQuery tables to a previous state using time travel.
        *   Deleting newly created or modified data by the migrated job.
        *   Coordinating with data consumers if data integrity was compromised.
5.  **Communication**:
    *   Immediately inform all relevant stakeholders (e.g., business users, dependent teams, operations) about the rollback, its reasons, and the current status of the job.
6.  **Post-Rollback Analysis**:
    *   Conduct a thorough root cause analysis of the issues that necessitated the rollback to address them before attempting re-migration.