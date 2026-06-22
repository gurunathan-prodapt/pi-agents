# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_aurd_rechstan.ksh` KornShell script. This script, originally responsible for orchestrating the generation of invoice data snapshots for demand scoring, has been migrated to Google Cloud Platform (GCP).

The migration involved:
*   Re-implementing the KornShell wrapper logic into a Python script (`r_aurd_rechstan.py`).
*   Translating legacy utility functions (for logging, date handling, and parameter validation) into a Python module (`utils.py`).
*   Orchestrating the new Python components using an Airflow DAG (`r_aurd_rechstan_dag.py`) within a Cloud Composer environment.
*   The core data processing logic, originally residing in `k_aurd_rechstan.ksh`, is pending analysis and migration to BigQuery SQL/Stored Procedures.

The target platform is Google Cloud Platform, utilizing Cloud Composer for orchestration, Python for scripting, and BigQuery for future data processing and storage.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`r_aurd_rechstan.py`**
    *   **Role:** This Python script is the direct re-implementation of the original `r_aurd_rechstan.ksh` wrapper. It handles command-line argument parsing (`-s Stichtag`, `-l Wiederanlaufwert`), date defaulting (to system date if `Stichtag` is not provided), and invokes a placeholder function (`run_core_job`) for the actual data processing. It integrates with Python's standard `logging` module for output.
*   **`utils.py`**
    *   **Role:** This Python module contains re-implementations of various legacy utility functions found in the original KornShell environment. This includes functions for date formatting (`get_date_formatted`), parameter validation (`pruefeParameterGesetzt`), and simplified versions of the `DWMSG_*` logging and error handling functions. It aims to provide functional parity for these common utilities.
*   **`r_aurd_rechstan_dag.py`**
    *   **Role:** This Airflow DAG defines the orchestration workflow for the migrated job. It uses a `PythonOperator` to execute `r_aurd_rechstan.py`. It also includes a `BashOperator` placeholder for the core data processing logic, which will be replaced by BigQuery tasks once `k_aurd_rechstan.ksh` is analyzed and migrated. The DAG manages scheduling, task dependencies, and integrates with Airflow's monitoring capabilities.
*   **BigQuery SQL/Stored Procedures (Planned)**
    *   **Role:** (Not yet generated) These will contain the actual data extraction, transformation, and loading (ETL) logic derived from the `k_aurd_rechstan.ksh` script. They will operate on BigQuery tables to produce the invoice data snapshots.

## 3. Key design decisions

*   **Wrapper Re-implementation in Python:** The original KornShell wrapper (`r_aurd_rechstan.ksh`) was re-implemented in Python (`r_aurd_rechstan.py`). This decision was made to leverage Python's robust libraries, better integration with GCP services (like Cloud Logging and BigQuery clients), and native compatibility with Airflow for orchestration.
*   **Airflow for Orchestration:** Airflow (via Cloud Composer) was chosen as the orchestration engine to manage the job's scheduling, dependencies, monitoring, and parameter passing, replacing the legacy shell-based scheduling and execution.
*   **Modularization of Utilities:** Legacy custom utility functions (e.g., `DWMSG_*`, `h_alis_*`, date functions) were extracted and re-implemented into a separate Python module (`utils.py`). This promotes code reusability, maintainability, and keeps the main `r_aurd_rechstan.py` script focused on its primary orchestration logic.
*   **BigQuery as Target Data Platform:** The core data processing logic (from `k_aurd_rechstan.ksh`) is slated for migration to BigQuery. This decision aligns with GCP's data warehousing strategy, offering scalability, performance, and cost-effectiveness for analytical workloads.
*   **Cloud-Native Logging:** The legacy `DWMSG_*` logging framework was replaced with Python's standard `logging` module, which seamlessly integrates with GCP's Cloud Logging. This centralizes log management, provides advanced filtering, and simplifies monitoring.
*   **Parameter Handling with `argparse`:** Python's `argparse` module was used for command-line argument parsing in `r_aurd_rechstan.py`, providing a structured and robust way to handle inputs like `Stichtag` and `Wiederanlaufwert`, mirroring the original script's behavior.
*   **Trade-offs:**
    *   **Phased Migration due to Unknown Core Logic:** The most significant trade-off is the inability to fully complete the migration without the `k_aurd_rechstan.ksh` script. This necessitates a phased approach, with the core data processing being a "B4 (Redesign)" item.
    *   **Re-implementation Effort:** Re-implementing custom legacy utilities in Python requires careful analysis to ensure functional equivalence, which can be time-consuming.
    *   **Error Code Mapping:** While legacy error codes are mimicked, a full, standardized mapping to cloud-native error handling or a more generic set of exit codes might be a future refinement.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup:**
    *   Ensure a dedicated GCP project is provisioned and configured for data warehousing workloads.
    *   Verify billing is enabled for the project.
2.  **Cloud Composer / Airflow Environment:**
    *   Provision and configure a Cloud Composer environment (or a self-managed Airflow instance) in the target GCP project.
    *   Ensure the Airflow environment has sufficient resources and is accessible.
3.  **BigQuery Datasets and Tables:**
    *   **Create Target Datasets:** Create the necessary BigQuery datasets (e.g., `isbert_dwh_snapshots`) to store the output of the invoice data snapshots.
    *   **Define Target Schemas:** Once the `k_aurd_rechstan.ksh` logic is analyzed, define and create the schemas for the target BigQuery tables that will hold the invoice data.
    *   **Source Data Access:** Identify and ensure access to the BigQuery tables that will serve as source data (e.g., "Vertrags-Cache" equivalent tables) for the core processing logic.
4.  **IAM Permissions:**
    *   **Service Account Configuration:** The service account associated with the Airflow environment must have the following roles/permissions:
        *   `BigQuery Data Editor` (or more granular permissions) on the target datasets/tables for writing data.
        *   `BigQuery Data Viewer` (or more granular permissions) on any source datasets/tables for reading data.
        *   `Logging Log Writer` for writing logs to Cloud Logging.
        *   `Secret Manager Secret Accessor` if any sensitive parameters (e.g., external system credentials) are stored in Secret Manager.
        *   `Storage Object Viewer` and `Storage Object Creator` for Airflow DAG deployment and logs.
5.  **Connection Strings and Secrets:**
    *   If the `k_aurd_rechstan.ksh` script (or its BigQuery equivalent) connects to any external systems, configure these connection details within Airflow Connections or securely store them in GCP Secret Manager and integrate their retrieval into the Python scripts/BigQuery procedures.
6.  **Deployment of Artifacts:**
    *   Upload `r_aurd_rechstan.py`, `utils.py`, and `r_aurd_rechstan_dag.py` to the Airflow DAGs folder in Cloud Storage (for Cloud Composer) or the appropriate DAGs directory for self-managed Airflow.
    *   (Once developed) Deploy the BigQuery SQL scripts or stored procedures to the BigQuery environment.
7.  **Scheduling Configuration:**
    *   Define the desired schedule for the `r_aurd_rechstan_wrapper` DAG within Airflow based on business requirements (e.g., daily, weekly).
    *   Configure any default DAG run parameters (e.g., default `stichtag` if not passed manually via a trigger).
8.  **Environment Variables:**
    *   Review the legacy `$HOME/.dw_init` script. Any critical environment variables defined there that are still relevant must be configured as Airflow environment variables or directly within the Python scripts.

## 5. Known gaps & unresolved references

The following items are known gaps or unresolved references that require further action:

*   **Core Processing Logic (`k_aurd_rechstan.ksh`) - B4 Item:**
    *   **Description:** The most critical unresolved item is the content and logic of the `k_aurd_rechstan.ksh` script. This script contains the actual data extraction and transformation logic.
    *   **Impact:** Without this, the BigQuery ETL design, source/target table identification, and full data flow cannot be completed. The `run_core_job` function in `r_aurd_rechstan.py` and the `core_processing_placeholder` task in the Airflow DAG are currently placeholders.
    *   **Resolution:** Obtain the source code for `k_aurd_rechstan.ksh`, analyze its data sources, transformations, and target schemas, and then design and implement the equivalent BigQuery SQL/Stored Procedures.
*   **Legacy DWH Tables:**
    *   **Description:** The specific tables and their schemas in the legacy Data Warehouse (referred to as "Vertrags-Cache im DWH") that `k_aurd_rechstan.ksh` interacts with are currently unknown.
    *   **Impact:** This directly impacts the identification of source tables in BigQuery for the core processing logic.
    *   **Resolution:** Identify the exact legacy DWH tables and their corresponding BigQuery equivalents (or design new ones if not already migrated).
*   **Custom Framework Complexity:**
    *   **Description:** The legacy `DWMSG_*` and `h_alis_*` utility functions are custom implementations. While basic equivalents are provided in `utils.py`, their full complexity, edge cases, and side effects (e.g., specific error handling, database interactions) need to be thoroughly understood.
    *   **Impact:** Incomplete understanding could lead to subtle behavioral differences or missed functionality.
    *   **Resolution:** Conduct a deeper analysis of the original utility scripts to ensure complete functional parity or a suitable cloud-native replacement.
*   **Error Codes and Logic:**
    *   **Description:** The legacy script uses custom error codes (e.g., `ErrNr=192`, `ErrNr=193`). The current Python implementation mimics these for specific cases but a comprehensive mapping or redesign of error handling to a cloud-native approach is needed.
    *   **Impact:** Inconsistent error reporting or handling could complicate debugging and monitoring.
    *   **Resolution:** Define a standardized error handling strategy for the migrated jobs, potentially mapping legacy codes to a new set or leveraging Cloud Logging's structured logging capabilities for detailed error information.
*   **`BERT_DIR_ROOT` and `JobKennung`:**
    *   **Description:** The legacy script relies on `BERT_DIR_ROOT` for file paths and `JobKennung` for logging. `JobKennung` is currently hardcoded in `r_aurd_rechstan.py`.
    *   **Impact:** These need to be properly configured in the new environment.
    *   **Resolution:** `BERT_DIR_ROOT` should be replaced by appropriate Cloud Storage paths or removed if not applicable. `JobKennung` should be configurable, potentially passed via Airflow DAG parameters or environment variables.
*   **Log File Management in `utils.py`:**
    *   **Description:** The `utils.py` functions currently accept a `log_file` parameter, which would write to a local file.
    *   **Impact:** This is not ideal for cloud environments where centralized logging (Cloud Logging) is preferred.
    *   **Resolution:** Ensure all logging from `utils.py` (and `r_aurd_rechstan.py`) is directed to Python's `logging` module, which will then be ingested by Cloud Logging. Remove local file writing capabilities unless specifically required for debugging.

## 6. Validation

Validation of the migrated job involves a multi-faceted approach to ensure functional correctness and operational stability.

### How to run the tests:

1.  **Unit Tests (Local Development):**
    *   Execute Python unit tests for `r_aurd_rechstan.py` and `utils.py` using a testing framework like `pytest`.
    *   **Command Example:** `pytest test_r_aurd_rechstan.py test_utils.py`
2.  **Integration Tests (Local/Dev Environment):**
    *   Manually execute `r_aurd_rechstan.py` from the command line with various valid and invalid parameters (e.g., `-s 01012023`, `-s invalid_date`, no `-s`, `-l 100`).
    *   Observe console output for correct parameter parsing, date defaulting, and error messages.
    *   (Once core logic is migrated) Execute the BigQuery SQL/Stored Procedures directly in BigQuery to verify their data processing logic.
3.  **Airflow DAG Execution (Cloud Composer/Airflow UI):**
    *   Deploy `r_aurd_rechstan_dag.py` to the Airflow environment.
    *   Trigger the DAG manually from the Airflow UI, optionally providing `Stichtag` and `Wiederanlaufwert` via the "Trigger DAG w/ config" option.
    *   Monitor the DAG run in the Airflow UI for task status and logs.
4.  **End-to-End Tests (Staging/Production Environment):**
    *   Execute the full Airflow DAG (including the migrated core BigQuery logic once available) with representative production-like data.
    *   Compare the output in the target BigQuery tables with the output generated by the legacy `r_aurd_rechstan.ksh` job for the same input parameters and source data. This may involve data comparison tools or manual verification.

### What "passing" means:

*   **Python Script Execution:**
    *   `r_aurd_rechstan.py` executes without unhandled exceptions.
    *   Correctly parses command-line arguments.
    *   Defaults `Stichtag` to the current system date when not provided.
    *   Validates `Stichtag` format (DDMMYYYY) and exits with an appropriate error code (e.g., 193) for invalid formats.
    *   The `run_core_job` placeholder is invoked with the correct `job_kennung`, `stichtag`, and `wiederanlaufwert`.
    *   Logs informative messages to standard output (and thus to Cloud Logging).
*   **Utility Functions (`utils.py`):**
    *   `validate_date` correctly identifies valid and invalid date strings.
    *   `get_date_formatted` returns dates in the specified format with correct offsets.
    *   `pruefeParameterGesetzt` raises `DWError` when a parameter is `None` or empty.
    *   `DWMSG_*` functions log messages to standard output as expected.
*   **Airflow DAG Execution:**
    *   The `r_aurd_rechstan_wrapper` DAG runs successfully (all tasks turn green) in the Airflow UI.
    *   Logs generated by the Python scripts are visible in Cloud Logging, associated with the correct DAG run and task.
    *   The DAG's schedule (if configured) triggers runs at the expected times.
*   **Data Validation (Post-Core-Logic Migration):**
    *   The target BigQuery tables contain the expected invoice data snapshots.
    *   The data in BigQuery matches the output of the legacy system for identical inputs, both in terms of record count and data values.
    *   The schema of the target BigQuery tables is correct and matches the design.
    *   Performance metrics (e.g., execution time, BigQuery slot usage) are within acceptable limits.
*   **Error Handling:**
    *   Known error conditions (e.g., invalid input parameters) are handled gracefully, resulting in appropriate error messages in logs and non-zero exit codes for the Python script/Airflow task.

## 7. Rollback procedure

In the event of critical issues detected during or after the go-live of the migrated `r_aurd_rechstan` job, the following rollback procedure should be followed:

1.  **Immediate Action (Stop New Runs):**
    *   **Pause Airflow DAG:** Immediately pause the `r_aurd_rechstan_wrapper` Airflow DAG in the Cloud Composer/Airflow UI to prevent any further execution of the migrated job.
    *   **Revert to Legacy Execution:** Re-enable or manually trigger the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh` script using its legacy scheduling mechanism to ensure business continuity.
2.  **Data Rollback (If Core Logic was Deployed):**
    *   **Identify Affected Data:** Determine which BigQuery tables or partitions were affected by the problematic migrated job run(s).
    *   **Delete/Revert Data:**
        *   If the BigQuery ETL (from `k_aurd_rechstan.ksh` migration) has written data, delete the newly generated data in the target BigQuery tables/partitions.
        *   If the BigQuery ETL modified existing data, restore the affected data from the most recent valid backup or snapshot, if available.
        *   *Note: The exact data rollback steps will depend on the specific BigQuery ETL design once `k_aurd_rechstan.ksh` is migrated.*
3.  **Code Rollback:**
    *   **Airflow DAG:** Remove or revert `r_aurd_rechstan_dag.py` from the Airflow DAGs folder to its previous stable version (or remove it entirely if it was a new deployment).
    *   **Python Scripts:** Remove or revert `r_aurd_rechstan.py` and `utils.py` from the Airflow environment (e.g., from the DAGs folder or any deployed Python packages).
    *   **BigQuery Assets:** If BigQuery SQL scripts or stored procedures were deployed as part of the core logic migration, revert them to their previous versions or drop them if they were newly created and are no longer needed.
4.  **Monitoring:**
    *   Closely monitor the legacy `r_aurd_rechstan.ksh` job to ensure it is running correctly and producing the expected output after the rollback.
    *   Monitor Cloud Logging for any residual errors or unexpected behavior from the rolled-back components.
5.  **Post-Rollback Analysis:**
    *   Once the system is stable on the legacy job, conduct a thorough root cause analysis of the issue that necessitated the rollback.
    *   Address the identified issues in the migrated code or infrastructure before attempting another deployment.