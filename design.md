# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_p_vertrag.ksh`. The script serves as a framework for synchronizing contract data within the `ta_p_vertrag` table. Its primary functions include environment setup, parameter parsing, error trapping, logging, and orchestrating the execution of a core synchronization script, `k_ausd_v_ta_p_vertrag.ksh`. The job is classified as an orchestration task with medium complexity.

## 2. Source Inventory
The job consists of a single source file:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`
*   **Technology:** KornShell
*   **Summary:** Framework script for synchronizing contract data in the 'ta_p_vertrag' table, handling environment setup, parameter parsing, and error trapping before calling a core script.
*   **Category:** shell
*   **Tool (Detected):** KornShell
*   **File Purpose:** Orchestration / ETL
*   **Complexity Tier:** Not available (missing `file_complexity` entry)
*   **Automation Bucket:** Not available (missing `automation_rate` entry)

## 3. Target Architecture
The migration targets Google Cloud Platform (GCP).
*   **Orchestration:** Cloud Composer (Airflow) will be used to manage the workflow, including environment setup, parameter passing, execution of the core synchronization logic, and error handling.
*   **Data Storage:** BigQuery will house the `ta_p_vertrag` table and any other related data.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring will replace the existing custom error reporting and logging mechanisms (`DWMSG_` functions).
*   **Core Transformation:** The logic within `k_ausd_v_ta_p_vertrag.ksh` is presumed to be database-centric (e.g., SQL). This logic will be migrated to BigQuery SQL, potentially within a BigQuery script or a stored procedure executed by an Airflow `BigQueryExecuteQueryOperator`.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_p_vertrag.ksh` script acts as an orchestrator.
1.  **Environment Setup:** It sources common environment initialization scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). In GCP, this will be handled by Airflow environment variables, custom hooks, or Python functions executed by `PythonOperator` tasks within the DAG.
2.  **Parameter Parsing:** The script parses command-line parameters (`getopts`). This logic will be translated into Airflow DAG parameters or Python code within the DAG.
3.  **Logging & Error Handling:** The script uses custom `DWMSG_` functions for logging job status and handling errors (e.g., `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`). These will be replaced by Airflow's native logging to Cloud Logging and integrated alerting via Cloud Monitoring.
4.  **Core Synchronization:** The script explicitly calls `k_ausd_v_ta_p_vertrag.ksh`, passing it parameters (`-j $JobKennung -f ${DW_EintragsNr}`). This indicates that `k_ausd_v_ta_p_vertrag.ksh` contains the actual logic for synchronizing data in `ta_p_vertrag`.
5.  **Data Interaction:** The `ta_p_vertrag` table is both an input and output for the overall job, implying it reads from and writes to this table.

**Proposed Airflow DAG Flow:**
*   `initialize_environment` (PythonOperator): Sets up environment variables, potentially calls shared utility functions.
*   `parse_parameters` (PythonOperator): Parses DAG run configuration/parameters.
*   `execute_core_sync` (BigQueryExecuteQueryOperator / PythonOperator calling BigQuery script): This task will contain the translated logic from `k_ausd_v_ta_p_vertrag.ksh` to interact with `ta_p_vertrag` in BigQuery.
*   `handle_success` (PythonOperator): Updates job status/logs success, leveraging Cloud Logging.
*   `handle_failure` (PythonOperator, triggered by Airflow's `on_failure_callback`): Handles error reporting and alerting via Cloud Monitoring.

## 5. Transformation Logic
The current script `r_ausd_v_ta_p_vertrag.ksh` itself is primarily an orchestrator and does not contain direct data transformation logic. Its transformation logic is limited to:
*   **Orchestration:** Sequential execution, parameter passing to a child script.
*   **Error Trapping:** `trap` commands for `INT` and `ERR` signals.
*   **Logging:** Invoking custom logging functions.

The actual data synchronization/transformation logic is delegated to the `k_ausd_v_ta_p_vertrag.ksh` script, which is currently an unknown. For the `r_ausd_v_ta_p_vertrag.ksh` wrapper, the transformation focuses on workflow management.

**Migration of Orchestration Logic:**
*   **Parameter Handling:** The `getopts` logic will be replaced by Airflow's native parameter handling for DAG runs.
*   **Environment Initialization:** `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` will be either replaced by Airflow's environment configuration, Python utility functions, or BigQuery-specific setup.
*   **Error Handling and Logging:** `trap` commands and `DWMSG_` calls will be replaced by Airflow's robust error handling (retries, callbacks) and logging capabilities integrated with Cloud Logging and Cloud Monitoring.
*   **Calling Core Script:** The execution of `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}` will be replaced by an Airflow task that executes the migrated `k_ausd_v_ta_p_vertrag.ksh` logic (e.g., a `BigQueryExecuteQueryOperator` for SQL, or a `PythonOperator` if it becomes Python).

**Placeholder for Core Synchronization Logic (from `k_ausd_v_ta_p_vertrag.ksh`):**
(This section requires further analysis of `k_ausd_v_ta_p_vertrag.ksh` once its source code is available.)
The generated design tool provided a generic SQL:
```sql
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.ta_p_vertrag` AS
WITH source_data AS (
    SELECT
        *
    FROM `your_project.your_dataset.source_vertrag`
),
normalized_data AS (
    SELECT
        *,
        CURRENT_DATE() AS load_date
    FROM source_data
),
final_data AS (
    SELECT
        *
    FROM normalized_data
)
SELECT
    *
FROM final_data
```
This is a simple load. The actual `k_ausd_v_ta_p_vertrag.ksh` will likely involve more complex DML operations (INSERT, UPDATE, MERGE) to `ta_p_vertrag` for synchronization.

## 6. External Dependencies
The script has the following external dependencies:
*   **`$HOME/.dw_init`:** Environment initialization script.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`:** Error messaging utility.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`:** Parameter handling utility.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`:** Date handling utility.
*   **`k_ausd_v_ta_p_vertrag.ksh`:** The core synchronization script.
*   **`ta_p_vertrag` (TABLE):** The primary table for contract data synchronization.

**Migration Strategy for Dependencies:**
*   **Shell Utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These common utility scripts will be reimplemented in Python as Airflow utility functions or external Python modules, or their functionality will be replaced by native Airflow/GCP features.
*   **`k_ausd_v_ta_p_vertrag.ksh`:** This core script needs to be analyzed separately. Its logic will likely be translated into BigQuery SQL (if it's SQL-based) or Python/PySpark if it's a more complex application. This migrated logic will be executed as a distinct task within the Airflow DAG.
*   **`ta_p_vertrag`:** This table will be migrated to BigQuery.

## 7. Unresolved / Risks
*   **`k_ausd_v_ta_p_vertrag.ksh` content:** The primary unresolved item is the actual code for `k_ausd_v_ta_p_vertrag.ksh`. The effectiveness and complexity of the BigQuery SQL migration will depend entirely on its contents. This is a significant risk.
*   **Missing Complexity/Automation Rate:** The absence of `file_complexity` and `automation_rate` for this file means the estimated effort and migration approach are based on manual analysis, without the benefit of automated assessment.
*   **Custom DWMSG functions:** The exact functionality of all `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`) needs to be fully understood to ensure proper replacement with Cloud Logging/Monitoring capabilities.
*   **Data Volume/Performance:** While BigQuery is highly performant, the synchronization logic of `k_ausd_v_ta_p_vertrag.ksh` could have performance implications if not optimized for BigQuery's columnar storage and distributed query execution.

## 8. Build Plan
The build plan focuses on creating an Airflow DAG in Python that orchestrates the data synchronization process.

1.  **Develop Airflow DAG (`vertragsdatenabgleich_ta_p_vertrag.py` - Python):**
    *   Define DAG structure, default arguments, and scheduling (if any).
    *   Implement `initialize_environment` task (PythonOperator) to handle environment setup.
    *   Implement `parse_parameters` task (PythonOperator) for argument parsing.
    *   Implement `execute_core_sync` task. This will be a placeholder initially, awaiting the migration of `k_ausd_v_ta_p_vertrag.ksh`. It will likely use a `BigQueryExecuteQueryOperator` for a BigQuery SQL script or stored procedure.
    *   Integrate Airflow's logging and error handling with Cloud Logging and Cloud Monitoring.
    *   Implement `handle_success` task for post-execution status updates.
    *   Implement `handle_failure` callback for error notifications.

2.  **Migrate `k_ausd_v_ta_p_vertrag.ksh` (BigQuery SQL / Python):**
    *   **Dependency:** This step requires the source code of `k_ausd_v_ta_p_vertrag.ksh`.
    *   Translate the synchronization logic into optimized BigQuery SQL (e.g., using MERGE statements for UPSERTs, or INSERT OVERWRITE for full loads).
    *   Alternatively, if `k_ausd_v_ta_p_vertrag.ksh` contains complex procedural logic, migrate it to a Python script that uses the BigQuery client library.

3.  **Migrate `ta_p_vertrag` table (BigQuery DDL):**
    *   Create the `ta_p_vertrag` table in BigQuery with an appropriate schema (e.g., partitioned by date, clustered by relevant keys).

4.  **Reimplement Shared Utilities (Python):**
    *   Translate `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` into reusable Python functions or modules that can be imported by the Airflow DAG.

5.  **Deploy and Test:**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Conduct unit, integration, and end-to-end testing, verifying data integrity and pipeline execution.