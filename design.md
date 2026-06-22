# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

## 1. Purpose & Scope

This script, `k_ausd_v_ta_notice.ksh`, serves as a control wrapper for a data processing job related to `ta_notice`. Its primary business purpose is to manage the execution of an underlying SQL script (`d_ausd_v_ta_notice.sql`), ensuring that active jobs are handled appropriately (ignored or deactivated), and to log the job's execution status and processed record counts. It handles parameter parsing and validation before invoking the core data processing logic.

The scope of this migration focuses on translating the KornShell orchestration, parameter handling, and job control mechanisms into a BigQuery-native solution, likely a stored procedure, while preparing for the subsequent migration of the embedded SQL logic from `d_ausd_v_ta_notice.sql`.

## 2. Source Inventory

| File Path                                                         | Technology  | Tier   | Automation Bucket |
| :---------------------------------------------------------------- | :---------- | :----- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` | KornShell | medium | semi_auto         |

## 3. Target Architecture

The target architecture in BigQuery will consist of:

*   **BigQuery Stored Procedure (`sp_control_ta_notice`):** This will replace the main KornShell script. It will handle parameter parsing, validation, job logging, and invoke the core data transformation logic.
*   **BigQuery Stored Procedure (`sp_d_ausd_v_ta_notice`):** This will encapsulate the migrated SQL logic from `d_ausd_v_ta_notice.sql`.
*   **BigQuery Tables:**
    *   `job_error_log`: To log errors during job execution.
    *   `job_run_log`: To log the start and completion status of job runs.
    *   `job_run_result`: To store the processed record counts, replacing the temporary file communication.
    *   `ta_notice` (or similar): The target table for data processed by `sp_d_ausd_v_ta_notice`.
*   **Orchestration Layer:** An external orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows) will be responsible for scheduling and calling the `sp_control_ta_notice` BigQuery stored procedure, passing necessary parameters.

## 4. Data Flow & Lineage

The current data flow is as follows:
1.  The `k_ausd_v_ta_notice.ksh` script is executed, likely by a scheduler or another wrapper script, with job-specific parameters (`p_JobKennung`, `p_EintragsNr`).
2.  The script initializes environment variables by sourcing `.dw_init` and utility functions from various `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` files.
3.  It parses the input parameters and performs validation using `pruefeParameterGesetzt`.
4.  If parameters are invalid, it logs an error using `DWMSG_MeldeFehler` and exits.
5.  It defines the path to the SQL script `d_ausd_v_ta_notice.sql` and a temporary file path.
6.  It calls `starteSQLSkript` (a shell function) which is responsible for executing `d_ausd_v_ta_notice.sql`, potentially passing parameters to it. This SQL script is where the actual data processing (reads/writes) occurs, affecting the `ta_notice` table and possibly other job-related tables.
7.  The `starteSQLSkript` function likely writes the count of processed records to the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`).
8.  After the SQL script execution, `k_ausd_v_ta_notice.ksh` reads this temporary file to capture the record count into `v_records`.
9.  Finally, it prints completion messages.

In the BigQuery target, this flow will be:
1.  An orchestrator invokes `sp_control_ta_notice` with `p_JobKennung` and `p_EintragsNr` as parameters.
2.  `sp_control_ta_notice` logs the job start in `job_run_log`.
3.  `sp_control_ta_notice` validates input parameters. If invalid, it logs to `job_error_log` and exits.
4.  `sp_control_ta_notice` calls `sp_d_ausd_v_ta_notice`, passing relevant parameters.
5.  `sp_d_ausd_v_ta_notice` executes the core data transformation logic, reading from source tables (implicit) and writing to target tables (e.g., `ta_notice`). It will return the processed record count.
6.  `sp_control_ta_notice` captures the record count from `sp_d_ausd_v_ta_notice` and logs it to `job_run_result`.
7.  `sp_control_ta_notice` logs job completion in `job_run_log`.

## 5. Transformation Logic

The KornShell script itself does not contain direct data transformation logic. Its role is orchestration. The actual data transformations and aggregations are expected to be within the SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_notice.sql`.

Upon migration, the SQL logic from `d_ausd_v_ta_notice.sql` will be translated into BigQuery SQL and encapsulated within the `sp_d_ausd_v_ta_notice` stored procedure. This procedure will perform the following conceptual steps:
1.  Logic to deactivate old active jobs.
2.  Logic to ignore currently active jobs.
3.  Logic to insert/update entries in the job tracking tables.
4.  The core data processing logic that affects the `ta_notice` table.
5.  Calculation of the number of processed records.

The migration of `d_ausd_v_ta_notice.sql` will require a separate analysis to translate its specific SQL constructs and table interactions to BigQuery SQL, considering partitioning, clustering, and other BigQuery best practices.

## 6. External Dependencies

The original script has several external dependencies:

*   **Environment Initialization:** Sourcing of `$HOME/.dw_init`.
    *   **Replacement:** This will be replaced by direct parameter passing to the BigQuery stored procedure or configured through the orchestrator (e.g., Airflow DAG variables). Static paths will be defined as constants in BigQuery or metadata.
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus wrapper functions, including `starteSQLSkript`.
    *   **Replacement:**
        *   Error messaging will be handled by BigQuery's `ASSERT` statements, `RAISE` for errors, and logging to `job_error_log` and `job_run_log` tables.
        *   Date handling will use BigQuery's built-in date/time functions.
        *   Parameter parsing will be replaced by BigQuery stored procedure input parameters.
        *   The `starteSQLSkript` functionality will be replaced by direct BigQuery stored procedure calls (`CALL sp_d_ausd_v_ta_notice`).
*   **SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_notice.sql`.
    *   **Replacement:** This will be migrated into a BigQuery stored procedure `sp_d_ausd_v_ta_notice`, as described in Section 5.
*   **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`.
    *   **Replacement:** The communication of processed record counts will be handled by returning values from `sp_d_ausd_v_ta_notice` to `sp_control_ta_notice`, which then persists it to the `job_run_result` table.

## 7. Unresolved / Risks

*   **Content of `d_ausd_v_ta_notice.sql`:** The specifics of the data transformation logic within `d_ausd_v_ta_notice.sql` are unknown. This SQL script is critical and needs to be analyzed and migrated separately.
*   **`starteSQLSkript` behavior:** The exact implementation of `starteSQLSkript` (e.g., dynamic SQL, error handling within the SQL execution, handling of active jobs) is not fully known. Assumptions have been made that its core function is to execute the SQL and capture a record count. Any complex logic within it, such as job locking or complex active job checks, will need explicit migration.
*   **Job Deactivation Logic:** The comment "alte aktive Jobs werden einfach dekativiert" (old active jobs are simply deactivated) implies specific logic in the SQL or shell. This needs to be carefully re-implemented in BigQuery.
*   **Error Handling Fidelity:** The custom error handling framework (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) will be replaced by BigQuery's native error mechanisms and custom logging tables. Functional parity needs to be ensured.
*   **Orchestration Context:** The manner in which `k_ausd_v_ta_notice.ksh` is invoked (e.g., by another script, a scheduler) will determine the exact BigQuery orchestration solution (e.g., Cloud Composer, simple scheduled query).

## 8. Build Plan

The build plan will involve creating the following BigQuery components:

1.  **BigQuery Tables:**
    *   Create `project.dataset.job_error_log` (schema: `job_kennung STRING, eintrags_nr STRING, err_nr INT64, err_arg STRING, created_ts TIMESTAMP`).
    *   Create `project.dataset.job_run_log` (schema: `job_kennung STRING, eintrags_nr STRING, tab_name STRING, script_name STRING, status STRING, created_ts TIMESTAMP`).
    *   Create `project.dataset.job_run_result` (schema: `job_kennung STRING, eintrags_nr STRING, tab_name STRING, record_count INT64, created_ts TIMESTAMP`).
    *   Ensure the target `ta_notice` table exists with its appropriate schema.

2.  **BigQuery Stored Procedure `sp_d_ausd_v_ta_notice` (language: BigQuery SQL):**
    *   Migrate the SQL code from `d_ausd_v_ta_notice.sql` into this stored procedure.
    *   Modify the SQL to adhere to BigQuery syntax and best practices.
    *   Add an `OUT` parameter for `v_records` to return the count of processed records.

3.  **BigQuery Stored Procedure `sp_control_ta_notice` (language: BigQuery SQL):**
    *   Create the stored procedure as outlined in the pseudocode from the MCP output.
    *   Implement parameter validation logic.
    *   Integrate logging to `job_run_log` and `job_error_log`.
    *   Call `sp_d_ausd_v_ta_notice` and capture its `v_records` output.
    *   Log `v_records` to `job_run_result`.

4.  **Orchestration Configuration (e.g., Cloud Composer/Airflow DAG, language: Python/YAML):**
    *   Create a DAG or workflow definition to schedule `sp_control_ta_notice`.
    *   Define parameters (`p_JobKennung`, `p_EintragsNr`) as DAG configuration or task arguments.
    *   Configure retry mechanisms and alerting as needed.