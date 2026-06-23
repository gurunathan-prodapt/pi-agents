# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh

## 1. Purpose & Scope
This job, `r_drop_temp_table.ksh`, serves as a wrapper script designed to delete temporary intermediate tables that were not explicitly removed by the BERT process. It provides robust parameter handling, logging, and error management capabilities. The script's primary function is to orchestrate the execution of a core cleanup script, `k_drop_temp_table.ksh`, by passing relevant parameters such as the reference date (Stichtag) and a restart threshold (Wiederanlaufwert). The job ensures proper environment setup, handles script-level errors, and records execution status.

## 2. Source Inventory
The job consists of a single KornShell script:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh`
  - **Technology:** KornShell
  - **Complexity Tier:** medium
  - **Automation Bucket:** semi_auto
  - **Purpose:** ETL (script role is wrapper/orchestrator)

## 3. Target Architecture
The target platform is Google Cloud BigQuery. The migration will involve:
- **BigQuery Stored Procedures:** The wrapper logic (parameter parsing, validation, logging, and orchestration) will be reimplemented as a BigQuery Stored Procedure. The core cleanup logic from `k_drop_temp_table.ksh` (which this wrapper invokes) will also be migrated into a separate BigQuery Stored Procedure.
- **BigQuery Tables for Logging & Status:** Instead of file-based logging, job execution logs, error messages, and status updates will be written to dedicated BigQuery tables (`job_log` and `job_status`).
- **Cloud Workflows / Cloud Composer / Cloud Run (Optional):** For higher-level orchestration, especially if complex scheduling, error handling (beyond BigQuery's `BEGIN...EXCEPTION...END`), or integration with other services is required, Cloud Workflows, Cloud Composer, or Cloud Run could be utilized. This would manage the invocation of the BigQuery stored procedure and any non-SQL side effects.

## 4. Data Flow & Lineage
The original script is invoked by a UC4 job: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_DROP_TEMP_TABLE.xml` INVOKES `SCRIPT:R_DROP_TEMP_TABLE.KSH`.

The data flow within the `r_drop_temp_table.ksh` script is as follows:
1. **Environment Initialization:** Sources `$HOME/.dw_init` and helper scripts for error handling, parameter parsing, and date functions.
2. **Parameter Parsing:** Reads command-line arguments `-s` (Stichtag) and `-l` (Wiederanlaufwert) using `getopts`.
3. **Parameter Defaults & Validation:**
   - `p_wiederanlaufWert` defaults to `0` if not provided.
   - `p_stichtag` defaults to the system date (in `DDMMYYYY` format) if not provided.
   - Parameters are validated for presence. If validation fails, an error is logged, usage is displayed, and the script exits.
4. **Logging Setup:** Initializes job-specific logging, including a unique entry number and log file name using `DWMSG_ErmittleNr` and `DWMSG_Logdateiname`.
5. **Error Trapping:** Sets `trap` commands to catch `INT`, `STOP`, `CONT`, and `ERR` signals, triggering `DWMSG_Fehlerbehandlung` for error logging.
6. **Job Execution:** Invokes the core cleanup script, `k_drop_temp_table.ksh`, passing the parsed parameters and redirecting its output to the log file.
7. **Success Handling:** If the core script completes without error, a success message is logged, and the job status is updated to OK.
8. **Exit:** Clears traps and exits with a status of `0`.

In the target BigQuery environment, this flow will be handled by a BigQuery Stored Procedure, potentially orchestrated by a higher-level tool.

## 5. Transformation Logic
The KornShell script logic will be transformed into BigQuery SQL, primarily utilizing BigQuery Stored Procedures:

- **Parameter Handling:**
  - Shell script parameters (`-s`, `-l`) will become `IN` parameters for the BigQuery stored procedure.
  - Shell environment variables (e.g., `$BERT_DIR_ROOT`, `$HOME`) will be replaced by constants within the BigQuery procedure, values retrieved from BigQuery configuration tables, or passed as additional parameters.
- **Date Functions:**
  - `DWDate_Gib_Zeitraum` will be replaced with BigQuery SQL functions like `CURRENT_DATE()`, `FORMAT_DATE()`, and `PARSE_DATE()`.
- **Conditional Logic:**
  - `if [[ -z ... ]]`, `if [ ! $ErrNr -eq 0 ]`, and `case` statements will be translated to `IF...THEN...ELSE...END IF` and `ASSERT` statements in BigQuery SQL for parameter validation and flow control.
- **Error Handling and Logging:**
  - `set -e` and `trap` mechanisms will be replaced by BigQuery's native `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks within stored procedures for robust error handling.
  - `print`, `echo`, and `tee` commands for logging to files will be replaced by `INSERT` statements into a BigQuery logging table (`project.dataset.job_log`).
  - `DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, and `DWMSG_Fehlerbehandlung` will be reimplemented as `INSERT` operations into logging and status tables.
- **External Script Invocation:**
  - The call to `Name_Kernskript` (`k_drop_temp_table.ksh`) will be transformed into a `CALL` statement to a separate BigQuery stored procedure (e.g., `project.dataset.k_drop_temp_table`).
- **Data Deletion:**
  - The actual "dropping of temporary tables" logic (contained within `k_drop_temp_table.ksh`) will be translated into `DROP TABLE` or `DELETE` statements on BigQuery tables. This will be encapsulated in the dedicated `k_drop_temp_table` BigQuery stored procedure.

**BigQuery SQL Pseudocode (Wrapper SP):**
(Refer to the `call_cm_mcp` output for detailed pseudocode.)
The pseudocode outlines the creation of a stored procedure `BERT_DROP_TEMP_TABLE` that takes `p_stichtag` and `p_wiederanlaufWert` as input. It manages parameter defaults, validation, logging to a `job_log` table, and then calls a placeholder stored procedure `k_drop_temp_table` for the core logic, all within an exception-handling block.

## 6. External Dependencies
The original script has the following external dependencies and their proposed replacements:

- **Environment Initialization (`. $HOME/.dw_init`):**
  - **Legacy:** Sources a shell script to set up environment variables.
  - **Target:** Configuration values will be managed through BigQuery variables, parameters passed to stored procedures, or metadata tables. For external orchestration, environment variables can be set in Cloud Workflows/Composer/Run configurations.
- **Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):**
  - **Legacy:** Shell scripts providing common functions for error handling, parameter parsing, and date manipulation.
  - **Target:** These functionalities will be rewritten using BigQuery SQL's built-in functions, control flow statements, and custom UDFs/stored procedures as needed. Logging functions will be replaced by `INSERT` statements into the BigQuery logging table.
- **Core Cleanup Script (`${BERT_DIR_ROOT}/aufbereitung/bin/k_drop_temp_table.ksh`):**
  - **Legacy:** A separate KornShell script containing the actual logic for dropping temporary tables.
  - **Target:** This script's logic will be migrated into a dedicated BigQuery stored procedure, `k_drop_temp_table`.
- **UC4 Job Scheduler:**
  - **Legacy:** The job is invoked by a UC4 scheduler (`DW.BERT_DROP_TEMP_TABLE.xml`).
  - **Target:** This scheduling will be replaced by native Google Cloud scheduling mechanisms such as Cloud Composer (for Airflow DAGs), Cloud Scheduler, or Cloud Workflows, which will trigger the BigQuery stored procedure.

There are no direct external systems like databases (Oracle, SFTP, S3) referenced by *this specific wrapper script*.

## 7. Unresolved / Risks
- **`k_drop_temp_table.ksh` content:** The actual logic within the core cleanup script (`k_drop_temp_table.ksh`) is not provided in this analysis. Its complexity will dictate the migration effort for its corresponding BigQuery stored procedure. Any non-SQL operations within it (e.g., file system interactions) would require Python/Cloud Run for execution.
- **Shell-Specific Features:** Direct translation of shell `trap` commands for signal handling and low-level file manipulations (like `tee`) is not possible in BigQuery SQL. These are addressed by BigQuery's `EXCEPTION` blocks and dedicated logging tables.
- **Error Code Mapping:** The specific `ErrNr` values (e.g., `192`, `193`) would need to be consistently mapped to appropriate BigQuery error codes or custom error messages for the logging table.
- **Parameter `l` (`Wiederanlaufwert`):** The exact usage and data type of `p_wiederanlaufWert` in the downstream `k_drop_temp_table.ksh` are assumed. This needs confirmation to ensure correct type handling in BigQuery.

## 8. Build Plan
1. **Define BigQuery Dataset:** Create the target BigQuery dataset (e.g., `project.dataset`) where the new objects will reside.
2. **Create Logging and Status Tables:**
   - Create `project.dataset.job_log` table (schema: `eintragsnr INT64, job_kennung STRING, log_level STRING, err_nr INT64, err_arg STRING, message STRING, stichtag STRING, restart_value STRING, created_at TIMESTAMP`).
   - Create `project.dataset.job_status` table (schema: `eintragsnr INT64, job_kennung STRING, status STRING, updated_at TIMESTAMP`).
3. **Develop `k_drop_temp_table` BigQuery Stored Procedure:**
   - Analyze the source `k_drop_temp_table.ksh` script (when available) to identify the exact SQL `DROP TABLE` or `DELETE` statements or other logic.
   - Translate this logic into a BigQuery Stored Procedure (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.k_drop_temp_table(...)`).
   - Define parameters as needed (e.g., `job_kennung`, `stichtag`, `eintragsnr`, `wiederanlaufWert`).
   - Language: BigQuery SQL.
4. **Develop `BERT_DROP_TEMP_TABLE` BigQuery Stored Procedure:**
   - Implement the wrapper logic as detailed in Section 5.
   - This procedure will call `project.dataset.k_drop_temp_table`.
   - Language: BigQuery SQL.
5. **Develop Orchestration Layer (Optional but Recommended):**
   - Create a Cloud Composer DAG or Cloud Workflow to schedule and invoke the `project.dataset.BERT_DROP_TEMP_TABLE` stored procedure, passing runtime parameters. This will replace the UC4 scheduler.
   - Language: Python (for Airflow DAGs) or YAML (for Cloud Workflows).
6. **Testing:** Unit test each BigQuery stored procedure and the orchestration layer.
7. **Deployment:** Deploy BigQuery objects and orchestration components.