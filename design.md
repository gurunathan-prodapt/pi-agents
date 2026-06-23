# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` from the legacy system to Google BigQuery.

The purpose of this script is to act as a wrapper/orchestration script for contract data reconciliation specifically for the `ta_period` table. It handles environment initialization, logging, error handling, and invokes a core processing script (`k_ausd_v_ta_period.ksh`) where the actual business logic resides. The script does not contain direct data transformation logic itself.

The scope of this migration is to re-implement the orchestration and error handling mechanisms of this wrapper script within the BigQuery environment, assuming the core script it invokes will also be migrated or called appropriately.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Not available (complexity analysis returned no rows)
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** ETL orchestrator/wrapper
    *   **Summary:** Wrapper script for contract data reconciliation (`ta_period`). Initializes environment, handles parameters, logging, and error trapping, then executes a core script `k_ausd_v_ta_period.ksh`.

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery Stored Procedures for the orchestration logic.

*   **BigQuery Stored Procedure:** The `r_ausd_v_ta_period.ksh` script will be converted into a BigQuery Stored Procedure, e.g., `project.dataset.sp_vertragsdatenabgleich`. This procedure will manage parameter handling, logging, and error trapping.
*   **BigQuery Tables for Logging & Status:** The file-based logging and status tracking will be replaced by dedicated BigQuery tables. Proposed tables include:
    *   `project.dataset.dw_job_registry`: To track job entry numbers.
    *   `project.dataset.dw_job_log`: To store detailed job execution logs.
    *   `project.dataset.dw_job_status`: To store job status updates (OK/ERR).
    *   `project.dataset.dw_job_error`: To record error details.
    *   `project.dataset.dw_job_stichtag`: To store reference date information.
*   **Orchestration (Optional, for invocation):** If `sp_vertragsdatenabgleich` needs to be externally scheduled or invoked, a Cloud Composer DAG or Cloud Scheduler job could be used.
*   **Core Script Migration:** It is assumed that the invoked core script (`k_ausd_v_ta_period.ksh`) will also be migrated to a BigQuery Stored Procedure (e.g., `project.dataset.sp_k_ausd_v_ta_period`) or another suitable BigQuery-compatible component.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_period.ksh` acts as a control flow orchestrator.

**Legacy Flow:**
1.  `r_ausd_v_ta_period.ksh` starts.
2.  Initializes environment by sourcing `$HOME/.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
3.  Parses command-line parameters (e.g., `-h`).
4.  Sets up logging mechanism (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`).
5.  Configures `trap` for `INT` and `ERR` signals for error handling.
6.  Invokes `k_ausd_v_ta_period.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`).
7.  Redirects output of `k_ausd_v_ta_period.ksh` to a log file.
8.  Upon successful completion, updates job status (`DWMSG_SetzeStatusOK`).

**Target BigQuery Flow:**
1.  **Invocation:** A mechanism (e.g., Cloud Scheduler, Cloud Composer, or manual execution) calls `project.dataset.sp_vertragsdatenabgleich`.
2.  **`sp_vertragsdatenabgleich` Execution:**
    *   Declares variables for program name, version, error numbers, job ID (`JobKennung`), and system date (`v_sysdate`).
    *   Handles `-h` parameter for displaying usage.
    *   Determines a unique job entry number from `dw_job_registry`.
    *   Generates a logical log filename for auditing.
    *   Inserts initial job start log entry into `dw_job_log`.
    *   Inserts stichtag information into `dw_job_stichtag`.
    *   **Main Logic Block (TRY-CATCH equivalent):**
        *   Inserts log entry for job header.
        *   **Calls** the migrated core script stored procedure: `CALL project.dataset.sp_k_ausd_v_ta_period(JobKennung, DW_EintragsNr);`
        *   Inserts success log entry into `dw_job_log`.
        *   Updates job status to 'OK' in `dw_job_status`.
    *   **Error Handling (EXCEPTION WHEN ERROR THEN):**
        *   If an error occurs during the main logic block, it inserts an error message into `dw_job_error`.
        *   Updates job status to 'ERR' in `dw_job_status`.
        *   Raises an error to signal failure to the caller.

## 5. Transformation Logic

**Original Shell Script Constructs vs. BigQuery SQL Equivalents:**

| Shell Script Construct           | BigQuery SQL Equivalent                                       | Notes                                                                                                                                                                                                            |
| :------------------------------- | :------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `#!/bin/ksh`                     | `CREATE OR REPLACE PROCEDURE ...`                             | Shell execution environment replaced by BigQuery Stored Procedure.                                                                                                                                               |
| `ProgName="Name"`                | `DECLARE ProgName STRING DEFAULT 'Name';`                     | Shell variables become BigQuery declared variables.                                                                                                                                                              |
| `. $HOME/.dw_init`               | Replaced by explicit BigQuery configuration/parameters.       | Environment sourcing is not directly replicable. Configuration will be managed within BigQuery datasets or passed as parameters.                                                                                |
| `. ${BERT_DIR_ROOT}/...util/bin/f_alis_msgerr.ksh` | Replaced by BigQuery error handling (`EXCEPTION WHEN ERROR THEN`) and logging tables. | Utility script functionalities (error/parameter/date handling) will be integrated directly into the stored procedure logic or handled by new BigQuery functions/procedures.                                      |
| `set -eu`                        | BigQuery's default strict execution behavior.                 | `set -e` (exit on error) is implicitly handled by BigQuery's error propagation. `set -u` (fail on unset variable) is handled by explicit variable declarations.                                                  |
| `while getopts ...`              | Stored procedure input parameters.                            | Command-line parameter parsing is replaced by named input parameters for the BigQuery stored procedure.                                                                                                        |
| `if [ ! $ErrNr -eq 0 ]`          | `IF ErrNr != 0 THEN ... END IF;`                              | Conditional logic is directly translatable.                                                                                                                                                                      |
| `Name_Kernskript="..."`          | `DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.sp_k_ausd_v_ta_period';` | Path to core script becomes the name of the invoked BigQuery stored procedure.                                                                                                                                   |
| `typeset -u JobKennung="BERT_V_TA_PERIOD"` | `DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_PERIOD';`       | Variable declaration. `typeset -u` for uppercase is handled by direct assignment of uppercase string.                                                                                                            |
| `v_sysdate=$(date +%d%m%Y)`      | `DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());` | Date formatting function.                                                                                                                                                                                        |
| `DWMSG_ErmittleNr DW_EintragsNr` | `SET DW_EintragsNr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM dw_job_registry WHERE job_kennung = JobKennung);` | Custom function for job number generation replaced by querying a BigQuery registry table.                                                                                                                        |
| `DWMSG_Logdateiname LogDatei ...`| `SET LogDatei = CONCAT('log_', JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');` | Log file naming becomes a string concatenation for auditability within log tables, not an actual file.                                                                                                          |
| `DWMSG_ErzeugeEintrag ... >> $LogDatei 2>&1` | `INSERT INTO dw_job_log (...) VALUES (...);`          | Logging to files replaced by inserts into a BigQuery logging table.                                                                                                                                              |
| `trap "..." INT` / `trap "..." ERR` | `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`                | Shell signal trapping replaced by BigQuery's structured error handling in a `BEGIN...END` block.                                                                                                               |
| `print "Message"`                | `INSERT INTO dw_job_log ... 'Message'`                        | Console output replaced by logging to a table. For direct user feedback, the stored procedure could return a result set, but typically logs are preferred.                                                      |
| `${Name_Kernskript} -j ... >> $LogDatei 2>&1` | `CALL project.dataset.sp_k_ausd_v_ta_period(JobKennung, DW_EintragsNr);` | Execution of another script replaced by calling another BigQuery stored procedure. All logging is handled by `INSERT` statements into the log table.                                                         |
| `tee -a $LogDatei`               | Insert into `dw_job_log` with the message.                    | `tee` functionality (write to stdout and file) is not directly applicable; the equivalent is logging to a table and, if needed, returning the message via a `SELECT` statement in the procedure for stdout equivalent. |
| `exit 0`                         | Successful completion of the stored procedure.                | Explicit exit code is implicit in BigQuery stored procedure success/failure.                                                                                                                                     |
| `exit $ErrNr`                    | `RAISE USING MESSAGE = 'AppError: Abbruch';`                  | Raising an error to indicate failure.                                                                                                                                                                            |

## 6. External Dependencies

The original script has the following dependencies:

*   **Filesystem-based utilities and environment scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   These will be replaced by:
        *   BigQuery dataset/project level configurations.
        *   Parameters passed to the BigQuery Stored Procedure.
        *   Direct implementation of common utility functions within BigQuery SQL or Python external functions (if complex).
        *   Dedicated BigQuery logging and status tables.
*   **Core processing script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh`
    *   This is expected to be migrated to a BigQuery Stored Procedure, `project.dataset.sp_k_ausd_v_ta_period`, which will be invoked by `sp_vertragsdatenabgleich`.
*   **Operating System Commands:** `date`, `getopts`, `tee`.
    *   `date`: Replaced by BigQuery's `CURRENT_DATE()`, `FORMAT_DATE()`, etc.
    *   `getopts`: Replaced by BigQuery Stored Procedure parameters.
    *   `tee`: Replaced by inserting into a BigQuery logging table.

No external systems (like Oracle, SFTP, S3) were detected as directly referenced by this specific wrapper script.

## 7. Unresolved / Risks

*   **Unresolved Core Logic:** The actual data reconciliation logic is within `k_ausd_v_ta_period.ksh`, which is invoked by this script. The successful migration of this wrapper heavily depends on the successful migration of `k_ausd_v_ta_period.ksh` into a BigQuery-compatible component (likely another stored procedure).
*   **Missing Complexity Data:** The `file_complexity` table returned no rows, so the specific complexity tier and migration flags for this file are unknown. This could hide potential challenges.
*   **Custom Shell Functions:** The `DWMSG_*` functions are custom shell functions. Their exact implementation details are not available from the script itself. The proposed migration assumes standard logging and error handling patterns, but if these functions contain complex logic, a deeper dive into their implementation will be required.
*   **Environment Initialization (`. $HOME/.dw_init`):** The contents of this environment file are unknown. Any critical environment variables or settings defined here need to be identified and replicated as BigQuery procedure parameters or configuration values.
*   **Parameter List `ParamList="s:l:"`:** While `getopts` is used, the script itself doesn't explicitly handle `-s` or `-l` parameters. This implies they might be for the invoked core script or historical. Confirm if these parameters are still relevant and how they are used by `k_ausd_v_ta_period.ksh`.

## 8. Build Plan

The build plan focuses on generating the BigQuery Stored Procedure and necessary supporting tables.

1.  **Define BigQuery Datasets:**
    *   Create `project.dataset` (if not already existing) for housing procedures and tables.
2.  **Create Logging and Status Tables:**
    *   `dw_job_registry`: `job_kennung STRING, job_entry_nr INT64, ...`
    *   `dw_job_log`: `job_entry_nr INT64, job_kennung STRING, script_name STRING, log_file_name STRING, log_message STRING, log_ts TIMESTAMP, ...`
    *   `dw_job_status`: `job_entry_nr INT64, job_kennung STRING, status_code STRING, status_message STRING, status_ts TIMESTAMP, ...`
    *   `dw_job_error`: `job_entry_nr INT64, job_kennung STRING, error_message STRING, error_ts TIMESTAMP, ...`
    *   `dw_job_stichtag`: `job_entry_nr INT64, stichtag_value STRING, stichtag_format STRING, ...`
    *   (Language: BigQuery DDL)
3.  **Generate BigQuery Stored Procedure for `r_ausd_v_ta_period.ksh`:**
    *   Name: `project.dataset.sp_vertragsdatenabgleich`
    *   Input Parameter: `p_help STRING` (for the -h option)
    *   Logic: Implement the pseudocode provided in the CM MCP analysis, including variable declarations, parameter handling, logging to the new tables, and error handling using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`.
    *   (Language: BigQuery SQL)
4.  **Migrate/Generate `sp_k_ausd_v_ta_period`:**
    *   This is a prerequisite step. The core logic from `k_ausd_v_ta_period.ksh` must be migrated to a BigQuery Stored Procedure or equivalent. This procedure will be called by `sp_vertragsdatenabgleich`.
    *   (Language: BigQuery SQL or Python if complex non-SQL logic is involved).
5.  **Deployment Scripts:**
    *   Scripts to deploy the BigQuery DDL for tables and the BigQuery SQL for stored procedures.
    *   (Language: Bash/Python using `bq` CLI or client libraries)
6.  **Orchestration (Optional):**
    *   If scheduled execution is required, create a Cloud Scheduler job to invoke the BigQuery Stored Procedure, or define a Cloud Composer DAG.
    *   (Language: YAML/Python)