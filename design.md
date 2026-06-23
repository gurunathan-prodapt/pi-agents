# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_vvl_upgrade.ksh`. The script serves as an orchestration wrapper for a core data reconciliation process related to the `ta_vvl_upgrade` table. Its primary purpose is to set up the execution environment, handle command-line parameters, manage job and error logging, and invoke a core "kernel script" (`k_ausd_v_ta_vvl_upgrade.ksh`) which performs the actual data processing. The migration aims to re-implement this orchestration logic and its dependencies within Google Cloud's BigQuery environment, primarily using BigQuery Stored Procedures, and potentially external orchestration services like Cloud Composer or Cloud Run for any non-SQL compatible components.

## 2. Source Inventory
| File Name                                                               | Technology  | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                |
| :---------------------------------------------------------------------- | :---------- | :----- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh` | KornShell   | medium | semi_auto         | This is a KornShell wrapper script that orchestrates the execution of a core data reconciliation script for the `ta_vvl_upgrade` table, handling environment setup, parameter parsing, error trapping, and logging. It invokes an internal "kernel script" named `k_ausd_v_ta_vvl_upgrade.ksh` and utilizes several internal shell utility scripts for common functions like error handling and date management. |

## 3. Target Architecture
The target architecture in BigQuery will consist of:
-   **BigQuery Stored Procedures:**
    -   `sp_vertragsdatenabgleich`: This will be the main orchestrating stored procedure, replacing the `r_ausd_v_ta_vvl_upgrade.ksh` script. It will handle parameter parsing, job logging, error handling, and the invocation of the core reconciliation logic.
    -   `sp_k_ausd_v_ta_vvl_upgrade`: This stored procedure will encapsulate the core data reconciliation logic currently present in `k_ausd_v_ta_vvl_upgrade.ksh` (assuming it can be converted to SQL).
    -   Helper Stored Procedures: For common framework functions like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`.
-   **BigQuery Tables:**
    -   `job_audit_log`: A table to store detailed job execution logs, replacing file-based logging.
    -   `job_control`: A table to manage job status and control parameters, potentially replacing internal control mechanisms and `stichtag` (reference date) information.
-   **External Orchestration (Optional/Conditional):**
    -   **Cloud Composer / Cloud Run:** If `k_ausd_v_ta_vvl_upgrade.ksh` contains complex file I/O or OS-level operations not directly translatable to BigQuery SQL, an external orchestrator might be used to call Python scripts or other services that handle these specific non-SQL tasks, while still triggering the BigQuery stored procedures for data manipulation.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_vvl_upgrade.ksh` orchestrates the execution. The lineage analysis did not provide specific `INVOKES`, `READS`, or `WRITES` edges for this script. However, based on the script content:

**Legacy Data Flow:**
1.  **Environment Setup:** `r_ausd_v_ta_vvl_upgrade.ksh` sources `$HOME/.dw_init` and other utility scripts for environment variables and common functions.
2.  **Parameter Parsing:** Reads command-line arguments (`-h`, `-s`, `-l`).
3.  **Logging Initialization:** Initializes job ID (`DW_EintragsNr`), job name (`JobKennung`), and log file (`LogDatei`) using framework functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`).
4.  **Error Handling:** Sets up `trap` commands for `INT` (interrupt) and `ERR` (error) signals, which invoke `DWMSG_Fehlerbehandlung`.
5.  **Core Logic Invocation:** Executes the "kernel script" `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh` passing job-specific parameters. All output is redirected to the `LogDatei`.
6.  **Status Update:** On successful completion, updates job status (`DWMSG_SetzeStatusOK`) and prints a success message.

**Target Data Flow (BigQuery):**
1.  **`sp_vertragsdatenabgleich` (Main Orchestrator):**
    -   Receives input parameters (e.g., `param_h`, `param_s`, `param_l`).
    -   Performs parameter validation.
    -   Generates a unique job entry number and log identifier (replicated by querying/inserting into `job_audit_log`).
    -   Logs job start and parameter information into `job_audit_log`.
    -   Updates `job_control` with `stichtag` information.
    -   **INVOKES `sp_k_ausd_v_ta_vvl_upgrade`:** Calls the core data reconciliation stored procedure.
    -   **Error Handling:** Uses BigQuery `EXCEPTION WHEN ERROR` blocks to catch errors from `sp_k_ausd_v_ta_vvl_upgrade` or other BigQuery operations, logging error details to `job_audit_log` and updating `job_control` status.
    -   **Success Handling:** On successful completion, logs a success message to `job_audit_log` and updates `job_control` status to 'OK'.

## 5. Transformation Logic
The `r_ausd_v_ta_vvl_upgrade.ksh` script itself is an orchestration layer; it does not contain direct data transformation logic. Its transformation will be primarily in migrating shell scripting constructs to BigQuery SQL stored procedure constructs.

**Key Transformations:**
-   **Environment Sourcing (`. $HOME/.dw_init`):** Replaced by explicit BigQuery stored procedure parameters or configuration tables.
-   **Parameter Parsing (`getopts`):** Replaced by `IN` parameters in the BigQuery stored procedure definition. Conditional logic for parameter validation will be translated to `IF` statements.
-   **Logging (`DWMSG_...` functions, `print`, `tee -a $LogDatei`):** Replaced by `INSERT` statements into `job_audit_log` and `job_control` tables.
-   **Error Trapping (`trap INT ERR`):** Replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks for robust error handling.
-   **Core Script Invocation (`${Name_Kernskript} ...`):** Replaced by a `CALL` statement to the `sp_k_ausd_v_ta_vvl_upgrade` BigQuery stored procedure.
-   **Date Manipulation (`date +%d%m%Y`):** Replaced by BigQuery date functions like `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
-   **Control Flow (`if`, `while`, `exit`):** Translated to BigQuery's `IF`, `WHILE`, `LEAVE`, `RETURN` (implicit), and `SIGNAL SQLSTATE` (for error exits).

The actual data reconciliation logic (`k_ausd_v_ta_vvl_upgrade.ksh`) is assumed to be migrated into `sp_k_ausd_v_ta_vvl_upgrade`, likely involving SQL DML operations on `ta_vvl_upgrade` or related tables.

## 6. External Dependencies
**Legacy External Dependencies:**
-   **Shell Environment:** Relies on a specific `ksh` environment, including `$HOME/.dw_init` and various utility scripts under `${BERT_DIR_ROOT}`.
-   **Filesystem:** Generates and appends to a log file.
-   **Core Kernel Script:** `k_ausd_v_ta_vvl_upgrade.ksh` (content unknown, assumed to contain core reconciliation logic and potentially other dependencies).

**Target Replacement/Handling:**
-   **Shell Environment:** Replaced by BigQuery's execution environment. Configuration values will be passed as parameters or retrieved from BigQuery configuration tables.
-   **Filesystem (Log File):** Replaced by the `job_audit_log` BigQuery table for persistent, queryable logging.
-   **Core Kernel Script:**
    -   If `k_ausd_v_ta_vvl_upgrade.ksh` contains only SQL-compatible logic, it will be migrated to `sp_k_ausd_v_ta_vvl_upgrade`.
    -   If `k_ausd_v_ta_vvl_upgrade.ksh` contains non-SQL compatible logic (e.g., complex file manipulations, external system calls), that specific logic might need to be re-implemented in Python and run via Cloud Run or a Cloud Composer DAG, which then orchestrates the BigQuery stored procedure calls.

No direct external systems (like Oracle, SFTP, S3) were identified in the `lineage_assembled_jobs` analysis for this specific job, indicating this wrapper itself does not directly interact with them. Any such interactions would be within the kernel script.

## 7. Unresolved / Risks
-   **Content of `k_ausd_v_ta_vvl_upgrade.ksh`:** The primary risk is the unknown content and complexity of the invoked kernel script. If it contains complex shell-specific logic, non-SQL data manipulations, or interactions with external systems (databases, APIs, filesystems) that are not easily translated to BigQuery SQL, this will require a separate, detailed migration design and potentially introduce Python/Cloud Run components.
-   **Framework Functions (`DWMSG_...`):** The exact functionality of the `DWMSG_` utility functions is not fully known. It's assumed they map to basic logging and status updates, which are handled by `job_audit_log` and `job_control` tables. If they perform more complex operations (e.g., calling external APIs, complex data lookups), those would need further analysis and migration.
-   **Performance:** Shell scripts can sometimes perform operations (e.g., small file processing) very efficiently. Migration to BigQuery stored procedures might introduce performance considerations if the translated SQL is not optimized or if the original script's efficiency relied on specific shell behaviors.
-   **`set -eu`:** This strict error handling in shell scripts needs careful translation to BigQuery's error handling (`EXCEPTION WHEN ERROR`) to ensure equivalent robustness.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Define BigQuery Schema:**
    -   Create `job_audit_log` table (e.g., `(job_name STRING, job_version STRING, job_entry_no INT64, log_file_name STRING, event_type STRING, error_no INT64, error_arg STRING, event_message STRING, stichtag STRING, stichtag_format STRING, event_ts TIMESTAMP)`).
    -   Create `job_control` table (e.g., `(job_name STRING, job_entry_no INT64, job_status STRING, stichtag STRING, stichtag_format STRING, updated_ts TIMESTAMP, status_ts TIMESTAMP)`).
    -   (Conditional) Define DDL for `ta_vvl_upgrade` and any other tables manipulated by `k_ausd_v_ta_vvl_upgrade.ksh`.

2.  **Develop BigQuery Helper Stored Procedures:**
    -   `sp_ermittle_nr`, `sp_logdateiname`, `sp_erzeuge_eintrag`, `sp_setze_stichtag_info`, `sp_fehlerbehandlung`, `sp_setze_status_ok` (or integrate their logic directly into the main procedure).

3.  **Migrate Core Kernel Script (`k_ausd_v_ta_vvl_upgrade.ksh`):**
    -   Analyze the content of `k_ausd_v_ta_vvl_upgrade.ksh`.
    -   If SQL-compatible: Develop `sp_k_ausd_v_ta_vvl_upgrade` BigQuery stored procedure.
    -   If not SQL-compatible: Develop Python script(s) for the non-SQL parts and plan for their execution via Cloud Run/Composer.

4.  **Develop Main Orchestrator Stored Procedure (`sp_vertragsdatenabgleich`):**
    -   Translate the shell script `r_ausd_v_ta_vvl_upgrade.ksh` into `sp_vertragsdatenabgleich` (BigQuery SQL).
    -   Implement parameter handling, job logging, date functions, and error handling as per the pseudocode.
    -   Integrate calls to `sp_k_ausd_v_ta_vvl_upgrade` or external Python scripts.

5.  **External Orchestration (if needed):**
    -   If any part of the logic remains outside BigQuery (e.g., Python scripts), develop a Cloud Composer DAG or Cloud Run service to orchestrate the execution flow, including triggering `sp_vertragsdatenabgleich`.

6.  **Testing:**
    -   Unit test each BigQuery stored procedure and external component.
    -   Integration test the entire workflow from the orchestrator.
    -   Validate data reconciliation results against legacy system outputs.

**Build Artifacts:**
-   `job_audit_log.sql` (BigQuery DDL)
-   `job_control.sql` (BigQuery DDL)
-   `sp_vertragsdatenabgleich.sql` (BigQuery Stored Procedure)
-   `sp_k_ausd_v_ta_vvl_upgrade.sql` (BigQuery Stored Procedure, if applicable)
-   `(optional) k_ausd_v_ta_vvl_upgrade.py` (Python script for non-SQL logic)
-   `(optional) airflow_dag_vertragsdatenabgleich.py` (Cloud Composer DAG)
-   Deployment scripts (e.g., `bq mk`, `bq query`, `gcloud builds submit`)