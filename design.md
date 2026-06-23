# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_p_vertrag.ksh`, which serves as a wrapper or orchestration script for the reconciliation of contract data related to the `ta_p_vertrag` table. Its primary functions include:
*   Initializing the execution environment.
*   Validating command-line parameters.
*   Setting up logging and error handling mechanisms.
*   Invoking a core processing script, `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`, which contains the actual business logic for contract data reconciliation.
*   Updating job completion status.

The scope of this migration is to re-platform this KornShell script and its immediate dependencies to Google Cloud's BigQuery platform, leveraging BigQuery stored procedures and associated services for orchestration and logging.

## 2. Source Inventory
The job consists of a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`
    *   **Technology:** KornShell (KornShell)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** Orchestration, wrapper for data reconciliation process.

## 3. Target Architecture
The migrated solution will primarily reside within BigQuery, utilizing:
*   **BigQuery Stored Procedures:** The wrapper logic will be translated into a BigQuery stored procedure (e.g., `project.dataset.sp_vertragsdatenabgleich_wrapper`). The core processing script `k_ausd_v_ta_p_vertrag.ksh` is assumed to be migrated into its own BigQuery stored procedure (e.g., `project.dataset.sp_k_ausd_v_ta_p_vertrag`) or a series of SQL queries.
*   **BigQuery Tables for Logging and Control:**
    *   `project.dataset.job_control`: To store job metadata, status, entry numbers, and timestamps (replacing `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`).
    *   `project.dataset.job_runtime_log`: To capture runtime messages and print statements from the original script.
    *   `project.dataset.job_error_log`: To record detailed error information.
*   **External Orchestration (Optional):** If the core script (`k_ausd_v_ta_p_vertrag.ksh`) cannot be fully translated into BigQuery SQL, or if there are external system interactions, Cloud Composer (for Airflow DAGs), Cloud Workflows, or Cloud Functions might be used to orchestrate the BigQuery stored procedures and any remaining external logic.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_p_vertrag.ksh` follows this general flow:
1.  **Environment Setup:** Sourcing of `$HOME/.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Parsing:** Reads command-line parameters using `getopts`.
3.  **Error Handling Initialization:** Sets up shell traps for `INT` and `ERR` signals and initializes error variables.
4.  **Job Metadata Generation:** Determines a unique job entry number (`DW_EintragsNr`), generates a log file name (`LogDatei`), and sets a system date (`v_sysdate`).
5.  **Logging Initiation:** Creates an initial log entry and sets "Stichtag" information using `DWMSG_` functions.
6.  **Core Script Invocation:** Executes the core script `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`).
7.  **Job Completion:** If the core script exits successfully, prints a success message and updates the job status using `DWMSG_SetzeStatusOK`.
8.  **Error Termination:** If any errors occur during execution, `DWMSG_Fehlerbehandlung` is called, and the script exits with an error.

In BigQuery, this flow will be re-engineered within the `sp_vertragsdatenabgleich_wrapper` stored procedure:
*   **Input Parameters:** The shell script's command-line parameters (e.g., `-j`, `-f`, `-s`, `-l`) will become explicit parameters of the BigQuery stored procedure.
*   **Environment Setup:** Environmental variables will be replaced by stored procedure parameters, configuration tables, or session variables. Sourcing of utility scripts will be replaced by direct BigQuery procedural logic or calls to other BigQuery stored procedures/UDFs that encapsulate the functionality of `DWMSG_` functions.
*   **Logging and Error Handling:** All `print` statements and `DWMSG_` function calls for logging and error reporting will be replaced by `INSERT` statements into the `job_runtime_log`, `job_error_log`, and `job_control` BigQuery tables. Shell `trap` mechanisms will be handled by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.
*   **Core Logic Invocation:** The call to `${Name_Kernskript}` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure `project.dataset.sp_k_ausd_v_ta_p_vertrag`.

## 5. Transformation Logic
The transformation from KornShell to BigQuery SQL will involve:

**a. Environment and Variables:**
*   **Shell Variables:** `ProgName`, `ProgVersion`, `ErrNr`, `ErrArg`, `ErrVal`, `DW_EintragsNr`, `JobKennung`, `v_sysdate`, `LogDatei`, `Name_Kernskript` will be declared as `DECLARE` variables within the BigQuery stored procedure.
*   **Environmental Sourcing:** `. $HOME/.dw_init` and other sourced utility scripts will need to be analyzed. If they set environment variables, these will become BigQuery stored procedure parameters or entries in a configuration table. If they contain reusable functions (like `DWMSG_*`), these functions will be translated into separate BigQuery stored procedures or UDFs, or their logic will be embedded directly into the wrapper procedure.

**b. Parameter Handling:**
*   `getopts` and `case` statement for parameter parsing will be replaced by:
    *   BigQuery stored procedure input parameters (`IN p_param_s STRING`, `IN p_param_l STRING`, `IN p_jobkennung STRING`, `IN p_stichtag DATE`).
    *   `IF` conditions and assignments to validate parameters and set `v_errnr`, `v_errarg`.

**c. Error Handling:**
*   `set -eu`: This strict mode will be implicitly handled by BigQuery's default error behavior and explicit `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for capturing and handling exceptions.
*   `trap` commands: These will be replaced by the `EXCEPTION WHEN ERROR THEN` block at the procedure level, which will log errors and update job status.
*   `DWMSG_MeldeFehler`: This function will map to an `INSERT` statement into `job_error_log` and potentially `SIGNAL SQLSTATE` to raise a user-defined error.
*   Exit codes: `exit $ErrNr` will be replaced by `SIGNAL SQLSTATE` to indicate failure, and successful completion will simply be the procedure running to its end.

**d. Logging and Job Control:**
*   `DWMSG_ErmittleNr`: Replaced by a `SELECT COALESCE(MAX(eintragsnr), 0) + 1 FROM job_control WHERE job_kennung = v_jobkennung` to generate a unique job identifier.
*   `DWMSG_Logdateiname`: Replaced by string concatenation for `v_logdatei`.
*   `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`: These will map to `INSERT` and `UPDATE` statements on the `job_control` table.
*   `print` and `tee -a $LogDatei`: These output operations will be replaced by `INSERT` statements into the `job_runtime_log` table.

**e. Core Script Execution:**
*   `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}`: This shell command will be replaced by `CALL project.dataset.sp_k_ausd_v_ta_p_vertrag(v_jobkennung, v_eintragsnr, p_param_s, p_param_l, COALESCE(p_stichtag, CURRENT_DATE()))`. This assumes the `k_ausd_v_ta_p_vertrag.ksh` script is also migrated to a BigQuery stored procedure.

## 6. External Dependencies
The original script has no direct external system integrations (like Oracle, SFTP, S3) that are explicitly visible in the wrapper script. Its dependencies are primarily other shell scripts and the underlying file system.

*   **Sourced Utility Scripts:**
    *   `$HOME/.dw_init`: Likely contains environment variables; these will be migrated to BigQuery procedure parameters or configuration tables.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: This error messaging framework will be replaced by BigQuery's native exception handling (`BEGIN...EXCEPTION`) and logging to `job_error_log`.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: This parameter handling utility will be replaced by BigQuery stored procedure parameter validation logic.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling functions will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`: This is the most significant dependency. Its migration is critical. It will be transformed into a separate BigQuery stored procedure (`sp_k_ausd_v_ta_p_vertrag`). If this script interacts with external systems or performs non-SQL operations, those parts will need further analysis and potential migration to Cloud Functions, Dataflow, or other GCP services.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_p_vertrag.ksh`) Logic:** The content and complexity of `k_ausd_v_ta_p_vertrag.ksh` are currently unknown. The success of this migration heavily relies on the ability to translate its logic into BigQuery SQL or other suitable GCP services. If it contains complex file I/O, external system calls (e.g., to databases other than what BigQuery can connect to, or external APIs), or custom binaries, the migration effort will increase significantly.
*   **`DWMSG_*` Functions Fidelity:** While the `DWMSG_*` functions are mapped to BigQuery logging tables, ensuring exact functional parity (e.g., unique ID generation, specific logging formats) may require detailed inspection of the original `f_alis_msgerr.ksh` and other utilities.
*   **Environment Variable Resolution:** The exact values and usage of `HOME` and `BERT_DIR_ROOT` need to be confirmed. These will become input parameters or configurable values in the BigQuery environment.
*   **Date Format Sensitivity:** The script uses `date +%d%m%Y`. While BigQuery can format dates, ensuring consistent date interpretations across the entire data pipeline is crucial.
*   **Missing Lineage Edges:** The absence of `lineage_edges` for this file means the tool could not infer its direct data flow relationships. While the script content itself clarifies its primary invocation, a broader lineage understanding (what `k_ausd_v_ta_p_vertrag.ksh` reads from/writes to) is still missing and crucial for a complete data flow picture.

## 8. Build Plan

1.  **Define BigQuery Datasets:**
    *   `CREATE SCHEMA IF NOT EXISTS project.dataset;`
2.  **Create BigQuery Job Control and Logging Tables:**
    *   `job_control` table (e.g., `eintragsnr`, `job_kennung`, `script_name`, `log_name`, `stichtag_info`, `status`, `created_ts`, `finished_ts`).
    *   `job_runtime_log` table (e.g., `eintragsnr`, `job_kennung`, `log_level`, `message`, `log_ts`).
    *   `job_error_log` table (e.g., `job_kennung`, `error_nr`, `error_arg`, `error_ts`, `source_proc`).
3.  **Develop BigQuery Stored Procedure for Wrapper:**
    *   Translate `r_ausd_v_ta_p_vertrag.ksh` into `project.dataset.sp_vertragsdatenabgleich_wrapper`.
    *   This will involve replacing shell constructs with BigQuery SQL procedural language, `INSERT` statements for logging, and `CALL` to the core processing procedure.
4.  **Migrate Core Processing Logic (High Priority Item):**
    *   Analyze `k_ausd_v_ta_p_vertrag.ksh` (and any scripts it calls).
    *   Design and implement `project.dataset.sp_k_ausd_v_ta_p_vertrag` or a series of BigQuery SQL statements/views/functions that encapsulate its data processing logic. This will require a separate, detailed design document.
5.  **Configuration Management:**
    *   Establish a mechanism for parameters like `BERT_DIR_ROOT` (if still relevant), `project`, `dataset` names to be passed or configured for the BigQuery stored procedures.
6.  **Orchestration (if needed):**
    *   If any parts of the process cannot be fully contained within BigQuery stored procedures (e.g., external system interactions from `k_ausd_v_ta_p_vertrag.ksh`), design and implement a Cloud Composer DAG or Cloud Workflow to orchestrate the BigQuery components and any external steps.

**Build Output Language:** BigQuery SQL (for stored procedures and DDL) and potentially Python (for Cloud Functions/Cloud Composer DAGs).