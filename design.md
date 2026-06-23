# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_acc_ref.ksh`. The job's primary purpose is described as "Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_acc_ref" (Wrapper script for the reconciliation of contract data: table ta_acc_ref). It serves as an orchestration script responsible for parsing command-line parameters, initializing the runtime environment and error handling framework, creating job log metadata, invoking a core processing script, and recording the final success or failure status. The `purpose_note` from the assembled job further confirms its role as a job assembled from 1 component, with a medium stage distribution.

## 2. Source Inventory
The primary source file for this job is:
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh`**
  - **Technology:** Shell Script (KornShell)
  - **Complexity Tier:** Unknown (file_complexity returned no rows)
  - **Automation Bucket:** semi_auto
  - **Purpose:** ETL Orchestrator (as derived from its wrapper nature)
  - **Content Summary:** This script is a wrapper. It sources environment and utility scripts, handles command-line arguments (`-h`, `-s`, `-l`), sets up logging and error traps, and then executes a core script `k_ausd_v_ta_acc_ref.ksh` with specific parameters, redirecting its output to a log file. It manages job status and error reporting.

**Dependencies identified within the script:**
- **Invoked Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh` (Lineage shows `SCRIPT:K_AUSD_V_TA_ACC_REF.KSH`)
- **Sourced Utility Scripts:**
    - `$HOME/.dw_init`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
- **Utility Functions (likely defined in sourced scripts):** `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`.

## 3. Target Architecture
The target platform for this migration is Google BigQuery.
The `r_ausd_v_ta_acc_ref.ksh` script, being an orchestration wrapper, will be migrated to a **BigQuery Stored Procedure**. This stored procedure will handle the parameter parsing, environment setup (using BigQuery variables or configuration tables), logging, error handling, and the invocation of the core business logic.

- **Orchestration:** BigQuery Stored Procedures will be used to replicate the wrapper logic.
- **Logging:** Dedicated BigQuery tables (e.g., `job_log`, `job_error_log`, `job_log_detail`) will replace the file-based logging system.
- **Core Business Logic:** The content of `k_ausd_v_ta_acc_ref.ksh` (which is currently unknown) will also need to be migrated. Depending on its complexity, it will likely be translated into another BigQuery Stored Procedure, BigQuery SQL queries, or potentially a Python script orchestrated by Cloud Workflows or Cloud Composer if it involves complex procedural logic or external interactions.
- **Configuration:** Environment variables and configuration files (`.dw_init`) will be replaced by BigQuery tables or parameters passed to the stored procedure.

## 4. Data Flow & Lineage
The original shell script executes the following flow:
1. **Initialization:** Sources `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` to set up the environment and utility functions.
2. **Parameter Parsing:** Uses `getopts` to parse command-line arguments (`-h`, `-s`, `-l`).
3. **Error Handling Setup:** Initializes error variables and sets up `trap` commands for `INT` and `ERR` signals, directing error output to a log file via `DWMSG_Fehlerbehandlung`.
4. **Logging Setup:** Calls `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, and `DWMSG_SetzeStichtagInfo` to prepare log entries and a log file.
5. **Core Logic Invocation:** Executes the external script `k_ausd_v_ta_acc_ref.ksh` passing job-specific parameters (`-j $JobKennung -f $DW_EintragsNr`). The standard output and error of this core script are appended to the determined log file.
6. **Completion Status:** If `k_ausd_v_ta_acc_ref.ksh` completes successfully, a success message is printed, and `DWMSG_SetzeStatusOK` is called to update the job status.
7. **Exit:** The script exits with status 0 on success, or an error code if parameter parsing failed or an error trap was triggered.

**Lineage/Dependencies:**
- `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh`
    - `INVOKES` `SCRIPT:K_AUSD_V_TA_ACC_REF.KSH`
    - `SENDS_MAIL` `FUNCTION:DWMSG_ERMITTLENR` (This is likely a misclassification of a logging/notification function based on script content, which indicates logging to file rather than direct email sending).

## 5. Transformation Logic
The `r_ausd_v_ta_acc_ref.ksh` script primarily serves an orchestration role and does not contain direct data transformation logic. Its migration focuses on replicating its control flow, parameter handling, and logging mechanisms within BigQuery.

**Mapping Legacy KornShell to BigQuery SQL:**
- **Environment Variables:** Shell variables like `ProgName`, `ProgVersion`, `JobKennung`, `Name_Kernskript` will be mapped to `DECLARE`d variables within the BigQuery Stored Procedure. Paths like `$HOME` and `${BERT_DIR_ROOT}` will be replaced with BigQuery dataset/table references or configuration parameters.
- **Parameter Parsing:** The `getopts` logic for parsing command-line parameters will be replaced by `IN` parameters of the BigQuery Stored Procedure.
- **Conditional Logic:** `if [ ... ]` statements will be translated into `IF THEN ELSE END IF;` constructs. `case` statements can be mapped to nested `IF/ELSEIF` or BigQuery's `CASE` expressions where applicable.
- **Function Calls:** Sourced utility scripts and their functions (e.g., `DWMSG_ErmittleNr`) will be reimplemented as separate BigQuery Stored Procedures or User-Defined Functions (UDFs).
- **Date Handling:** `date +%d%m%Y` will use BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
- **Logging:** `print` and `tee -a $LogDatei` operations will be replaced by `INSERT` statements into BigQuery logging tables (`dataset.job_log`, `dataset.job_error_log`, `dataset.job_log_detail`).
- **Error Trapping:** The shell `trap` mechanism will be replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks for robust error handling. `SIGNAL SQLSTATE` will be used to raise custom errors.
- **Core Script Execution:** The invocation of `k_ausd_v_ta_acc_ref.ksh` will be translated into a `CALL` statement to a corresponding BigQuery Stored Procedure (e.g., `CALL dataset.k_ausd_v_ta_acc_ref(JobKennung, DW_EintragsNr)`). The business logic contained within `k_ausd_v_ta_acc_ref.ksh` will need to be analyzed separately and converted to BigQuery SQL or Python.

## 6. External Dependencies
The `lineage_assembled_jobs` did not report any external systems for this job (`external_systems` was empty). Based on the script content and analysis:

- **File System:** The original script heavily relies on the file system for sourcing scripts and writing log files. In BigQuery, these dependencies will be replaced:
    - **Sourced Scripts:** Configuration values (like `BERT_DIR_ROOT`) and utility functions will be stored in BigQuery configuration tables or implemented as BigQuery UDFs/procedures.
    - **Log Files:** Will be replaced by BigQuery logging tables.
- **Operating System Utilities:** Standard shell commands like `getopts`, `date`, `print`, `tee` will be replaced by their BigQuery SQL equivalents or BigQuery Stored Procedure features.
- **Core Script `k_ausd_v_ta_acc_ref.ksh`:** This is the most significant dependency. Its content must be fully understood and migrated to BigQuery. If it interacts with external databases, APIs, or files, those interactions will need to be re-engineered for the BigQuery ecosystem (e.g., using federated queries, Cloud Functions, Cloud Storage, or external tables).

## 7. Unresolved / Risks
- **Core Script `k_ausd_v_ta_acc_ref.ksh`:** The business logic and data flow within `k_ausd_v_ta_acc_ref.ksh` are currently unknown. This is the biggest unresolved item and potential risk. Its complexity will dictate the final migration strategy for the core processing. If it contains complex file manipulations, intricate logic, or interactions with non-BigQuery compatible systems, it might require migration to Python (e.g., using PySpark on Dataproc or Cloud Functions) rather than pure BigQuery SQL.
- **"SENDS_MAIL" Classification:** The `lineage_edges` indicated `SENDS_MAIL` for `FUNCTION:DWMSG_ERMITTLENR`. While the script content suggests it's a logging function, if there's any actual email sending functionality, it will need to be re-implemented using Google Cloud services (e.g., Pub/Sub notifications, Cloud Functions with email APIs).
- **"Unknown" Complexity Tier:** The absence of complexity tier information for the main script means the effort for its migration (even as a wrapper) is not fully quantified. However, the `semi_auto` bucket suggests it's within the scope of automated or semi-automated migration.
- **Sourced Environment Initialization (`.dw_init`):** The exact contents and side effects of `$HOME/.dw_init` are unknown. Any critical environment setups (e.g., database connections, critical paths) need to be identified and replicated in the BigQuery environment through explicit configurations or parameters.
- **No `complexity_signals`:** This implies less granular insight into specific transformation patterns within the script that could assist in migration.

## 8. Build Plan

**Overall Strategy:**
The migration will proceed in stages, starting with the foundational logging and configuration structures in BigQuery, followed by the wrapper stored procedure, and finally the core business logic (after its detailed analysis).

**Phase 1: Detailed Analysis of Core Script (`k_ausd_v_ta_acc_ref.ksh`)**
- **Objective:** Understand data sources, transformation logic, and output targets of `k_ausd_v_ta_acc_ref.ksh`.
- **Language/Tool:** Manual analysis, potentially `read_source_file` and another `call_cm_mcp` for `k_ausd_v_ta_acc_ref.ksh`.

**Phase 2: BigQuery Logging & Configuration Schema Design**
- **Objective:** Create necessary BigQuery tables for logging and configuration.
- **Language/Tool:** BigQuery DDL.
    - `CREATE TABLE dataset.job_log`: To store overall job execution status (equivalent to `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`).
    - `CREATE TABLE dataset.job_error_log`: To store error details (equivalent to `DWMSG_MeldeFehler`).
    - `CREATE TABLE dataset.job_log_detail`: To store detailed messages (equivalent to `LogDatei` content).
    - `CREATE TABLE dataset.configuration`: To store environment variables or paths that were previously sourced (e.g., `BERT_DIR_ROOT` equivalent).

**Phase 3: BigQuery Stored Procedure for Wrapper (`vertragsdatenabgleich`)**
- **Objective:** Convert `r_ausd_v_ta_acc_ref.ksh` into a BigQuery Stored Procedure.
- **Language/Tool:** BigQuery SQL (using `CREATE OR REPLACE PROCEDURE`).
- **Steps:**
    1. Implement parameter handling (e.g., `-h`, `-s`, `-l`) using `IN` parameters.
    2. Define internal variables for `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate`, etc.
    3. Replace calls to `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` with `INSERT` and `UPDATE` statements into the BigQuery logging tables.
    4. Implement error handling using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks to replace shell `trap`s, updating `job_error_log` and raising `SIGNAL SQLSTATE` on errors.
    5. Define the invocation of the core logic as a `CALL` to the future `k_ausd_v_ta_acc_ref` BigQuery Stored Procedure.

**Phase 4: Migration of Core Business Logic (`k_ausd_v_ta_acc_ref.ksh`)**
- **Objective:** Migrate `k_ausd_v_ta_acc_ref.ksh` to BigQuery.
- **Language/Tool:** BigQuery SQL (for transformations and data manipulation) or Python (if complex procedural logic, external API calls, or specific libraries are required).
- **Steps:**
    1. Analyze existing SQL queries and shell commands within `k_ausd_v_ta_acc_ref.ksh`.
    2. Convert data extraction, transformation, and loading (ETL) logic to BigQuery SQL queries, potentially within a BigQuery Stored Procedure.
    3. If `k_ausd_v_ta_acc_ref.ksh` involves significant file system operations, these will need to be re-engineered to use Cloud Storage and BigQuery external tables or data loading jobs.
    4. If the logic is highly procedural or requires external integrations, a Python-based solution on Cloud Run, Cloud Functions, or orchestrated by Cloud Composer may be considered, interacting with BigQuery.

**Phase 5: Utility Functions/Scripts**
- **Objective:** Reimplement the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
- **Language/Tool:** BigQuery SQL UDFs or BigQuery Stored Procedures.
- **Steps:**
    1. Identify the specific functionalities provided by these scripts.
    2. Reimplement them as BigQuery UDFs (for simple scalar functions) or BigQuery Stored Procedures (for more complex logic).

**Ordered List of Files/Components to Generate:**
1. BigQuery DDL for `job_log`, `job_error_log`, `job_log_detail` tables. (BigQuery SQL)
2. BigQuery DDL for `configuration` table (if needed). (BigQuery SQL)
3. BigQuery SQL for utility UDFs/procedures (e.g., `f_alis_msgerr`, `h_alis_parameter`, `h_alis_date` equivalents). (BigQuery SQL)
4. BigQuery Stored Procedure `dataset.vertragsdatenabgleich` (equivalent to `r_ausd_v_ta_acc_ref.ksh`). (BigQuery SQL)
5. BigQuery Stored Procedure or Python script for `dataset.k_ausd_v_ta_acc_ref` (equivalent to `k_ausd_v_ta_acc_ref.ksh`). (BigQuery SQL or Python)