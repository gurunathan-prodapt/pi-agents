# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_apn.ksh`, serves as an orchestration wrapper for initiating the provisioning of selected base products for the BERT system. Its primary functions include parsing command-line arguments for a reference date (`Stichtag`) and a restart value (`Wiederanlaufwert`), setting up the execution environment, handling basic logging, and invoking a core processing script (`k_ausd_bp_ta_bpr_apn.ksh`) to perform the actual data preparation.

The business purpose is to generate a snapshot of contract cache data from the Data Warehouse (DWH) for use in scoring and other downstream applications. It supports a restart mechanism to reprocess specific ranges of `DWH_VERTRAG_ID` based on the `Wiederanlaufwert`.

The scope of this migration focuses on transforming this KornShell wrapper script into a BigQuery-native equivalent, primarily a BigQuery Stored Procedure, while adapting its control flow, parameter handling, and logging mechanisms to the BigQuery ecosystem. The core data processing logic, assumed to be in `k_ausd_bp_ta_bpr_apn.ksh`, will also need to be migrated, likely as a separate BigQuery Stored Procedure.

## 2. Source Inventory
The job consists of a single primary source file, which is a KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`
    *   **Technology:** KornShell
    *   **Tier:** Medium (Assumed, as `file_complexity` table returned no data for this file. It contains conditional logic, parameter parsing, and external script invocation, making it more than "simple" but not "complex" without deeper transformation logic).
    *   **Automation Bucket:** Semi-Auto (Assumed, as `automation_rate` table returned no data. The script's orchestration nature suggests semi-automated conversion is feasible with BigQuery Stored Procedures and an orchestrator like Cloud Composer.)
    *   **Summary:** Orchestrates the initial provision of selected base products for BERT, including parameter parsing, environment setup, and calling a core processing script.
    *   **Defines:** Functions (`usage`), Variables (`ProgName`, `ProgVersion`, `JobKennung`).
    *   **References:** External scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `k_ausd_bp_ta_bpr_apn.ksh`), and implicitly, via the core script, tables like `DWH.TA_C_VERTRAG` and `FOS-Tabelle`.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will leverage BigQuery's capabilities for procedural logic, data storage, and integration with cloud orchestration services.

*   **BigQuery Stored Procedures:**
    *   `project.dataset.ausd_bp_ta_bpr_apn`: This will be the main entry point, analogous to the wrapper ksh script. It will handle parameter validation, date defaulting, job auditing, and delegate to the core processing logic.
    *   `project.dataset.k_ausd_bp_ta_bpr_apn`: This will encapsulate the core data processing logic previously found in the `k_ausd_bp_ta_bpr_apn.ksh` script (placeholder provided by MCP tool). It will contain the `DELETE` and `INSERT` logic for the `fos_table`.
*   **BigQuery Tables:**
    *   `project.dataset.job_audit`: An audit table to replace the file-based logging. It will store job entry numbers, status, messages, and parameter values.
    *   `project.dataset.fos_table`: The target table for the processed "contract cache" data.
    *   `project.dataset.contract_cache`: The source table containing the DWH contract data (equivalent to `DWH.TA_C_VERTRAG` mentioned in the original script's comments).
*   **Orchestration:** Google Cloud Composer (Apache Airflow) or Google Cloud Workflows can be used to schedule and manage the execution of the BigQuery Stored Procedures, handle dependencies, and manage retries/alerts.

## 4. Data Flow & Lineage
The original script's data flow is primarily control flow and invocation, with actual data movement handled by a sub-script.

**Legacy Flow:**
1.  `r_ausd_bp_ta_bpr_apn.ksh` starts execution.
2.  Environment variables are sourced from `$HOME/.dw_init`.
3.  Helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are sourced for error handling, parameter parsing, and date utilities.
4.  Command-line parameters (`-s` for `Stichtag`, `-l` for `Wiederanlaufwert`) are parsed.
5.  If `Stichtag` is not provided, it defaults to the system date.
6.  `DWMSG_` functions (logging framework) are initialized and used to record job start/status.
7.  `k_ausd_bp_ta_bpr_apn.ksh` is invoked with parsed parameters (`-j`, `-s`, `-f`, `-l`).
8.  `k_ausd_bp_ta_bpr_apn.ksh` (assumed) reads data from `DWH.TA_C_VERTRAG` (or equivalent) and possibly `FOS-Tabelle`, processes it based on `Stichtag` and `Wiederanlaufwert`, and writes to `FOS-Tabelle`.
9.  `r_ausd_bp_ta_bpr_apn.ksh` records job completion status.

**Target BigQuery Flow:**
1.  An orchestrator (e.g., Cloud Composer) triggers the `project.dataset.ausd_bp_ta_bpr_apn` BigQuery Stored Procedure.
2.  The stored procedure receives `p_stichtag` and `p_wiederanlaufWert` as parameters.
3.  It determines the job entry number (`v_eintragsnr`) from `project.dataset.job_audit`.
4.  It calculates `v_sysdate` using BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`.
5.  It applies defaulting logic for `p_wiederanlaufWert` and `p_stichtag`.
6.  Parameter validation is performed, and an error is raised if `Stichtag` is missing, with details logged to `project.dataset.job_audit`.
7.  Job start information, including parameters, is inserted into `project.dataset.job_audit`.
8.  The stored procedure `CALL`s `project.dataset.k_ausd_bp_ta_bpr_apn`, passing the derived parameters.
9.  `project.dataset.k_ausd_bp_ta_bpr_apn` performs the core logic:
    *   If `v_restart_threshold > 0`, it `DELETE`s records from `project.dataset.fos_table` where `dwh_vertrag_id >= v_restart_threshold`.
    *   It `INSERT`s data into `project.dataset.fos_table` from `project.dataset.contract_cache`, applying date filtering (`gueltig_von`, `gueltig_bis`, `ladedatum`) and `dwh_vertrag_id` filtering based on `v_restart_threshold`.
    *   It records its completion status in `project.dataset.job_audit`.
10. Upon successful return from `k_ausd_bp_ta_bpr_apn`, the wrapper procedure records a success message in `project.dataset.job_audit`.

## 5. Transformation Logic
The transformation logic involves re-implementing shell script control flow and environment setup using BigQuery Stored Procedure constructs.

**Original (KornShell) -> Target (BigQuery SQL Stored Procedure):**

*   **Environment Setup (`. $HOME/.dw_init`, `BERT_DIR_ROOT`):** Replaced by BigQuery dataset/project configuration, stored procedure parameters, or constant declarations within the stored procedure. Sensitive environment variables should be managed via Secret Manager and injected at runtime by the orchestrator.
*   **Parameter Parsing (`getopts`):** Direct stored procedure parameters (`IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`) replace shell `getopts`.
*   **Conditional Logic (`if/then/else`, `[[ -z "$var" ]]`):** Migrates directly to BigQuery's `IF` control statements.
*   **Date Calculation (`DWDate_Gib_Zeitraum`, `v_sysdate`):** BigQuery's `CURRENT_DATE()`, `FORMAT_DATE()`, and `PARSE_DATE()` functions will be used.
*   **Logging (`DWMSG_...`, `print`, `>> $LogDatei`):** Replaced by `INSERT INTO project.dataset.job_audit` statements for structured logging.
*   **Error Handling (`set -e`, `trap`, `DWMSG_MeldeFehler`, `exit $ErrNr`):** BigQuery Stored Procedures use `RAISE` for errors. Orchestration layers (Composer) will handle retries and alerts based on stored procedure exit status or logged error messages.
*   **External Script Invocation (`${Name_Kernskript} ...`):** Replaced by `CALL project.dataset.k_ausd_bp_ta_bpr_apn(...)`.
*   **Data Processing (within `k_ausd_bp_ta_bpr_apn.ksh`):** The core `DELETE` and `INSERT` / `SELECT` logic will be translated into standard BigQuery SQL within the `project.dataset.k_ausd_bp_ta_bpr_apn` stored procedure. Column names and types will need to be mapped precisely from the source DWH tables to BigQuery tables.

## 6. External Dependencies
The original script has no direct external system dependencies other than the execution environment and other shell scripts.

*   **Original Script Dependencies:**
    *   **Sourced Utility Scripts:** `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
    *   **Core Processing Script:** `k_ausd_bp_ta_bpr_apn.ksh`.
    *   **Database (implied):** `DWH.TA_C_VERTRAG`, `FOS-Tabelle` (accessed by `k_ausd_bp_ta_bpr_apn.ksh`).

*   **Target BigQuery Replacements:**
    *   **Utility Scripts:** Their functionality (error handling, parameter defaults, date functions) will be absorbed directly into the BigQuery Stored Procedures or replaced by native BigQuery SQL functions.
    *   **Core Processing Script:** Migrated to a dedicated BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_apn`).
    *   **Database:** The Oracle DWH tables (`DWH.TA_C_VERTRAG`, `FOS-Tabelle`) will be migrated to BigQuery tables (`project.dataset.contract_cache`, `project.dataset.fos_table`). Data ingestion into `project.dataset.contract_cache` will be handled by a separate upstream migration process.

## 7. Unresolved / Risks
*   **`file_complexity` and `automation_rate` missing:** The database queries for `file_complexity` and `automation_rate` returned no rows. This means the actual complexity tier and automation bucket for this specific file are unknown. The assessment of "Medium" and "Semi-Auto" is based on general script characteristics. This could lead to underestimating the migration effort if hidden complexities exist.
*   **Content of `k_ausd_bp_ta_bpr_apn.ksh`:** The actual data transformation logic resides in the invoked script `k_ausd_bp_ta_bpr_apn.ksh`, which was not part of the `component_files` for this job. A full migration design requires analyzing this core script to accurately model its BigQuery equivalent. The provided pseudocode for `k_ausd_bp_ta_bpr_apn` is a placeholder based on general understanding.
*   **Error Codes and Messages:** The original script uses numeric error codes (`ErrNr=193`). These will need a clear mapping to BigQuery error handling or a centralized error message catalog.
*   **`DWMSG_SetzeStatusOK`:** The original script explicitly sets a "status OK". The BigQuery equivalent will implicitly succeed if no `RAISE` occurs and the procedure completes. If an explicit audit of "OK" status is required, it must be inserted into the `job_audit` table.
*   **`tee -a $LogDatei`:** Appending to a log file will be replaced by `INSERT` into the `job_audit` table. Careful consideration must be given to ensuring all relevant messages from the original script are captured.

## 8. Build Plan
The build plan will involve creating BigQuery DDL for tables and BigQuery SQL for stored procedures, followed by orchestration setup.

1.  **BigQuery Table DDL:**
    *   Create `project.dataset.job_audit` table.
    *   Create `project.dataset.contract_cache` table (schema based on `DWH.TA_C_VERTRAG`).
    *   Create `project.dataset.fos_table` table (schema based on `FOS-Tabelle`).
    *   **Language:** BigQuery DDL (SQL)

2.  **BigQuery Stored Procedure `k_ausd_bp_ta_bpr_apn`:**
    *   Translate the core processing logic from `k_ausd_bp_ta_bpr_apn.ksh` into BigQuery SQL, incorporating `DELETE` and `INSERT` statements with appropriate filtering.
    *   **Language:** BigQuery SQL

3.  **BigQuery Stored Procedure `ausd_bp_ta_bpr_apn`:**
    *   Implement the wrapper logic from `r_ausd_bp_ta_bpr_apn.ksh` as a BigQuery Stored Procedure.
    *   Include parameter parsing, defaulting logic, validation, and auditing.
    *   Add a `CALL` statement to `project.dataset.k_ausd_bp_ta_bpr_apn`.
    *   **Language:** BigQuery SQL

4.  **Orchestration (Cloud Composer/Workflows):**
    *   Develop a Cloud Composer DAG or Cloud Workflow definition to schedule and execute the `project.dataset.ausd_bp_ta_bpr_apn` stored procedure.
    *   Configure parameters, error handling, retries, and alerts.
    *   **Language:** Python (for Composer DAGs) or YAML/JSON (for Workflows).