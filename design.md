# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This KornShell (KSH) script, `r_ausd_bp_ta_cntrct_dist.ksh`, acts as an orchestration and wrapper script for preparing selected base products for BERT (a downstream system, likely for analysis or reporting). Its primary purpose is to:
*   Parse and validate input parameters, specifically a cutoff date (`Stichtag`) and an optional restart value.
*   Set up the execution environment, including sourcing helper scripts for error handling, parameter parsing, and date manipulation.
*   Orchestrate the execution of a core business logic script, `k_ausd_bp_ta_cntrct_dist.ksh`, passing the resolved parameters.
*   Implement robust error handling and logging mechanisms, including traps for script interruption and error reporting.
*   Report the overall job status.

The script extracts contract cache data from the Data Warehouse (DWH) and makes it available for "Forderungsscoring" (FOS), implying that DWH is the source and FOS is a conceptual target for the processed data.

## 2. Source Inventory
| File Name                                                                     | Technology | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                              |
| :---------------------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh` | KornShell  | medium          | semi_auto         | This KSH script prepares selected base products for BERT by extracting contract cache data from DWH. It handles date parameter parsing, error logging, and orchestrates a core processing script. |

## 3. Target Architecture
The migration target is Google Cloud BigQuery. The existing KornShell wrapper script will be refactored into a BigQuery Stored Procedure, which will handle parameter management, logging, and orchestration of the core logic. The core business logic residing in `k_ausd_bp_ta_cntrct_dist.ksh` (which was not analyzed as part of this job) will need to be separately migrated, likely also into a BigQuery Stored Procedure or a series of SQL statements/views.

**BigQuery Components:**
*   **BigQuery Stored Procedure:** `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` (replacing the `r_ausd_bp_ta_cntrct_dist.ksh` wrapper script).
*   **BigQuery Tables (for logging and control):**
    *   `project.dataset.job_control`: To track job execution status, parameters, and timestamps.
    *   `project.dataset.job_audit_log`: For detailed operational logs and messages.
    *   `project.dataset.job_error_log`: To record specific errors encountered during execution.
*   **BigQuery Stored Procedure (for core logic):** `project.dataset.ausd_bp_ta_cntrct_dist_core` (this will contain the logic currently in `k_ausd_bp_ta_cntrct_dist.ksh`, which needs separate design).
*   **Orchestration:** Cloud Composer, Cloud Workflows, or Cloud Scheduler for scheduling and triggering the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_cntrct_dist.ksh` script itself is an orchestrator and does not directly read or write data from/to databases in the provided code snippet.
*   **Input:** Command-line parameters: `Stichtag` (cutoff date) and `Wiederanlaufwert` (restart value).
*   **Internal Flow:**
    1.  Initialization and environment setup (`. $HOME/.dw_init`).
    2.  Parameter parsing (`getopts` and helper scripts like `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    3.  Date calculation and validation (`DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`).
    4.  Error handling setup (`f_alis_msgerr.ksh`, `trap`).
    5.  Invocation of core script: `${Name_Kernskript}` (`k_ausd_bp_ta_cntrct_dist.ksh`) with processed parameters.
    6.  Logging of job status and messages (`DWMSG_*` functions).
*   **Output:**
    *   Log file (flat file in legacy system, to be replaced by BigQuery `job_audit_log` table).
    *   Job status update (implicit in legacy, explicit in BigQuery `job_control` table).
    *   The core script, `k_ausd_bp_ta_cntrct_dist.ksh`, is responsible for extracting data from DWH and providing it to "Forderungsscoring" (FOS).

**Target Data Flow:**
1.  Cloud Composer/Workflows/Scheduler triggers `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`.
2.  The `ausd_bp_ta_cntrct_dist_wrapper` stored procedure parses parameters, performs validation, and logs its activities to `project.dataset.job_control`, `project.dataset.job_audit_log`, and `project.dataset.job_error_log`.
3.  `ausd_bp_ta_cntrct_dist_wrapper` then calls `project.dataset.ausd_bp_ta_cntrct_dist_core` (the migrated core logic).
4.  `ausd_bp_ta_cntrct_dist_core` will perform data extraction from relevant BigQuery source tables (migrated DWH tables) and transformation, then write to BigQuery target tables (for FOS).

## 5. Transformation Logic
The `r_ausd_bp_ta_cntrct_dist.ksh` script primarily handles orchestration and parameter processing.

**Legacy Logic (in `r_ausd_bp_ta_cntrct_dist.ksh`):**
*   **Parameter Defaulting:**
    *   `p_wiederanlaufWert` defaults to `0` if not provided.
    *   `p_stichtag` defaults to `v_sysdate` (current system date) if not provided.
*   **Date Formatting:** Dates are handled in `DDMMYYYY` format.
*   **Validation:** Checks if `p_stichtag` is set.
*   **Environment Sourcing:** `. $HOME/.dw_init`, along with several helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
*   **Error Handling:** Uses `set -e` and `trap` commands to catch errors and invoke `DWMSG_Fehlerbehandlung`.
*   **Job Metadata:** Sets `ProgName`, `ProgVersion`, `JobKennung`, `LogDatei`, `DW_EintragsNr` for logging.

**Target Transformation (BigQuery Stored Procedure `ausd_bp_ta_cntrct_dist_wrapper`):**
*   **Parameter Handling:** Input parameters `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) for the stored procedure.
*   **Defaulting Logic:**
    *   `v_restart_value` will be `IFNULL(p_wiederanlaufWert, 0)`.
    *   `v_sysdate` will use `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    *   `v_effective_stichtag` will be `IFNULL(NULLIF(p_stichtag, ''), v_sysdate)`.
*   **Validation:** An `IF` statement checks for `NULL` or empty `v_effective_stichtag`. If invalid, an error is logged to `job_error_log`, and the procedure exits.
*   **Logging:** All logging activities (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler`) will be replaced with `INSERT` statements into `job_control`, `job_audit_log`, and `job_error_log` tables.
*   **Orchestration:** The invocation of `k_ausd_bp_ta_cntrct_dist.ksh` will be replaced by a `CALL` statement to the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_dist_core`.

The actual data transformations (selection, filtering, joining of DWH contract cache data) are presumed to be within `k_ausd_bp_ta_cntrct_dist.ksh` and will be part of the `ausd_bp_ta_cntrct_dist_core` BigQuery Stored Procedure. The `usage` description hints at filtering based on `Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`, and conditional deletion based on `DWH_VERTRAG_ID > Wiederanlaufwert`.

## 6. External Dependencies
The current script references several local shell scripts and environment variables.

| Dependency           | Type      | Current Usage                                              | Replacement in Target Platform                                                                 |
| :------------------- | :-------- | :--------------------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| `$HOME/.dw_init`     | File      | Environment initialization.                                | BigQuery Stored Procedure does not require this; environment variables managed by orchestrator. |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` | File      | Error handling framework.                                  | Replaced by `INSERT` statements into `job_error_log` table within BigQuery Stored Procedure. |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` | File      | Helper for parameter parsing.                              | Replaced by BigQuery Stored Procedure input parameters and conditional logic.                |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` | File      | Helper for date manipulation.                              | Replaced by BigQuery date functions (`CURRENT_DATE()`, `FORMAT_DATE`).                     |
| `k_ausd_bp_ta_cntrct_dist.ksh` | File      | Core business logic script invoked by this wrapper.        | Migrated to BigQuery Stored Procedure: `project.dataset.ausd_bp_ta_cntrct_dist_core`.        |
| `DWDate_Gib_Zeitraum` | Function  | Retrieves date information.                                | Replaced by BigQuery date functions.                                                           |
| `pruefeParameterGesetzt` | Function  | Validates if parameters are set.                           | Replaced by `IF ... THEN SIGNAL` procedural checks in BigQuery Stored Procedure.             |
| `DWMSG_*` functions  | Functions | Logging and error messaging (e.g., `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`). | Replaced by `INSERT` statements into `job_control`, `job_audit_log`, and `job_error_log` tables. |
| DWH Contract Cache   | Database  | Source of contract cache data.                             | Migrated to BigQuery tables.                                                                   |
| FOS (Forderungsscoring) | Downstream System | Target for processed data.                                 | BigQuery target tables for FOS.                                                                |

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_bp_ta_cntrct_dist.ksh`) Logic:** The content and detailed logic of `k_ausd_bp_ta_cntrct_dist.ksh` were not provided. The migration of this core script is critical and will require a separate, detailed design and implementation into `project.dataset.ausd_bp_ta_cntrct_dist_core`. This is currently a significant unknown.
*   **Data Sources in DWH:** The exact tables and schemas within the "DWH Contract Cache" are not known. These need to be identified and mapped to BigQuery tables.
*   **"FOS-Tabelle" specifics:** The structure and exact destination of the "FOS-Tabelle" for "Forderungsscoring" are not specified. This will need to be defined as BigQuery target tables.
*   **`trap` Signal Handling:** The `trap` command in KornShell is used for OS-level signal handling. This functionality is not directly replicable in BigQuery SQL. Orchestration tools (like Cloud Composer) will need to handle job cancellation, retries, and error notifications at a higher level.
*   **`tee -a $LogDatei`:** Appending to a log file will be replaced by inserts into BigQuery log tables.
*   **`DW_EintragsNr` generation:** The current script generates an entry number. In BigQuery, this will be handled by auto-incrementing keys or sequence emulation for job control tables.

## 8. Build Plan
1.  **Define BigQuery Schema for Logging and Control:**
    *   Create `project.dataset.job_control` table (e.g., `job_nr` (INT64, PK), `job_kennung` (STRING), `source_program` (STRING), `stichtag` (STRING), `sysdate` (STRING), `restart_value` (INT64), `created_at` (TIMESTAMP), `finished_at` (TIMESTAMP), `status` (STRING)).
    *   Create `project.dataset.job_audit_log` table (e.g., `log_id` (INT64, PK), `job_nr` (INT64), `job_kennung` (STRING), `log_file_name` (STRING), `stichtag` (STRING), `sysdate` (STRING), `message` (STRING), `created_at` (TIMESTAMP)).
    *   Create `project.dataset.job_error_log` table (e.g., `error_id` (INT64, PK), `job_kennung` (STRING), `err_nr` (INT64), `err_arg` (STRING), `created_at` (TIMESTAMP), `message` (STRING)).

2.  **Migrate Core Business Logic (`k_ausd_bp_ta_cntrct_dist.ksh`):**
    *   **Phase:** Design and build
    *   **Output:** BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_dist_core`.
    *   **Language:** BigQuery SQL.
    *   **Note:** This is a prerequisite step, and its design is outside the scope of this document but crucial for the overall migration.

3.  **Develop BigQuery Stored Procedure for Wrapper Logic:**
    *   **Phase:** Build
    *   **Output:** BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`.
    *   **Language:** BigQuery SQL.
    *   **Steps:**
        *   Implement parameter parsing and defaulting logic as described in Section 5.
        *   Integrate logging to `job_control`, `job_audit_log`, and `job_error_log` tables.
        *   Replace shell script invocation with `CALL project.dataset.ausd_bp_ta_cntrct_dist_core(...)`.
        *   Implement error handling using `IF` statements and appropriate error logging.

4.  **Develop Orchestration Mechanism:**
    *   **Phase:** Build
    *   **Output:** Cloud Composer DAG, Cloud Workflow definition, or Cloud Scheduler job.
    *   **Language:** Python (for Composer), YAML/JSON (for Workflows), or Console configuration.
    *   **Steps:**
        *   Configure the chosen orchestrator to trigger `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` with the necessary parameters.
        *   Set up monitoring and alerting for job success/failure.

5.  **Data Migration:**
    *   **Phase:** Execute
    *   Migrate the DWH Contract Cache data to BigQuery tables.
    *   Identify and migrate target FOS tables to BigQuery.

6.  **Testing:**
    *   Develop unit tests for BigQuery Stored Procedures.
    *   Develop integration tests for the full workflow (orchestrator -> wrapper SP -> core SP -> target tables).
    *   Validate data consistency between legacy and target systems.