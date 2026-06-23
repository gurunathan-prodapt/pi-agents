# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope
This job is responsible for the initial provisioning of selected base products for the BERT (Basisprodukte für Ertragswert-Reporting) system. Its primary function is to extract a snapshot of contract cache data from the Data Warehouse (DWH) and make it available for the Forderungsscoring (debt scoring) application. The job uses a cutoff date (`Stichtag`) and an optional restart/resume value (`Wiederanlaufwert`) to control its processing scope, allowing for incremental or full loads.

The overall workflow is orchestrated by a KornShell (ksh) wrapper script (`r_ausd_bp_ta_msisdn.ksh`), which calls a controller ksh script (`k_ausd_bp_ta_msisdn.ksh`), which in turn executes a core Oracle SQL*Plus script (`d_ausd_bp_ta_msisdn.sql`) to perform the actual data transformations.

## 2. Source Inventory

| File Path                                                                   | Technology       | Complexity Tier | Automation Bucket | Description                                                                                                                                                                                                                                           |
| :-------------------------------------------------------------------------- | :--------------- | :-------------- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh` | KornShell Script | Medium          | Semi-Auto         | The primary wrapper script. Handles environment setup, command-line parameter parsing (`-s` for Stichtag, `-l` for Wiederanlaufwert), date derivation, error handling, and invokes `k_ausd_bp_ta_msisdn.ksh`.                               |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh` | KornShell Script | Medium (Inferred)| Semi-Auto (Inferred)| The controller script. Further parses parameters passed from the wrapper, performs date checks, and executes the core SQL script (`d_ausd_bp_ta_msisdn.sql`) using a helper function.                                                                |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql` | Oracle SQL*Plus  | Medium (Inferred)| Semi-Auto (Inferred)| The core data transformation script. Derives a date from a log table, truncates a target table (`sof$ta_msisdn`), and inserts the latest valid MSISDN records from a history table (`sof$ta_msisdn_his`).                                      |

## 3. Target Architecture
The migration target platform is Google Cloud Platform, leveraging BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) or Workflows for orchestration.

*   **Data Storage:** BigQuery datasets will host the migrated tables.
    *   Source tables: `isbert_schema.dwtk_meldungen` and `sof_ta_msisdn_his` will be ingested into BigQuery.
    *   Target table: `sof_ta_msisdn` will be created in BigQuery.
    *   Audit/Log tables: Dedicated BigQuery tables (e.g., `project.dataset.job_log`, `project.dataset.job_registry`) will replace file-based logging.
*   **Transformation Logic:** BigQuery Stored Procedures will encapsulate the business logic from the ksh scripts and the core Oracle SQL.
    *   `project.dataset.r_ausd_bp_ta_msisdn_wrapper`: Main orchestration procedure.
    *   `project.dataset.k_ausd_bp_ta_msisdn_controller`: Intermediate controller procedure.
    *   `project.dataset.d_ausd_bp_ta_msisdn_transform`: Core data transformation procedure.
*   **Orchestration:** Cloud Composer (Airflow) will schedule and manage the execution of the BigQuery Stored Procedures, providing robust error handling, monitoring, and dependency management.

## 4. Data Flow & Lineage
The job executes in a cascading fashion, starting from the wrapper script and delegating to the core SQL logic.

1.  **`r_ausd_bp_ta_msisdn.ksh` (Wrapper)**:
    *   Receives `Stichtag` (DDMMYYYY) and `Wiederanlaufwert` as command-line arguments.
    *   Initializes environment and logging.
    *   Invokes `k_ausd_bp_ta_msisdn.ksh`.
2.  **`k_ausd_bp_ta_msisdn.ksh` (Controller)**:
    *   Receives parameters from the wrapper.
    *   Performs date validity checks.
    *   Executes `d_ausd_bp_ta_msisdn.sql`, passing relevant parameters.
3.  **`d_ausd_bp_ta_msisdn.sql` (Core Transformation)**:
    *   **Reads from:**
        *   `isbert_schema.dwtk_meldungen`: To determine a `v_datum` based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   `sof$ta_msisdn_his`: The history table containing MSISDN records.
    *   **Writes to:**
        *   `sof$ta_msisdn`: The target table, which is truncated and then populated with the selected MSISDN data.
    *   **Transformation:** Selects the latest valid MSISDN records for each `bpri_com_id` from `sof$ta_msisdn_his` using an analytic `MAX() OVER (PARTITION BY...)` function. Handles `NULL` `valid_to` dates by replacing them with a far future date (`4712-12-31`).

## 5. Transformation Logic

### 5.1 `r_ausd_bp_ta_msisdn.ksh` (Wrapper Logic)
*   **Input Parameters:** `p_stichtag` (DDMMYYYY), `p_wiederanlaufWert`.
*   **Defaulting:** `p_wiederanlaufWert` defaults to `0` if not provided. `p_stichtag` defaults to the current system date if not provided.
*   **Validation:** Checks if `p_stichtag` is set. If not, an error is logged, and the script exits.
*   **Logging & Error Handling:** Uses `DWMSG` utility functions for error reporting and log file creation. `trap` statements are used for robust error handling.
*   **Core Action:** Invokes `k_ausd_bp_ta_msisdn.ksh` with parameters: `JobKennung`, `p_stichtag`, `DW_EintragsNr`, `p_wiederanlaufWert`.
*   **BigQuery Migration:** This will be translated into a BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_msisdn_wrapper`). It will handle parameter parsing, defaulting, and call `project.dataset.k_ausd_bp_ta_msisdn_controller`. Logging will be performed by inserting records into BigQuery audit tables (`project.dataset.job_log`, `project.dataset.job_registry`).

### 5.2 `k_ausd_bp_ta_msisdn.ksh` (Controller Logic)
*   **Input Parameters:** `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
*   **Utility Includes:** Sources various helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
*   **Date Check:** Calls `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
*   **Core Action:** Defines `Name_SQLskript` as `"${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql"` and executes it via the `starteSQLSkript` function, passing all relevant parameters.
*   **BigQuery Migration:** The logic will be integrated into a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_msisdn_controller`). It will manage input parameters, perform any necessary date checks (using BigQuery date functions), and then call the core transformation procedure `project.dataset.d_ausd_bp_ta_msisdn_transform`.

### 5.3 `d_ausd_bp_ta_msisdn.sql` (Core Transformation Logic)
*   **Variable Definition:** Uses `DEFINE` and `COLUMN ... NEW_VALUE` for SQL*Plus variables. `v_carmen` is defined as `"@pcrs1"`. `v_datum` is derived by querying `isbert_schema.dwtk_meldungen` for the `MAX(timecreated)` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted as `YYYYMMDD`. Defaults to `'19000101'` if no records are found.
*   **Error Handling:** `WHENEVER SQLERROR CONTINUE` and `WHENEVER SQLERROR EXIT FAILURE` for SQL*Plus error management.
*   **Truncation:** Calls a PL/SQL procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to `TRUNCATE TABLE sof$ta_msisdn REUSE STORAGE;`.
*   **Data Insertion:** Performs an `INSERT INTO sof$ta_msisdn` with the following columns: `BPR_INSTANCE_ID`, `MSISDN`, `CALLNUMBER_ROLE_ID`, `VALID_TO`.
    *   **Source:** `sof$ta_msisdn_his` aliased as `cn`.
    *   **Logic:** A subquery identifies the `max(valid_to)` for each `bpri_com_id` (partitioned by `bpri_com_id`). `NULL` `valid_to` values are treated as `4712-12-31`. The outer query then selects records where `valid_to` matches this `max_valid_to`, effectively selecting the most recent record for each `bpri_com_id`.
    *   **Column Mapping:**
        *   `cn1.bpri_com_id` -> `BPR_INSTANCE_ID`
        *   `cn1.msisdn` -> `MSISDN`
        *   `cn1.callnumber_role_id` -> `CALLNUMBER_ROLE_ID`
        *   `NVL(cn1.valid_to, TO_DATE('47121231','yyyymmdd'))` -> `VALID_TO`
*   **BigQuery Migration:** This will be converted to a BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_msisdn_transform`) containing direct BigQuery SQL statements. Oracle-specific functions (`NVL`, `TO_DATE`, `TO_CHAR`) will be replaced with their BigQuery equivalents (`COALESCE`, `DATE 'YYYY-MM-DD'`, `FORMAT_DATE`). The `TRUNCATE TABLE` will be directly translated.

## 6. External Dependencies
### Original System Dependencies:
*   **Oracle Database:** Source tables `isbert_schema.dwtk_meldungen`, `sof$ta_msisdn_his` and target table `sof$ta_msisdn`. Uses Oracle SQL*Plus for execution.
*   **KornShell (ksh) Environment:** Relies on ksh shell for script execution, environment variables (`$HOME`, `$BERT_DIR_ROOT`), and built-in commands (`getopts`, `print`, `trap`, `set`).
*   **Utility Scripts:**
    *   `. $HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date manipulation helper.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Script to get yesterday's date (indirectly used by `k_ausd_bp_ta_msisdn.ksh`).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution helper (contains `starteSQLSkript`).
*   **PL/SQL Procedures:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for DDL operations.

### BigQuery Replacement Strategy:
*   **Oracle Database:** All referenced Oracle tables will be migrated to native BigQuery tables.
*   **KornShell Environment:**
    *   Environment variables (`$HOME`, `$BERT_DIR_ROOT`) will be replaced by BigQuery Stored Procedure parameters or values retrieved from BigQuery configuration tables.
    *   Shell built-ins and control flow (`if`, `case`, `while`) will be implemented using BigQuery SQL control structures (`IF`, `CASE`, `LOOP` within procedures).
*   **Utility Scripts:**
    *   The logic within these helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `gestern.ksh`, `h_alis_sqlplus.ksh`) will be analyzed and their functionalities absorbed into the BigQuery Stored Procedures.
    *   Error logging functions (e.g., `DWMSG_MeldeFehler`) will be replaced by inserts into BigQuery audit/log tables.
    *   Date manipulation functions (e.g., `DWDate_Gib_Zeitraum`, `DWDate_Datum_Check`) will be replaced by BigQuery's native date and timestamp functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`).
*   **PL/SQL Procedures:** The call to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` will be directly translated to BigQuery's `TRUNCATE TABLE` statement within the `d_ausd_bp_ta_msisdn_transform` procedure.

## 7. Unresolved / Risks
*   **Schema Confirmation:** The precise schema (column names, data types, nullability) for `isbert_schema.dwtk_meldungen`, `sof$ta_msisdn_his`, and `sof$ta_msisdn` in the Oracle source system needs to be fully confirmed. Assumed types have been used for BigQuery pseudocode, but a detailed DDL review is critical.
*   **Full Utility Script Analysis:** The entire content and full functionality of the sourced ksh utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `gestern.ksh`, `h_alis_sqlplus.ksh`) have not been exhaustively analyzed. There might be hidden dependencies, complex logic, or external system interactions within them that need specific BigQuery/Python/GCP service translations.
*   **`starteSQLSkript` Function Logic:** The `starteSQLSkript` function within `h_alis_sqlplus.ksh` is crucial. While assumed to primarily execute the SQL, its full implementation must be reviewed to ensure no other side effects (e.g., specific connection handling, file operations, additional logging) are missed.
*   **Commented-Out Code:** The `k_ausd_bp_ta_msisdn.ksh` script contains extensive commented-out `sed`, `sort`, and `join` commands. It is critical to confirm whether this functionality is entirely obsolete or represents dormant logic that might need to be reactivated or handled in the migration.
*   **Character Encoding:** Special characters (`ü`, `ä`, `ö`) in comments and strings in the source scripts indicate a specific character encoding in the legacy environment. This needs to be considered during data ingestion and script conversion to avoid encoding issues in BigQuery.
*   **Performance of Analytic Functions:** While BigQuery supports analytic functions, `PARALLEL` hints from Oracle are not directly transferable. BigQuery's automatic parallelization will apply, but performance testing will be required.

## 8. Build Plan

This build plan outlines the ordered steps to migrate the job to BigQuery.

1.  **Define BigQuery Schemas & Datasets:**
    *   Create a dedicated BigQuery dataset (e.g., `dwh_bert_dataset`).
    *   Create target BigQuery table `dwh_bert_dataset.sof_ta_msisdn`.
    *   Create source BigQuery tables `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his`, ensuring column names, data types, and constraints match the Oracle source.
    *   Define `project.dataset.job_log` and `project.dataset.job_registry` tables for audit and logging purposes, including columns for `job_name`, `job_version`, `job_entry_nr`, `log_level`, `error_code`, `error_argument`, `log_message`, `created_at`, `status`, `finished_at`.

2.  **Migrate Data:**
    *   Ingest historical and ongoing data from `isbert_schema.dwtk_meldungen` and `sof$ta_msisdn_his` into their respective BigQuery tables (`dwh_bert_dataset.dwtk_meldungen`, `dwh_bert_dataset.sof_ta_msisdn_his`).

3.  **Create BigQuery Stored Procedure for Core Transformation (`d_ausd_bp_ta_msisdn_transform`):**
    *   **Language:** BigQuery SQL (within a Stored Procedure).
    *   **Content:**
        *   Implement the `v_datum` derivation logic using `DECLARE` and `SET` statements with `COALESCE` and `FORMAT_DATE`/`PARSE_DATE` functions.
        *   Use `TRUNCATE TABLE dwh_bert_dataset.sof_ta_msisdn;`.
        *   Translate the `INSERT INTO ... SELECT` statement from `d_ausd_bp_ta_msisdn.sql` to BigQuery SQL, converting Oracle functions (`NVL`, `TO_DATE`) to BigQuery equivalents (`COALESCE`, `DATE 'YYYY-MM-DD'`).

4.  **Create BigQuery Stored Procedure for Controller Logic (`k_ausd_bp_ta_msisdn_controller`):**
    *   **Language:** BigQuery SQL (Stored Procedure).
    *   **Content:**
        *   Define input parameters: `p_JobKennung STRING`, `p_Stichtag STRING`, `p_EintragsNr INT64`, `p_wiederanlaufWert STRING`.
        *   Implement parameter validation and date checks (e.g., using `SAFE.PARSE_DATE` and conditional logic).
        *   Call the `d_ausd_bp_ta_msisdn_transform` procedure, passing necessary arguments.
        *   Absorb relevant logic from the original ksh utility scripts (e.g., date formatting, parameter handling) into this procedure or as helper functions/UDFs if reusable.

5.  **Create BigQuery Stored Procedure for Wrapper Logic (`r_ausd_bp_ta_msisdn_wrapper`):**
    *   **Language:** BigQuery SQL (Stored Procedure).
    *   **Content:**
        *   Define input parameters: `p_stichtag_in STRING`, `p_wiederanlaufWert_in STRING`.
        *   Implement parameter defaulting logic for `p_wiederanlaufWert` and `p_stichtag`.
        *   Implement detailed logging into `dwh_bert_dataset.job_log` and `dwh_bert_dataset.job_registry` for job start, progress, and end status.
        *   Include BigQuery error handling (`BEGIN ... EXCEPTION WHEN ERROR THEN ... END`).
        *   Call `k_ausd_bp_ta_msisdn_controller` with the processed parameters.

6.  **Orchestration with Cloud Composer / Workflows:**
    *   **Language:** Python for Cloud Composer DAGs or YAML for Workflows.
    *   **Content:**
        *   Develop an Airflow DAG or Workflows definition to schedule and execute the `project.dataset.r_ausd_bp_ta_msisdn_wrapper` BigQuery Stored Procedure.
        *   Pass runtime parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`) to the BigQuery procedure.
        *   Configure Airflow operators (e.g., `BigQueryExecuteStoredProcedureOperator`) or Workflows steps.
        *   Ensure robust error handling, retry mechanisms, and monitoring are in place.