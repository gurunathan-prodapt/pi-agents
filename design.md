# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh

## 1. Purpose & Scope
This document outlines the migration design for the `k_ausd_bp_ta_apn_carmen.ksh` KornShell script to Google BigQuery. The original script acts as a control and orchestration layer for a core SQL script (`d_ausd_bp_ta_apn_carmen.sql`). Its primary purpose is to parse and validate input parameters (job identifier, entry number, key date, restart value), perform date validation, determine "today" and "yesterday" dates, execute the main SQL script, capture the number of processed records, and prepare for job logging. The scope of this migration is to re-implement this orchestration logic and its underlying data processing in BigQuery.

## 2. Source Inventory
The job consists of a single primary source file, `k_ausd_bp_ta_apn_carmen.ksh`.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh`
    *   **Technology:** Shell (KornShell)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2)
    *   **Summary:** This ksh script acts as a control script, parsing parameters, validating dates, and orchestrating the execution of a core SQL script to process data for the 'PoolBasisprodukt' table.
    *   **Dependencies (inferred from code):**
        *   `$HOME/.dw_init` (environment setup)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling utility)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation utility)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing utility)
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus execution utility)
        *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (script to determine yesterday's and today's dates)
        *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_apn_carmen.sql` (core SQL script for data processing)
        *   Temporary file: `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_carmen.tmp` (for record count)
        *   Database table: `PoolBasisprodukt` (referenced as `v_TabName`)

## 3. Target Architecture
The migration will leverage BigQuery Stored Procedures for the orchestration logic and BigQuery SQL for the core data processing.

*   **Main Orchestration:** A BigQuery Stored Procedure, tentatively named `project.dataset.r_ausd_bp_ta_apn_carmen`, will encapsulate the parameter parsing, validation, date calculations, and execution flow.
*   **Core Data Logic:** The SQL logic from `d_ausd_bp_ta_apn_carmen.sql` will be migrated into a separate BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_bp_ta_apn_carmen`, which will be called by the main orchestration procedure.
*   **Parameter Handling:** Input parameters will be defined as `IN` arguments to the main BigQuery Stored Procedure.
*   **Error Handling:** Shell error handling (`DWMSG_MeldeFehler`) will be replaced with BigQuery's `RAISE USING MESSAGE` statements.
*   **Date Utilities:** Built-in BigQuery functions like `CURRENT_DATE()` and `DATE_SUB()` will replace `gestern.ksh` and `h_alis_date.ksh` functionality.
*   **Temporary Data:** The temporary file for record counting will be replaced by BigQuery variables or by querying the target table directly.
*   **Job Logging:** The commented-out `FOSJobErzeugeEintrag` logic will be migrated to an `INSERT` statement into a dedicated BigQuery job log table (e.g., `project.dataset.job_log_table`).
*   **Tables:** Any source tables used by `d_ausd_bp_ta_apn_carmen.sql` (if not already in BigQuery) will be migrated to BigQuery tables. The `PoolBasisprodukt` table, if actively used for data, will also be migrated.

## 4. Data Flow & Lineage
The original shell script orchestrates the following flow:
1.  **Environment Setup & Utilities:** Sources various utility scripts.
2.  **Parameter Parsing:** Reads `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` using `getopts`.
3.  **Parameter Validation:** Checks if required parameters are set using `pruefeParameterGesetzt`.
4.  **Date Validation:** Validates `p_Stichtag` format using `DWDate_Datum_Check`.
5.  **Date Calculation:** Executes `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
6.  **SQL Script Execution:** Calls `starteSQLSkript` from `h_alis_sqlplus.ksh` to execute `d_ausd_bp_ta_apn_carmen.sql` with various parameters. This script is assumed to perform data processing and potentially write a record count to `tmpFile`.
7.  **Record Count:** Reads the record count from `tmpFile`.
8.  **Job Logging (Commented):** Prepares for logging an entry in a job table.

In BigQuery, this flow will be implemented as follows:
1.  The main stored procedure `r_ausd_bp_ta_apn_carmen` will receive input parameters.
2.  Input validation will be performed using `IF` conditions and `RAISE USING MESSAGE` for errors.
3.  Date calculations for "today" and "yesterday" will use `CURRENT_DATE()` and `DATE_SUB()`.
4.  The core data processing, previously in `d_ausd_bp_ta_apn_carmen.sql`, will be encapsulated in `d_ausd_bp_ta_apn_carmen` stored procedure and invoked with `CALL`.
5.  The record count will be obtained either as an `OUT` parameter from the `d_ausd_bp_ta_apn_carmen` procedure or by querying the target table after the data processing.
6.  Job logging will be implemented as an `INSERT` statement into a BigQuery log table.

## 5. Transformation Logic

**Parameter Handling:**
*   **Legacy:** `getopts` for `j:f:s:l:` (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Target:** BigQuery Stored Procedure parameters (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`, `IN p_Stichtag STRING`, `IN p_wiederanlaufWert STRING`).

**Parameter Validation:**
*   **Legacy:** `pruefeParameterGesetzt` function calls and `if [ ! $ErrNr -eq 0 ]`.
*   **Target:** `IF p_JobKennung IS NULL OR p_JobKennung = '' THEN RAISE USING MESSAGE = 'FEHLER: Missing parameter Jobkennung'; END IF;` (similar for other parameters).

**Date Validation:**
*   **Legacy:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
*   **Target:** `IF NOT REGEXP_CONTAINS(p_Stichtag, r'^[0-9]{8}$')$ THEN RAISE USING MESSAGE = 'FEHLER: Invalid date format for Stichtag'; END IF;` followed by `SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);`.

**Restart Value Initialization:**
*   **Legacy:** `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi`.
*   **Target:** `IF p_wiederanlaufWert_local IS NULL OR p_wiederanlaufWert_local = '' THEN SET p_wiederanlaufWert_local = '0'; END IF;`.

**Date Determination (`gestern.ksh`):**
*   **Legacy:** `set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` ` to get `p_datum_heute` and `p_datum_gestern`.
*   **Target:** `SET p_datum_heute = CURRENT_DATE(); SET p_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.

**SQL Script Execution (`d_ausd_bp_ta_apn_carmen.sql`):**
*   **Legacy:** `starteSQLSkript ... $Name_SQLskript ...`.
*   **Target:** `CALL project.dataset.d_ausd_bp_ta_apn_carmen(...);` where `d_ausd_bp_ta_apn_carmen` is a separate BigQuery Stored Procedure.

**Record Count from Temporary File:**
*   **Legacy:** `eval "v_records=`cat $tmpFile`"`.
*   **Target:** `SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table_or_staging` WHERE ...);`

**Job Logging (`FOSJobErzeugeEintrag` - commented):**
*   **Legacy:** `FOSJobErzeugeEintrag $v_TabName 'A' 'I' $p_Stichtag ...`.
*   **Target:** `INSERT INTO `project.dataset.job_log_table` (...) VALUES (...);`.

## 6. External Dependencies
The original script has several external dependencies, mainly other shell scripts and a core SQL script.

*   **Utility Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These shell scripts provide environment setup, error handling, date validation, parameter parsing, and SQL*Plus invocation. In BigQuery, their functionality will be absorbed directly into the main stored procedure using BigQuery SQL features (parameters, `RAISE`, `REGEXP_CONTAINS`, `PARSE_DATE`, `CURRENT_DATE`, `DATE_SUB`, etc.).
*   **Date Script (`gestern.ksh`):** This script calculates "yesterday" and "today." This will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` in BigQuery.
*   **Core SQL Script (`d_ausd_bp_ta_apn_carmen.sql`):** This is the most significant dependency. Its content will be migrated into a dedicated BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_apn_carmen`) that will be called by the main orchestration procedure.
*   **`PoolBasisprodukt` Table:** This table is referenced by `v_TabName`. Its role needs to be confirmed; if it's a source/target for data, it will be migrated to a BigQuery table. If it's purely for metadata/job management, a corresponding BigQuery table will be created.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_carmen.tmp`):** This file is used to store the record count. In BigQuery, this will be handled by assigning the count to a variable within the stored procedure or by querying a staging/target table directly.

## 7. Unresolved / Risks
*   **Custom Logic in `gestern.ksh`:** While the common case of `gestern.ksh` is to return `CURRENT_DATE()` and `DATE_SUB()`, if `gestern.ksh` contains complex, custom calendar logic (e.g., handling holidays, fiscal periods), this logic will need a dedicated analysis and careful re-implementation in BigQuery SQL or a UDF.
*   **Complexity of `d_ausd_bp_ta_apn_carmen.sql`:** The actual complexity of the core SQL script is unknown without its content. The migration effort for this job will heavily depend on the complexity and size of `d_ausd_bp_ta_apn_carmen.sql`. This is considered the primary risk.
*   **Environment Variables:** The script relies on `$HOME`, `$BERT_DIR_ROOT`, and `$DW_DIR_UTL`. These environment variables will need to be replaced with BigQuery project/dataset names or configuration parameters passed to the stored procedure.
*   **Commented Code:** The `sed/sort/join` block at the end of the script is commented out. It will be ignored for the initial migration. If this functionality is ever reactivated, it would require a separate migration to BigQuery table transformations.
*   **SQL*Plus Specific Features:** If `d_ausd_bp_ta_apn_carmen.sql` or the `starteSQLSkript` helper utilizes highly specific SQL*Plus features or database-specific functions not directly transferable to BigQuery SQL, these will require custom conversion or alternative BigQuery patterns.

## 8. Build Plan
The migration will result in the creation of the following BigQuery artifacts:

1.  **DDL for `job_log_table` (if not existing):**
    *   `CREATE TABLE project.dataset.job_log_table (...)`
2.  **DDL for `PoolBasisprodukt` (if migrating data table):**
    *   `CREATE TABLE project.dataset.PoolBasisprodukt (...)`
3.  **BigQuery Stored Procedure for `d_ausd_bp_ta_apn_carmen.sql`:**
    *   `CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_bp_ta_apn_carmen(...)`
    *   **Language:** BigQuery SQL
4.  **BigQuery Stored Procedure for `k_ausd_bp_ta_apn_carmen.ksh` orchestration:**
    *   `CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_apn_carmen(...)`
    *   **Language:** BigQuery SQL
5.  **Orchestration Configuration:**
    *   YAML/JSON configuration files for external orchestration (e.g., Cloud Composer/Workflows) to call `project.dataset.r_ausd_bp_ta_apn_carmen` with required parameters.
    *   **Language:** YAML/JSON