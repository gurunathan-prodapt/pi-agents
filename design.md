# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_bcp_msisdn.ksh` to Google BigQuery. The script serves as a control script for data preparation within the `isbert` environment. Its primary responsibilities include environment setup, parameter parsing and validation, date validation, orchestration of an underlying SQL script for data processing (specifically for `PoolBasisprodukt`), and handling post-processing bookkeeping such as recording job status and processed record counts. The current implementation interacts with a job management system (though commented out) and relies on several utility shell scripts. The scope of this migration is to re-implement the orchestration logic and the underlying SQL processing in a BigQuery-native environment, aiming for `semi_auto` migration.

## 2. Source Inventory
The job is primarily composed of one KornShell script and several utility scripts it sources or executes.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`
    *   **Technology:** Shell (KornShell)
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Orchestrates data preparation, handling parameter parsing, date validation, error handling, and execution of an SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`). It also interacts with job management (commented out).

**Key Dependencies identified from script content:**
*   **Environment Initialization:** `$HOME/.dw_init`
*   **Error Handling:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
*   **Date Check:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
*   **Parameter Parsing:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
*   **SQL*Plus Wrapper:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
*   **Date Calculation:** `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
*   **Core Data Processing SQL:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql`
*   **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`

## 3. Target Architecture
The target architecture will leverage Google BigQuery's capabilities for data storage and processing, with BigQuery Stored Procedures for orchestration and parameter handling.

*   **Orchestration:** BigQuery Stored Procedure(s) will encapsulate the control logic currently in the KornShell script.
*   **Data Processing:** The core SQL logic from `d_ausd_bp_ta_bcp_msisdn.sql` will be migrated to BigQuery SQL, potentially as part of the stored procedure or as separate BigQuery SQL scripts executed by the stored procedure.
*   **Parameter Management:** BigQuery scripting variables will handle input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Job Control & Logging:** A dedicated BigQuery table (e.g., `dataset.job_control_table`) will replace the temporary file for record counting and the commented-out FOS job management system for status updates and logging.
*   **Utility Functions:** Shell utility functions (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh`) will be re-implemented using BigQuery's native functions, UDFs (User-Defined Functions), or incorporated directly into the stored procedure logic.
*   **External Orchestration (Optional):** For scheduling and external triggering, Cloud Composer (Airflow), Cloud Workflows, or Cloud Run could be employed, especially if the job becomes part of a larger data pipeline.

## 4. Data Flow & Lineage
The original shell script orchestrates the following flow:

1.  **Environment Initialization:** Sources `$HOME/.dw_init` and several utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Acquisition:** Parses command-line arguments using `getopts` for `Jobkennung (-j)`, `EintragsNr (-f)`, `Stichtag (-s)`, and `wiederanlaufWert (-l)`.
3.  **Parameter Validation:** Checks if required parameters (`Jobkennung`, `Stichtag`, `EintragsNr`) are set using `pruefeParameterGesetzt`.
4.  **Date Validation:** Validates the format of `p_Stichtag` using `DWDate_Datum_Check` (expects `DDMMYYYY`).
5.  **Date Calculation:** Executes `gestern.ksh` to derive `p_datum_heute` and `p_datum_gestern`.
6.  **SQL Execution:** Invokes `starteSQLSkript` with various parameters, including `d_ausd_bp_ta_bcp_msisdn.sql`, which is the main data processing component.
7.  **Record Count:** Reads the number of processed records from `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`.
8.  **Job Logging (Intended):** Attempts to create an entry in a job table using `FOSJobErzeugeEintrag` (currently commented out).

**Migrated BigQuery Data Flow:**
1.  **Stored Procedure Invocation:** The BigQuery Stored Procedure `dataset.r_ausd_bp_ta_bcp_msisdn` is called with input parameters.
2.  **Parameter Validation:** `IF/RAISE` statements within the stored procedure validate required parameters.
3.  **Date Validation:** `PARSE_DATE` is used to validate and convert `p_Stichtag` to a `DATE` type.
4.  **Date Calculation:** `CURRENT_DATE()` and `DATE_SUB` derive today's and yesterday's dates.
5.  **Core Data Processing:** The migrated BigQuery SQL logic (from `d_ausd_bp_ta_bcp_msisdn.sql`) is executed within the stored procedure, performing transformations and inserts into target tables.
6.  **Record Count:** A `SELECT COUNT(*)` on the target table (or the result of the data processing) populates a variable for record count.
7.  **Job Logging:** An `INSERT` statement populates the `dataset.job_control_table` with job metadata and the record count.

## 5. Transformation Logic
The transformation logic primarily resides within the `d_ausd_bp_ta_bcp_msisdn.sql` script, which is executed by the shell wrapper. The shell script itself contains orchestration logic.

**Shell Script Logic Mapping to BigQuery:**

*   **Environment Sourcing (`. $HOME/.dw_init`, etc.):** This is a shell-specific concept and will not have a direct equivalent. Necessary environment variables will need to be passed as stored procedure parameters, set as session variables (if applicable), or hardcoded/derived within the stored procedure.
*   **Parameter Parsing (`getopts`):** Replaced by input parameters to the BigQuery Stored Procedure.
*   **Parameter Validation (`pruefeParameterGesetzt`):** Replaced by `IF ... THEN RAISE` statements in the BigQuery Stored Procedure.
*   **Date Validation (`DWDate_Datum_Check`):** Replaced by `PARSE_DATE('%d%m%Y', p_Stichtag)` and an `IF IS NULL THEN RAISE` check.
*   **SQL Script Execution (`starteSQLSkript`):** The content of `d_ausd_bp_ta_bcp_msisdn.sql` will be directly incorporated into the BigQuery Stored Procedure or called as a separate BigQuery SQL script. The `h_alis_sqlplus.ksh` wrapper's functionality will be superseded by BigQuery's native SQL execution.
*   **Temporary File (`tmpFile`) for Record Count:** Replaced by `SELECT COUNT(*)` into a BigQuery scripting variable after the data processing.
*   **Date Derivation (`gestern.ksh`):** Replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB` functions.
*   **Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag` - commented):** If activated, these would be replaced by `INSERT` or `UPDATE` statements against a BigQuery job control table.

**Core Data Transformation (`d_ausd_bp_ta_bcp_msisdn.sql`):**
This SQL script needs to be analyzed separately and migrated to BigQuery SQL. The assumption is that it performs data selection, manipulation, and insertion into a target table related to `PoolBasisprodukt`. This migration will involve:
*   Converting Oracle/legacy SQL syntax to BigQuery Standard SQL.
*   Mapping source tables and columns to their BigQuery equivalents.
*   Replacing any proprietary functions with BigQuery functions or UDFs.
*   Optimizing for BigQuery performance.

## 6. External Dependencies
The original `lineage_assembled_jobs` record indicates no formal `external_systems` linked to this job in the lineage graph. The script itself also does not make direct external API calls or network calls.

**Internal "External" Dependencies (Legacy Environment):**
*   **Legacy Filesystem:** The script relies heavily on sourcing other shell scripts from specific paths (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`). In BigQuery, these dependencies will be internalized (functions re-implemented) or passed as parameters.
*   **SQL Database:** The `d_ausd_bp_ta_bcp_msisdn.sql` script interacts with a database, likely Oracle based on the context of SQL*Plus wrappers. This Oracle database will be replaced by BigQuery datasets and tables.
*   **Job Management System (FOS):** The commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` suggest an interaction with a legacy job management system. This will be replaced by a BigQuery job control table if these functionalities are needed in the target environment.

## 7. Unresolved / Risks
*   **Unresolved Targets:** The `lineage_assembled_jobs` record indicates no `unresolved_targets`.
*   **Commented-out Code:** The script contains significant commented-out sections (e.g., FOS job management, `sed`/`sort`/`join` for post-processing files). It's crucial to confirm whether these functionalities are still required or can be safely discarded. If required, they will need re-implementation in BigQuery SQL or Python (e.g., using Dataflow/Dataproc if complex file manipulation is needed).
*   **`eval`-based Command Substitution:** The `eval "v_records=\`cat $tmpFile\`"` construct is highly shell-specific and not directly transferable. This will be replaced by BigQuery's direct record counting mechanism.
*   **Environment Variable Resolution:** The dynamic resolution of paths like `${BERT_DIR_ROOT}` and `${DW_DIR_UTL}` needs to be translated into explicit dataset/table references or configurable parameters in BigQuery.
*   **SQL Script Migration:** The actual data transformation logic within `d_ausd_bp_ta_bcp_msisdn.sql` is assumed to be migratable to BigQuery SQL. Any complex, proprietary SQL features in that script will be a risk point and require detailed analysis.
*   **Error Semantics:** The exact error handling (`DWMSG_MeldeFehler`, `exit $ErrNr`) may need careful mapping to BigQuery's `RAISE` statements and logging mechanisms to maintain equivalent behavior.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Data Model Migration:**
    *   Migrate all source tables referenced by `d_ausd_bp_ta_bcp_msisdn.sql` to BigQuery. This includes creating DDLs for BigQuery tables.
    *   Design and create DDL for a new `job_control_table` in BigQuery to replace the functionality of `FOSJobErzeugeEintrag` and the temporary record count.
    *   **Language:** BigQuery DDL

2.  **SQL Logic Migration:**
    *   Translate the SQL code from `d_ausd_bp_ta_bcp_msisdn.sql` into BigQuery Standard SQL.
    *   This will involve syntax conversion, function mapping, and performance optimization.
    *   **Language:** BigQuery SQL

3.  **Orchestration Logic Implementation:**
    *   Create a BigQuery Stored Procedure, `dataset.r_ausd_bp_ta_bcp_msisdn`, to encapsulate the parameter handling, validation, date calculations, and execution of the migrated SQL logic.
    *   Implement the parameter validation and date validation logic directly within the stored procedure.
    *   Integrate the migrated `d_ausd_bp_ta_bcp_msisdn.sql` logic into this stored procedure.
    *   Implement the record counting and job logging to the new `job_control_table`.
    *   **Language:** BigQuery Stored Procedure Language (SQL)

4.  **Testing:**
    *   Develop unit and integration tests for the BigQuery Stored Procedure and migrated SQL.
    *   Compare results with the legacy system where possible.

5.  **Deployment & Scheduling:**
    *   Deploy the BigQuery DDLs and Stored Procedures.
    *   Configure a scheduler (e.g., Cloud Composer, native BigQuery scheduling, or external orchestrator) to invoke the BigQuery Stored Procedure with the necessary parameters.

This migration follows a `semi_auto` approach, as the shell script orchestration requires significant re-design into BigQuery Stored Procedures and the underlying SQL also needs explicit conversion.