# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope

This document outlines the migration design for the KornShell script `k_ausd_v_ta_cntrct_valid.ksh` to Google BigQuery.

**Purpose:** This ksh script acts as a control script. Its main responsibilities include:
*   Ignoring active jobs to prevent concurrent execution.
*   Invoking an SQL script (`d_ausd_v_ta_cntrct_valid.sql`) to perform data processing.
*   Registering the job execution in a job table.
*   Deactivating older active jobs.
The script handles parameter parsing and validation, environment setup, and orchestrates the execution of an SQL script to process data related to `ta_cntrct_valid`.

**Scope:** The migration encompasses the logic of the KornShell script, its parameter handling, environmental dependencies, the invocation of the SQL script, and the data interactions performed by the SQL script. The target platform is Google BigQuery for data processing and potentially Cloud Composer or Workflows for orchestration.

## 2. Source Inventory

The primary source file for this job is `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`.

*   **File:** `k_ausd_v_ta_cntrct_valid.ksh`
    *   **Technology:** KornShell Script
    *   **Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** This ksh script acts as a control script for 'r_ausd_vertrag.ksh', handling parameter parsing, environment setup, and orchestrating the execution of an SQL script to process data related to 'ta_cntrct_valid'.

**Associated Files/Objects:**
*   **SQL Script (invoked by ksh):** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql`
*   **Utility KornShell Scripts (sourced by ksh):**
    *   `.dw_init` (environment initialization)
    *   `f_alis_msgerr.ksh` (error handling)
    *   `h_alis_date.ksh` (date utilities)
    *   `h_alis_parameter.ksh` (parameter parsing/validation)
    *   `h_alis_sqlplus.ksh` (SQL execution wrapper)

## 3. Target Architecture

The target architecture will leverage Google BigQuery for data processing and storage.

*   **Core Logic:** The logic currently residing in the KornShell script (parameter validation, job control, SQL script invocation) will be migrated into a BigQuery Stored Procedure.
*   **Data Storage:** All tables currently read from or written to will be migrated to BigQuery tables.
    *   Source Tables: `DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`
    *   Target Tables: `SOF$TA_CNTRCT_VALID`, `VIA`
    *   Job Control/Logging Tables: New BigQuery tables will be created to manage job status, logging, and error handling, replacing the current job table and temporary file usage.
*   **SQL Script:** The `d_ausd_v_ta_cntrct_valid.sql` script's logic will either be embedded directly within the BigQuery Stored Procedure or transformed into a separate BigQuery Stored Procedure/series of DML statements, called from the main control procedure.
*   **Orchestration (Optional for simple cases, recommended for complex):** For more complex scenarios or if native BigQuery Stored Procedure capabilities are insufficient for external system interactions or complex job control flows, an orchestration layer like Cloud Composer (Airflow) or Google Cloud Workflows can be introduced. This Python-based layer would call the BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The current data flow starts with an external UC4 job, which invokes the primary KornShell script. The KornShell script then manages the execution of an SQL script that interacts with various database tables.

**Current Flow:**
1.  **UC4 Job:** `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_CNTRCT_VALID.xml`
2.  **Invokes:** `k_ausd_v_ta_cntrct_valid.ksh` (represented as `SHELL_SCRIPT:R_AUSD_V_TA_CNTRCT_VALID.KSH` in lineage).
3.  **KornShell Script Actions:**
    *   Parses parameters `j` (Jobkennung) and `f` (EintragsNr).
    *   Sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Executes SQL script `d_ausd_v_ta_cntrct_valid.sql`.
    *   Manages job status (likely through the `h_alis_sqlplus.ksh` and an external job table).
    *   Writes temporary record count to `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_valid_$$.tmp`.
4.  **SQL Script `d_ausd_v_ta_cntrct_valid.sql`:**
    *   **Reads from:** `TABLE:DWTK_MELDUNGEN`, `TABLE:CDS$TA_CNTRCT_VALIDITY`
    *   **Writes to:** `TABLE:SOF$TA_CNTRCT_VALID`, `TABLE:VIA`
    *   **Uses:** `PACKAGE:DWPA_UTIL_SKRIPT`

**Target Flow (BigQuery Stored Procedure centric):**
1.  **Orchestrator (UC4 or Cloud Composer/Workflows):** Invokes the primary BigQuery Stored Procedure.
2.  **BigQuery Stored Procedure `r_ausd_vertrag_control` (replacing `k_ausd_v_ta_cntrct_valid.ksh`):**
    *   Receives `p_JobKennung` and `p_EintragsNr` as input parameters.
    *   Performs parameter validation.
    *   Manages job status (update/insert into a BigQuery job control table).
    *   Calls a nested BigQuery Stored Procedure `d_ausd_v_ta_cntrct_valid` (replacing the SQL script).
    *   Retrieves record count from the results of `d_ausd_v_ta_cntrct_valid` or from a dedicated result table.
    *   Logs execution details to a BigQuery logging table.
3.  **BigQuery Stored Procedure `d_ausd_v_ta_cntrct_valid` (replacing `d_ausd_v_ta_cntrct_valid.sql`):**
    *   **Reads from:** BigQuery Tables `DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`.
    *   **Writes to:** BigQuery Tables `SOF$TA_CNTRCT_VALID`, `VIA`.
    *   Replaces `PACKAGE:DWPA_UTIL_SKRIPT` with BigQuery UDFs or equivalent logic.

## 5. Transformation Logic

The migration involves transforming KornShell and potentially Oracle SQL constructs into BigQuery SQL and stored procedure logic.

**KornShell to BigQuery Stored Procedure:**

*   **Parameter Handling:** The `getopts` based parameter parsing (`-j`, `-f`) will be replaced by input parameters of the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`).
*   **Environment Variables:** Shell environment variables like `$HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL` will be replaced by:
    *   BigQuery Stored Procedure variables.
    *   Configuration parameters passed to the stored procedure or managed by the orchestration layer.
    *   Dataset/project references in BigQuery SQL.
*   **Sourced Utility Scripts:** The logic within `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be:
    *   Re-implemented as BigQuery SQL functions (UDFs) or inline SQL logic for simpler utilities.
    *   Integrated into the main BigQuery Stored Procedure for validation and error handling.
    *   Replaced by BigQuery's native error handling (`RAISE`, `IF`/`THEN`/`ELSE`).
*   **Conditional Logic:** Shell `if` and `case` statements will be translated to BigQuery SQL `IF`/`ELSEIF`/`ELSE` or `CASE` statements.
*   **Temporary Files:** The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_valid_$$.tmp` for storing record counts will be replaced by:
    *   An `OUT` parameter from the nested BigQuery Stored Procedure.
    *   A dedicated logging/result table in BigQuery.
*   **Job Control:** The logic for ignoring active jobs, registering jobs, and deactivating old jobs will be implemented using DML statements (UPDATE, INSERT) against a BigQuery job control table.
*   **SQL Script Execution:** The `starteSQLSkript` function will be replaced by a `CALL` statement to the migrated BigQuery Stored Procedure that encapsulates the `d_ausd_v_ta_cntrct_valid.sql` logic.

**SQL Script `d_ausd_v_ta_cntrct_valid.sql` to BigQuery SQL:**
The actual SQL within `d_ausd_v_ta_cntrct_valid.sql` needs to be reviewed for Oracle-specific syntax and rewritten for BigQuery SQL compatibility. This includes:
*   Data types conversion.
*   Function translations (e.g., `NVL` to `IFNULL`, `TO_CHAR` to `FORMAT_DATE`).
*   Package usage (`DWPA_UTIL_SKRIPT`) will be replaced by BigQuery UDFs or equivalent BigQuery SQL logic.
*   Any procedural logic within the SQL will be wrapped in a BigQuery Stored Procedure.

## 6. External Dependencies

*   **UC4 (Scheduler):** The UC4 job that currently invokes the KornShell script will need to be reconfigured.
    *   **Replacement:** The UC4 job can be updated to trigger a Cloud Composer DAG or a Google Cloud Workflow that then invokes the BigQuery Stored Procedure. Alternatively, a simpler BigQuery Scheduled Query can be used if the orchestration requirements are minimal.
*   **Database (Oracle/Source):** The tables `DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`, `SOF$TA_CNTRCT_VALID`, `VIA` are currently assumed to be in an Oracle or similar legacy database.
    *   **Replacement:** These tables must be migrated to BigQuery. This would involve a data migration strategy (e.g., one-time load, CDC) to move the data from the source database to BigQuery.
*   **Temporary File System:** The use of `/tmp` or `$DW_DIR_UTL` for temporary files.
    *   **Replacement:** BigQuery Stored Procedures handle intermediate results in-memory or through temporary tables. Final results or logging should go into permanent BigQuery tables.
*   **Shell Utilities:** Basic shell commands like `cat`, `eval`, `print`, `echo`.
    *   **Replacement:** These will be replaced by BigQuery SQL constructs for logging (`SELECT` statements for output, `INSERT` into log tables) and variable assignment.

## 7. Unresolved / Risks

*   **Detailed SQL Logic:** The actual SQL code within `d_ausd_v_ta_cntrct_valid.sql` was not available for detailed analysis. Oracle-specific features or complex procedural SQL may require significant re-engineering in BigQuery.
*   **Utility Script Logic:** The full logic of the sourced utility ksh scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is unknown. Their functionalities need to be thoroughly analyzed and re-implemented in BigQuery SQL or the orchestration layer.
*   **Job Table Schema:** The schema and specific update/insert logic for the "job table" mentioned in the script are not known. This needs to be defined and implemented in BigQuery.
*   **`DWPA_UTIL_SKRIPT` Package:** The functionality of this Oracle package needs to be fully understood to convert it to BigQuery UDFs or equivalent logic.
*   **Error Handling:** The exact error codes (`ErrNr`, `ErrArg`) and `DWMSG_MeldeFehler` mechanism need to be mapped to a BigQuery-compatible error logging and reporting strategy.
*   **Semi-Auto Bucket:** The `semi_auto` migration bucket indicates that some manual intervention or review will be necessary, likely due to the complexities mentioned above (utility script logic, specific SQL details, job control mechanisms).

## 8. Build Plan

The build plan will focus on creating BigQuery assets to replicate the functionality.

1.  **Data Migration:**
    *   Migrate `DWTK_MELDUNGEN`, `CDS$TA_CNTRCT_VALIDITY`, `SOF$TA_CNTRCT_VALID`, `VIA` tables from source to BigQuery. (Tool: Data Transfer Service, custom ETL, etc.)
2.  **BigQuery Utility Functions/Procedures:**
    *   Develop BigQuery UDFs or small Stored Procedures to replicate the functionality of `h_alis_date.ksh` and `DWPA_UTIL_SKRIPT`. (Language: BigQuery SQL)
    *   Create a BigQuery logging table and a Stored Procedure for error handling (`f_alis_msgerr.ksh` replacement). (Language: BigQuery SQL)
3.  **BigQuery Job Control Tables:**
    *   Create BigQuery tables for job registration, status, and deactivation, replacing the implicit job table usage. (Language: BigQuery DDL)
4.  **SQL Core Logic Migration:**
    *   Migrate the SQL from `d_ausd_v_ta_cntrct_valid.sql` into a BigQuery Stored Procedure, e.g., `d_ausd_v_ta_cntrct_valid_bq`. This will involve translating Oracle-specific syntax to BigQuery SQL. (Language: BigQuery SQL)
5.  **Main Control BigQuery Stored Procedure:**
    *   Create the main BigQuery Stored Procedure `r_ausd_vertrag_control` (as per the pseudocode in section 5) to handle parameter input, validation, job control updates, and invocation of `d_ausd_v_ta_cntrct_valid_bq`. (Language: BigQuery SQL)
6.  **Orchestration Layer (If needed):**
    *   If using Cloud Composer/Workflows, develop a Python DAG or Workflow definition to invoke the `r_ausd_vertrag_control` BigQuery Stored Procedure, passing required parameters. (Language: Python, YAML/JSON)
7.  **Scheduler Integration:**
    *   Update the UC4 scheduler to call the new orchestration layer or directly trigger the BigQuery Stored Procedure (via BigQuery Scheduled Queries or a custom API call). (Configuration Update)

The provided pseudocode for the `r_ausd_vertrag_control` BigQuery Stored Procedure will serve as the foundation for step 5. The implementation of `d_ausd_v_ta_cntrct_valid_bq` will depend on the detailed analysis of the original SQL script.