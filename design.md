# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_discount_rr.ksh` to Google Cloud BigQuery.

The original script acts as a control script (`Kontrollscript`) for a related process (`r_ausd_vertrag.ksh`), primarily orchestrating the execution of a SQL script for the `ta_discount_rr` table. Its main functions include:
*   Ignoring already active jobs to prevent redundant execution.
*   Invoking a specific SQL script (`d_ausd_v_ta_discount_rr.sql`) to perform data processing.
*   Registering job entries (likely in a job tracking table).
*   Deactivating old active jobs.
*   Handling parameter validation and error reporting.

The script itself does not perform direct data transformations but rather acts as an orchestration layer for a downstream SQL script.

## 2. Source Inventory
The primary source component for this job is a single KornShell script.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh`
*   **Technology:** KornShell (shell script)
*   **Complexity Tier:** Not available (No rows in `file_complexity` table)
*   **Automation Bucket:** Semi-Automatic (B2)
*   **Purpose:** Orchestration, job control, parameter validation, execution of a SQL script.

**Inferred Dependent Files:**
*   `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql`: This SQL script is invoked by the ksh wrapper and is expected to contain the core data manipulation logic for `ta_discount_rr`.
*   Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`): These provide common functions for error handling, date management, parameter parsing, and SQL*Plus interaction within the KornShell environment.

## 3. Target Architecture
The target platform is Google Cloud BigQuery. The migration will leverage BigQuery's capabilities for data processing, stored procedures for procedural logic, and potentially Cloud Composer (Apache Airflow) for orchestration if complex scheduling or job dependency management is required.

**Components:**
*   **BigQuery Stored Procedure:** The core logic of `k_ausd_v_ta_discount_rr.ksh` (parameter validation, orchestration flow, error handling) will be converted into a BigQuery Stored Procedure. This procedure will accept parameters and call other BigQuery SQL statements or procedures.
*   **BigQuery SQL Script/Procedure:** The data manipulation logic from `d_ausd_v_ta_discount_rr.sql` will be migrated into a separate BigQuery SQL script or another BigQuery Stored Procedure, depending on its complexity and reusability.
*   **BigQuery Tables:** All source tables and the target `ta_discount_rr` table will reside in BigQuery.
*   **Logging Table:** A dedicated BigQuery table for logging (e.g., `project.dataset.error_log`, `project.dataset.job_log`) will replace the shell script's `DWMSG_MeldeFehler` and job registration mechanisms.
*   **Orchestration (Optional):** If the original job has external scheduling or dependencies (not evident from the single script, but mentioned as "ignore active jobs"), Cloud Composer (Airflow) could be used to manage the execution of the BigQuery Stored Procedure. For simpler scheduling, BigQuery Scheduled Queries might suffice.

**Layout:**
*   All migrated SQL logic and stored procedures will reside within a designated BigQuery dataset.
*   Logging tables will also be in BigQuery.

## 4. Data Flow & Lineage
The original script performs the following data flow:
1.  **Parameter Input:** Receives `p_JobKennung` and `p_EintragsNr` via command-line arguments.
2.  **Validation:** Validates the input parameters.
3.  **SQL Script Execution:** Calls the `starteSQLSkript` function, which in turn executes `d_ausd_v_ta_discount_rr.sql`. This SQL script is presumed to read from source tables and write to the `ta_discount_rr` table.
4.  **Record Count Output:** The SQL execution (via `starteSQLSkript`) is expected to write the number of processed records to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_discount_rr_$$.tmp`).
5.  **Record Count Read:** The ksh script then reads this record count from the temporary file into the `v_records` variable.
6.  **Job Completion/Error Logging:** Logs completion or error messages.

**Target BigQuery Data Flow:**
1.  **Trigger:** The BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`) is invoked (either manually, via a scheduled query, or through an orchestration tool like Cloud Composer). Parameters `p_JobKennung` and `p_EintragsNr` are passed directly to the procedure.
2.  **Parameter Validation:** The stored procedure will perform parameter validation using `IF` statements.
3.  **Logging:** Error messages will be inserted into a BigQuery error logging table.
4.  **SQL Stored Procedure Call:** The main stored procedure will call the BigQuery SQL procedure that encapsulates the logic of `d_ausd_v_ta_discount_rr.sql`. This sub-procedure will perform the data reads and writes within BigQuery.
5.  **Record Count Retrieval:** The record count will be retrieved directly within the BigQuery stored procedure, for example, using `SELECT COUNT(*)` on the target table or by capturing `@@row_count` from the DML statement. This replaces the temporary file mechanism.
6.  **Job Status Logging:** Final job status, including the record count, will be inserted into a BigQuery job logging table.

## 5. Transformation Logic

**`k_ausd_v_ta_discount_rr.ksh` (Orchestration Script)**

*   **Parameter Handling:**
    *   **Source:** `getopts` command to parse `-j` and `-f` parameters.
    *   **Target:** BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING, IN p_EintragsNr STRING`).
*   **Environment Sourcing:**
    *   **Source:** `. $HOME/.dw_init`, utility scripts for error handling, date, parameter parsing, and SQL*Plus.
    *   **Target:** BigQuery Stored Procedures do not directly source external scripts. The functionality provided by these utilities will be either:
        *   Replaced by native BigQuery procedural logic (e.g., `CURRENT_TIMESTAMP()` for date, `IF` for error checks).
        *   Implemented as separate BigQuery functions or procedures.
        *   Managed by the orchestration layer (e.g., Python scripts in Airflow for logging/metadata).
*   **Error Handling:**
    *   **Source:** `pruefeParameterGesetzt` function and `DWMSG_MeldeFehler` for custom error logging.
    *   **Target:** `IF` conditions for parameter validation. `RAISE USING MESSAGE` for raising errors and `INSERT INTO error_log_table` for persistent error logging in BigQuery.
*   **SQL Script Execution:**
    *   **Source:** `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
    *   **Target:** `CALL project.dataset.d_ausd_v_ta_discount_rr(p_EintragsNr, p_JobKennung);` (assuming `d_ausd_v_ta_discount_rr` is also migrated as a stored procedure).
*   **Record Count Exchange:**
    *   **Source:** Writing to and reading from a temporary file (`$tmpFile`) via `eval "v_records=\`cat $tmpFile\`"`.
    *   **Target:** The record count will be obtained directly from the DML statement in BigQuery (e.g., `SELECT COUNT(*)` after `INSERT` or `UPDATE`, or `@@row_count` in a procedural context) and stored in a BigQuery variable within the stored procedure. This value can then be logged to the job log table.

**`d_ausd_v_ta_discount_rr.sql` (Inferred Data Processing Script)**

*   **Source:** Original SQL script, likely Oracle SQL based on `h_alis_sqlplus.ksh` and `ta_discount_rr` naming convention.
*   **Target:** The SQL logic within `d_ausd_v_ta_discount_rr.sql` will be converted to BigQuery Standard SQL. This may involve:
    *   Syntax adjustments for functions, data types, and specific SQL constructs.
    *   Migration of DDL for table `ta_discount_rr`.
    *   Optimization for BigQuery's columnar storage and distributed query execution.
    *   Encapsulation within a BigQuery Stored Procedure if it contains procedural elements or is intended for reuse.

## 6. External Dependencies
The original script exhibits the following dependencies:

*   **External Systems:** The `lineage_assembled_jobs` query showed no explicit external systems (`[]`). However, given the context of a KornShell script calling a SQL script, an Oracle database is strongly implied. The `h_alis_sqlplus.ksh` utility further supports this.
    *   **Source:** Oracle Database (implied by SQL*Plus utilities and typical legacy ETL patterns).
    *   **Target:** The Oracle database will be replaced by Google Cloud BigQuery. All relevant tables, including the `ta_discount_rr` target table, will be migrated to BigQuery.
*   **Shell Utilities:**
    *   **Source:** `f_alis_msgerr.ksh` (error messaging), `h_alis_date.ksh` (date functions), `h_alis_parameter.ksh` (parameter parsing), `h_alis_sqlplus.ksh` (SQL*Plus wrapper).
    *   **Target:** These utilities will be replaced by native BigQuery procedural statements, functions, or managed by the orchestration layer (e.g., Python in Cloud Composer for logging).
*   **Temporary File System:**
    *   **Source:** `$DW_DIR_UTL/bert_k_ausd_v_ta_discount_rr_$$.tmp` for inter-process communication (record count).
    *   **Target:** This mechanism will be eliminated. Record counts will be managed via BigQuery variables within stored procedures or directly logged to BigQuery tables.

## 7. Unresolved / Risks
*   **`starteSQLSkript` Functionality:** The exact implementation of `starteSQLSkript` is not available. It's crucial to understand if it performs additional logic beyond just executing the SQL (e.g., connection management, transaction handling, error recovery, job status updates to a central job table). This "active jobs" logic and "entry in job table" need to be fully understood to ensure accurate migration. This represents a moderate risk.
*   **`d_ausd_v_ta_discount_rr.sql` Content:** The actual SQL logic within this file is unknown. Its complexity, use of specific Oracle features (PL/SQL, proprietary functions, hierarchical queries), and data volume will determine the effort for conversion to BigQuery SQL. This is a primary technical risk.
*   **Absence of `file_complexity` data:** The lack of complexity tier and migration flags for `k_ausd_v_ta_discount_rr.ksh` from the `file_complexity` table means some potential migration challenges might not have been identified upfront. This implies an increased need for manual code review and testing.
*   **Orchestration Details:** The extent of job control implied by "ignore active jobs" and "deactivate old active jobs" is not fully understood. If this involves complex scheduling, external triggers, or cross-job dependencies, a full Cloud Composer implementation might be necessary, adding to the complexity.

## 8. Build Plan

The migration will be implemented in BigQuery Standard SQL, primarily using Stored Procedures.

1.  **Analyze `d_ausd_v_ta_discount_rr.sql`:**
    *   Obtain the source code for `d_ausd_v_ta_discount_rr.sql`.
    *   Analyze its SQL syntax, data types, and any procedural logic.
    *   Identify source tables it reads from and transformations it applies before writing to `ta_discount_rr`.
2.  **Migrate `d_ausd_v_ta_discount_rr.sql` to BigQuery Stored Procedure:**
    *   **File:** `bq_d_ausd_v_ta_discount_rr.sql` (BigQuery SQL)
    *   **Content:** Convert the SQL logic to BigQuery Standard SQL. Create a BigQuery Stored Procedure (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_v_ta_discount_rr(...)`) that encapsulates this logic. This procedure should accept `p_EintragsNr` and `p_JobKennung` as input parameters.
3.  **Create BigQuery Logging Tables:**
    *   **File:** `bq_logging_tables.sql` (BigQuery DDL)
    *   **Content:** Define DDL for `project.dataset.error_log` and `project.dataset.job_log` tables to store execution details, errors, and processed record counts.
4.  **Migrate `k_ausd_v_ta_discount_rr.ksh` to BigQuery Stored Procedure:**
    *   **File:** `bq_k_ausd_v_ta_discount_rr_control.sql` (BigQuery SQL)
    *   **Content:** Implement the orchestration logic as a BigQuery Stored Procedure (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_vertrag_control(...)`).
        *   Include input parameters `p_JobKennung STRING, p_EintragsNr STRING`.
        *   Implement parameter validation logic.
        *   Replace `DWMSG_MeldeFehler` with `INSERT` statements into `project.dataset.error_log` and `RAISE` for immediate error handling.
        *   Replace the `starteSQLSkript` call with `CALL project.dataset.d_ausd_v_ta_discount_rr(p_EintragsNr, p_JobKennung);`.
        *   Replace temporary file record count retrieval with BigQuery internal methods (e.g., `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) FROM project.dataset.ta_discount_rr WHERE ...);`).
        *   Insert job execution details, including `v_records`, into `project.dataset.job_log`.
5.  **Develop Orchestration (if needed):**
    *   If complex scheduling or external dependencies are identified, create a Cloud Composer DAG (`k_ausd_v_ta_discount_rr_dag.py`).
    *   The DAG will trigger `project.dataset.r_ausd_vertrag_control` and manage any upstream/downstream tasks.
    *   If simple scheduling, use BigQuery Scheduled Queries to execute `CALL project.dataset.r_ausd_vertrag_control(...)`.
6.  **Testing:**
    *   Unit test each BigQuery Stored Procedure independently.
    *   Integration test the entire flow, including orchestration and logging.