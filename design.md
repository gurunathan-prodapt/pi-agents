# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_cntrct_templ.ksh`. This script acts as a control and orchestration component for a larger job (`r_ausd_vertrag.ksh` indirectly referenced). Its primary functions include environment setup, parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`), handling errors, registering/deactivating job entries in a job table, and executing an associated SQL script, `d_ausd_v_ta_cntrct_templ.sql`, which likely updates the `ta_cntrct_templ` table. The script also captures the number of records processed by the SQL script via a temporary file mechanism. The scope of this migration focuses on transforming the KornShell orchestration logic and its interaction with the SQL script into a BigQuery-native solution, orchestrated by Cloud Composer.

## 2. Source Inventory
The job consists of a single primary source file:
*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`
*   **Technology:** KornShell Script (ksh)
*   **Summary:** A control script responsible for environment initialization, command-line parameter parsing and validation, error handling, job registration/deactivation, and the orchestrated execution of an SQL script (`d_ausd_v_ta_cntrct_templ.sql`) that modifies the `ta_cntrct_templ` table. It captures execution metrics (record count) via a temporary file.
*   **Complexity Tier:** Not available from `file_complexity` table. However, due to its role as an orchestrator with parameter handling, error logic, and external script execution, it is considered at least **Medium** complexity. It is 101 lines of code.
*   **Automation Bucket:** Not available from `automation_rate` table. Given its orchestration nature and the suggested target of Cloud Composer, this component is best suited for **Semi-Automated (B2)** migration, implying significant architectural transformation.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform (GCP) services, primarily:
*   **BigQuery:**
    *   **Stored Procedure:** A BigQuery Stored Procedure (`project.dataset.r_ausd_v_ta_cntrct_templ`) will encapsulate the shell script's orchestration logic, parameter handling, error reporting, and job table updates.
    *   **BigQuery SQL:** The logic from `d_ausd_v_ta_cntrct_templ.sql` will be translated into BigQuery SQL, either as an embedded part of the stored procedure or as a separate BigQuery script executed from within the procedure.
    *   **BigQuery Tables:** The target table `ta_cntrct_templ` will reside in BigQuery. Dedicated BigQuery tables will be created for job metadata (`job_table`) and error logging (`job_error_log`).
*   **Cloud Composer (Apache Airflow):** A Cloud Composer DAG will serve as the primary scheduler and orchestrator, replacing the shell script's role in managing the overall workflow and triggering the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
**Legacy Data Flow:**
1.  The `k_ausd_v_ta_cntrct_templ.ksh` script is invoked, likely by `r_ausd_vertrag.ksh`, with command-line parameters (`-j` for `p_JobKennung`, `-f` for `p_EintragsNr`).
2.  The script initializes its environment by sourcing several utility KornShell scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  Command-line parameters are parsed using `getopts` and validated using `pruefeParameterGesetzt`.
4.  If validation fails, an error is reported via `DWMSG_MeldeFehler` to standard output, and the script exits.
5.  The script then executes `d_ausd_v_ta_cntrct_templ.sql` by calling the `starteSQLSkript` function, passing parameters. This SQL script interacts with and likely updates the `ta_cntrct_templ` database table.
6.  A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`) is used to store the record count, which is then read back into the `v_records` shell variable.

**Target Data Flow:**
1.  A Cloud Composer DAG is scheduled and triggered.
2.  The Cloud Composer DAG executes a BigQuery Stored Procedure `project.dataset.r_ausd_v_ta_cntrct_templ`, passing `p_JobKennung` and `p_EintragsNr` as input parameters.
3.  Inside the BigQuery Stored Procedure:
    a.  Parameters are validated using BigQuery SQL control flow (`IF` statements).
    b.  Job status (e.g., 'STARTED') is recorded in the `project.dataset.job_table`.
    c.  Older active jobs for `ta_cntrct_templ` are deactivated in `project.dataset.job_table`.
    d.  The migrated BigQuery SQL logic (from `d_ausd_v_ta_cntrct_templ.sql`) is executed, interacting with the `project.dataset.ta_cntrct_templ` table.
    e.  The record count is determined by a `COUNT(*)` query on the affected rows and stored in a `DECLARE` variable.
    f.  The final job status ('FINISHED') and record count are recorded in `project.dataset.job_table`.
    g.  Any errors encountered are logged to `project.dataset.job_error_log`, and the procedure signals an error.
4.  The Cloud Composer DAG monitors the completion status of the BigQuery Stored Procedure.

## 5. Transformation Logic
The transformation will involve a significant architectural shift from a shell-script-driven, file-system-dependent process to a serverless, managed BigQuery and Cloud Composer solution.

*   **Orchestration (KornShell to Cloud Composer):** The main shell script `k_ausd_v_ta_cntrct_templ.ksh` will be replaced by a Python-based Apache Airflow DAG in Cloud Composer. This DAG will manage the scheduling and execution of the BigQuery Stored Procedure.
*   **Core Logic (KornShell to BigQuery Stored Procedure):** The imperative logic within the KornShell script (parameter parsing, validation, error handling, job status updates) will be re-implemented as a BigQuery Stored Procedure named `project.dataset.r_ausd_v_ta_cntrct_templ`. This will utilize BigQuery's `DECLARE`, `IF`, `INSERT`, `UPDATE`, and `SIGNAL SQLSTATE` statements.
*   **SQL Execution (`starteSQLSkript` to Direct BigQuery SQL):** The `starteSQLSkript` function's role of executing `d_ausd_v_ta_cntrct_templ.sql` will be replaced by direct execution of the migrated BigQuery SQL within the stored procedure. The contents of `d_ausd_v_ta_cntrct_templ.sql` will be translated into BigQuery-compliant SQL.
*   **Environment Variables & Sourced Scripts:**
    *   Environment variables like `BERT_DIR_ROOT`, `DW_DIR_UTL` will be replaced by configuration in Cloud Composer or parameters passed to the BigQuery Stored Procedure.
    *   Functionality from sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be re-implemented directly in the BigQuery Stored Procedure using native SQL constructs, or as BigQuery User-Defined Functions (UDFs) if the logic is complex and reusable.
*   **Temporary File Handling:** The reliance on `tmpFile` for record counting will be eliminated. The BigQuery Stored Procedure will directly calculate the record count using `COUNT(*)` queries on the target table after the SQL logic execution and store it in an internal `DECLARE` variable, subsequently logging it to the `job_table`.
*   **Job Management:** The implicit job table updates will be formalized into explicit `INSERT` and `UPDATE` statements against a new `project.dataset.job_table` in BigQuery. Error logging will be directed to `project.dataset.job_error_log`.

## 6. External Dependencies
The original script has several external dependencies, primarily other shell scripts, a database, and the file system. These will be replaced or absorbed into the GCP ecosystem.

*   **Legacy Shell Utility Scripts:**
    *   `$HOME/.dw_init` (environment setup)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing/validation)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL execution wrapper)
    *   **Replacement:** The functionalities of these scripts will be integrated into the BigQuery Stored Procedure using native BigQuery SQL capabilities (e.g., `IF`, string functions, date functions) or as BigQuery SQL UDFs. Environment variables will be handled via Cloud Composer environment variables or BigQuery procedure parameters.
*   **SQL Script `d_ausd_v_ta_cntrct_templ.sql`:**
    *   **Replacement:** This script's logic will be translated into BigQuery SQL and executed within the BigQuery Stored Procedure.
*   **Database:**
    *   The database hosting `ta_cntrct_templ` and any implicit job management tables will be migrated to BigQuery.
    *   **Replacement:** All data storage and processing will occur within BigQuery.
*   **File System (`tmpFile`):**
    *   **Replacement:** The temporary file mechanism for inter-process communication and record counting will be replaced by BigQuery's in-procedure variables (`DECLARE`) and direct SQL queries.

## 7. Unresolved / Risks
*   **Missing Source Code for Dependencies:** The specific implementation details of the sourced utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and especially the core SQL script `d_ausd_v_ta_cntrct_templ.sql` were not available during this design phase. A thorough review and migration of these dependencies are critical for a complete and correct solution.
*   **Complexity & Automation Data:** The absence of `file_complexity` and `automation_rate` data from the database means the precise migration effort and automation potential are estimated based on general patterns. This introduces a slight risk in project planning.
*   **Parent Script `r_ausd_vertrag.ksh`:** The current design focuses solely on `k_ausd_v_ta_cntrct_templ.ksh`. The calling script `r_ausd_vertrag.ksh` will also need a migration design to ensure end-to-end functionality.
*   **`starteSQLSkript` Functionality:** The `starteSQLSkript` function likely handles various aspects of SQL execution (connection, error handling, result processing). Its exact behavior needs to be understood and replicated accurately in the BigQuery Stored Procedure context.

## 8. Build Plan
1.  **Migrate `d_ausd_v_ta_cntrct_templ.sql` to BigQuery SQL:**
    *   Analyze the `d_ausd_v_ta_cntrct_templ.sql` file content.
    *   Translate all DDL and DML statements to BigQuery standard SQL syntax.
    *   Verify data types and functions compatibility.
    *   **Output:** `d_ausd_v_ta_cntrct_templ.bqsql` (BigQuery SQL script).
2.  **Create BigQuery Schema Objects:**
    *   Define and deploy DDL for `project.dataset.job_error_log` table.
    *   Define and deploy DDL for `project.dataset.job_table` table.
    *   Ensure `project.dataset.ta_cntrct_templ` table exists or is migrated.
    *   **Output:** BigQuery DDL scripts for `job_error_log` and `job_table`.
3.  **Develop BigQuery Stored Procedure `r_ausd_v_ta_cntrct_templ`:**
    *   Create `CREATE OR REPLACE PROCEDURE` statement.
    *   Implement `DECLARE` variables for `v_TabName`, `v_records`, `ErrNr`, `ErrArg`.
    *   Implement parameter validation logic using `IF` statements based on `p_JobKennung` and `p_EintragsNr`.
    *   Implement error logging to `project.dataset.job_error_log` and `SIGNAL SQLSTATE` for error propagation.
    *   Implement `INSERT` statements to `project.dataset.job_table` for job 'STARTED' status.
    *   Implement `UPDATE` statements to `project.dataset.job_table` for 'DEACTIVATED' older jobs.
    *   Integrate the migrated `d_ausd_v_ta_cntrct_templ.bqsql` logic.
    *   Implement record counting and `UPDATE` `project.dataset.job_table` for 'FINISHED' status.
    *   **Output:** `r_ausd_v_ta_cntrct_templ.bqsql` (BigQuery Stored Procedure DDL).
4.  **Develop Cloud Composer DAG:**
    *   Create a Python script for an Airflow DAG.
    *   Define a `BigQueryExecuteQueryOperator` to call `project.dataset.r_ausd_v_ta_cntrct_templ`, passing run-time parameters.
    *   Configure error handling and retry mechanisms within the DAG.
    *   **Output:** `k_ausd_v_ta_cntrct_templ_dag.py` (Python Airflow DAG).
5.  **Unit and Integration Testing:**
    *   Write unit tests for the BigQuery Stored Procedure.
    *   Write integration tests for the Cloud Composer DAG calling the Stored Procedure.
    *   **Output:** Test scripts/framework.
6.  **Deployment:**
    *   Deploy BigQuery DDL (tables and stored procedure).
    *   Deploy the Airflow DAG to Cloud Composer environment.
    *   **Output:** Deployment scripts/instructions.