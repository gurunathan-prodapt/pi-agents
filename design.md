# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_barrier.ksh` to Google Cloud's BigQuery platform. The script serves as a wrapper or orchestration framework for a contract data reconciliation job against the `ta_barrier` table. Its primary function is to initialize the runtime environment, handle command-line parameters, set up logging and error handling, invoke the core business logic contained in a kernel script, and record the overall job status. The job was assembled from a single component and is categorized as having a 'medium' stage distribution.

## 2. Source Inventory
The job consists of a single source file:

*   **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh`
*   **Technology:** KornShell (`.ksh`)
*   **Category:** shell
*   **Complexity Tier:** medium
*   **Automation Bucket:** semi_auto
*   **File Purpose:** Orchestration / Wrapper script

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data processing and persistence, with orchestration handled by a suitable Google Cloud orchestrator.

*   **Core Logic:** The wrapper script's functionality, including parameter handling, logging setup, and invocation of the kernel script, will be migrated into a BigQuery Stored Procedure.
*   **Kernel Logic:** The core business logic residing in `k_ausd_v_ta_barrier.ksh` will also be migrated into one or more BigQuery Stored Procedures or a series of SQL statements.
*   **Utility Functions:** Reusable shell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be converted into BigQuery Stored Procedures or User-Defined Functions (UDFs).
*   **Logging & Status:** All logging information and job status updates, originally written to a file, will be directed to dedicated BigQuery logging and status tables.
*   **Error Handling:** Shell `trap` mechanisms will be replaced by BigQuery scripting's `EXCEPTION WHEN ERROR THEN` blocks within the stored procedures.
*   **Orchestration:** A Google Cloud orchestration service (e.g., Cloud Composer with Airflow DAGs, Cloud Workflows, or Dataform) will be used to schedule and execute the main BigQuery stored procedure, handling parameter injection and monitoring.

## 4. Data Flow & Lineage
Based on static analysis of the KornShell script, the data flow involves:

1.  **Environment Initialization:** The script sources `$HOME/.dw_init` and various utility KornShell scripts.
2.  **Parameter Processing:** Command-line parameters are parsed and validated.
3.  **Logging & Error Handling Setup:** Custom `DWMSG_*` functions are called to establish job numbering, log file naming, and error trapping.
4.  **Core Business Logic Invocation:** The script executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier.ksh` (the kernel script) with specific arguments (`-j`, `-f`).
5.  **Output & Status Update:** Standard output and error messages from the kernel script are redirected to a log file. Upon completion, a success message is printed, and `DWMSG_SetzeStatusOK` is called.

**Target BigQuery Data Flow:**

*   **Orchestrator Trigger:** An orchestrator (e.g., Cloud Composer DAG) initiates the main BigQuery stored procedure (`r_ausd_v_ta_barrier_sp`).
*   **`r_ausd_v_ta_barrier_sp` Execution:**
    *   Declares variables representing script parameters and internal state.
    *   Performs parameter validation based on input to the stored procedure.
    *   Calls BigQuery stored procedures (`DWMSG_ErmittleNr_SP`, `DWMSG_Logdateiname_SP`, `DWMSG_ErzeugeEintrag_SP`, `DWMSG_SetzeStichtagInfo_SP`) to manage job entries and logging within dedicated BigQuery tables.
    *   Invokes the migrated kernel stored procedure (`k_ausd_v_ta_barrier_sp`) with appropriate parameters.
    *   Logs messages and status updates to a `job_log_table` in BigQuery.
    *   Utilizes BigQuery's `EXCEPTION WHEN ERROR THEN` block for robust error handling, calling `DWMSG_Fehlerbehandlung_SP` upon error.
    *   Upon successful completion, calls `DWMSG_SetzeStatusOK_SP` to update the job status in a BigQuery table.

*Note: The `lineage_edges` query for this specific `run_id` did not return any explicit dependency edges, indicating that the detailed runtime lineage for internal script calls might not have been captured in the database. The above flow is derived from code analysis.*

## 5. Transformation Logic
The transformation will involve translating KornShell constructs, environment sourcing, and external program invocations into BigQuery SQL stored procedures and UDFs.

**Original Script (`r_ausd_v_ta_barrier.ksh`) Key Logic:**

*   **Environment Sourcing:** `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
*   **Variable Declarations:** `ProgName`, `ProgVersion`, `ErrNr`, `ErrArg`, `ErrVal`, `DW_EintragsNr`, `JobKennung`, `v_sysdate`, `Name_Kernskript`, `LogDatei`.
*   **Command-line Parsing:** `getopts` loop to parse `-h`, `-s`, `-l` parameters.
*   **Conditional Logic:** `if [ ! $ErrNr -eq 0 ]` and `case $param in` statements.
*   **Date Generation:** `date +%d%m%Y`.
*   **Logging/Messaging Functions:** `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`.
*   **Program Execution:** `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`.
*   **Output:** `print`, `cat <<EOF`, `tee -a`.
*   **Error Trapping:** `trap` commands for `INT` and `ERR` signals.

**Target BigQuery SQL Transformation (within `r_ausd_v_ta_barrier_sp` procedure):**

*   **Environment Variables & Parameters:**
    *   Shell variables will be replaced by `DECLARE` statements for internal variables and input parameters for external configuration (e.g., `JobKennung` as a procedure parameter).
*   **Sourced Scripts:**
    *   The logic from `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` will be refactored into separate BigQuery Stored Procedures or UDFs that are `CALL`ed or used in expressions.
*   **Command-line Arguments:**
    *   The `getopts` loop will be replaced by direct input parameters to the BigQuery stored procedure. Validation logic will be implemented using `IF` statements.
*   **Conditional Logic:**
    *   Shell `if` and `case` statements will be directly translated to BigQuery SQL `IF` / `ELSEIF` / `END IF` or `CASE` expressions within the procedural block.
*   **Date Generation:**
    *   `date +%d%m%Y` will be replaced by `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Logging/Messaging Functions:**
    *   Each `DWMSG_*` function call will be translated into a `CALL` to its corresponding BigQuery stored procedure (e.g., `CALL DWMSG_MeldeFehler_SP(...)`) which will then perform `INSERT` operations into a BigQuery log table.
*   **Program Execution:**
    *   The execution of the kernel script will be replaced by a `CALL` to its migrated BigQuery stored procedure: `CALL k_ausd_v_ta_barrier_sp(JobKennung, DW_EintragsNr);`.
*   **Output:**
    *   `print` and `tee -a` will be replaced by `INSERT` statements into the BigQuery log table (`job_log_table`) for structured logging. Usage messages (`usage()`) will either be returned as a string from a procedure or handled by the orchestration layer.
*   **Error Trapping:**
    *   The `trap` commands will be handled by a `BEGIN...EXCEPTION WHEN ERROR THEN...END;` block in the BigQuery stored procedure, ensuring that errors are caught, logged via `DWMSG_Fehlerbehandlung_SP`, and potentially re-raised or handled gracefully.

**BigQuery SQL Pseudocode (Main Wrapper Procedure):**

```sql
-- BigQuery Script: r_ausd_v_ta_barrier_sp
-- Wrapper for the contract data reconciliation job

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.r_ausd_v_ta_barrier_sp`(
    p_job_kennung STRING,
    p_s STRING, -- Corresponds to original -s param
    p_l STRING  -- Corresponds to original -l param
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE LogDatei STRING;
  DECLARE Name_Kernskript STRING DEFAULT 'k_ausd_v_ta_barrier_sp'; -- Name of the migrated kernel SP

  -- --- Parameter Validation (replaces getopts logic) ---
  -- This section would contain logic to validate p_s and p_l parameters
  -- based on their original usage. If invalid, set ErrNr and ErrArg.
  IF p_s IS NULL OR p_l IS NULL THEN -- Example validation
    SET ErrNr = 193; -- Missing argument
    SET ErrArg = 's/l';
  END IF;

  IF ErrNr != 0 THEN
    CALL `project_id.dataset_id.DWMSG_MeldeFehler_SP`(DW_EintragsNr, 'E', ErrNr, ErrArg);
    SELECT CONCAT('Programm: ', ProgName, '\nVersion:  ', ProgVersion, '\nAufruf:   CALL r_ausd_v_ta_barrier_sp(job_kennung, s, l)') AS usage_info;
    RAISE USING MESSAGE = CONCAT('Parameter Error: ', CAST(ErrNr AS STRING), ' - ', ErrArg);
  END IF;

  -- --- Job Initialization ---
  CALL `project_id.dataset_id.DWMSG_ErmittleNr_SP`(DW_EintragsNr); -- Get a unique job entry number
  CALL `project_id.dataset_id.DWMSG_Logdateiname_SP`(LogDatei, p_job_kennung, DW_EintragsNr); -- Determine log file equivalent name
  CALL `project_id.dataset_id.DWMSG_ErzeugeEintrag_SP`(DW_EintragsNr, p_job_kennung, 'BQ_SCRIPT', LogDatei); -- Create initial log entry
  CALL `project_id.dataset_id.DWMSG_SetzeStichtagInfo_SP`(DW_EintragsNr, v_sysdate, 'DDMMYYYY'); -- Set reference date

  -- --- Job Banner Logging ---
  INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
  VALUES (
    DW_EintragsNr,
    p_job_kennung,
    CONCAT('Job-Nr: ', CAST(DW_EintragsNr AS STRING), ', JobKennung: ', p_job_kennung, ', Logdatei: ', LogDatei),
    CURRENT_TIMESTAMP(),
    'INFO'
  );

  -- --- Core Kernel Logic Execution ---
  BEGIN
    CALL `project_id.dataset_id.k_ausd_v_ta_barrier_sp`(p_job_kennung, DW_EintragsNr); -- Execute the migrated kernel SP
    
    -- --- Success Handling ---
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
      DW_EintragsNr,
      p_job_kennung,
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP(),
      'INFO'
    );
    CALL `project_id.dataset_id.DWMSG_SetzeStatusOK_SP`(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    -- --- Error Handling (replaces trap ERR/INT) ---
    CALL `project_id.dataset_id.DWMSG_Fehlerbehandlung_SP`(DW_EintragsNr);
    INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
      DW_EintragsNr,
      p_job_kennung,
      CONCAT('AppError: Abbruch - ', @@error.message),
      CURRENT_TIMESTAMP(),
      'ERROR'
    );
    RAISE; -- Re-raise the error to the orchestrator
  END;

END;
```

## 6. External Dependencies
The original script has several implicit and explicit dependencies that need to be addressed in the migration:

*   **`$HOME/.dw_init`:**
    *   **Legacy:** A sourced KornShell script for environment initialization.
    *   **Replacement:** Configuration and environment variables will be passed as parameters to the BigQuery stored procedure or managed by the orchestration layer. Any specific initializations can be integrated directly into the BQ procedure or relevant UDFs/SPs.
*   **`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`:**
    *   **Legacy:** Sourced KornShell utility scripts providing error messaging, parameter handling, and date functions.
    *   **Replacement:** These will be re-implemented as independent BigQuery Stored Procedures (e.g., `DWMSG_MeldeFehler_SP`, `DWMSG_ErmittleNr_SP`, etc.) or BigQuery UDFs (for pure function logic like date manipulation).
*   **`k_ausd_v_ta_barrier.ksh`:**
    *   **Legacy:** The core kernel KornShell script that performs the actual contract data reconciliation logic.
    *   **Replacement:** This script is a critical dependency and must be fully migrated to a BigQuery Stored Procedure (e.g., `k_ausd_v_ta_barrier_sp`). The design of this kernel migration is outside the scope of this document but is essential for the overall job's functionality.
*   **Filesystem Logging:**
    *   **Legacy:** Log messages and script output redirected to a dynamically named log file (`$LogDatei`).
    *   **Replacement:** All logging will be consolidated into a BigQuery table (`job_log_table`). Custom logging procedures will `INSERT` records into this table, capturing job ID, timestamp, message, and severity. This can be integrated with Cloud Logging for centralized log management.
*   **Operating System:**
    *   **Legacy:** Relies on standard Unix/Linux utilities and KornShell runtime environment.
    *   **Replacement:** BigQuery provides a serverless SQL execution environment. Shell-specific commands (`print`, `tee`, `exit`, `trap`) will be replaced by BigQuery SQL equivalents and procedural constructs.

## 7. Unresolved / Risks
*   **Kernel Script Complexity:** The most significant unknown is the complexity and migration path for `k_ausd_v_ta_barrier.ksh`. Its internal logic, data sources, transformations, and dependencies will heavily influence the overall migration effort and success.
*   **Undocumented Parameter Usage (`-s`, `-l`):** The original script's `getopts` accepts `-s` and `-l`, but their usage is not visible within the provided wrapper script. Their actual function and any dependencies they introduce need to be clarified to ensure complete migration.
*   **`DWMSG_*` Implementation Details:** The specific internal logic of the `DWMSG_*` utility functions (e.g., how `DW_EintragsNr` is generated, where status is persisted) is not fully known. Assumptions have been made that they interact with a logging/status system that can be replicated in BigQuery tables. A deeper dive into these scripts might be required.
*   **Error Handling Granularity:** While BigQuery scripting provides `EXCEPTION WHEN ERROR THEN`, mapping the exact nuances of shell `trap` behavior (e.g., specific signals, exit codes) might require careful design, potentially involving the orchestration layer for more advanced fault tolerance.
*   **Sourced Environment (`.dw_init`):** The content of `$HOME/.dw_init` is unknown. If it contains complex environment setups or paths specific to the legacy system, these will need careful translation to BigQuery configuration or procedure parameters.

## 8. Build Plan

1.  **Analysis of `k_ausd_v_ta_barrier.ksh` (High Priority):**
    *   **Goal:** Understand the core reconciliation logic, data sources, and transformations.
    *   **Output:** Detailed migration design for `k_ausd_v_ta_barrier.ksh` to BigQuery SQL Stored Procedure(s).
    *   **Language:** N/A (analysis phase)

2.  **Migrate Utility Scripts to BigQuery Stored Procedures/UDFs:**
    *   **Files:** `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and relevant parts of `$HOME/.dw_init`.
    *   **Output:**
        *   `DWMSG_MeldeFehler_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_ErmittleNr_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_Logdateiname_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_ErzeugeEintrag_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_SetzeStichtagInfo_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_Fehlerbehandlung_SP` (BigQuery SQL Stored Procedure)
        *   `DWMSG_SetzeStatusOK_SP` (BigQuery SQL Stored Procedure)
        *   Any necessary date/parameter UDFs.
    *   **Language:** BigQuery SQL

3.  **Create BigQuery Logging and Status Tables:**
    *   **Output:**
        *   `job_log_table` (BigQuery Table Schema)
        *   `job_status_table` (BigQuery Table Schema, if separate status table is implied)
    *   **Language:** BigQuery DDL

4.  **Implement `k_ausd_v_ta_barrier_sp` (BigQuery Stored Procedure):**
    *   **Logic:** Based on the design from Step 1.
    *   **Output:** `k_ausd_v_ta_barrier_sp.sql` (BigQuery SQL Stored Procedure)
    *   **Language:** BigQuery SQL

5.  **Create `r_ausd_v_ta_barrier_sp` (Main Wrapper BigQuery Stored Procedure):**
    *   **Logic:** Implement the BigQuery SQL pseudocode detailed in Section 5.
    *   **Output:** `r_ausd_v_ta_barrier_sp.sql` (BigQuery SQL Stored Procedure)
    *   **Language:** BigQuery SQL

6.  **Develop Orchestration Component:**
    *   **Tool:** Cloud Composer (Airflow) is recommended for its extensibility and control flow capabilities.
    *   **Logic:** Create an Airflow DAG that schedules and executes `r_ausd_v_ta_barrier_sp` using the `BigQueryExecuteStoredProcedureOperator` or a `BigQueryInsertJobOperator` for scripted BQ statements. Handle parameter passing, monitoring, and error reporting.
    *   **Output:** `r_ausd_v_ta_barrier_dag.py` (Python Airflow DAG)
    *   **Language:** Python