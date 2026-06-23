# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_disc_zusgf.ksh`. The script acts as a control and orchestration wrapper. Its primary purpose is to manage job execution, handle parameters, and orchestrate the execution of a SQL script, `d_ausd_v_ta_disc_zusgf.sql`, which in turn updates the `ta_disc_zusgf` table. This job was assembled from a single component and is described as a "Job assembled from 1 component(s); stage dist: medium=1". The script ensures that active jobs are handled appropriately, calls the core SQL logic, and records job status.

## 2. Source Inventory
The migration job consists of a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`
    *   **Technology:** Shell (KornShell)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2)
    *   **Summary:** Control script for `r_ausd_vertrag.ksh` that manages job execution, handles parameters, and orchestrates the execution of a SQL script (`d_ausd_v_ta_disc_zusgf.sql`) to update the `ta_disc_zusgf` table.

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data processing and possibly Cloud Composer (Apache Airflow) or Cloud Workflows for orchestration, given the script's control-flow nature.

*   **BigQuery Stored Procedure:** The core logic of `k_ausd_v_ta_disc_zusgf.ksh` (parameter parsing, validation, and orchestration of the SQL execution) will be migrated to a BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_vertrag_control`.
*   **BigQuery SQL Script/Stored Procedure:** The business logic contained within `d_ausd_v_ta_disc_zusgf.sql` will be converted to a standalone BigQuery SQL script or another BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_v_ta_disc_zusgf`.
*   **BigQuery Tables:**
    *   `ta_disc_zusgf`: The target table for data updates.
    *   `project.dataset.job_error_log`: A new logging table to capture error messages and details.
    *   `project.dataset.job_run_log`: A new logging table to track job execution status and metrics (e.g., records processed).
*   **Parameter Configuration:** Environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) and hardcoded values will be managed via BigQuery stored procedure parameters, session variables, or a dedicated configuration table.
*   **Orchestration (Optional):** If the calling context `r_ausd_vertrag.ksh` is also being migrated and requires external scheduling, Cloud Composer or Cloud Workflows can be used to trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script's data flow involves several steps:

1.  **Initialization:** The script sources several utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) for environment setup, error handling, date functions, parameter parsing, and SQL*Plus routines.
2.  **Parameter Input:** The script accepts two primary command-line parameters: `p_JobKennung` (via `-j`) and `p_EintragsNr` (via `-f`).
3.  **Parameter Validation:** Parameters are validated using `pruefeParameterGesetzt`. If validation fails, an error is reported via `DWMSG_MeldeFehler`, and the script exits.
4.  **SQL Script Execution:** The script defines `Name_SQLskript` as `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql`. It then calls the `starteSQLSkript` function, passing `p_EintragsNr`, `Name_SQLskript`, `p_EintragsNr`, and `p_JobKennung`. This function is responsible for executing the SQL script, presumably interacting with an Oracle database. The summary indicates this SQL script updates the `ta_disc_zusgf` table.
5.  **Record Count:** A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_disc_zusgf_$$.tmp`) is used to store the count of processed records, which is then read into the `v_records` variable.

In the BigQuery target architecture, this flow will be:

*   **Input:** The BigQuery Stored Procedure `r_ausd_vertrag_control` will accept `p_JobKennung` and `p_EintragsNr` as `IN` parameters.
*   **Parameter Validation:** SQL `IF` statements will replace shell conditionals for validation. Errors will be logged to `project.dataset.job_error_log` and potentially raise an SQLSTATE signal.
*   **Core Logic Execution:** The BigQuery Stored Procedure `r_ausd_vertrag_control` will directly call or embed the transformed BigQuery SQL for `d_ausd_v_ta_disc_zusgf.sql`, which will perform the DML operations on `ta_disc_zusgf`.
*   **Logging and Metrics:** Instead of temporary files, `COUNT(*)` or `ROW_COUNT` in BigQuery will determine the number of records processed, which will then be inserted into `project.dataset.job_run_log`.

## 5. Transformation Logic
The transformation will involve converting KornShell constructs and patterns into BigQuery SQL and Stored Procedure logic:

*   **Environment Variables:** Shell environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by BigQuery stored procedure parameters, BigQuery script variables, or values from a configuration table.
*   **Parameter Parsing (`getopts`):** The `getopts` logic will be replaced by direct `IN` parameters to the BigQuery Stored Procedure `r_ausd_vertrag_control`.
*   **Conditional Logic (`if` statements):** Shell `if` conditions (e.g., for parameter validation) will be translated into BigQuery `IF ... THEN ... END IF;` constructs within the stored procedure.
*   **Function Calls (`pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, `starteSQLSkript`):**
    *   `pruefeParameterGesetzt` will be absorbed into direct `IF` checks for `NULL` or empty parameters within the main stored procedure.
    *   `DWMSG_MeldeFehler` will be replaced by inserts into a BigQuery error logging table (`project.dataset.job_error_log`) and potentially BigQuery's `SIGNAL SQLSTATE` for error propagation.
    *   `starteSQLSkript`: This function's functionality (executing the SQL script and handling job registration/deactivation) will be directly implemented within the `r_ausd_vertrag_control` stored procedure. This will involve calling the BigQuery equivalent of `d_ausd_v_ta_disc_zusgf.sql` and performing necessary updates to job tracking tables.
*   **File Operations (`cat $tmpFile`):** The temporary file `tmpFile` used for transferring record counts will be replaced by direct assignment of `COUNT(*)` results from BigQuery DML operations into BigQuery script variables, or by inserting results into a dedicated logging table.
*   **SQL Script Content (`d_ausd_v_ta_disc_zusgf.sql`):** The actual SQL within this file must be translated from its current (likely Oracle) dialect to BigQuery Standard SQL. This is a separate, critical migration task.
*   **Strict Mode (`set -eu`):** BigQuery Stored Procedures inherently operate with strict error handling. Explicit error handling for specific conditions (e.g., parameter validation) will use `IF` statements and error logging.

**Pseudocode for BigQuery Stored Procedure (`r_ausd_vertrag_control`):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_disc_zusgf';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (error_ts, procedure_name, err_nr, err_arg, job_kennung, eintrags_nr)
    VALUES
    (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', ErrNr, ErrArg, p_JobKennung, p_EintragsNr);

    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  -- Call the core SQL logic (equivalent of d_ausd_v_ta_disc_zusgf.sql)
  -- This assumes d_ausd_v_ta_disc_zusgf has been migrated to a separate SP or as inline SQL
  CALL `project.dataset.d_ausd_v_ta_disc_zusgf`(p_EintragsNr, p_JobKennung);

  -- Example result counting - assuming d_ausd_v_ta_disc_zusgf updates ta_disc_zusgf
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.ta_disc_zusgf`
    WHERE eintragsnr = p_EintragsNr -- Example condition, adjust as per actual SQL logic
  );

  INSERT INTO `project.dataset.job_run_log`
  (log_ts, procedure_name, job_kennung, eintrags_nr, tab_name, records_processed, status)
  VALUES
  (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', p_JobKennung, p_EintragsNr, v_TabName, v_records, 'DONE');

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
  SELECT v_records AS records_processed;
END;
```

## 6. External Dependencies
The original script has several implicit and explicit dependencies that need to be addressed:

*   **Environment Initialization (`. $HOME/.dw_init`):** This file configures the shell environment. In BigQuery, equivalent configurations will be handled via stored procedure parameters, BigQuery project/dataset settings, or environment variables in a calling orchestrator (e.g., Cloud Composer).
*   **Utility Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These scripts provide common functions. Their functionalities will be replaced by native BigQuery capabilities:
    *   Error handling (`f_alis_msgerr.ksh`): Replaced by BigQuery error logging tables and `SIGNAL SQLSTATE`.
    *   Date functions (`h_alis_date.ksh`): Replaced by BigQuery's built-in date/time functions.
    *   Parameter parsing (`h_alis_parameter.ksh`): Replaced by BigQuery stored procedure parameters.
    *   SQL*Plus wrapper (`h_alis_sqlplus.ksh`): Replaced by direct BigQuery SQL execution.
*   **Core SQL Script (`d_ausd_v_ta_disc_zusgf.sql`):** This is a critical dependency, containing the actual data transformation logic. This script needs to be migrated separately to BigQuery Standard SQL. The `ta_disc_zusgf` table is implicitly an output target of this SQL script.
*   **Referenced Table (`ta_disc_zusgf`):** This table is updated by the core SQL logic. It will be migrated to a BigQuery table.
*   **Orchestrating Script (`r_ausd_vertrag.ksh`):** The summary indicates this script is a "control script for `r_ausd_vertrag.ksh`". This suggests an invocation hierarchy where `r_ausd_vertrag.ksh` calls `k_ausd_v_ta_disc_zusgf.ksh`. The migration of `r_ausd_vertrag.ksh` will need to be coordinated, and its invocation of this script will need to be updated to call the new BigQuery Stored Procedure.

No external systems (like Oracle, SFTP, S3) were explicitly identified in the `lineage_assembled_jobs` or `file_analysis` data for this specific script, though the `d_ausd_v_ta_disc_zusgf.sql` likely interacts with a database (presumably Oracle given the `SQL*Plus` context).

## 7. Unresolved / Risks
*   **Content of `d_ausd_v_ta_disc_zusgf.sql`:** The actual transformation logic within this SQL script is not analyzed in this design. Its complexity, source database dialect (likely Oracle), and specific DML operations will significantly influence its migration effort to BigQuery Standard SQL. This is the **primary unresolved item and risk**.
*   **`starteSQLSkript` Implementation:** The exact logic within `starteSQLSkript` related to job registration/deactivation and how it interacts with the database is not fully detailed in the provided shell script. This functionality needs to be reverse-engineered and re-implemented in BigQuery SQL.
*   **`r_ausd_vertrag.ksh` Context:** The fact that `k_ausd_v_ta_disc_zusgf.ksh` is a "control script for `r_ausd_vertrag.ksh`" implies `r_ausd_vertrag.ksh` is its caller. The migration of `r_ausd_vertrag.ksh` is crucial, and its call to `k_ausd_v_ta_disc_zusgf.ksh` must be updated to invoke the new BigQuery Stored Procedure. The parameters passed from `r_ausd_vertrag.ksh` to `k_ausd_v_ta_disc_zusgf.ksh` should be identified and maintained in the new BigQuery interface.
*   **`tmpFile` Content and Origin:** The script reads `v_records` from `tmpFile`. The content and the process that populates this temporary file (likely the `starteSQLSkript` function or the `d_ausd_v_ta_disc_zusgf.sql` script) needs to be fully understood to replicate the record counting logic accurately in BigQuery.
*   **`semi_auto` Migration Bucket:** The `semi_auto` bucket indicates that some manual intervention or oversight will be required. This is consistent with the need to manually analyze and transform the referenced SQL script and re-implement utility functions.

## 8. Build Plan
The build plan will involve the following steps:

1.  **Migrate `d_ausd_v_ta_disc_zusgf.sql` to BigQuery SQL:**
    *   **Language:** BigQuery Standard SQL
    *   **Output:** `project.dataset.d_ausd_v_ta_disc_zusgf` (either a SQL script or a BigQuery Stored Procedure). This is the highest priority and will be a separate design/migration effort.
2.  **Create Logging Tables in BigQuery:**
    *   **Language:** BigQuery DDL
    *   **Output:** `project.dataset.job_error_log`, `project.dataset.job_run_log` tables.
3.  **Develop BigQuery Stored Procedure for `k_ausd_v_ta_disc_zusgf.ksh`:**
    *   **Language:** BigQuery Standard SQL (Stored Procedure)
    *   **Output:** `project.dataset.r_ausd_vertrag_control` stored procedure, encapsulating parameter handling, validation, error logging, and invocation of `project.dataset.d_ausd_v_ta_disc_zusgf`.
4.  **Update Caller (if applicable):**
    *   **Language:** Dependent on `r_ausd_vertrag.ksh`'s technology (e.g., if it's another shell script, then update shell script; if it's an orchestrator, update its configuration).
    *   **Output:** Modified invocation of `k_ausd_v_ta_disc_zusgf.ksh` to call `project.dataset.r_ausd_vertrag_control` in BigQuery.
5.  **Deployment and Orchestration:**
    *   **Language:** YAML/Python (if Cloud Composer/Workflows)
    *   **Output:** Configuration for deploying the BigQuery components and setting up an optional orchestrator.