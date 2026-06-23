# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_bpr_beschr.ksh`, is a KornShell control script designed to orchestrate an ETL process. Its primary purpose is to initialize the environment, parse and validate input parameters (Job ID, Entry Number, Reference Date), perform date format checks, and then execute an associated SQL script, `d_ausd_bp_ta_bpr_beschr.sql`, with the validated parameters. The job is responsible for managing the execution of a database extraction/load workflow, primarily concerning the table `PoolBasisprodukt`. The scope of this migration is to re-implement this orchestration logic and its underlying SQL execution within the Google Cloud Platform, specifically leveraging BigQuery for data processing and storage.

## 2. Source Inventory
The job consists of a single KornShell script.

- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`
  - **Technology**: KornShell
  - **Complexity Tier**: medium
  - **Automation Bucket**: semi_auto
  - **Purpose**: ETL control script. Orchestrates environment setup, parameter validation, date validation, SQL script execution, and post-processing of record count. Intended to run a database extraction/load workflow for table/process `PoolBasisprodukt`.

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery. The KornShell orchestration logic will be translated into BigQuery scripting, stored procedures, or potentially an external orchestrator like Cloud Composer (Airflow) or Cloud Workflows if more complex inter-service dependencies arise.

- **Orchestration**: The main control flow, parameter validation, and execution of the SQL logic will be implemented as a BigQuery Stored Procedure, utilizing BigQuery's scripting capabilities (DECLARE, SET, IF, BEGIN...END).
- **SQL Logic**: The referenced SQL script (`d_ausd_bp_ta_bpr_beschr.sql`) will be converted into a separate BigQuery Stored Procedure or a set of BigQuery DDL/DML statements.
- **Data Storage**: The `PoolBasisprodukt` table and any intermediate staging tables will be created and managed within BigQuery datasets.
- **Parameter Handling**: Input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) will be passed as arguments to the BigQuery Stored Procedure.
- **Date Handling**: Date validation and derivation of `heute` (today) and `gestern` (yesterday) will use BigQuery's native date functions (e.g., `CURRENT_DATE()`, `DATE_SUB`, `SAFE.PARSE_DATE`).
- **Error Handling**: The existing error concept will be replaced by BigQuery's procedural error handling (`RAISE`, `ASSERT`) and logging to dedicated audit/log tables.
- **Record Counting**: Record counts will be obtained directly via `SELECT COUNT(*)` queries within BigQuery and can be stored in audit tables or returned as output parameters.
- **File Manipulation (Commented out in source)**: If any of the commented-out file processing (sed, sort, join) becomes necessary in the future, it will be replicated using BigQuery SQL transformations (e.g., `REGEXP_REPLACE`, `DISTINCT`, `JOIN`) and `EXPORT DATA` for file output if required.

## 4. Data Flow & Lineage
The original script `k_ausd_bp_ta_bpr_beschr.ksh` acts as an orchestrator.
1.  **Initialization**: Sources common environment variables and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Parsing & Validation**: Reads `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` from command-line arguments using `getopts`. It validates that `p_JobKennung`, `p_Stichtag`, and `p_EintragsNr` are set and that `p_Stichtag` is in `DDMMYYYY` format.
3.  **Date Derivation**: Calls `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
4.  **SQL Script Execution**: Sets the `Name_SQLskript` to `d_ausd_bp_ta_bpr_beschr.sql` and then calls `starteSQLSkript`, passing numerous parameters including the entry number, job key, business date, root directory, today's date, and yesterday's date. This `d_ausd_bp_ta_bpr_beschr.sql` is expected to interact with the `PoolBasisprodukt` table.
5.  **Record Count**: After the SQL script execution, it reads the number of records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_beschr.tmp`).
6.  **Job Logging**: The script contains commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` for job management, likely for status updates to a job tracking system.

While `lineage_edges` did not explicitly capture these relationships, the `file_analysis` data and the source code confirm the execution flow and dependencies, particularly the invocation of `d_ausd_bp_ta_bpr_beschr.sql` and interaction with `PoolBasisprodukt`.

## 5. Transformation Logic

The KornShell script's logic will be transformed into a BigQuery Stored Procedure, `project.dataset.r_ausd_bp_ta_bpr_beschr`.

**Original Logic (KornShell) -> Target Logic (BigQuery Stored Procedure)**

- **Environment Setup**: The sourcing of `.dw_init` and other utility scripts will be replaced by:
    - Direct parameter passing for configurable values (e.g., `BERT_DIR_ROOT` as a procedure parameter or job-level variable).
    - Implementing utility functions (date checks, parameter validation) directly within the BigQuery Stored Procedure or as separate helper procedures.
- **Parameter Parsing**: `getopts` logic will be replaced by BigQuery Stored Procedure input parameters:
    - `p_JobKennung` (STRING)
    - `p_EintragsNr` (STRING)
    - `p_Stichtag` (STRING)
    - `p_wiederanlaufWert` (INT64)
- **Parameter Validation**: The `pruefeParameterGesetzt` calls and `if [ ! $ErrNr -eq 0 ]` blocks will translate to BigQuery `IF` statements and `RAISE` errors:
    ```sql
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
      RAISE USING MESSAGE = 'Jobkennung fehlt';
    END IF;
    -- Similar checks for p_Stichtag and p_EintragsNr
    ```
- **Date Validation**: `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will use `SAFE.PARSE_DATE`:
    ```sql
    DECLARE v_datum DATE;
    SET v_datum = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_datum IS NULL THEN
      RAISE USING MESSAGE = 'Ungueltiges Datumformat';
    END IF;
    ```
- **Date Derivation**: `set \`\${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh\`` will be replaced by BigQuery's date functions:
    ```sql
    DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
    DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    ```
- **SQL Script Execution**: The `starteSQLSkript` call will be replaced by a `CALL` statement to a BigQuery Stored Procedure that encapsulates the `d_ausd_bp_ta_bpr_beschr.sql` logic.
    ```sql
    CALL `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
      p_EintragsNr, p_JobKennung, p_Stichtag, v_restart, v_datum_heute, v_datum_gestern
    );
    ```
- **Record Count**: `eval "v_records=\`cat $tmpFile\`"` will be replaced by a `SELECT COUNT(*)` into a variable:
    ```sql
    DECLARE v_records INT64 DEFAULT 0;
    SELECT COUNT(*) INTO v_records FROM `project.dataset.PoolBasisprodukt` WHERE ...; -- Add appropriate WHERE clause
    ```
- **Job Logging (Commented)**: The `FOSJobErzeugeEintrag` call can be replaced by an `INSERT` statement into a BigQuery audit table.

**Example BigQuery Stored Procedure Pseudocode:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum DATE;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_restart INT64 DEFAULT 0;

  -- Default restart value
  SET v_restart = IFNULL(p_wiederanlaufWert, 0);

  -- Required parameter checks
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    RAISE USING MESSAGE = 'Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    RAISE USING MESSAGE = 'Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    RAISE USING MESSAGE = 'EintragsNr fehlt';
  END IF;

  -- Date validation for DDMMYYYY
  SET v_datum = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_datum IS NULL THEN
    RAISE USING MESSAGE = 'Ungueltiges Datumformat';
  END IF;

  -- Execute business SQL logic (assuming d_ausd_bp_ta_bpr_beschr.sql is converted to a stored procedure)
  CALL `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
    p_EintragsNr, p_JobKennung, p_Stichtag, v_restart, v_datum_heute, v_datum_gestern
  );

  -- Record count retrieval (example, adjust WHERE clause as per actual SQL script)
  SELECT COUNT(*) INTO v_records FROM `project.dataset.PoolBasisprodukt` WHERE TRUE; -- Placeholder

  -- Persist audit / job entry
  INSERT INTO `project.dataset.job_audit_table` (
    tab_name, job_status, load_type, stichtag, run_date, job_kind, restart_flag, record_count, message
  )
  VALUES (
    v_TabName, 'A', 'I', p_Stichtag, CAST(p_Stichtag AS DATE FORMAT 'DDMMYYYY'), 'J', 'N', v_records, 'Initialbefuellung'
  );
END;
```

## 6. External Dependencies
The original script does not explicitly interact with external systems (like Oracle, SFTP, S3) based on the `external_systems` analysis. All dependencies are local file system scripts or environment variables.

- **Environment initialization file (`$HOME/.dw_init`)**: This will be replaced by BigQuery dataset/project variables, procedure parameters, or environment variables managed by the orchestration layer (e.g., Cloud Composer).
- **Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: These shell utilities will be re-implemented as BigQuery native functions or stored procedures. For example, `h_alis_date.ksh` for date checks will use `SAFE.PARSE_DATE`. `h_alis_sqlplus.ksh`'s functionality (executing SQL) will be intrinsic to the BigQuery stored procedure calling other SQL procedures.
- **External script (`gestern.ksh`)**: This script's date calculation logic will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB` functions.
- **SQL script (`d_ausd_bp_ta_bpr_beschr.sql`)**: This is a critical dependency. This SQL script will be migrated to a BigQuery Stored Procedure, and its invocation will be a direct `CALL` from the main orchestration procedure.
- **Temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_beschr.tmp`)**: The temporary file used for record counting will be replaced by a BigQuery variable or a control table.
- **Job management calls (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`)**: These commented-out calls imply an external job management system. This functionality should be replaced by inserts into a BigQuery audit/control table or by integrating with GCP's native logging and monitoring services (e.g., Cloud Logging, Cloud Monitoring).

## 7. Unresolved / Risks
- **Unresolved Targets**: `lineage_assembled_jobs` reported no `unresolved_targets`.
- **Complexity Signals / File Purpose**: `file_analysis` for the source script showed empty `complexity_signals` and `file_purpose`. While a summary was available, a more detailed analysis of the original script's specific business logic for transformations would be beneficial.
- **Actual SQL Script (`d_ausd_bp_ta_bpr_beschr.sql`)**: The most critical unresolved part is the content and complexity of `d_ausd_bp_ta_bpr_beschr.sql`. The design assumes this SQL can be directly translated into BigQuery SQL and encapsulated in a stored procedure. A separate detailed migration design for this SQL script is required.
- **Commented-out Code**: The KornShell script contains significant commented-out sections for file manipulation (`sed`, `sort`, `join`). While these are currently inactive, their potential future relevance and migration strategy should be confirmed with business users. The current design proposes BigQuery SQL equivalents if they become active.
- **`starteSQLSkript` function**: The exact implementation of `starteSQLSkript` in the original environment is not fully known. The design assumes it's a wrapper for `sqlplus` or similar and can be adequately replaced by a direct BigQuery stored procedure call or dynamic SQL. Any advanced features of `starteSQLSkript` (e.g., specific error handling, retry mechanisms) would need to be replicated in BigQuery or the orchestration layer.
- **Job-table Integration**: The commented `FOSJobErzeugeEintrag` suggests an existing job control/auditing mechanism. The replacement with `job_audit_table` is a placeholder and needs specific definition based on the existing system's functionality.

## 8. Build Plan
The migration will involve creating the following components:

1.  **BigQuery Stored Procedure for Orchestration (BQSQL)**:
    -   `project.dataset.r_ausd_bp_ta_bpr_beschr`
    -   This procedure will encapsulate the parameter parsing, validation, date derivation, and invocation of the SQL logic.
2.  **BigQuery Stored Procedure for Business Logic (BQSQL)**:
    -   `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`
    -   This procedure will contain the translated SQL from the original `d_ausd_bp_ta_bpr_beschr.sql`. (Requires separate analysis and migration of the SQL file itself).
3.  **BigQuery Tables (BQSQL DDL)**:
    -   `project.dataset.PoolBasisprodukt`: Target table DDL.
    -   `project.dataset.job_audit_table`: DDL for a new audit table to replace the legacy job management system.
    -   (Optional) Staging tables as needed for the `d_ausd_bp_ta_bpr_beschr_proc` or if commented-out file processing becomes active.
4.  **Orchestration Layer (Cloud Composer / Python DAG)**:
    -   A Cloud Composer DAG (or Cloud Workflows) might be used to trigger the main BigQuery stored procedure, manage scheduling, and potentially handle any pre/post-processing or external interactions not suitable for BigQuery stored procedures. This would also manage the passing of parameters to the BigQuery procedure.
5.  **Configuration**:
    -   Configuration of BigQuery dataset and project.
    -   Deployment scripts for BigQuery stored procedures.
    -   (Optional) Cloud Composer/Airflow DAG definition in Python.