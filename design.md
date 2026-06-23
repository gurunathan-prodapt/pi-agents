# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`, which acts as a control script for a data processing workflow. Its primary purpose is to:
- Parse and validate input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
- Perform date validation for `p_Stichtag`.
- Orchestrate the execution of a core SQL script, identified as `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql`, passing validated parameters.
- Capture the number of records processed by the SQL script into a temporary file.
- Potentially log job status (currently commented out).

The scope of this migration is to translate this shell-based orchestration and its dependencies to Google Cloud Platform, specifically utilizing BigQuery Stored Procedures for the core logic and potentially minimal external orchestration if required.

## 2. Source Inventory
This job comprises a single source file with the following characteristics:

| File Name                                                         | Technology  | Complexity Tier | Automation Bucket | Purpose                                                                                                                                                                                                                         |
|:------------------------------------------------------------------|:------------|:----------------|:------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh` | KornShell   | Medium          | Semi-Auto         | A control script responsible for parameter parsing, validation, environment setup, and orchestrating the execution of a core SQL script for data processing.                                                                     |

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery's capabilities, with the following target components:
- **BigQuery Stored Procedure:** The core logic of `k_ausd_bp_ta_cntrct_dist.ksh`, including parameter parsing, validation, and the execution of the translated SQL logic from `d_ausd_bp_ta_cntrct_dist.sql`, will be encapsulated within a BigQuery Stored Procedure named `project.dataset.r_ausd_bp_ta_cntrct_dist`.
- **BigQuery Tables:**
    - Target tables for the data processed by the SQL script.
    - An optional job logging table (if the commented-out `FOSJobErzeugeEintrag` functionality becomes active).
- **Orchestration (Optional):** If external systems or complex file I/O (beyond what BigQuery can natively handle) are required, a minimal Python-based orchestrator (e.g., Cloud Functions, Cloud Run, or Airflow DAG on Cloud Composer) might be introduced to invoke the BigQuery Stored Procedure. However, the goal is to keep as much logic as possible within BigQuery.

## 4. Data Flow & Lineage
The original data flow involves the KornShell script orchestrating an SQL script. The migrated flow will simplify this by largely moving the orchestration into BigQuery.

**Original Flow:**
1. `k_ausd_bp_ta_cntrct_dist.ksh` (KornShell script) starts.
2. It sources environment and utility scripts:
    - `$HOME/.dw_init`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
3. Parameters are parsed (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
4. Parameters and `p_Stichtag` date format are validated.
5. The `starteSQLSkript` function is called, which executes the SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql`.
6. The SQL script performs data processing (reads from source, writes to target).
7. The record count is written to `$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp`.
8. The count is read back into the shell script.
9. (Commented out) `FOSJobErzeugeEintrag` might update a job table.

**Target BigQuery Flow:**
1. An external orchestrator (e.g., a scheduler or a Python script) invokes the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_cntrct_dist`, passing `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as parameters.
2. Inside the Stored Procedure:
    - Input parameters are declared and assigned.
    - Parameter validation (e.g., `IF EXISTS`, `RAISE USING MESSAGE`) is performed.
    - Date parsing and validation (`PARSE_DATE`) is performed.
    - The core SQL logic (translated from `d_ausd_bp_ta_cntrct_dist.sql`) is executed directly within the stored procedure, performing data reads and writes.
    - Record counts are obtained via `SELECT COUNT(*)` queries on the target tables.
    - (If activated) Job logging to a BigQuery control table is performed.
3. The Stored Procedure completes, and the orchestrator receives the status.

## 5. Transformation Logic
The transformation logic primarily involves translating the KornShell orchestration to BigQuery scripting language and porting the embedded SQL script.

**KornShell to BigQuery Scripting:**
- **Parameter Parsing:** The `getopts` mechanism will be replaced by direct input parameters to the BigQuery Stored Procedure.
- **Parameter Validation:** Shell `if [ ! $ErrNr -eq 0 ]` and `pruefeParameterGesetzt` calls will be replaced by BigQuery scripting `IF` statements and `RAISE USING MESSAGE` for error handling.
- **Date Validation:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be replaced by `PARSE_DATE('%d%m%Y', p_Stichtag)` within BigQuery, potentially wrapped in a `TRY_CAST` or `IF` for error handling.
- **Temporary File for Record Count:** The `cat $tmpFile` operation will be replaced by `SELECT COUNT(*)` directly on the BigQuery target tables within the stored procedure.
- **Date Calculation (`gestern.ksh`):** The external `gestern.ksh` execution will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
- **SQL Script Execution (`starteSQLSkript`):** The `starteSQLSkript` wrapper will be removed, and the content of `d_ausd_bp_ta_cntrct_dist.sql` will be directly embedded and translated into standard BigQuery SQL within the stored procedure.
- **Environment Sourcing:** The sourcing of `.dw_init` and other utility scripts will be replaced by defining required variables and functions directly within the BigQuery Stored Procedure or by passing configuration as parameters.

**SQL Script (`d_ausd_bp_ta_cntrct_dist.sql`) to BigQuery SQL:**
- This design assumes `d_ausd_bp_ta_cntrct_dist.sql` contains standard SQL statements (e.g., `INSERT`, `SELECT`, `UPDATE`). These will be translated to BigQuery SQL, addressing any dialect differences (e.g., data types, function names, specific syntax).
- If `d_ausd_bp_ta_cntrct_dist.sql` contains procedural logic (cursors, loops, etc.), it will need to be converted to BigQuery Scripting language or BigQuery user-defined functions/stored procedures.

**Example BigQuery Stored Procedure Pseudocode:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart STRING DEFAULT '0';

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart = '0';
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Required parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    RAISE USING MESSAGE = 'FEHLER: Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    RAISE USING MESSAGE = 'FEHLER: Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    RAISE USING MESSAGE = 'FEHLER: EintragsNr fehlt';
  END IF;

  -- Date validation and conversion
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- Execute translated SQL logic from d_ausd_bp_ta_cntrct_dist.sql
  -- This block will contain the actual BigQuery SQL from the migrated script.
  BEGIN
    -- Example: INSERT INTO target_table (...) SELECT ... FROM source_table WHERE some_date_column = v_stichtag_date;
    -- Actual SQL will replace this placeholder.
  END;

  -- Record count
  -- Replace 'target_table' and 'business_date' with actual table and column names
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.target_table`
    WHERE business_date = v_stichtag_date
  );

  -- Optional job logging (if functionality is activated)
  -- INSERT INTO `project.dataset.job_table` (tab_name, status, type_code, ...)
  -- VALUES (v_TabName, 'A', 'I', ...);

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

## 6. External Dependencies
The original script has no recorded external systems in `lineage_external_systems`. However, based on the script content:

- **Legacy Database:** The `d_ausd_bp_ta_cntrct_dist.sql` script implicitly interacts with a database (likely Oracle, given the `sqlplus` utility script).
    - **Replacement:** This database will be replaced by BigQuery for both source data and target data storage. Data will be ingested into BigQuery using appropriate data loading mechanisms (e.g., Cloud Storage, Dataflow, BigQuery Data Transfer Service) prior to this job's execution.
- **Filesystem Utilities (`gestern.ksh`, temporary files):**
    - **Replacement:** `gestern.ksh` functionality will be replaced by BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB`). Temporary file usage for record counts will be replaced by direct `COUNT(*)` queries within BigQuery.
- **Environment Variables (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`):**
    - **Replacement:** These will be replaced by BigQuery project/dataset references, Stored Procedure parameters, or configurable deployment parameters.

## 7. Unresolved / Risks
- **Unresolved Targets:** The lineage analysis reported no `unresolved_targets`.
- **SQL Script `d_ausd_bp_ta_cntrct_dist.sql` Content:** The most significant unknown is the actual content and complexity of `d_ausd_bp_ta_cntrct_dist.sql`.
    - **Risk:** If this SQL script contains highly procedural logic, complex PL/SQL, or non-standard SQL constructs, its migration to BigQuery SQL may be more complex than a direct translation and might require BigQuery Scripting, UDFs, or even a separate Dataflow/Spark job if it involves logic not well-suited for pure SQL.
    - **Mitigation:** A detailed analysis of `d_ausd_bp_ta_cntrct_dist.sql` is required as a follow-up step.
- **Commented-out Code:** The script contains commented-out sections for `sed`, `sort`, `join`, and `FOSJobDeaktivate`/`FOSJobErzeugeEintrag`.
    - **Risk:** If these functionalities are intended to be re-activated, they represent additional scope.
    - **Mitigation:** Clarify with business stakeholders whether these functionalities are still required. If so, they need to be integrated into the BigQuery design (e.g., `sed`/`sort`/`join` via BigQuery SQL, job logging via BigQuery tables).
- **`starteSQLSkript` Abstraction:** The `starteSQLSkript` function is an abstraction for executing SQL.
    - **Risk:** Its exact behavior regarding connection management, error handling, and parameter passing might hide complexities not immediately apparent.
    - **Mitigation:** Assume standard SQL*Plus execution; however, if issues arise during translation, a deeper dive into `h_alis_sqlplus.ksh` might be necessary.

## 8. Build Plan
The build plan focuses on implementing the BigQuery Stored Procedure and integrating it into an orchestration layer.

1.  **Analyze `d_ausd_bp_ta_cntrct_dist.sql`:**
    *   **Action:** Obtain the content of `d_ausd_bp_ta_cntrct_dist.sql`.
    *   **Tool:** `read_source_file` (if available), or manual retrieval.
    *   **Output:** `d_ausd_bp_ta_cntrct_dist.sql` code.
2.  **Translate `d_ausd_bp_ta_cntrct_dist.sql` to BigQuery SQL:**
    *   **Action:** Convert the SQL script to BigQuery-compatible SQL.
    *   **Tool:** CM MCP `hql_sql_to_bqsql_design` or similar, if applicable, otherwise manual.
    *   **Output:** BigQuery SQL code for the core logic.
3.  **Create BigQuery Stored Procedure DDL:**
    *   **Action:** Assemble the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_cntrct_dist` using the translated SQL logic and the BigQuery scripting pseudocode provided in this design.
    *   **Language:** BigQuery SQL (DDL).
    *   **Output:** `r_ausd_bp_ta_cntrct_dist.sql` (Stored Procedure definition).
4.  **Define Target BigQuery Tables:**
    *   **Action:** Create DDL for any new or modified BigQuery target tables.
    *   **Language:** BigQuery SQL (DDL).
    *   **Output:** DDL for target tables.
5.  **Implement Job Logging (if required):**
    *   **Action:** If `FOSJobErzeugeEintrag` is to be reactivated, create a BigQuery table for job logging and add `INSERT` statements to the Stored Procedure.
    *   **Language:** BigQuery SQL (DDL and DML).
    *   **Output:** DDL for job log table, modifications to `r_ausd_bp_ta_cntrct_dist.sql`.
6.  **Develop Orchestration Layer (if needed):**
    *   **Action:** If external orchestration is chosen over direct BigQuery scheduling, develop a minimal Python script or Airflow DAG to invoke the BigQuery Stored Procedure.
    *   **Language:** Python (for orchestration).
    *   **Output:** `invoke_r_ausd_bp_ta_cntrct_dist.py` or Airflow DAG definition.
7.  **Testing Plan:**
    *   **Action:** Develop unit and integration tests for the BigQuery Stored Procedure, covering parameter validation, date handling, and data processing logic.
    *   **Language:** BigQuery SQL (for direct SP testing), Python (for orchestration testing).
    *   **Output:** Test scripts and data.