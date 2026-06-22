# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope

This job, `k_ausd_bp_ta_rn_da_vda_tk.ksh`, is a KornShell control script primarily responsible for orchestrating the execution of a core SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) within the legacy environment. Its main functions include:
*   Loading environment variables and utility functions.
*   Parsing and validating command-line parameters such as job identifier (`p_JobKennung`), entry number (`p_EintragsNr`), and business date (`p_Stichtag`).
*   Performing date format validation.
*   Determining "today" and "yesterday" dates.
*   Executing the primary SQL script with the parsed parameters.
*   Capturing and reporting the record count resulting from the SQL execution.
*   Error handling and logging.

The scope of this migration design is to re-implement this orchestration logic and its associated SQL processing on the Google Cloud BigQuery platform.

## 2. Source Inventory

The job consists of a single primary file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`
    *   **Technology:** KornShell Script
    *   **Purpose:** ETL Orchestrator / Control Script
    *   **Summary:** Control script that initializes the environment, parses parameters, performs date validation, and orchestrates the execution of a core SQL script for data processing. It also contains commented-out sections for file-based data reformatting and joining.
    *   **Complexity Tier:** medium
    *   **Migration Automation Bucket:** semi_auto

**Dependencies Identified from Script Content:**

The KornShell script itself has several internal dependencies, primarily other shell scripts and one SQL script:

*   `$HOME/.dw_init`: Environment initialization.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility for validation.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: Helper for SQL execution (implies `sqlplus`).
*   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Script to determine yesterday's and today's dates.
*   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql`: The core SQL script containing the main business logic.

## 3. Target Architecture

The migrated solution will primarily reside within Google BigQuery as a Stored Procedure, leveraging BigQuery's native SQL capabilities and a potential orchestration layer (e.g., Cloud Composer/Airflow or native BigQuery Scheduled Queries) for scheduling and parameter passing.

*   **Orchestration:** BigQuery Stored Procedure, potentially invoked by an external scheduler (e.g., Cloud Composer).
*   **Data Processing:** BigQuery SQL within the Stored Procedure (migrated from `d_ausd_bp_ta_rn_da_vda_tk.sql`).
*   **Parameter Handling:** Stored Procedure parameters will replace command-line arguments.
*   **Logging:** Dedicated BigQuery logging tables will capture execution details, errors, and process messages, replacing console outputs and temporary files.
*   **Error Handling:** BigQuery's `ASSERT` statements and `RAISE` for controlled error exits, with error details inserted into a BigQuery error log table.
*   **Date Derivations:** BigQuery's native date functions like `CURRENT_DATE()` and `DATE_SUB()` will replace external shell scripts (`gestern.ksh`).
*   **Temporary Data:** Transient tables or Common Table Expressions (CTEs) within BigQuery SQL will replace temporary files.

## 4. Data Flow & Lineage

The original KornShell script acts as a sequential orchestrator. The migrated data flow will mirror this logical sequence within BigQuery:

1.  **Parameter Ingestion:** Input parameters (job ID, entry number, business date, restart value) are passed as arguments to the BigQuery Stored Procedure.
2.  **Environment Setup & Validation:**
    *   Internal BigQuery variables will be declared, replacing shell environment variables.
    *   Parameter validation (presence, date format) will be performed using BigQuery SQL conditional logic (`IF`, `ASSERT`).
    *   Error logging will occur for any validation failures, leading to procedure termination.
3.  **Date Calculation:** `CURRENT_DATE()` and `DATE_SUB()` will determine "today" and "yesterday" dates.
4.  **Core Business Logic Execution:** The logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` will be incorporated directly into the BigQuery Stored Procedure (or called as a nested stored procedure/script). This step performs the primary data transformations and data loading.
5.  **Record Count Capture:** After the core SQL logic, the count of processed records will be obtained from the target BigQuery table or a temporary result set.
6.  **Job Status Logging:** The captured record count and job status information will be inserted into a BigQuery job status/log table (replacing the commented-out `FOSJobErzeugeEintrag` call and the `tmpFile`).

## 5. Transformation Logic

The KornShell script itself contains minimal data transformation logic, serving mainly as an execution wrapper. The primary transformation logic is expected to reside in the external SQL script `d_ausd_bp_ta_rn_da_vda_tk.sql`.

**Migration of KornShell Constructs to BigQuery SQL:**

*   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These will be replaced by BigQuery Stored Procedure parameters, session variables, or configuration tables.
*   **Parameter Parsing (`getopts`):** Replaced by direct Stored Procedure input parameters.
*   **Conditional Logic (`if`, `case`):** Migrates directly to BigQuery SQL `IF...THEN...END IF` or `CASE` statements. `ASSERT` will be used for validation and error handling.
*   **Date Operations (`gestern.ksh`, `DWDate_Datum_Check`):** Replaced by `CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()` for parsing, and string manipulation/regex for format validation.
*   **External Script Execution (`starteSQLSkript`):** The functionality of executing an external SQL file will be replaced by embedding the SQL logic directly into the BigQuery Stored Procedure. The `h_alis_sqlplus.ksh` helper's role will become obsolete as BigQuery SQL handles direct DDL/DML.
*   **File I/O (`tmpFile`, `cat`, `sed`, `sort`, `join`):**
    *   The temporary file for record count will be replaced by a BigQuery variable (`DECLARE v_records INT64`) or by directly querying the target table count.
    *   The commented-out `sed`, `sort`, `join` operations (if ever activated) would need to be re-implemented using BigQuery SQL (e.g., `REPLACE`, `ORDER BY`, window functions, or multi-table joins).

**Pseudocode Example (from MCP tool, adapted):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_rn_da_vda_tk`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected format DDMMYYYY
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_errnr INT64 DEFAULT 0;

  -- Initialize restart value if null
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_errnr = 1; SET v_err = 'Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_errnr = 1; SET v_err = 'Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_errnr = 1; SET v_err = 'EintragsNr fehlt';
  END IF;

  -- Error Handling
  IF v_errnr <> 0 THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, v_errnr, v_err);
    RAISE USING MESSAGE = CONCAT('FEHLER: ', CAST(v_errnr AS STRING), ' ', v_err);
  END IF;

  -- Date Validation and Conversion
  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.error_log`
    VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, 194, 'Ungueltiges Datumsformat fuer Stichtag');
    RAISE USING MESSAGE = 'FEHLER: Ungueltiges Datumsformat fuer Stichtag';
  END;

  -- Log process start
  INSERT INTO `project.dataset.process_log`
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, 'Pruefe Datum OK');

  -- Placeholder for Core Business Logic from d_ausd_bp_ta_rn_da_vda_tk.sql
  -- This section must be replaced with the actual BigQuery SQL code.
  -- Example:
  -- INSERT INTO `project.dataset.target_table` (col1, col2, ..., business_date)
  -- SELECT ...
  -- FROM `project.dataset.source_table`
  -- WHERE business_date = v_stichtag_date;

  -- Simulate record count capture (replace with actual count from transformed data)
  -- SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE business_date = v_stichtag_date);
  SET v_records = 12345; -- Placeholder

  -- Log job entry (migrated from FOSJobErzeugeEintrag)
  INSERT INTO `project.dataset.job_table`
  VALUES (
    CURRENT_TIMESTAMP(),
    v_TabName,
    'A', 'I',
    v_stichtag_date, v_stichtag_date,
    'J', 'N',
    v_records,
    'Initialbefuellung'
  );

  -- Log process end
  INSERT INTO `project.dataset.process_log`
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, '---------- ENDE Datenverarbeitung ----------');
END;
```

## 6. External Dependencies

The `lineage_assembled_jobs` record indicates no external systems are directly associated with this specific assembled job (`external_systems: []`). Similarly, `lineage_unresolved` was empty.

Based on the script content analysis:

*   **Legacy DB Connection (via `sqlplus`):** The script leverages `h_alis_sqlplus.ksh` to execute a SQL script, implying a connection to an Oracle or similar SQL database.
    *   **Replacement:** The target BigQuery solution will connect directly to BigQuery tables. The original SQL logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` will be rewritten for BigQuery SQL syntax and executed natively.
*   **Filesystem (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** The script heavily relies on a local filesystem hierarchy for other scripts and temporary files.
    *   **Replacement:** All script invocations will be internalized into the BigQuery Stored Procedure or managed by the orchestration layer. Temporary file usage will be replaced by BigQuery temporary tables, CTEs, or direct logging tables.

## 7. Unresolved / Risks

*   **Core SQL Logic (`d_ausd_bp_ta_rn_da_vda_tk.sql`):** The design produced by the MCP tool focuses on the shell script orchestration. The actual business logic within `d_ausd_bp_ta_rn_da_vda_tk.sql` was not available for analysis in this context. This SQL script must be separately analyzed and migrated to BigQuery SQL, potentially as the body of the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure. This is the most significant unknown.
*   **`h_alis_job.ksh` and `FOSJobDeaktivate`/`FOSJobErzeugeEintrag`:** These job management functions were commented out in the source script but indicate a broader job control framework. If these were ever active or are needed for future functionality, a BigQuery-native job management system (e.g., metadata tables, Cloud Composer DAGs) would need to be designed and implemented. The current design includes a placeholder `job_table` for `FOSJobErzeugeEintrag`.
*   **Commented-out file processing (`sed`, `sort`, `join`):** The commented-out sections for file reformatting and joining (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) suggest potential legacy data processing steps. If these are active in other variants or need to be revived, their logic would require careful translation into BigQuery SQL.
*   **Complexity:** The job is classified as 'medium' complexity, and 'semi_auto' automation bucket, primarily due to the orchestration logic and external script dependencies that need re-architecting for BigQuery. The migration of the core SQL logic (not yet analyzed) could increase this complexity.

## 8. Build Plan

The build plan focuses on implementing the BigQuery Stored Procedure and supporting BigQuery assets.

1.  **Define BigQuery Datasets:** Create the necessary BigQuery dataset(s) (e.g., `project.dataset`) for the stored procedure, log tables, and target data.
2.  **Create Logging and Metadata Tables:**
    *   `project.dataset.error_log`: For capturing procedural errors.
    *   `project.dataset.process_log`: For capturing execution progress and informational messages.
    *   `project.dataset.job_table`: For tracking job status and record counts (replacing `FOSJobErzeugeEintrag`).
3.  **Migrate Core SQL Logic:**
    *   Analyze `d_ausd_bp_ta_rn_da_vda_tk.sql` for BigQuery compatibility.
    *   Rewrite `d_ausd_bp_ta_rn_da_vda_tk.sql` into BigQuery-compliant SQL, ensuring syntax, data types, and functions are correct.
    *   Integrate this rewritten SQL logic into the main stored procedure.
4.  **Develop BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL
    *   **Name:** `r_ausd_bp_ta_rn_da_vda_tk` (or similar, following BigQuery naming conventions)
    *   **Parameters:** `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (STRING), `p_wiederanlaufWert` (INT64)
    *   **Implementation:** Implement the logic as described in Section 5 (Transformation Logic) using BigQuery SQL.
        *   Include parameter validation and date parsing.
        *   Implement error handling using `ASSERT` and logging to `error_log`.
        *   Utilize `CURRENT_DATE()`, `DATE_SUB()` for date derivations.
        *   Embed the migrated core SQL logic.
        *   Update `job_table` and `process_log` with execution status.
5.  **Testing:**
    *   Unit tests for the BigQuery Stored Procedure with various parameter inputs, including edge cases (missing parameters, invalid date formats).
    *   Integration tests to verify data flow and correctness of transformations.
6.  **Orchestration (Optional, if external scheduling is required):**
    *   Create a Cloud Composer DAG or BigQuery Scheduled Query to invoke the `r_ausd_bp_ta_rn_da_vda_tk` stored procedure, passing necessary parameters.
    *   Configure scheduling and monitoring for the job.