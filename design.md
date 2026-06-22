# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

## 1. Purpose & Scope
This job, identified by `run_id 5af228f1-3847-4cc6-9310-ed82ed19407c`, is a control script (`k_ausd_bp_ta_bpr_basis_his.ksh`) responsible for orchestrating a data processing workflow. Its primary purpose is to:
*   Parse and validate runtime parameters (`JobKennung`, `EintragsNr`, `Stichtag`, `wiederanlaufWert`).
*   Prepare the execution environment by sourcing common utility shell scripts.
*   Validate the format of the `Stichtag` parameter.
*   Determine "today" and "yesterday" dates.
*   Execute a core SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) that performs the actual data processing.
*   Record the number of processed records, typically for auditing or monitoring.
The script acts as a wrapper around a database extraction/load (ETL_LOAD) process that targets the `PoolBasisprodukt` table and potentially produces a temporary file `bert_k_ausd_bp_ta_bpr_basis_his.tmp`.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`
    *   **Technology:** KornShell (KornShell script)
    *   **Complexity Tier (Inferred):** Medium (due to orchestration logic, parameter handling, and external script calls)
    *   **Automation Bucket (Inferred):** Semi-Auto (B2) - requires refactoring and replatforming of orchestration logic to Airflow and SQL logic to BigQuery.

## 3. Target Architecture
The target platform for this migration is Google Cloud Platform (GCP), utilizing BigQuery for data processing and Cloud Composer (Apache Airflow) for orchestration.

*   **Orchestration:** Cloud Composer (Airflow) will manage the workflow, replacing the shell script's role as an orchestrator. An Airflow DAG will be created to sequence tasks, including parameter passing, date derivation, and BigQuery stored procedure execution.
*   **Data Processing:** Google BigQuery will host the migrated data and SQL processing logic.
    *   The existing shell script's logic will be refactored into a BigQuery Stored Procedure, handling parameter validation, date calculations, and calling the core SQL logic.
    *   The `d_ausd_bp_ta_bpr_basis_his.sql` script, which contains the main data transformation, will also be migrated to a separate BigQuery Stored Procedure.
    *   The `PoolBasisprodukt` table will be created/maintained in BigQuery.
    *   Temporary file outputs (`.tmp`) and potential commented-out file processing (sed, sort, join) will be replaced with BigQuery temporary tables, CTEs, or direct table writes.
*   **Monitoring & Logging:** BigQuery will contain dedicated tables for error logging and job auditing, replacing the shell script's `DWMSG_MeldeFehler` and potential job table updates.
*   **Configuration:** Environment variables and sourced configuration (`.dw_init`) will be managed via Airflow DAG parameters, BigQuery stored procedure parameters, or dedicated BigQuery configuration tables.

## 4. Data Flow & Lineage
The original data flow in the legacy environment is as follows:
1.  The `k_ausd_bp_ta_bpr_basis_his.ksh` script is executed, typically with command-line parameters.
2.  It sources several utility shell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  It validates input parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`) and the `p_Stichtag` date format.
4.  It calls `gestern.ksh` to derive `p_datum_heute` and `p_datum_gestern`.
5.  The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) is invoked, executing `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_basis_his.sql` with various parameters.
6.  The SQL script processes data, likely writing to the `PoolBasisprodukt` table.
7.  The KornShell script captures the record count into `bert_k_ausd_bp_ta_bpr_basis_his.tmp`.
8.  (Commented out) Further file-based processing using `sed`, `sort`, `join` on `cibasis_data*.dat` and `cibasis_fax.dat` files, outputting to `cibasisprodukt.csv`.
9.  (Commented out) Update to a job tracking table.

**Migrated Data Flow in GCP:**
1.  **Airflow DAG Trigger:** An Airflow DAG is triggered, passing parameters to a BigQuery Stored Procedure.
2.  **Orchestrator Stored Procedure:** A BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_bp_ta_bpr_basis_his`) is executed.
    *   It receives parameters equivalent to the shell script's command-line arguments.
    *   It performs parameter validation and date format checks using BigQuery SQL.
    *   It derives `p_datum_heute` and `p_datum_gestern` using BigQuery date functions.
    *   It calls another BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_bp_ta_bpr_basis_his`), passing the necessary execution parameters. This procedure encapsulates the logic of the original `d_ausd_bp_ta_bpr_basis_his.sql`.
    *   It captures the count of processed records (e.g., from the target table or a staging table populated by `d_ausd_bp_ta_bpr_basis_his`).
    *   It logs execution status and record counts to BigQuery audit/log tables.
3.  **Core SQL Stored Procedure:** The `project.dataset.d_ausd_bp_ta_bpr_basis_his` procedure performs the primary ETL operations, writing to the `PoolBasisprodukt` table in BigQuery.
4.  **Optional File Processing:** If the commented-out file processing logic is activated, it will be implemented as BigQuery SQL statements (e.g., DML, views, or additional stored procedures) operating on tables representing the original `.dat` files.

**Lineage:**
*   **Inputs:** Parameters (job identifier, entry number, reference date), `d_ausd_bp_ta_bpr_basis_his.sql` (logical input to the orchestration), `gestern.ksh` (for dates).
*   **Outputs:** `PoolBasisprodukt` table, `bert_k_ausd_bp_ta_bpr_basis_his.tmp` (temporary record count), `cibasisprodukt.csv` (if file processing reactivated).
*   **Target Lineage:** Airflow DAG orchestrates `project.dataset.r_ausd_bp_ta_bpr_basis_his` (BigQuery SP), which in turn calls `project.dataset.d_ausd_bp_ta_bpr_basis_his` (BigQuery SP) to populate `project.dataset.PoolBasisprodukt`. Audit and error logs are written to `project.dataset.job_audit` and `project.dataset.error_log`.

## 5. Transformation Logic
The transformation will involve converting the KornShell orchestration logic and any inline data manipulations into BigQuery-native constructs.

**Original Shell Script Logic:**
*   **Environment Setup (`. $HOME/.dw_init`):** Replaced by Airflow's environment variables, BigQuery stored procedure parameters, or a configuration table within BigQuery.
*   **Parameter Parsing (`getopts`):** Translated directly into BigQuery Stored Procedure input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Parameter Validation (`pruefeParameterGesetzt`):** Implemented using BigQuery `IF` statements and `ASSERT` for mandatory parameter checks. Error messages will be logged to a BigQuery error table.
*   **Date Validation (`DWDate_Datum_Check`):** Replaced by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` within the BigQuery Stored Procedure.
*   **Date Derivation (`gestern.ksh`):** Replaced by BigQuery date functions like `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Execution (`starteSQLSkript d_ausd_bp_ta_bpr_basis_his.sql`):** The `d_ausd_bp_ta_bpr_basis_his.sql` will be migrated to a separate BigQuery Stored Procedure. The orchestrator SP will then call this new procedure.
*   **Record Count (`eval "v_records=`cat $tmpFile`"`):** Replaced by a `SELECT COUNT(*)` query on the target table or a staging table, storing the result in a BigQuery `DECLARE`d variable or inserting into an audit table.
*   **Error Handling (`DWMSG_MeldeFehler`, `exit $ErrNr`):** Replaced by BigQuery `INSERT` statements into an error log table, `ASSERT` statements for critical failures, and BigQuery's transaction/exception handling where appropriate.
*   **Commented-out File Processing (`sed`, `sort`, `join`):** If re-enabled, these operations will be translated into BigQuery SQL queries using functions like `TRIM`, `REPLACE`, `DISTINCT`, `UNION ALL`, `JOIN`, and Common Table Expressions (CTEs) for intermediate steps.

**BigQuery Stored Procedure Pseudocode (Orchestrator - `r_ausd_bp_ta_bpr_basis_his`):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_basis_his`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE p_datum_heute DATE;
  DECLARE p_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';

  -- Parameter checks and validation using IF and ASSERT
  -- Date validation using SAFE.PARSE_DATE
  -- Default restart value if null

  -- Derive today and yesterday dates
  SET p_datum_heute = CURRENT_DATE();
  SET p_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Call the migrated core SQL logic stored procedure
  CALL `project.dataset.d_ausd_bp_ta_bpr_basis_his`(
    p_EintragsNr, p_JobKennung, p_Stichtag, p_wiederanlaufWert,
    CAST(p_datum_heute AS STRING), CAST(p_datum_gestern AS STRING)
  );

  -- Capture record count from target table
  SET v_records = (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` WHERE ...); -- Filter conditions will be specific to the job

  -- Log job audit information
  INSERT INTO `project.dataset.job_audit` (...) VALUES (...);

EXCEPTION WHEN ERROR THEN
  INSERT INTO `project.dataset.error_log` (...) VALUES (...);
  RAISE;
END;
```

**BigQuery SQL Pseudocode (for commented-out file processing):**
```sql
-- Example for sed, sort, join operations
WITH
  cibasis_data24_cleaned AS (
    SELECT
      TRIM(REGEXP_REPLACE(col1, ' ', '')) AS key_col,
      TRIM(REGEXP_REPLACE(col2, ' ', '')) AS data24_col
    FROM `project.dataset.cibasis_data24_raw`
  ),
  cibasis_data96_cleaned AS (
    SELECT
      TRIM(REGEXP_REPLACE(col1, ' ', '')) AS key_col,
      TRIM(REGEXP_REPLACE(col2, ' ', '')) AS data96_col
    FROM `project.dataset.cibasis_data96_raw`
  ),
  cibasis_fax_cleaned AS (
    SELECT
      TRIM(REGEXP_REPLACE(col1, ' ', '')) AS key_col,
      TRIM(REGEXP_REPLACE(col2, ' ', '')) AS fax_col
    FROM `project.dataset.cibasis_fax_raw`
  ),
  joined_data AS (
    SELECT
      COALESCE(d24.key_col, d96.key_col, cfax.key_col) AS join_key,
      d24.data24_col,
      d96.data96_col,
      cfax.fax_col
    FROM cibasis_data24_cleaned d24
    FULL OUTER JOIN cibasis_data96_cleaned d96 ON d24.key_col = d96.key_col
    LEFT JOIN cibasis_fax_cleaned cfax ON COALESCE(d24.key_col, d96.key_col) = cfax.key_col
  )
-- SELECT * FROM joined_data; -- Final output to a new BigQuery table or view
```

## 6. External Dependencies
The original script has several external dependencies, primarily other shell scripts and an implicit database connection.

*   **Sourced Shell Scripts:**
    *   `. $HOME/.dw_init`: Environment initialization. **Replacement:** Airflow environment variables, BigQuery stored procedure parameters, or BigQuery configuration table.
    *   `f_alis_msgerr.ksh`: Error handling. **Replacement:** BigQuery error logging table (`project.dataset.error_log`) and `ASSERT`/`RAISE` in stored procedures.
    *   `h_alis_date.ksh`: Date validation/helpers. **Replacement:** BigQuery `SAFE.PARSE_DATE` and other date functions.
    *   `h_alis_parameter.ksh`: Parameter parsing/validation. **Replacement:** BigQuery Stored Procedure parameter declarations and `IF`/`ASSERT` statements.
    *   `h_alis_sqlplus.ksh` (specifically `starteSQLSkript`): Wrapper for SQL execution. **Replacement:** Direct `CALL` to a BigQuery Stored Procedure.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Date derivation (yesterday/today). **Replacement:** BigQuery `CURRENT_DATE()` and `DATE_SUB()`.
*   **Core SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_basis_his.sql`: Contains the main business logic. **Replacement:** Migrated to a dedicated BigQuery Stored Procedure `project.dataset.d_ausd_bp_ta_bpr_basis_his`.
*   **Database:** The script implicitly connects to a database (likely Oracle, given `SQL*Plus` context) to run the SQL script. **Replacement:** BigQuery, with all data and SQL logic residing within.
*   **Temporary Files (`bert_k_ausd_bp_ta_bpr_basis_his.tmp`, `cibasis_data*.dat`, `cibasis_fax.dat`, `cibasisprodukt.csv`):**
    *   `bert_k_ausd_bp_ta_bpr_basis_his.tmp`: Replaced by BigQuery `DECLARE`d variables for record counts or directly inserting into audit tables.
    *   `.dat` and `.csv` files (commented out processing): If these are to be processed, they should be ingested into BigQuery tables (e.g., via Cloud Storage and BigQuery external tables or loads) and then processed using BigQuery SQL.

## 7. Unresolved / Risks
*   **Missing Lineage Edges:** While `lineage_edges` did not explicitly capture relationships for this specific file, the `file_analysis` and source code provided sufficient detail to reconstruct dependencies.
*   **Core SQL Script (`d_ausd_bp_ta_bpr_basis_his.sql`) Migration:** The detailed migration plan for `d_ausd_bp_ta_bpr_basis_his.sql` is a prerequisite and must be addressed separately. Its complexity will influence the overall migration effort.
*   **Commented-out Logic:** The `sed`, `sort`, `join` operations on data files are currently commented out. A decision needs to be made whether to reactivate this functionality in the target environment. If so, it will require defining how these `.dat` files are ingested into BigQuery and then translating the logic into BigQuery SQL.
*   **FOS Job Management:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` functions indicate a legacy job management system. These need to be replaced with a GCP-native job monitoring/auditing solution, likely involving BigQuery audit tables and Cloud Monitoring/Logging.
*   **Error Handling Detail:** The `f_alis_msgerr.ksh` script provides a specific error handling framework. The BigQuery replacement should aim to capture similar levels of detail for debugging and operational support.
*   **Character Encoding:** The comment `Andre Lbbers` suggests potential character encoding issues (e.g., Latin-1 to UTF-8) that should be handled during data ingestion and processing in BigQuery.

## 8. Build Plan
The migration will follow these ordered steps:

1.  **Define BigQuery Datasets:** Create target BigQuery datasets (e.g., `project.dataset` for tables and procedures, `project.audit` for logs).
2.  **Migrate Core SQL Script (`d_ausd_bp_ta_bpr_basis_his.sql`):**
    *   Analyze `d_ausd_bp_ta_bpr_basis_his.sql` to identify source tables, transformations, and target tables (`PoolBasisprodukt`).
    *   Create DDL for `PoolBasisprodukt` and any intermediate tables in BigQuery.
    *   Convert the SQL script into a BigQuery Stored Procedure: `project.dataset.d_ausd_bp_ta_bpr_basis_his`.
3.  **Create BigQuery Error and Audit Tables:** Define DDL for `project.audit.error_log` and `project.audit.job_audit` tables to capture migration-specific logging.
4.  **Migrate Orchestration Shell Script (`k_ausd_bp_ta_bpr_basis_his.ksh`):**
    *   Create a BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_basis_his` based on the provided pseudocode in Section 5. This procedure will encapsulate parameter validation, date logic, and call the `project.dataset.d_ausd_bp_ta_bpr_basis_his` procedure.
    *   Implement error handling (inserts into `error_log`) and job auditing (inserts into `job_audit`) within this procedure.
5.  **Develop Airflow DAG (Python):**
    *   Create a Python-based Airflow DAG to schedule and orchestrate the execution of the `project.dataset.r_ausd_bp_ta_bpr_basis_his` BigQuery Stored Procedure.
    *   The DAG will be responsible for passing runtime parameters and handling retries/monitoring.
6.  **Optional: Migrate Commented-out File Processing (BigQuery SQL):**
    *   If decided to reactivate, define ingestion methods (e.g., Cloud Storage to BigQuery loads) for the raw `.dat` files.
    *   Translate the `sed`, `sort`, `join` logic into BigQuery SQL statements (views, tables, or additional procedures).
7.  **Testing and Validation:** Thoroughly test the migrated BigQuery Stored Procedures and the Airflow DAG for functional correctness, performance, and data integrity.
8.  **Deployment:** Deploy the BigQuery assets and the Airflow DAG to the production GCP environment.