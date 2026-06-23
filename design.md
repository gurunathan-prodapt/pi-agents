# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This KornShell script (`k_ausd_bp_ta_bcp_msisdn.ksh`) acts as a control script for a data processing job. Its primary purpose is to:
*   Initialize the environment and parse command-line parameters.
*   Validate input parameters, especially a reference date (`Stichtag`).
*   Execute a core SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) that performs the main business logic (likely data extraction/transformation related to `PoolBasisprodukt`).
*   Handle error conditions and provide logging.
*   Potentially perform post-processing on output files (currently commented out but noted for migration consideration).
The overall business purpose is to prepare data related to `PoolBasisprodukt`.

## 2. Source Inventory
The job consists of a single KornShell script: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`.
*   **Technology:** KornShell script
*   **Complexity Tier:** Not explicitly available from `file_complexity` table. However, given its nature as a control script orchestrating SQL execution and its `semi_auto` migration bucket, it can be classified as *medium* complexity due to external script sourcing, parameter handling, and conditional logic.
*   **Automation Bucket:** `semi_auto` (B2)

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve translating the shell script's orchestration logic and embedded SQL execution into BigQuery-native components.

*   **BigQuery Stored Procedure:** The main shell script's logic (parameter parsing, validation, and SQL script invocation) will be re-implemented as a BigQuery Stored Procedure. This procedure will accept parameters, perform validation, and orchestrate the execution of the main data logic.
*   **BigQuery SQL Scripts:** The underlying SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) will be converted into BigQuery-compatible SQL. This could be embedded directly within the stored procedure or called as separate BigQuery scripts/statements.
*   **BigQuery Tables:** All input and output data will reside in BigQuery tables. Temporary files used in the original script will be replaced by temporary BigQuery tables or CTEs within BigQuery SQL.
*   **Orchestration (External):** The overall scheduling and execution of the BigQuery Stored Procedure will be handled by an external orchestration tool like Google Cloud Composer (Apache Airflow) or Dataform.

## 4. Data Flow & Lineage
The original script's data flow is as follows:

1.  **Input Parameters:** `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (reference date in `DDMMYYYY`), `p_wiederanlaufWert` (optional restart value).
2.  **Environment Initialization:** Sourcing of `$HOME/.dw_init` and several utility KornShell scripts.
3.  **Date Derivation:** Execution of `gestern.ksh` to get today's and yesterday's dates.
4.  **SQL Script Execution:** The script dynamically constructs the path to `d_ausd_bp_ta_bcp_msisdn.sql` and executes it via the `starteSQLSkript` function, passing various parameters including `$BERT_DIR_ROOT`, `p_Stichtag`, and `p_JobKennung`. This SQL script is expected to read data from source tables and write to a target table or temporary file.
5.  **Temporary File Handling:** A temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`) is used to store the record count resulting from the SQL execution.
6.  **Record Count Retrieval:** The script reads the record count from the temporary file.
7.  **Job Tracking (Commented):** An entry into a job-tracking table (`FOSJobErzeugeEintrag`) is commented out.
8.  **Post-processing (Commented):** A section with `sed`, `sort`, and `join` commands suggests further file-based processing to produce a `cibasisprodukt.csv`. This implies potential intermediate files like `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`, and `cibasis_24_96.tmp`.

In BigQuery, this flow will be:
1.  **Orchestration Trigger:** Cloud Composer DAG or Dataform job triggers the BigQuery Stored Procedure.
2.  **Stored Procedure Execution:**
    *   Parameters are passed directly to the procedure.
    *   Date validation using BigQuery's date functions.
    *   Date derivation for today/yesterday using `CURRENT_DATE()` and `DATE_SUB()`.
    *   Execution of BigQuery SQL statements (migrated from `d_ausd_bp_ta_bcp_msisdn.sql`) which read from source BigQuery tables and write to target BigQuery tables.
    *   Record counting performed directly within BigQuery SQL using `COUNT(*)` and stored in a variable.
    *   The commented file post-processing will be implemented as a series of BigQuery DML statements (e.g., `CREATE TABLE AS SELECT ...`, `INSERT INTO ...`) using CTEs or intermediate tables.
3.  **Output:** Final data written to target BigQuery tables, and record counts available within the procedure or logged.

## 5. Transformation Logic
The transformation logic is primarily contained within the `d_ausd_bp_ta_bcp_msisdn.sql` script, which is invoked by `k_ausd_bp_ta_bcp_msisdn.ksh`. The shell script itself contains orchestration and parameter handling logic.

**Shell Script Logic to BigQuery Stored Procedure:**

*   **Parameter Parsing:** The `getopts` logic will be replaced by direct BigQuery Stored Procedure parameters.
*   **Validation:** Shell functions like `pruefeParameterGesetzt` and `DWDate_Datum_Check` will be replaced by `IF` conditions, `RAISE` statements, and `SAFE.PARSE_DATE` in BigQuery SQL.
*   **Environment Variables:** Shell environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by BigQuery project/dataset references, procedure parameters, or configuration values.
*   **Script Sourcing:** The `. $SCRIPT_PATH` commands will be replaced by embedding the logic of those utility scripts into the BigQuery Stored Procedure or by calling specific BigQuery UDFs/procedures if they represent reusable logic.
*   **SQL Script Execution (`starteSQLSkript`):** This external call will be replaced by direct execution of the migrated BigQuery SQL from `d_ausd_bp_ta_bcp_msisdn.sql`.
*   **Temporary File Record Count:** The `cat $tmpFile` and `eval` to get records will be replaced by a `SELECT COUNT(*)` on the target table, storing the result in a `DECLARE`d variable.

**Example BigQuery SQL Pseudocode (from MCP tool):**
```sql
-- BigQuery Stored Procedure Pseudocode
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bcp_msisdn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Parameter validation and date validation
  -- ... (as detailed in MCP output)

  -- Equivalent of gestern.ksh
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Main SQL logic migrated from d_ausd_bp_ta_bcp_msisdn.sql
  -- This is where the core data transformation would happen, e.g.:
  -- CREATE OR REPLACE TABLE `project.dataset.target_table` AS
  -- SELECT ... FROM `project.dataset.source_table` WHERE business_date = v_stichtag_date;

  -- Record count
  SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE business_date = v_stichtag_date);

  -- Optional job tracking (migrated from commented FOSJobErzeugeEintrag)
  -- INSERT INTO `project.dataset.job_table` VALUES (...);

END;
```

**Commented File Post-Processing (sed, sort, join) to BigQuery SQL:**
This section, if reactivated, would be translated into BigQuery DML operations. `sed` for blank removal would be `REPLACE()` functions. `sort -u` would be `DISTINCT` and `ORDER BY`. `join` commands would be BigQuery `JOIN` operations.

## 6. External Dependencies
The original script has several external dependencies:

*   **Sourced KornShell Scripts:**
    *   `$HOME/.dw_init`: Environment setup.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus utilities.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Date calculation (yesterday/today).
*   **SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql`: Core business logic SQL.
*   **External Commands:** `getopts`, `print`, `set`, `cat`, `eval`, `sed`, `sort`, `join`.
*   **Temporary Files:** `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_msisdn.tmp`.
*   **Job Tracking System (Commented):** `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`.

**Replacement in BigQuery:**

*   **Sourced KornShell Scripts:** The logic within these scripts will be re-implemented directly in the BigQuery Stored Procedure using BigQuery SQL functions and control flow. Generic utilities might become BigQuery UDFs or separate helper procedures.
*   **SQL Script (`d_ausd_bp_ta_bcp_msisdn.sql`):** This will be fully converted into BigQuery-compliant SQL, defining transformations, joins, and DML operations between BigQuery tables.
*   **External Commands:** Replaced by native BigQuery SQL functions (e.g., `SAFE.PARSE_DATE`, `COUNT(*)`, `REPLACE`, `JOIN`, `DISTINCT`, `ORDER BY`).
*   **Temporary Files:** Replaced by BigQuery temporary tables, CTEs, or in-memory variables within the stored procedure.
*   **Job Tracking System:** If active and required, this would need to be re-implemented using BigQuery tables for logging and potentially integrated with an external orchestration tool's monitoring capabilities.

## 7. Unresolved / Risks
*   **`d_ausd_bp_ta_bcp_msisdn.sql` Content:** The actual content of this SQL script is crucial and needs to be analyzed and migrated separately. Without it, the full scope of data transformations cannot be precisely defined. The migration of this SQL is a critical path item.
*   **Orchestration Tool Integration:** The choice of the BigQuery orchestration tool (Cloud Composer, Dataform, Scheduled Queries) needs to be finalized, and the stored procedure needs to be integrated appropriately.
*   **Error Handling Details:** The `f_alis_msgerr.ksh` script provides specific error concepts. The migration should ensure equivalent error logging and reporting mechanisms are in place in BigQuery and the orchestration layer.
*   **Commented Code Reactivation:** The commented sections for `sed`, `sort`, `join`, and `FOSJobDeaktivate`/`FOSJobErzeugeEintrag` need clarification. If these are to be reactivated in the BigQuery environment, their logic must be explicitly designed and implemented in BigQuery SQL or an appropriate orchestration component. The current design assumes they are either reactivated (post-processing) or not essential (job tracking).
*   **`_dw_init` and other sourced scripts:** The exact functionality of `$HOME/.dw_init` and other sourced `.ksh` scripts needs to be fully understood to ensure all environment setups and utility functions are replicated in BigQuery.
*   **Performance:** Shell script execution patterns (e.g., looping, external calls) might not directly translate to optimal BigQuery performance. The migrated BigQuery SQL should be optimized for BigQuery's columnar storage and distributed processing.

## 8. Build Plan

1.  **Migrate `d_ausd_bp_ta_bcp_msisdn.sql` to BigQuery SQL:**
    *   Analyze the SQL script to identify source tables, target tables, and transformation logic.
    *   Convert SQL syntax to BigQuery standard SQL.
    *   Create necessary BigQuery tables (if they don't already exist).
    *   *Language:* BigQuery SQL
2.  **Develop BigQuery Stored Procedure (`r_ausd_bp_ta_bcp_msisdn`):**
    *   Implement parameter parsing and validation logic from `k_ausd_bp_ta_bcp_msisdn.ksh`.
    *   Integrate the migrated BigQuery SQL from step 1.
    *   Implement date derivation logic.
    *   Implement record counting logic.
    *   Implement the commented post-processing logic (sed, sort, join) as BigQuery DML if required.
    *   *Language:* BigQuery SQL (for Stored Procedure)
3.  **Define Orchestration:**
    *   If using Cloud Composer, create an Airflow DAG to schedule and trigger the BigQuery Stored Procedure, passing required parameters.
    *   If using Dataform, define a Dataform workflow that executes the stored procedure.
    *   *Language:* Python (for Airflow DAG), Dataform SQLX/JSON (for Dataform)
4.  **Configuration:**
    *   Define configuration parameters (e.g., `Jobkennung`, `Stichtag`) to be passed to the BigQuery Stored Procedure, possibly via environment variables in the orchestration tool or a dedicated configuration table.
    *   *Language:* YAML/JSON (for configs), Python (for Airflow variables)
5.  **Testing and Validation:**
    *   Develop comprehensive test cases to validate the migrated logic against the original script's output.
    *   Perform unit, integration, and end-to-end testing.
    *   *Language:* SQL, Python (for testing frameworks)