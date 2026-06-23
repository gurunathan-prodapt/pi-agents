# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_apn_vertrag.ksh`, serves as a control/wrapper script for a data preparation process. Its primary purpose is to perform preliminary checks on input parameters and dates, initialize the runtime environment with error handling, and orchestrate the execution of an external SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) for data processing. After the SQL execution, it reads the number of generated records from a temporary file. The overall job's purpose, as noted in the lineage system, is "Job assembled from 1 component(s); stage dist: medium=1". The script is written in KornShell.

## 2. Source Inventory
The job consists of a single primary source file:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology:** KornShell (shell script)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** ETL Orchestration/Wrapper Script. It manages parameter parsing, validation, environment setup, and triggers the main SQL data transformation.

## 3. Target Architecture
The migration will convert this KornShell script and its dependencies into a BigQuery-native solution.
*   **Main Component:** A BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_apn_vertrag`) will encapsulate the orchestration logic, parameter handling, validation, and invocation of the core data transformation.
*   **Data Transformation:** The logic currently residing in `d_ausd_bp_ta_apn_vertrag.sql` (which is executed by this KSH script) will be migrated into a separate BigQuery SQL script or directly embedded within the BigQuery Stored Procedure, depending on its complexity and reusability.
*   **Logging & Auditing:** Any job tracking or logging (like the creation of a job-table entry) will be directed to dedicated BigQuery tables (e.g., `project.dataset.job_log`).
*   **Temporary Data:** Temporary files (like `bert_k_ausd_bp_ta_apn_vertrag.tmp`) will be replaced by BigQuery temporary tables (`CREATE TEMP TABLE`) or in-memory variables within the stored procedure.
*   **Orchestration (External):** If this job is part of a larger workflow, its execution will be managed by an external orchestrator like Cloud Composer (Apache Airflow), which will call the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Initialization:** Sourcing environment variables (`. $HOME/.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Input:** The script accepts command-line parameters (`-j`, `-f`, `-s`, `-l`) for `JobKennung`, `EintragsNr`, `Stichtag`, and `wiederanlaufWert`.
3.  **Date Derivation:** It executes `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
4.  **Validation:** Parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`) and the `Stichtag` date format (`DDMMYYYY`) are validated.
5.  **SQL Script Execution:** The main data processing is performed by executing `d_ausd_bp_ta_apn_vertrag.sql` via the `starteSQLSkript` function, passing various parameters.
6.  **Record Count:** The number of processed records is read from `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`.
7.  **Job Tracking (Commented/Optional):** The script has commented sections for `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` that would manage job states and log record counts.
8.  **Post-processing (Commented):** There are commented sections for `sed`, `sort`, and `join` commands that suggest further file-based post-processing steps (e.g., `cibasis_data24.dat`, `cibasisprodukt.csv`).

**Target BigQuery Data Flow:**
1.  **Orchestration Trigger:** An external orchestrator (e.g., Cloud Composer) invokes the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_apn_vertrag`, passing job parameters.
2.  **Parameter Handling:** The stored procedure directly receives parameters, replacing the shell's `getopts`.
3.  **Date Derivation & Validation:** BigQuery SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB`, `PARSE_DATE`, `REGEXP_CONTAINS`) will perform date derivations and validations.
4.  **Logging:** Log entries for job start, status, and completion will be inserted into `project.dataset.job_log`.
5.  **Core Transformation:** The SQL logic from `d_ausd_bp_ta_apn_vertrag.sql` will be executed within the stored procedure, potentially referencing source tables (e.g., `project.dataset.source_table`).
6.  **Record Count:** `COUNT(*)` queries against the transformed data will replace reading from a temporary file.
7.  **Post-processing:** The commented `sed`, `sort`, and `join` operations will be translated into BigQuery SQL transformations (e.g., `REGEXP_REPLACE`, `SELECT DISTINCT`, `CREATE TABLE AS SELECT`, `FULL OUTER JOIN`, `LEFT JOIN`) on BigQuery tables, creating cleaned or aggregated output tables (e.g., `project.dataset.cibasisprodukt`).

## 5. Transformation Logic
*   **Shell Script to BigQuery Stored Procedure:**
    *   The overall control flow (parameter parsing, validation, conditional logic) will be rewritten using BigQuery Scripting (`BEGIN ... END`, `DECLARE`, `IF`, `RAISE`).
    *   Shell variable assignments will become BigQuery `DECLARE` statements.
    *   Error handling (`DWMSG_MeldeFehler`) will be replaced with `RAISE USING MESSAGE` in BigQuery, or log entries into a BigQuery audit table.
    *   The `pruefeParameterGesetzt` function will be replaced by `IF NULL` checks and `RAISE` statements.
    *   The `DWDate_Datum_Check` function will be replaced by `REGEXP_CONTAINS` and `PARSE_DATE` checks.
    *   The `starteSQLSkript` call will be replaced by direct execution of the migrated SQL logic, or by calling another BigQuery Stored Procedure that encapsulates the `d_ausd_bp_ta_apn_vertrag.sql` content.
    *   The record count (`eval "v_records=\`cat $tmpFile\`"`) will be replaced by `SELECT COUNT(*)` into a `DECLARE` variable.
*   **Commented Post-processing (Shell to BigQuery SQL):**
    *   `sed s/\\ //g` for whitespace removal: `REGEXP_REPLACE(column, r' ', '')`.
    *   `sort -u -k 1 -t ';'` for unique sort: `SELECT DISTINCT ... ORDER BY`.
    *   `join` commands: Translated directly to `JOIN` clauses in BigQuery SQL, specifying join keys and output columns.

**Example BigQuery SQL Pseudocode for the main procedure:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_apn_vertrag`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_wiederanlaufWert_final STRING;

  SET v_wiederanlaufWert_final = COALESCE(p_wiederanlaufWert, '0');

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN RAISE USING MESSAGE = 'Jobkennung fehlt'; END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN RAISE USING MESSAGE = 'EintragsNr fehlt'; END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN RAISE USING MESSAGE = 'Stichtag fehlt'; END IF;
  IF NOT REGEXP_CONTAINS(p_Stichtag, r'^[0-9]{8}$') THEN RAISE USING MESSAGE = 'Stichtag hat ungueltiges Format'; END IF;

  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Log Job Start
  INSERT INTO `project.dataset.job_log` (...) VALUES (...);

  -- Execute Core SQL Logic (migrated from d_ausd_bp_ta_apn_vertrag.sql)
  -- This section would contain the actual SQL logic, e.g.,
  -- CREATE OR REPLACE TABLE `project.dataset.final_output_table` AS
  -- SELECT ... FROM `project.dataset.source_table` WHERE ...;
  -- For demonstration, using a placeholder:
  CREATE TEMP TABLE tmp_processed_data AS
  SELECT * FROM `project.dataset.source_table` WHERE business_date = v_stichtag_date;

  SET v_records = (SELECT COUNT(*) FROM tmp_processed_data);

  -- Log Job Finish
  INSERT INTO `project.dataset.job_log` (...) VALUES (...);

  SELECT v_records AS records_processed;
END;
```

## 6. External Dependencies
The original script did not explicitly list external systems in the `lineage_assembled_jobs` metadata. However, from code analysis:
*   **Legacy Environment Variables and Paths:** `$HOME/.dw_init`, `${BERT_DIR_ROOT}`, `$DW_DIR_UTL`. These define the environment and paths to libraries and temporary directories.
    *   **Replacement:** In BigQuery, these will be replaced by:
        *   Configuration parameters passed to the BigQuery Stored Procedure.
        *   Hardcoded dataset and table names.
        *   BigQuery's internal temporary table mechanisms.
        *   If necessary, environment variables for an external orchestrator (e.g., Airflow variables).
*   **Included Shell Scripts (`. *.ksh`):** `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.
    *   **Replacement:** The functionalities of these utility scripts will be translated into BigQuery SQL functions or directly integrated into the main stored procedure's logic. For example, date checking and parameter validation logic has direct BigQuery SQL equivalents.
*   **Executed Shell Script (`gestern.ksh`):** Used to derive yesterday's and today's dates.
    *   **Replacement:** BigQuery SQL functions `CURRENT_DATE()` and `DATE_SUB()` will directly achieve this.
*   **External SQL Script (`d_ausd_bp_ta_apn_vertrag.sql`):** Contains the core data transformation logic.
    *   **Replacement:** This SQL content will be migrated into a BigQuery SQL script, a separate BigQuery Stored Procedure, or embedded directly within `project.dataset.r_ausd_bp_ta_apn_vertrag`.
*   **Filesystem Operations (`cat`, `sed`, `sort`, `join`):** Used for temporary file handling and post-processing of output files.
    *   **Replacement:** These will be replaced by BigQuery SQL transformations on tables (e.g., `CREATE TEMP TABLE`, `SELECT DISTINCT`, `JOIN`, `REGEXP_REPLACE`).
*   **Job Management System (implied `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These commented lines hint at an external job management system.
    *   **Replacement:** If job status tracking is required, dedicated BigQuery logging tables (`project.dataset.job_log`) will be used to store job execution details and status.

## 7. Unresolved / Risks
*   **Core SQL (`d_ausd_bp_ta_apn_vertrag.sql`) Content:** The actual content of the main SQL script is unknown. Its complexity, source dialect, and dependencies will dictate the effort required for its BigQuery migration. This is the biggest unknown.
*   **Detailed Logic of Included KSH Scripts:** While the purpose of utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) is inferred, their exact implementation details are not fully known. This could introduce minor complexities during the conversion of their functionalities.
*   **Commented-out Code:** The script contains several commented-out sections (e.g., `AL?? FOSJobDeaktivate`, `sed/sort/join` block). It's unclear if these functionalities are still needed, were deprecated, or are part of a future plan. For this design, the commented post-processing (`sed/sort/join`) is assumed to be a candidate for migration, but the job management calls are treated as optional based on explicit requirement.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` parameter is initialized but not explicitly used in the provided script. Its purpose or downstream impact is unknown, but it has been carried over as a parameter in the BigQuery stored procedure.
*   **Data Sources and Targets for `d_ausd_bp_ta_apn_vertrag.sql`:** The exact input tables and output tables/files used by the SQL script are not available from the current analysis. This is crucial for defining the BigQuery schema.
*   **Error Handling Details:** The `DWMSG_MeldeFehler` implies a structured error logging mechanism. The exact details of this system and how errors are handled are important for replicating the error behavior in BigQuery.

## 8. Build Plan
1.  **Analyze `d_ausd_bp_ta_apn_vertrag.sql`:** Obtain the content of `d_ausd_bp_ta_apn_vertrag.sql`. Perform a detailed analysis to understand its input/output tables, transformation logic, and SQL dialect. This is the highest priority.
2.  **Define BigQuery Schema:** Based on the analysis of `d_ausd_bp_ta_apn_vertrag.sql` and the commented post-processing, define the target BigQuery datasets and table schemas (including staging, intermediate, and final output tables like `cibasisprodukt`).
3.  **Migrate Utility Logic:**
    *   Create BigQuery user-defined functions (UDFs) or internal stored procedure logic for date validation (e.g., `DWDate_Datum_Check`).
    *   Implement parameter validation within the main stored procedure.
    *   Design a BigQuery logging table (`project.dataset.job_log`) to replace the legacy error messaging and job tracking.
4.  **Develop Core SQL Transformation:**
    *   Convert `d_ausd_bp_ta_apn_vertrag.sql` into BigQuery-compliant SQL. This might involve creating a dedicated BigQuery Stored Procedure or a series of SQL scripts.
5.  **Develop Main Orchestration Stored Procedure:**
    *   Create the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_apn_vertrag` based on the provided pseudocode, incorporating the migrated utility logic and invoking the core SQL transformation.
6.  **Migrate Post-processing Logic:**
    *   Translate the commented `sed`, `sort`, and `join` operations into BigQuery SQL statements, creating new tables (e.g., `project.dataset.cibasis_data24_clean`, `project.dataset.cibasisprodukt`).
7.  **Integrate with Orchestration Tool:**
    *   If part of a larger workflow, create an Airflow DAG (e.g., using Cloud Composer) to schedule and execute the `project.dataset.r_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure.
8.  **Testing:** Implement unit and integration tests for the BigQuery stored procedures and SQL scripts.

**Build Artifacts:**
*   `project.dataset.r_ausd_bp_ta_apn_vertrag.sql` (BigQuery Stored Procedure)
*   `project.dataset.<core_data_transformation>.sql` (BigQuery SQL Script/Stored Procedure for main logic)
*   `project.dataset.job_log.sql` (BigQuery DDL for logging table)
*   `project.dataset.cibasis_data24_clean.sql`, `project.dataset.cibasis_data96_clean.sql`, `project.dataset.cibasis_fax_clean.sql`, `project.dataset.cibasis_24_96.sql`, `project.dataset.cibasisprodukt.sql` (BigQuery SQL for post-processing tables)
*   (Optional) Airflow DAG Python file for orchestration.