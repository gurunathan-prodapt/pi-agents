# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope

This migration job, `k_ausd_bp_ta_rn_vertrag.ksh`, is a KornShell control script designed to orchestrate a database extraction/load process. Its primary function is to validate input parameters, perform date checks, execute a core SQL script (`d_ausd_bp_ta_rn_vertrag.sql`), and retrieve a record count from the executed operation. The job processes data relating to `PoolBasisprodukt` and is categorized as a medium-complexity job with semi-automatic migration potential.

The core data flow involves:
- **Reading** from `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` tables.
- **Writing** to the `SOF$TA_RN_VERTRAG` table.

The scope of this migration is to re-implement this orchestration logic and its embedded SQL processing natively within Google Cloud's BigQuery environment, leveraging BigQuery Stored Procedures and BigQuery SQL.

## 2. Source Inventory

The assembled job consists of one primary component:

- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`
    - **Technology**: KornShell script (.ksh), orchestrating embedded SQL.
    - **Complexity Tier**: `medium`
    - **Automation Bucket**: `semi_auto`
    - **Description**: This script handles parameter parsing (`-j`, `-f`, `-s`, `-l`), validates date inputs, sources several utility shell scripts for common functions (error handling, date validation, SQLPlus wrappers), sets up environment variables, and executes the `d_ausd_bp_ta_rn_vertrag.sql` script. It also includes commented-out sections for `sed`, `sort`, and `join` operations, indicating potential file-based post-processing that is currently inactive.
    - **Associated SQL File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql`
        - **Description**: This SQL script performs the main data transformation, reading from `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` and inserting data into `SOF$TA_RN_VERTRAG`. It also appears to use an Oracle package `DWPA_UTIL_SKRIPT`.

## 3. Target Architecture

The migrated job will reside in Google BigQuery.

-   **Orchestration Layer**: The KornShell script (`k_ausd_bp_ta_rn_vertrag.ksh`) will be transformed into a **BigQuery Stored Procedure**. This stored procedure will handle parameter intake, validation, and control the execution of the core data transformation logic.
-   **Data Transformation Layer**: The SQL logic from `d_ausd_bp_ta_rn_vertrag.sql` will be directly embedded and executed as **BigQuery SQL DML statements** within the BigQuery Stored Procedure.
-   **Data Storage**: All source and target tables (`DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`, `SOF$TA_RN_VERTRAG`) will be BigQuery tables.
-   **Temporary Data**: Any temporary files used by the original script (e.g., for record counts) will be replaced by BigQuery `DECLARE` variables or temporary tables within the stored procedure.
-   **Scheduling/Workflow**: If external scheduling is required (e.g., for daily execution or integration with other jobs), a Cloud Composer (Airflow) DAG can be used to invoke the BigQuery Stored Procedure, passing necessary parameters.

## 4. Data Flow & Lineage

The data flow for this job can be summarized as follows:

1.  **Invocation**: The BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_rn_vertrag`) is invoked, likely by a scheduler (e.g., Cloud Composer) or manually, providing parameters such as `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert`.
2.  **Parameter Validation**: The stored procedure first validates the provided input parameters for completeness and correctness.
3.  **Date Validation & Derivation**: The `p_Stichtag` parameter is validated for the `DDMMYYYY` format. `CURRENT_DATE()` and `DATE_SUB()` are used to derive `today` and `yesterday` dates.
4.  **Data Extraction & Transformation (Core SQL Logic)**: The embedded BigQuery SQL performs the main ETL operation:
    -   **Reads From**:
        -   `project.dataset.DWTK_MELDUNGEN`
        -   `project.dataset.SOF$TA_RN_EINZELN`
    -   **Transformation**: The `SELECT` statement joins data from these two sources based on `MELDUNG_CD` and `MELDUNG_GUELTIG_CD`.
    -   **Writes To**:
        -   `project.dataset.SOF$TA_RN_VERTRAG` (via `INSERT INTO` statement)
5.  **Record Count & Logging**: After the data transformation, the procedure calculates the number of records processed and potentially logs this information to a BigQuery logging table. Error messages are handled via `SIGNAL SQLSTATE` or logging.

## 5. Transformation Logic

The migration involves translating KornShell and Oracle SQL constructs to BigQuery SQL and BigQuery Stored Procedure scripting:

**KornShell Script (`k_ausd_bp_ta_rn_vertrag.ksh`) to BigQuery Stored Procedure:**

-   **Parameter Handling**: `getopts` command-line parsing will be replaced by `IN` parameters of the BigQuery Stored Procedure.
-   **Environment Variables**: Shell environment variables like `${BERT_DIR_ROOT}`, `${DW_DIR_UTL}`, `$HOME` will be replaced by BigQuery `DECLARE` constants or configuration parameters passed to the procedure.
-   **Utility Script Sourcing**:
    -   `f_alis_msgerr.ksh` (error handling): Replaced by BigQuery's native error handling (`SIGNAL SQLSTATE`, `ASSERT`) and inserts into a dedicated BigQuery error log table.
    -   `h_alis_date.ksh` (date validation): Replaced by BigQuery functions like `REGEXP_CONTAINS`, `PARSE_DATE`, `FORMAT_DATE`.
    -   `h_alis_parameter.ksh` (parameter validation): Replaced by `IF` conditions within the stored procedure.
    -   `h_alis_sqlplus.ksh` (SQLPlus wrapper): No direct equivalent; the SQL will be executed natively within BigQuery.
    -   `gestern.ksh` (date derivation): Replaced by BigQuery functions `CURRENT_DATE()` and `DATE_SUB()`.
-   **SQL Script Execution**: The `starteSQLSkript` invocation will be replaced by direct execution of the transformed SQL within the BigQuery Stored Procedure.
-   **Temporary Files**: The temporary file (`tmpFile`) used for record counts will be replaced by a BigQuery `DECLARE` variable (`v_records`) populated by `SELECT COUNT(*)` queries.
-   **Commented File Processing**: The commented `sed`, `sort`, `join` operations would be translated to BigQuery SQL `REPLACE`, `REGEXP_REPLACE`, `TRIM`, `SELECT DISTINCT`, `ORDER BY`, `JOIN`, and `UNION ALL` if they were to become active.

**SQL Script (`d_ausd_bp_ta_rn_vertrag.sql`) to BigQuery SQL:**

-   The core `SELECT T1.MELDUNG_CD, ... FROM isbert_schema.dwtk_meldungen T1, sof$ta_rn_einzeln T2 WHERE ... INTO sof$ta_rn_vertrag;` statement will be translated to a BigQuery `INSERT INTO project.dataset.SOF$TA_RN_VERTRAG SELECT ... FROM project.dataset.DWTK_MELDUNGEN AS T1 JOIN project.dataset.SOF$TA_RN_EINZELN AS T2 ON ...` statement.
-   The Oracle package call `DWPA_UTIL_SKRIPT.runstatement(SQL_STATEMENT)` needs to be analyzed for its exact functionality. If it's a simple dynamic SQL executor, it can be removed as BigQuery Stored Procedures handle dynamic SQL directly. If it contains complex logic, it would need to be re-implemented as a BigQuery UDF or another BigQuery Stored Procedure.

**BigQuery Pseudocode Example:**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_rn_vertrag`(
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
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1; SET ErrArg = 'Jobkennung';
  END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET ErrNr = 1; SET ErrArg = 'Stichtag';
  END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1; SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    -- Log error and raise exception
    INSERT INTO `project.dataset.error_log` (timestamp, job_name, error_code, error_argument, job_kennung, eintrags_nr, stichtag)
    VALUES (CURRENT_TIMESTAMP(), v_TabName, ErrNr, ErrArg, p_JobKennung, p_EintragsNr, p_Stichtag);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('FEHLER: Notwendiges Argument fehlt - ', ErrArg);
  END IF;

  -- Date Validation (DDMMYYYY)
  IF NOT REGEXP_CONTAINS(p_Stichtag, r'^[0-3][0-9][0-1][0-9][0-9]{4}$') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Ungueltiges Datumformat fuer Stichtag, erwartet DDMMYYYY';
  END IF;
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- Derive yesterday and today dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Initialize p_wiederanlaufWert
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  -- Main SQL logic from d_ausd_bp_ta_rn_vertrag.sql
  INSERT INTO `project.dataset.SOF$TA_RN_VERTRAG` (MELDUNG_CD, MELDUNG_TEXT, EINTRAG_NR, VERTRAG_NR, MELDUNG_GUELTIG_CD, MELDUNG_GUELTIG_TEXT)
  SELECT
    T1.MELDUNG_CD,
    T1.MELDUNG_TEXT,
    T2.EINTRAG_NR,
    T2.VERTRAG_NR,
    T2.MELDUNG_GUELTIG_CD,
    T2.MELDUNG_GUELTIG_TEXT
  FROM `project.dataset.DWTK_MELDUNGEN` AS T1
  JOIN `project.dataset.SOF$TA_RN_EINZELN` AS T2
    ON T1.MELDUNG_CD = T2.MELDUNG_CD
    AND T1.MELDUNG_GUELTIG_CD = T2.MELDUNG_GUELTIG_CD;

  SET v_records = (SELECT COUNT(*) FROM `project.dataset.SOF$TA_RN_VERTRAG` WHERE -- add appropriate filter for records just inserted if needed --);

  -- Optional: Log job completion/metrics
  INSERT INTO `project.dataset.job_tracking` (timestamp, job_name, job_kennung, eintrags_nr, stichtag, records_processed, description)
  VALUES (CURRENT_TIMESTAMP(), v_TabName, p_JobKennung, p_EintragsNr, p_Stichtag, v_records, 'Initialbefuellung');

  SELECT '---------- ENDE Datenverarbeitung ----------' AS StatusMessage, v_records AS RecordsProcessed;

END;
```

## 6. External Dependencies

Based on the analysis, there are no direct external system dependencies (e.g., Oracle, SFTP, S3) identified for this specific job in its current execution.

The job primarily relies on:
-   **Database Access**: Oracle database (implied by `SQLPlus` wrappers and `isbert_schema`). This will be replaced by BigQuery's native data storage.
-   **Internal Utility Scripts**: Several KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) are sourced. These will be internalized and replaced by BigQuery Stored Procedure logic or UDFs as described in Section 5.
-   **Oracle Package**: `DWPA_UTIL_SKRIPT` invoked within the SQL script. This needs specific investigation for its functionality and re-implementation in BigQuery if essential.

All these dependencies will be replaced by native BigQuery features, eliminating external system calls.

## 7. Unresolved / Risks

-   **`starteSQLSkript` and `DWPA_UTIL_SKRIPT` Functionality (High Risk)**: The exact internal logic of `starteSQLSkript` (a wrapper for SQLPlus) and the `DWPA_UTIL_SKRIPT.runstatement()` Oracle package call are not fully transparent. The current design assumes they primarily execute the provided SQL. A detailed understanding or re-engineering of these components is crucial to ensure accurate migration. This may involve custom BigQuery procedures or functions.
-   **Environment Variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`) (Medium Risk)**: The values and usage patterns of these variables, which define directories, need to be understood. They will be translated into BigQuery dataset/project references, constants within the stored procedure, or configuration parameters if dynamic.
-   **Commented-Out Code (Low Risk)**: The `sed`, `sort`, `join` operations are commented out, but their presence indicates they might have been part of the workflow or could be re-activated. Confirmation that these are permanently defunct is needed. If not, their migration using BigQuery SQL equivalents would be required.
-   **Error Logging and Job Tracking (Low Risk)**: The original script includes `DWMSG_MeldeFehler` and a commented `FOSJobErzeugeEintrag`. A standardized BigQuery error logging and job tracking mechanism (e.g., dedicated logging tables) should be designed and implemented to replicate this functionality.

## 8. Build Plan

The build plan outlines the sequence of steps to implement the migration in BigQuery:

1.  **Define BigQuery Datasets**:
    -   Create source and target datasets (e.g., `project.dataset`) in BigQuery if they don't already exist.

2.  **Migrate Source Data to BigQuery Tables**:
    -   Ensure `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF$TA_RN_EINZELN` tables are created and populated in BigQuery. This might involve data ingestion pipelines from the legacy Oracle source.
    -   **Language**: BigQuery DDL, Data Loading (e.g., `bq load`, Cloud Dataflow, Cloud Storage transfer).

3.  **Create Target BigQuery Table**:
    -   Create the `project.dataset.SOF$TA_RN_VERTRAG` table with the appropriate schema.
    -   **Language**: BigQuery DDL.

4.  **Implement Auxiliary BigQuery Components (if necessary)**:
    -   **Error Logging Table**: Create a table (e.g., `project.dataset.error_log`) to capture error messages and job status.
    -   **Job Tracking Table**: Create a table (e.g., `project.dataset.job_tracking`) to log job execution details, similar to the original commented `FOSJobErzeugeEintrag`.
    -   **Language**: BigQuery DDL.

5.  **Develop BigQuery Stored Procedure (`r_ausd_bp_ta_rn_vertrag`)**:
    -   Translate the parameter parsing, validation, date derivation, and the core SQL logic into a BigQuery Stored Procedure as detailed in Section 5.
    -   Address the `DWPA_UTIL_SKRIPT` functionality. If it's a simple dynamic SQL executor, it can be omitted. If it has complex logic, re-implement it as an internal BigQuery function or stored procedure.
    -   Integrate error handling (`SIGNAL SQLSTATE`) and logging calls into the error and job tracking tables.
    -   **Language**: BigQuery Stored Procedure SQL.

6.  **Implement Orchestration (Optional, if external scheduling needed)**:
    -   Create a Cloud Composer (Airflow) DAG that invokes the BigQuery Stored Procedure, passing the required parameters.
    -   Define Airflow variables or connections for BigQuery project/dataset IDs.
    -   **Language**: Python (for Airflow DAG).

7.  **Testing**:
    -   Thoroughly test the BigQuery Stored Procedure with various parameter combinations, including edge cases and invalid inputs.
    -   Validate data integrity and correctness of the output in `SOF$TA_RN_VERTRAG`.
    -   Compare record counts and output data with the legacy system if possible.
    -   **Language**: BigQuery SQL, Python (for orchestration testing).