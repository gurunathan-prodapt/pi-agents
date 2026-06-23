# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

## 1. Purpose & Scope
The KornShell script `k_ausd_v_ta_notice.ksh` serves as a control script for a data processing job related to `r_ausd_vertrag.ksh`. Its primary purpose is to orchestrate the execution of an underlying SQL script (`d_ausd_v_ta_notice.sql`), manage job identifiers, handle parameter validation, and provide basic error reporting. It's also responsible for ignoring active jobs, making an entry into a job table, and deactivating old active jobs. This specific job (lineage run ID `6d73ee79-8207-4271-b787-9644c913bf51`) involves a single component file and has a medium complexity distribution.

## 2. Source Inventory
The job is composed of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh`
*   **Technology:** KornShell (`shell` category)
*   **Complexity Tier:** Medium
*   **Automation Bucket:** Semi-Auto (B2)
*   **Purpose:** Orchestration, parameter handling, SQL script invocation.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily BigQuery for data storage and processing, and Cloud Composer (Apache Airflow) or Cloud Workflows for orchestration.

*   **Data Processing:** The core logic of the `k_ausd_v_ta_notice.ksh` script (parameter handling, job state management, error handling) will be refactored into a BigQuery Stored Procedure. The underlying `d_ausd_v_ta_notice.sql` will be translated into BigQuery SQL statements, potentially incorporated directly into the BigQuery Stored Procedure or executed as separate BigQuery queries.
*   **Data Storage:** All source and target data will reside in BigQuery tables. This includes the `ta_notice` table, a new `job_table` for tracking job execution statuses, and an `error_log` table for capturing exceptions.
*   **Orchestration:** Cloud Composer (Airflow) will be used to schedule and execute the BigQuery Stored Procedure. It will be responsible for passing parameters to the stored procedure and monitoring its execution.
*   **Environment Variables:** Legacy environment variables like `$HOME`, `$BERT_DIR_ROOT`, and `$DW_DIR_UTL` will be replaced by explicit BigQuery project/dataset references, Airflow Variables, or configuration stored in other GCP services like Secret Manager or Parameter Store.

## 4. Data Flow & Lineage
The original process flow for `k_ausd_v_ta_notice.ksh` involves:

1.  **Environment Setup:** Sourcing `$HOME/.dw_init` and several utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Parsing:** Reads `p_JobKennung` (job identifier) and `p_EintragsNr` (entry number) from command-line arguments using `getopts`.
3.  **Parameter Validation:** Checks if required parameters are set. If not, logs an error using `DWMSG_MeldeFehler` and exits.
4.  **SQL Script Execution:** Calls a wrapper function `starteSQLSkript` to execute `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_notice.sql`. This script is expected to perform the actual data processing and database updates.
5.  **Record Count Retrieval:** Reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`).
6.  **Job Completion:** Prints an end-of-processing message.

**Target BigQuery Data Flow:**

1.  **Airflow DAG:** A Cloud Composer DAG will trigger a BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as arguments.
2.  **BigQuery Stored Procedure (`project.dataset.r_ausd_v_ta_notice`):**
    *   Receives input parameters.
    *   Performs parameter validation using `IF` statements. If validation fails, it inserts an error record into an `error_log` table and exits.
    *   Inserts or updates a `job_table` with the job's status.
    *   Executes the translated BigQuery SQL logic derived from `d_ausd_v_ta_notice.sql`, which will interact with source and target BigQuery tables (e.g., `project.dataset.ta_notice`).
    *   Calculates the number of processed records (e.g., using `COUNT(*)` or `ROW_COUNT()`) and updates the `job_table` with this count.
    *   Handles exceptions using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks, logging errors to the `error_log` table.

## 5. Transformation Logic
The transformation logic for the shell script itself is primarily orchestrational. The CM MCP tool provided the following BigQuery SQL pseudocode for the `k_ausd_v_ta_notice.ksh` script's logic:

```sql
-- BigQuery Stored Procedure pseudocode
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_notice`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_notice';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    INSERT INTO `project.dataset.error_log`
    (error_number, error_argument, procedure_name, created_at)
    VALUES
    (ErrNr, ErrArg, 'r_ausd_v_ta_notice', CURRENT_TIMESTAMP());

    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    LEAVE;
  END IF;

  -- Main processing block
  BEGIN
    -- Equivalent of starteSQLSkript
    -- Replace with the actual BigQuery SQL logic from d_ausd_v_ta_notice.sql

    -- Example job handling logic
    INSERT INTO `project.dataset.job_table`
    (job_kennung, eintrags_nr, tab_name, status, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', CURRENT_TIMESTAMP());

    -- Example data processing placeholder
    -- INSERT/UPDATE/MERGE statements derived from d_ausd_v_ta_notice.sql

    -- Record count replacement for temp file
    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.ta_notice`
      WHERE eintrags_nr = p_EintragsNr
    );

    UPDATE `project.dataset.job_table`
    SET status = 'DEACTIVATED',
        record_count = v_records,
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND status = 'ACTIVE';

    SELECT '---------- ENDE Datenverarbeitung ----------' AS message;
    SELECT v_records AS records_processed;

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.error_log`
    (error_number, error_argument, procedure_name, created_at, error_message)
    VALUES
    (ErrNr, ErrArg, 'r_ausd_v_ta_notice', CURRENT_TIMESTAMP(), @@error.message);

    SELECT FORMAT('FEHLER: %s', @@error.message) AS message;
    RAISE;
  END;
END;
```

The content of `d_ausd_v_ta_notice.sql` needs to be analyzed separately and translated into BigQuery SQL.

## 6. External Dependencies
The original job has the following identified dependencies:

*   **Oracle/SQL*Plus (Implicit):** The script uses an "SQL execution wrapper likely invoking Oracle/SQL*Plus or similar." This indicates an underlying database dependency that will be migrated to BigQuery.
*   **Local File System/Temporary Files:** The script uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`) to store record counts. In BigQuery, this will be replaced by:
    *   BigQuery Stored Procedure variables.
    *   Direct updates to audit or job tracking tables.
*   **Shell Environment/Utility Scripts:** The script sources several utility KornShell scripts and relies on environment variables. These will be replaced by:
    *   BigQuery Stored Procedures for shared logic.
    *   Airflow Variables or GCP Parameter Store for configuration values.
    *   Explicit BigQuery project.dataset.table naming conventions.

No other external systems (SFTP, S3, etc.) were identified in the `lineage_assembled_jobs` record.

## 7. Unresolved / Risks
*   **`d_ausd_v_ta_notice.sql` Analysis:** The actual business logic and data transformations are presumed to reside within `d_ausd_v_ta_notice.sql`. This SQL script requires separate, detailed analysis and translation to BigQuery SQL. Any Oracle-specific SQL constructs within this file will need careful rewriting.
*   **Complex SQL Logic:** If `d_ausd_v_ta_notice.sql` contains highly procedural or incompatible SQL logic, it may not be directly translatable to BigQuery SQL. In such cases, parts of the logic might need to be re-implemented in Python (e.g., using Cloud Functions or Cloud Run) and integrated into the Airflow orchestration.
*   **Undocumented Job Table Schema:** The exact schema and purpose of the "job table" mentioned in the script are not fully defined in the provided metadata. This requires investigation to ensure correct migration.
*   **Uncaptured Lineage:** The absence of `lineage_edges` for this specific job suggests that the automated lineage discovery for this run might not have captured all direct dependencies. Manual code review was crucial here to identify sourced scripts and the executed SQL file. This indicates a potential risk for other similar jobs.
*   **Error Handling Complexity:** The `DWMSG_MeldeFehler` function and exit codes indicate a custom error handling framework. This will need to be fully understood and re-implemented using BigQuery's error handling features (e.g., `EXCEPTION WHEN ERROR`) and integrated with Cloud Logging.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Analyze and Translate `d_ausd_v_ta_notice.sql`:**
    *   Extract the content of `d_ausd_v_ta_notice.sql`.
    *   Translate its DDL/DML statements into BigQuery SQL. Address any Oracle-specific syntax or features.
    *   (Language: BigQuery SQL)
2.  **Design BigQuery Stored Procedure (`r_ausd_v_ta_notice`):**
    *   Implement the orchestration logic from `k_ausd_v_ta_notice.ksh` as a BigQuery Stored Procedure.
    *   Incorporate the translated `d_ausd_v_ta_notice.sql` logic within this stored procedure.
    *   Include parameter validation, job table updates, and error handling as per the pseudocode in Section 5.
    *   (Language: BigQuery SQL)
3.  **Define BigQuery Tables:**
    *   Create the target `ta_notice` table schema in BigQuery.
    *   Define the schema for a `job_table` (e.g., `job_kennung`, `eintrags_nr`, `tab_name`, `status`, `record_count`, `created_at`, `updated_at`).
    *   Define the schema for an `error_log` table (e.g., `error_number`, `error_argument`, `procedure_name`, `created_at`, `error_message`).
    *   (Language: BigQuery DDL)
4.  **Develop Cloud Composer DAG (Airflow):**
    *   Create an Airflow DAG to schedule and trigger the `project.dataset.r_ausd_v_ta_notice` BigQuery Stored Procedure.
    *   Configure the DAG to pass `p_JobKennung` and `p_EintragsNr` as parameters.
    *   Set up appropriate scheduling and dependency management within the DAG.
    *   (Language: Python for Airflow DAG)
5.  **Refactor Utility Scripts (if needed):**
    *   If any functionality from the sourced utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is still required outside of the main stored procedure, consider rewriting it as BigQuery UDFs, Python modules for Airflow, or standalone Cloud Functions.
    *   (Language: BigQuery SQL/Python)
6.  **Testing and Validation:**
    *   Develop comprehensive test cases for the BigQuery Stored Procedure and the Airflow DAG.
    *   Validate data integrity and processing correctness against legacy outputs.