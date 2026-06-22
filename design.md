# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

## 1. Purpose & Scope
This KornShell script (`r_ausd_bp_ta_cntrct_evn.ksh`) serves as an orchestration job responsible for the initial provisioning of selected base products (Basisprodukte), specifically contract events, for the BERT system. Its primary purpose is to extract a snapshot of contract cache data from the Data Warehouse (DWH) and make it available for demand scoring (Forderungsscoring). The script handles date determination (cutoff date/Stichtag) and manages restart values (`Wiederanlaufwert`) for incremental processing. It acts as a wrapper that prepares parameters and environment, then delegates the core data processing logic to another shell script (`k_ausd_bp_ta_cntrct_evn.ksh`).

## 2. Source Inventory
The job consists of one primary source file and several implicit or explicit dependencies.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh`
    *   **Technology**: KornShell Script (`.ksh`)
    *   **Tier**: medium
    *   **Automation Bucket**: semi_auto
    *   **Summary**: Orchestrates the preparation of selected base products (Basisprodukte) for the BERT system, specifically contract events. It extracts data from the DWH and makes it available for demand scoring (Forderungsscoring), handling date determination and restart values.
*   **Invoked Script (Core Logic)**: `k_ausd_bp_ta_cntrct_evn.ksh` (relative path from `BERT_DIR_ROOT}/aufbereitung/bin/`) - This script contains the actual data transformation and extraction logic.
*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init` (environment initialization)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling framework)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)

## 3. Target Architecture
The target architecture on BigQuery will involve:
*   **BigQuery Stored Procedures**:
    *   A main stored procedure, `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`, to replace the orchestrating KornShell script. This procedure will handle parameter parsing, validation, date defaulting, logging, and then call the core logic procedure.
    *   A separate stored procedure, `project.dataset.ausd_bp_ta_cntrct_evn_core`, to encapsulate the core data transformation and extraction logic previously held in `k_ausd_bp_ta_cntrct_evn.ksh`.
*   **BigQuery Tables**:
    *   `project.dataset.job_log`: An audit table for logging job execution details, status, and messages, replacing file-based logging.
    *   `project.dataset.job_control`: A control table to manage job numbers and status, potentially replacing aspects of the `DW_EintragsNr` and status tracking.
    *   Source tables for contract cache data (from DWH, e.g., `DWH$TA_C_VERTRAG`), which will be external tables or replicated tables in BigQuery.
    *   Target staging/FOS tables to store the prepared contract event data.
*   **Orchestration**: Cloud Composer (Airflow) or Google Cloud Workflows to schedule and execute the BigQuery stored procedures. This will replace the shell script's execution context.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_cntrct_evn.ksh` functions as follows:
1.  **Initialization**: Sources common environment and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). Initializes variables for error handling and job metadata.
2.  **Parameter Input**: Accepts command-line parameters:
    *   `-s`: Stichtag (cutoff date in DDMMYYYY format)
    *   `-l`: Wiederanlaufwert (restart value for DWH_VERTRAG_ID)
3.  **Parameter Processing**:
    *   Defaults `p_wiederanlaufWert` to 0 if not provided.
    *   Determines `v_sysdate`.
    *   Defaults `p_stichtag` to `v_sysdate` if not provided. The original script also includes commented-out logic for `MIN(sysdate,maxladedatum)` which suggests a dependency on a `maxladedatum` for `DWH$TA_C_VERTRAG`. This should be investigated in the core script.
    *   Validates that `p_stichtag` is set. If not, an error is logged, usage is displayed, and the script exits.
4.  **Logging Setup**: Sets up a job identifier (`JobKennung`), determines an entry number (`DW_EintragsNr`), and configures a log file (`LogDatei`) using custom `DWMSG_*` functions.
5.  **Error Handling**: Installs shell `trap` commands to catch `INT`, `STOP`, `CONT`, and `ERR` signals for robust error handling and logging.
6.  **Core Logic Invocation**: Calls the core processing script `${Name_Kernskript}` (which resolves to `k_ausd_bp_ta_cntrct_evn.ksh`) with the processed parameters: `-j $JobKennung`, `-s $p_stichtag`, `-f ${DW_EintragsNr}`, `-l ${p_wiederanlaufWert}`. All output is redirected to the configured `LogDatei`.
7.  **Completion**: If the core script executes successfully, a success message is logged, and the job status is updated via `DWMSG_SetzeStatusOK`. Traps are cleared, and the script exits with status 0.

**BigQuery Data Flow**:
The `ausd_bp_ta_cntrct_evn_wrapper` stored procedure will replicate this orchestration flow. It will manage parameters, logging to `job_log` and `job_control`, and then invoke `ausd_bp_ta_cntrct_evn_core`. The `ausd_bp_ta_cntrct_evn_core` procedure will perform the actual data extraction from DWH source tables (e.g., `source_table`) and load it into the target FOS staging table, applying date and restart value filters as described in the original script's purpose.

## 5. Transformation Logic
The transformation will focus on porting the shell script's orchestration logic and parameter handling to BigQuery Stored Procedures.

**Original Script Logic Components and BigQuery Equivalents**:

*   **Parameter Handling (`getopts`)**: Replaced by BigQuery Stored Procedure input parameters (`IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`).
*   **Defaulting Parameters**: `IFNULL()` function for `p_wiederanlaufWert` and conditional logic (`IF p_stichtag IS NULL THEN ... ELSE ... END IF`) for `v_stichtag`.
*   **Date Determination (`DWDate_Gib_Zeitraum`, `h_alis_date.ksh`)**: Replaced by BigQuery's `CURRENT_DATE()` for system date and `PARSE_DATE('%d%m%Y', p_stichtag)` for parsing the input Stichtag.
*   **Parameter Validation (`pruefeParameterGesetzt`)**: Replaced by `IF ... THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ... END IF` in BigQuery Stored Procedures for explicit error signaling.
*   **Logging (`DWMSG_*` functions, file redirection)**: Replaced by `INSERT INTO project.dataset.job_log` to store structured log entries in a BigQuery table.
*   **Error Handling (`trap`)**: Replaced by BigQuery Stored Procedure `EXCEPTION WHEN ERROR THEN ... END;` blocks for structured error trapping and logging.
*   **Core Script Invocation (`${Name_Kernskript}`)**: Replaced by `CALL project.dataset.ausd_bp_ta_cntrct_evn_core(...)`, ensuring the core logic runs within BigQuery.
*   **Data Extraction/Transformation (within `k_ausd_bp_ta_cntrct_evn.ksh`)**: The details are not available here, but the `ausd_bp_ta_cntrct_evn_core` will be responsible for implementing the SQL-based logic, including filtering by `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` based on `p_stichtag` and `p_wiederanlaufWert`.

**Pseudocode (BigQuery SQL)**:

```sql
-- Wrapper Stored Procedure
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(
  IN p_stichtag_str STRING, -- Input as string "DDMMYYYY"
  IN p_wiederanlaufWert_input INT64 -- Input as integer, can be NULL
)
BEGIN
  DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
  DECLARE v_stichtag DATE;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_job_nr INT64;
  -- DECLARE v_log_id STRING; -- Not strictly needed for structured logging
  DECLARE v_error_message STRING;

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert_input, 0);

  -- Determine cutoff date
  IF p_stichtag_str IS NULL OR TRIM(p_stichtag_str) = '' THEN
    SET v_stichtag = v_sysdate;
  ELSE
    SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_str);
  END IF;

  -- Validate required parameter
  IF v_stichtag IS NULL THEN
    SET v_error_message = 'Required parameter Stichtag is missing or invalid (expected DDMMYYYY).';
    INSERT INTO `project.dataset.job_log` (job_kennung, log_ts, log_level, message)
    VALUES (v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', v_error_message);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Generate job number and log entry
  -- This requires a job_control table to manage sequence or rely on BigQuery's native logging
  -- For this example, assuming a simple increment or UUID for job_nr.
  -- A more robust solution might use a sequence table or a dedicated job control procedure.
  SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_control`);
  
  INSERT INTO `project.dataset.job_log`
  (job_nr, job_kennung, log_ts, log_level, message, stichtag, restart_value)
  VALUES
  (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO',
   'Job started', v_stichtag, v_wiederanlaufWert);

  INSERT INTO `project.dataset.job_control` (job_nr, job_kennung, start_ts, status)
  VALUES (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'RUNNING');

  BEGIN
    -- Call the core processing procedure
    CALL `project.dataset.ausd_bp_ta_cntrct_evn_core`(
      v_jobkennung,
      v_stichtag,
      v_job_nr,
      v_wiederanlaufWert
    );

    INSERT INTO `project.dataset.job_log`
    (job_nr, job_kennung, log_ts, log_level, message)
    VALUES
    (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'INFO',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

    UPDATE `project.dataset.job_control`
    SET status = 'OK', end_ts = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr;

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = CONCAT('AppError: Abbruch - ', @@error.message);
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_kennung, log_ts, log_level, message)
    VALUES
    (v_job_nr, v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', v_error_message);

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR', end_ts = CURRENT_TIMESTAMP(), error_message = v_error_message
    WHERE job_nr = v_job_nr;

    RAISE; -- Re-raise the exception
  END;

END;
```

```sql
-- Core Processing Stored Procedure (Placeholder)
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_core`(
  IN p_jobkennung STRING,
  IN p_stichtag DATE,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Placeholder for downstream logic originally executed by k_ausd_bp_ta_cntrct_evn.ksh
  -- This procedure will contain the actual SELECT, DELETE, INSERT statements
  -- to extract from DWH source tables and load into FOS target tables.

  -- Example: Delete existing data for restart (if applicable)
  -- DELETE FROM `project.dataset.fos_target_table`
  -- WHERE dwh_vertrag_id >= p_wiederanlaufWert;

  -- Example: Insert new/updated data
  -- INSERT INTO `project.dataset.fos_target_table` (...)
  -- SELECT
  --   -- Selected columns
  -- FROM `project.dataset.dwh_ta_c_vertrag_source` -- Replace with actual DWH table
  -- WHERE
  --   gueltig_von <= p_stichtag
  --   AND p_stichtag < gueltig_bis
  --   AND ladedatum < p_stichtag
  --   AND dwh_vertrag_id > p_wiederanlaufWert; -- Apply restart filter
  
  -- Log progress or specific actions as needed
  INSERT INTO `project.dataset.job_log` (job_nr, job_kennung, log_ts, log_level, message)
  VALUES (p_job_nr, p_jobkennung, CURRENT_TIMESTAMP(), 'INFO', 'Core processing completed successfully.');

END;
```

## 6. External Dependencies
The original script has no direct external system dependencies detected in `lineage_assembled_jobs`. However, based on the script content and purpose, the following can be inferred:

*   **DWH (Data Warehouse)**: The script extracts data from a DWH. In a BigQuery migration, this DWH source data would need to be ingested into BigQuery, either as managed tables or external tables linked to a cloud storage solution (e.g., Cloud Storage). The reference to `DWH$TA_C_VERTRAG` implies an Oracle or similar RDBMS source that must be migrated.
*   **FOS (Forderungsscoring)**: The processed data is made available for a "demand scoring" system. This implies a downstream consumer. In BigQuery, this would typically involve making the target BigQuery table accessible to the FOS system, possibly via BigQuery APIs, exports to Cloud Storage, or direct table access.
*   **Operating System Utilities**: Standard Unix/Linux shell commands (`print`, `cat`, `set`, `trap`, `getopts`, `exit`, `tee`) and file system operations (`.`, `>>`). These will be replaced by BigQuery SQL constructs, stored procedure mechanisms, and cloud orchestration tools.

## 7. Unresolved / Risks
*   **`k_ausd_bp_ta_cntrct_evn.ksh` Logic**: The actual business logic within the invoked core script (`k_ausd_bp_ta_cntrct_evn.ksh`) is not available. This needs to be analyzed separately and translated into BigQuery SQL within the `ausd_bp_ta_cntrct_evn_core` stored procedure. Without this, the migration of the full job is incomplete.
*   **DWH Source Table Schema**: The exact schema of `DWH$TA_C_VERTRAG` (or equivalent) is unknown. This is critical for translating the data extraction queries.
*   **`maxladedatum` Logic**: The commented-out `FOSHoleLadedatum` and related `MIN(sysdate,maxladedatum)` logic suggests a more complex date determination. This needs to be fully understood and implemented in BigQuery if it's still required.
*   **Utility Script Logic**: The exact functions performed by the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are not fully detailed. While basic replacements are proposed, any complex logic within them would need careful porting.
*   **Job Control and Audit Tables**: The design assumes the creation of `project.dataset.job_log` and `project.dataset.job_control` tables. Their exact schema and management (e.g., how `job_nr` is generated uniquely and reliably) need to be defined.
*   **FOS Integration**: The mechanism by which the target FOS system consumes the data from BigQuery needs to be clearly defined and implemented.

## 8. Build Plan
1.  **Define BigQuery Schemas**:
    *   Create `project.dataset.job_log` table (e.g., `job_nr INT64, job_kennung STRING, log_ts TIMESTAMP, log_level STRING, message STRING, stichtag DATE, restart_value INT64, error_message STRING`).
    *   Create `project.dataset.job_control` table (e.g., `job_nr INT64, job_kennung STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, status STRING, error_message STRING`).
    *   Define schemas for DWH source tables (e.g., `project.dataset.dwh_ta_c_vertrag_source`) and FOS target tables (e.g., `project.dataset.fos_target_table`).
2.  **Translate Core Logic**: Analyze `k_ausd_bp_ta_cntrct_evn.ksh` to identify the SQL equivalent of its data extraction, transformation, and loading logic.
3.  **Implement `ausd_bp_ta_cntrct_evn_core`**: Create the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_evn_core` using the translated core logic. (Language: BigQuery SQL)
4.  **Implement `ausd_bp_ta_cntrct_evn_wrapper`**: Create the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` as designed above, handling parameters, logging, and calling the core procedure. (Language: BigQuery SQL)
5.  **Develop Orchestration**: Create an Airflow DAG (using Cloud Composer) or a Google Cloud Workflow to schedule and trigger the `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` stored procedure. This will also manage passing parameters (`p_stichtag_str`, `p_wiederanlaufWert_input`) at runtime. (Language: Python for Airflow DAG, YAML/JSON for Workflows)
6.  **Data Ingestion**: Establish pipelines for ingesting data from the legacy DWH (e.g., `DWH$TA_C_VERTRAG`) into BigQuery. This might involve tools like Cloud Data Fusion, Dataflow, or custom ETL processes.
7.  **FOS Integration**: Implement the mechanism for the FOS system to consume data from the BigQuery target table.