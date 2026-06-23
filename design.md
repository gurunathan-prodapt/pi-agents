# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_geschaeftspartner.ksh`. This script serves as an orchestration wrapper, responsible for the initial provisioning of contract caches for the Forderungsscoring (FOS) system. It manages parameter parsing (cutoff date and restart value), handles error logging, and delegates the core data generation logic to a separate kernel script, `k_ausd_geschaeftspartner.ksh`. The script produces a snapshot extraction based on a given cutoff date.

## 2. Source Inventory
The job consists of a single KornShell script:
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh`
- **Technology**: KornShell
- **Complexity Tier**: Medium
- **Migration Automation Bucket**: Semi-automated
- **Purpose**: Orchestration, parameter handling, and logging for a data provisioning process.

## 3. Target Architecture
The legacy KornShell orchestration script will be migrated to BigQuery using a combination of:
-   **BigQuery Stored Procedures**: The primary orchestration logic, parameter handling, and conditional flows will be implemented as a BigQuery stored procedure.
-   **BigQuery Tables for Logging/Auditing**: Legacy file-based logging and job status tracking will be replaced by dedicated BigQuery tables (e.g., `job_log`, `job_registry`).
-   **External Orchestration (Optional)**: For more complex error handling, retries, or signal management (like `trap` in shell scripts) that are not natively supported in BigQuery SQL, a managed orchestration service such as Cloud Composer (Apache Airflow), Workflows, or Cloud Run could be employed.
-   **Core Logic Reimplementation**: The actual data provisioning and transformation logic, currently residing in `k_ausd_geschaeftspartner.ksh`, will need to be reimplemented in BigQuery SQL (e.g., within another stored procedure) or as a separate data pipeline (e.g., using Dataflow or Spark on Dataproc) that the BigQuery orchestrator invokes.

**BigQuery SQL Pseudocode for Orchestration:**
```sql
-- BigQuery stored procedure wrapper for the shell orchestration logic
CREATE OR REPLACE PROCEDURE `project.dataset.sp_initial_befuellung_vertrags_cache_fos`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'BERT_P_GESCHAEFTSP';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- System date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_wiederanlaufWert = 0;
  ELSE
    SET v_wiederanlaufWert = p_wiederanlaufWert;
  END IF;

  -- Default cutoff date
  IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
    SET v_stichtag = v_sysdate;
  ELSE
    SET v_stichtag = p_stichtag;
  END IF;

  -- Validate required parameter
  IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  IF v_errnr <> 0 THEN
    INSERT INTO `project.dataset.job_log`
      (job_kennung, event_ts, level, errnr, errarg, message)
    VALUES
      (v_jobkennung, CURRENT_TIMESTAMP(), 'ERROR', v_errnr, v_errarg, 'Missing required parameter');
    RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg);
  END IF;

  -- Job number / log metadata replacement
  INSERT INTO `project.dataset.job_registry`
    (job_kennung, created_ts, stichtag, sysdate, restart_value, status)
  VALUES
    (v_jobkennung, CURRENT_TIMESTAMP(), v_stichtag, v_sysdate, v_wiederanlaufWert, 'STARTED');

  SET v_eintragsnr = (
    SELECT MAX(eintragsnr)
    FROM `project.dataset.job_registry`
    WHERE job_kennung = v_jobkennung
  );

  SET v_logdatei = CONCAT('BQ_LOG_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING));

  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (v_jobkennung, v_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     CONCAT('Job started. Stichtag=', v_stichtag, ', Restart=', CAST(v_wiederanlaufWert AS STRING)));

  -- Delegate core business logic to another stored procedure
  CALL `project.dataset.sp_ausd_geschaeftspartner`(
    v_jobkennung,
    v_stichtag,
    v_eintragsnr,
    v_wiederanlaufWert
  );

  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintragsnr, event_ts, level, message)
  VALUES
    (v_jobkennung, v_eintragsnr, CURRENT_TIMESTAMP(), 'INFO',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

  UPDATE `project.dataset.job_registry`
  SET status = 'OK',
      finished_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = v_jobkennung
    AND eintragsnr = v_eintragsnr;

END;
```

## 4. Data Flow & Lineage
The `r_ausd_geschaeftspartner.ksh` script acts as the entry point and orchestrator.
-   **Inputs**:
    -   Command-line parameters: `-s` (cutoff date DDMMYYYY), `-l` (restart/resume value for `DWH_VERTRAG_ID`).
    -   Environment variables and sourced shell scripts for initialization, error handling, and date utilities (e.g., `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
-   **Processing**:
    -   Parses input parameters.
    -   Initializes environment and logging mechanisms.
    -   Determines the effective cutoff date (defaults to system date if not provided).
    -   Validates parameters.
    -   Invokes the core script `k_ausd_geschaeftspartner.ksh` with derived parameters (`JobKennung`, `p_stichtag`, `DW_EintragsNr`, `p_wiederanlaufWert`).
-   **Outputs**:
    -   Writes job execution logs and status messages to a dynamically named log file.
    -   Updates job status (e.g., `OK`) via logging functions.
    -   The core data output (FOS cache tables) is produced by the invoked script `k_ausd_geschaeftspartner.ksh`.

## 5. Transformation Logic
The `r_ausd_geschaeftspartner.ksh` script itself does not contain direct data transformation logic. Its primary functions are:
-   **Parameter Handling**: Parsing command-line arguments (`-s`, `-l`) and setting default values for the cutoff date (`p_stichtag`) and restart value (`p_wiederanlaufWert`).
-   **Environment Setup**: Sourcing initialization scripts like `$HOME/.dw_init` and utility scripts for error handling (`f_alis_msgerr.ksh`), parameter processing (`h_alis_parameter.ksh`), and date operations (`h_alis_date.ksh`).
-   **Logging and Error Handling**: Implementing a robust logging framework using functions like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`. It uses `set -e` and `trap` for error and interrupt management.
-   **Orchestration**: Invoking the external kernel script `k_ausd_geschaeftspartner.ksh` with the prepared parameters.

The actual data extraction, transformation, and loading into the FOS cache tables are performed by `k_ausd_geschaeftspartner.ksh`. This includes:
-   Selecting source data based on `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`.
-   Handling deletion/reloading of records based on the `p_wiederanlaufWert` (contracts with `DWH_VERTRAG_ID > restart_value` processed, existing entries `>= restart_value` deleted).

## 6. External Dependencies
The source script has the following external dependencies:
-   **Environment Initialization Script**: `$HOME/.dw_init` - This sets up the shell environment and necessary paths. In BigQuery, this will be replaced by explicit variable declarations or parameters within stored procedures, or by environment configuration in the orchestrator (e.g., Cloud Composer).
-   **Utility Scripts**:
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utilities.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities (e.g., `DWDate_Gib_Zeitraum`).
    These utility functions will be replaced by:
        -   Built-in BigQuery functions (e.g., `FORMAT_DATE`, `CURRENT_DATE`).
        -   Custom BigQuery UDFs or separate helper stored procedures for complex logic not directly available in BigQuery SQL.
        -   Logging functionality integrated into BigQuery audit/log tables.
-   **Core Business Logic Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` - This is the most critical dependency, as it holds the actual data processing logic. This script needs to be fully analyzed and its SQL logic reimplemented as a separate BigQuery stored procedure or a data pipeline (e.g., Dataflow job).
-   **FOS-Tabelle**: This is a critical table referenced in the script's description (Forderungsscoring table). This will be migrated to a BigQuery table.
-   **DWH_VERTRAG_ID**: A critical identifier for contracts, likely a column in source/target tables.

There were no `external_systems` identified in the `lineage_assembled_jobs` for this particular job, indicating no direct calls to external databases, SFTP, S3, etc., from this specific shell script.

## 7. Unresolved / Risks
-   **Core Logic Obscurity**: The most significant unresolved item is the detailed content and logic of the invoked script `k_ausd_geschaeftspartner.ksh`. Without its analysis, the full end-to-end migration design cannot be completed. This script is marked as 'critical' in `references_out`.
-   **Shell-specific Features**:
    -   `getopts`: Shell parameter parsing will be replaced by BigQuery stored procedure parameters.
    -   `trap` statements for `INT`, `STOP`, `CONT`, `ERR`: These signal handling mechanisms cannot be directly replicated in BigQuery SQL. Error handling will rely on `RAISE` in BigQuery SQL and external orchestration features for retries and failure notifications.
    -   File-based logging: This will be replaced by inserts into BigQuery log tables.
    -   Sourced shell libraries: These require careful review to identify equivalent BigQuery functions or to rewrite as UDFs/procedures.
-   **Date Logic Discrepancy**: The source script's comments suggest defaulting to `MIN(sysdate, maxladedatum)` if `p_stichtag` is not set, but the active code defaults to `p_stichtag=$v_sysdate` only, with `FOSHoleLadedatum` logic commented out. Clarification is needed on the intended default behavior for `p_stichtag`.
-   **Job Identification**: The `DW_EintragsNr` is dynamically determined. This needs to be managed consistently in the BigQuery environment, potentially via sequence generation or metadata table management.

## 8. Build Plan
1.  **Define BigQuery Schema for Logging and Registry**:
    -   Create `project.dataset.job_log` table (e.g., `job_kennung STRING`, `eintragsnr INT64`, `event_ts TIMESTAMP`, `level STRING`, `errnr INT64`, `errarg STRING`, `message STRING`).
    -   Create `project.dataset.job_registry` table (e.g., `job_kennung STRING`, `created_ts TIMESTAMP`, `finished_ts TIMESTAMP`, `stichtag STRING`, `sysdate STRING`, `restart_value INT64`, `status STRING`).
    -   **Language**: BigQuery DDL
2.  **Migrate Orchestration Script to BigQuery Stored Procedure**:
    -   Translate `r_ausd_geschaeftspartner.ksh` into the BigQuery stored procedure `project.dataset.sp_initial_befuellung_vertrags_cache_fos`, handling parameter passing, default values, and basic validation as outlined in the pseudocode.
    -   Replace shell-specific date and logging calls with BigQuery SQL equivalents (e.g., `FORMAT_DATE`, `CURRENT_DATE`, `INSERT INTO job_log`).
    -   Implement error handling using `RAISE` and appropriate `IF` conditions.
    -   **Language**: BigQuery SQL
3.  **Analyze and Migrate `k_ausd_geschaeftspartner.ksh`**:
    -   **CRITICAL STEP**: Fully analyze the content of `k_ausd_geschaeftspartner.ksh` to understand its data extraction, transformation, and loading logic.
    -   Reimplement this logic as a separate BigQuery stored procedure, e.g., `project.dataset.sp_ausd_geschaeftspartner`, taking parameters like `job_kennung`, `stichtag`, `eintragsnr`, and `wiederanlaufWert`.
    -   Identify all source tables (e.g., those from which data for `FOS-Tabelle` is derived) and define their BigQuery equivalents.
    -   **Language**: BigQuery SQL
4.  **Integrate Core Logic Call**:
    -   Modify `sp_initial_befuellung_vertrags_cache_fos` to `CALL` the newly created `sp_ausd_geschaeftspartner` with the correct parameters.
    -   **Language**: BigQuery SQL
5.  **Develop External Orchestration (if needed)**:
    -   If complex error recovery, scheduling, or retry mechanisms are required beyond BigQuery's native capabilities, design and implement an orchestration layer using tools like Cloud Composer (Airflow) or Workflows to trigger `sp_initial_befuellung_vertrags_cache_fos`.
    -   **Language**: Python (for Airflow DAGs), YAML/JSON (for Workflows).
6.  **Testing**:
    -   Unit test the BigQuery stored procedures.
    -   Integration test the full workflow from external trigger to data loading, verifying logs and target table contents.