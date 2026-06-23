# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

## 1. Purpose & Scope
This document outlines the migration design for the `r_aurd_rechstan.ksh` KornShell script to Google Cloud Platform, specifically targeting BigQuery. The script's primary purpose is to orchestrate the generation of a snapshot (extract) of contract-related data from the Data Warehouse (DWH) to make it available for the credit scoring system (Forderungsscoring). It acts as a wrapper, handling parameter parsing (reference date, restart value), environment setup, logging, and error handling, before invoking a core processing script (`k_aurd_rechstan.ksh`) which contains the actual data extraction and transformation logic. The scope of this migration design focuses on replicating the orchestration and parameter management aspects of `r_aurd_rechstan.ksh`.

## 2. Source Inventory
The job is composed of a single KornShell script:

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh`
*   **Technology:** KornShell (shell script)
*   **Complexity Tier:** Medium
*   **Migration Bucket:** Semi-Auto
*   **Purpose:** Orchestration and parameter management for a data extract job.

## 3. Target Architecture
The `r_aurd_rechstan.ksh` script will be migrated to a BigQuery Stored Procedure. This stored procedure will handle the parameter input, validation, default value assignment, job logging, and invocation of the core data processing logic.

*   **Main Component:** A BigQuery Stored Procedure (e.g., `project.dataset.sp_erzeugung_abzug_rechnungsdaten`) to encapsulate the script's logic.
*   **Logging:** Dedicated BigQuery tables for job control, run logs, and error logs (e.g., `project.dataset.job_control`, `project.dataset.job_run_log`, `project.dataset.job_error_log`).
*   **Orchestration:** The UC4 job that currently invokes `r_aurd_rechstan.ksh` will be replaced, likely by Cloud Composer (Airflow DAG) or Google Cloud Workflows, to trigger the BigQuery Stored Procedure.
*   **Core Logic (`k_aurd_rechstan.ksh`):** The data extraction and transformation logic currently residing in `k_aurd_rechstan.ksh` will need to be migrated separately, likely into one or more BigQuery SQL scripts, views, or additional stored procedures, which will then be called by the `sp_erzeugung_abzug_rechnungsdaten` procedure.

## 4. Data Flow & Lineage
The original lineage shows that this job (`r_aurd_rechstan.ksh`) is invoked by an external UC4 job named `DW.BERT_RECHNUNGSDATEN.xml`. The `r_aurd_rechstan.ksh` script then invokes another shell script, `k_aurd_rechstan.ksh`, which presumably performs the actual data operations.

**Migration Data Flow:**
1.  **Trigger:** A Cloud Composer DAG or Google Cloud Workflow will initiate the `sp_erzeugung_abzug_rechnungsdaten` BigQuery Stored Procedure.
2.  **Parameter Handling & Logging:** The `sp_erzeugung_abzug_rechnungsdaten` procedure will receive parameters (reference date, restart value), validate them, apply defaults, and log job start information to `job_control` and `job_run_log` tables.
3.  **Core Data Processing:** The `sp_erzeugung_abzug_rechnungsdaten` procedure will then execute the migrated logic of `k_aurd_rechstan.ksh`. This will involve:
    *   Reading from source tables (e.g., `project.dataset.source_contract_cache`).
    *   Applying filtering based on the reference date (`gueltig_von`, `gueltig_bis`, `ladedatum`).
    *   Implementing restart logic (e.g., `DELETE` from target for `dwh_vertrag_id >= v_wiederanlaufWert`).
    *   Writing to target tables (e.g., `project.dataset.target_fos_table`).
4.  **Status Update:** Upon completion or error, the `sp_erzeugung_abzug_rechnungsdaten` procedure will update the status in the `job_control` table and log any errors to `job_error_log`.

## 5. Transformation Logic
The `r_aurd_rechstan.ksh` script itself performs no direct data transformations but acts as a control script. Its logic involves:

*   **Parameter Parsing:** Interprets command-line arguments `-s` (reference date `p_stichtag`) and `-l` (restart value `p_wiederanlaufWert`).
*   **Defaulting:** Initializes `p_wiederanlaufWert` to 0 if not provided. Determines `p_stichtag` based on the system date if not explicitly passed.
*   **Error Handling:** Uses `set -e`, helper scripts for error reporting (`f_alis_msgerr.ksh`), and `trap` for signal handling.
*   **Logging:** Uses a custom messaging framework (`DWMSG_`) to log job start, parameters, and final status to a log file.
*   **Invocation:** Calls `k_aurd_rechstan.ksh` with resolved parameters.

**BigQuery Stored Procedure (`sp_erzeugung_abzug_rechnungsdaten`) equivalent:**

```sql
-- BigQuery Stored Procedure: orchestration wrapper equivalent
CREATE OR REPLACE PROCEDURE `project.dataset.sp_erzeugung_abzug_rechnungsdaten`(
  IN p_stichtag STRING,              -- expected format: DDMMYYYY
  IN p_wiederanlaufWert INT64        -- restart threshold
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'BERT_RKOPF_STAN';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdateiname STRING;
  DECLARE v_status STRING DEFAULT 'RUNNING';
  DECLARE v_errmsg STRING DEFAULT NULL;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_wiederanlaufWert = 0;
  ELSE
    SET v_wiederanlaufWert = p_wiederanlaufWert;
  END IF;

  -- System date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default cutoff date if not provided
  IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
    SET v_effective_stichtag = v_sysdate;
  ELSE
    SET v_effective_stichtag = p_stichtag;
  END IF;

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_kennung, log_ts, error_code, error_message, stichtag, wiederanlaufwert)
    VALUES
      (v_jobkennung, CURRENT_TIMESTAMP(), '193', 'Stichtag missing', v_effective_stichtag, v_wiederanlaufWert);
    RAISE USING MESSAGE = 'Stichtag missing';
  END IF;

  -- Create job entry number and log file name equivalent
  -- (Assuming job_control table manages entry numbers)
  SET v_eintragsnr = (
    SELECT IFNULL(MAX(eintragsnr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_kennung = v_jobkennung
  );
  SET v_logdateiname = CONCAT(v_jobkennung, '_', CAST(v_eintragsnr AS STRING), '.log');

  -- Insert job start log
  INSERT INTO `project.dataset.job_control`
    (eintragsnr, job_kennung, script_name, logdateiname, stichtag, status, start_ts, sysdate)
  VALUES
    (v_eintragsnr, v_jobkennung, 'sp_erzeugung_abzug_rechnungsdaten', v_logdateiname,
     v_effective_stichtag, 'RUNNING', CURRENT_TIMESTAMP(), v_sysdate);

  BEGIN
    -- Core processing equivalent: This section should encapsulate the migrated logic from k_aurd_rechstan.ksh
    -- Placeholder for the actual data transformation and load logic.
    -- Example (as suggested by the source script's description):
    -- DELETE FROM `project.dataset.target_fos_table`
    -- WHERE dwh_vertrag_id >= v_wiederanlaufWert;

    -- INSERT INTO `project.dataset.target_fos_table`
    -- SELECT
    --   src.*
    -- FROM `project.dataset.source_contract_cache` AS src
    -- WHERE DATE(PARSE_DATE('%d%m%Y', src.gueltig_von)) <= PARSE_DATE('%d%m%Y', v_effective_stichtag)
    --   AND PARSE_DATE('%d%m%Y', v_effective_stichtag) < DATE(PARSE_DATE('%d%m%Y', src.gueltig_bis))
    --   AND DATE(PARSE_DATE('%d%m%Y', src.ladedatum)) < PARSE_DATE('%d%m%Y', v_stichtag_effective)
    --   AND src.dwh_vertrag_id > v_wiederanlaufWert;

    -- Placeholder for actual call to k_aurd_rechstan.ksh's migrated logic
    -- e.g., CALL `project.dataset.sp_k_aurd_rechstan`(v_jobkennung, v_effective_stichtag, v_eintragsnr, v_wiederanlaufWert);

    -- Assume success for wrapper logic demonstration
    SET v_status = 'OK';

    INSERT INTO `project.dataset.job_run_log`
      (eintragsnr, job_kennung, log_ts, message, status)
    VALUES
      (v_eintragsnr, v_jobkennung, CURRENT_TIMESTAMP(), 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', 'OK');

    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        end_ts = CURRENT_TIMESTAMP()
    WHERE eintragsnr = v_eintragsnr
      AND job_kennung = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';
    SET v_errmsg = @@error.message;

    INSERT INTO `project.dataset.job_error_log`
      (eintragsnr, job_kennung, log_ts, error_code, error_message, stichtag, wiederanlaufwert)
    VALUES
      (v_eintragsnr, v_jobkennung, CURRENT_TIMESTAMP(), 'APP_ERROR', v_errmsg, v_effective_stichtag, v_wiederanlaufWert);

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        end_ts = CURRENT_TIMESTAMP(),
        error_message = v_errmsg
    WHERE eintragsnr = v_eintragsnr
      AND job_kennung = v_jobkennung;

    RAISE USING MESSAGE = v_errmsg;
  END;
END;
```

## 6. External Dependencies
The original script has the following dependencies:

*   **UC4 Scheduler:** The job is triggered by a UC4 job (`DW.BERT_RECHNUNGSDATEN.xml`).
    *   **Replacement:** This will be replaced by a Cloud Composer (Airflow) DAG or Google Cloud Workflows scheduler on GCP.
*   **Shell Helper Scripts:**
    *   `. $HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.
    *   **Replacement:** These functionalities will be integrated directly into the BigQuery Stored Procedure using BigQuery SQL functions (e.g., `FORMAT_DATE`, `PARSE_DATE`) and BigQuery's error handling (`EXCEPTION WHEN ERROR`). Environment variables will be handled via BigQuery procedure parameters or configured at the orchestration layer. Logging will be replaced by inserts into dedicated BigQuery logging tables.
*   **Core Processing Script (`k_aurd_rechstan.ksh`):** This script contains the actual data logic.
    *   **Replacement:** This script's logic will need to be migrated to BigQuery SQL, potentially as another stored procedure or a series of SQL statements executed within the `sp_erzeugung_abzug_rechnungsdaten` procedure. This is the primary unresolved external dependency for this particular design document.

## 7. Unresolved / Risks
*   **Core Business Logic:** The most significant unresolved item is the actual data transformation logic contained within `k_aurd_rechstan.ksh`. This design document only covers the wrapper script. A separate design and migration effort will be required for `k_aurd_rechstan.ksh` to identify its source tables, transformations, and target tables.
*   **Schema Details:** The BigQuery SQL pseudocode uses placeholder table and dataset names (e.g., `project.dataset.job_control`, `project.dataset.source_contract_cache`). These need to be finalized with the actual target BigQuery schema.
*   **Completeness of Helper Script Migration:** While general replacements are proposed for helper scripts (e.g., date functions, error handling), a detailed analysis of each helper script's content will be needed to ensure all functionalities are accurately replicated in BigQuery.
*   **"Wiederanlaufwert" Impact:** The `Wiederanlaufwert` logic (deleting records with `DWH_VERTRAG_ID >= Wiederanlaufwert`) implies a specific data model and idempotent processing. This must be carefully replicated to avoid data loss or incorrect processing.
*   **BigQuery vs. Python for Core Logic:** Depending on the complexity of `k_aurd_rechstan.ksh`, its migration might require Python (e.g., PySpark on Dataproc/Serverless Spark) if the logic is too complex for BigQuery SQL or involves file system interactions not directly supported by BigQuery.

## 8. Build Plan
1.  **Define BigQuery Logging and Control Tables:**
    *   `job_control` (e.g., `eintragsnr`, `job_kennung`, `script_name`, `logdateiname`, `stichtag`, `status`, `start_ts`, `end_ts`, `error_message`, `sysdate`)
    *   `job_run_log` (e.g., `eintragsnr`, `job_kennung`, `log_ts`, `message`, `status`)
    *   `job_error_log` (e.g., `eintragsnr`, `job_kennung`, `log_ts`, `error_code`, `error_message`, `stichtag`, `wiederanlaufwert`)
    *   **Language:** BigQuery DDL
2.  **Develop BigQuery Stored Procedure for Wrapper Logic:**
    *   Create `sp_erzeugung_abzug_rechnungsdaten` based on the provided BigQuery SQL pseudocode.
    *   Implement parameter handling, validation, defaulting.
    *   Integrate logging to the new BigQuery control and log tables.
    *   Implement error handling using `BEGIN...EXCEPTION WHEN ERROR THEN...END`.
    *   **Language:** BigQuery SQL (Stored Procedure)
3.  **Migrate `k_aurd_rechstan.ksh` Core Logic (Separate Effort):**
    *   Analyze `k_aurd_rechstan.ksh` to identify source tables, transformations, and target tables.
    *   Develop BigQuery SQL (scripts, views, or stored procedures) to replicate this logic.
    *   Integrate the call to this migrated core logic within `sp_erzeugung_abzug_rechnungsdaten`.
    *   **Language:** BigQuery SQL / Python (if complex)
4.  **Develop Cloud Composer DAG / Cloud Workflow:**
    *   Create an Airflow DAG or Workflow definition to trigger `sp_erzeugung_abzug_rechnungsdaten`.
    *   Configure parameters to be passed to the stored procedure.
    *   **Language:** Python (for Airflow DAG) or YAML (for Cloud Workflows)
5.  **IAM and Networking Setup:**
    *   Configure appropriate IAM roles and permissions for the service accounts executing the DAG/Workflow and the BigQuery stored procedure.
    *   Ensure network connectivity if external systems are involved in the core logic.
    *   **Language:** gcloud CLI / Terraform / Pulumi