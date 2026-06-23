# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

## 1. Purpose & Scope

This job, originating from the KornShell script `r_ausd_bp_ta_cntrct_evn.ksh`, is a wrapper script responsible for orchestrating the initial provisioning of selected base products (Basisprodukte) for the BERT system. Its primary purpose is to prepare runtime context, validate input parameters, initialize logging and error handling, and determine the effective cutoff date. It then delegates the core business logic, which involves extracting contract event data from the Data Warehouse (DWH) and making it available for demand scoring (Forderungsscoring), to a kernel script (`k_ausd_bp_ta_cntrct_evn.ksh`). The job supports restart/resume capabilities via a "Wiederanlaufwert" parameter.

The scope of this migration design document focuses on converting this KornShell wrapper script to an equivalent BigQuery Stored Procedure. The detailed logic of the invoked kernel script (`k_ausd_bp_ta_cntrct_evn.ksh`) will require a separate, in-depth analysis and migration design.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh`
*   **Technology:** KornShell Script
*   **Complexity Tier:** Medium
*   **Automation Bucket:** Semi-Automatic
*   **Summary:** This script acts as an orchestration layer, handling parameter parsing, date determination, and calling a core processing script. It does not contain direct data transformation logic.
*   **Invokes:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_evn.ksh` (core logic)

## 3. Target Architecture

The target platform for this migration is Google BigQuery.

*   **Wrapper Script (`r_ausd_bp_ta_cntrct_evn.ksh`):** Will be migrated to a BigQuery Stored Procedure (e.g., `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`). This procedure will handle parameter validation, date calculations, and orchestrate the execution of the core logic.
*   **Core Logic Script (`k_ausd_bp_ta_cntrct_evn.ksh`):** Will be migrated to one or more BigQuery Stored Procedures or other BigQuery components (e.g., scheduled queries, views) depending on its internal logic. This will encapsulate the actual data extraction and transformation.
*   **Logging and Auditing:** Legacy file-based logging and status tracking (`DWMSG_*` functions) will be replaced by dedicated BigQuery audit/log tables (e.g., `project.dataset.job_log`, `project.dataset.job_control`).
*   **Orchestration:** The execution of the BigQuery Stored Procedure will be managed by a modern cloud orchestration service such as Google Cloud Composer (Apache Airflow), Cloud Workflows, or Cloud Run.

## 4. Data Flow & Lineage

The original KornShell script `r_ausd_bp_ta_cntrct_evn.ksh` functions as follows:

1.  **Initialization:** Sources environment files (`$HOME/.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Parsing:** Accepts optional command-line parameters:
    *   `-s <DDMMYYYY>`: Specifies the cutoff date (`p_stichtag`).
    *   `-l <restart_value>`: Specifies a restart threshold (`p_wiederanlaufWert`) for `DWH_VERTRAG_ID`.
3.  **Date Determination:** If `p_stichtag` is not provided, it defaults to the current system date (`v_sysdate`).
4.  **Parameter Validation:** Checks if the required parameters are set. If not, it logs an error and exits.
5.  **Logging Setup:** Initializes a custom logging framework (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, etc.), sets up `trap` commands for error handling, and logs job metadata.
6.  **Core Logic Invocation:** Executes the kernel script `k_ausd_bp_ta_cntrct_evn.ksh`, passing the determined parameters (`-j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`).
7.  **Status Update:** On successful completion, it logs a success message and updates the job status. In case of errors, the `trap` mechanism triggers error logging and an exit.

In the BigQuery target architecture, this flow will be replicated within the `ausd_bp_ta_cntrct_evn_wrapper` stored procedure, which will then `CALL` the `k_ausd_bp_ta_cntrct_evn` stored procedure.

## 5. Transformation Logic

The KornShell wrapper script's logic will be translated to BigQuery SQL scripting:

*   **Parameter Handling:**
    *   Shell `getopts` arguments (`-s`, `-l`) will become `IN` parameters for the `ausd_bp_ta_cntrct_evn_wrapper` BigQuery Stored Procedure (e.g., `p_stichtag STRING`, `p_wiederanlaufWert INT64`).
    *   Defaulting logic (`if [[ -z "$p_wiederanlaufWert" ]]`) will be implemented using `IFNULL`, `COALESCE`, and `IF ... THEN ... END IF` constructs.
*   **Date Determination:**
    *   Shell commands like `DWDate_Gib_Zeitraum` and references to `v_sysdate` will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Error Handling and Logging:**
    *   The KornShell's `set -e`, `trap` statements, and custom `DWMSG_*` functions will be re-engineered using BigQuery scripting's `BEGIN ... EXCEPTION ... END` blocks, `ASSERT` statements for validation, and `RAISE` for custom error signaling.
    *   All logging output (`print`, `tee`) will be converted to `INSERT` statements into the BigQuery `job_log` and `job_control` tables.
*   **Utility Script Replacement:** The functionalities of sourced utility scripts (`h_alis_parameter.ksh`, `h_alis_date.ksh`) will be either directly translated into BigQuery SQL logic within the procedure or, if complex, implemented as BigQuery User-Defined Functions (UDFs).
*   **Invoking Core Logic:** The shell command `${Name_Kernskript} ...` will be replaced by a `CALL` statement to the BigQuery Stored Procedure corresponding to `k_ausd_bp_ta_cntrct_evn.ksh`.
*   **No Direct Data Transformation:** It's important to reiterate that this wrapper script itself performs no direct data transformations or aggregations. Its role is solely to prepare the execution context for the downstream core script.

**BQ SQL Pseudocode for Wrapper:**

```sql
-- BigQuery Stored Procedure: wrapper equivalent of r_ausd_bp_ta_cntrct_evn.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_restart_value INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Simulate system date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default restart value if not provided
  SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);

  -- Default cutoff date to system date if not provided
  SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR v_effective_stichtag = '' THEN
    SET v_errnr = 193;
    SET v_errarg = 'Stichtag';
  END IF;

  IF v_errnr != 0 THEN
    INSERT INTO `project.dataset.job_log`
    (job_name, log_level, error_nr, error_arg, message, created_at)
    VALUES
    ('ausd_bp_ta_cntrct_evn', 'E', v_errnr, v_errarg, 'Required parameter missing', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('Error ', CAST(v_errnr AS STRING), ': ', v_errarg, ' - Required parameter missing.');
  END IF;

  -- Simulate job number and log file creation
  -- (Assuming job_control table exists and job_nr is auto-incremented or managed)
  SET v_eintragsnr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_name = v_jobkennung
  );

  SET v_logdatei = CONCAT('job_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING), '.log');

  INSERT INTO `project.dataset.job_control`
  (job_nr, job_name, script_name, log_file, stichtag_info, status, created_at)
  VALUES
  (
    v_eintragsnr,
    v_jobkennung,
    'ausd_bp_ta_cntrct_evn_wrapper',
    v_logdatei,
    v_sysdate,
    'RUNNING',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Job header log
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'I',
     CONCAT('Job started. Stichtag=', v_effective_stichtag,
            ', RestartValue=', CAST(v_restart_value AS STRING)),
     CURRENT_TIMESTAMP());

    -- Downstream kernel logic invocation (placeholder for k_ausd_bp_ta_cntrct_evn.ksh equivalent)
    CALL `project.dataset.k_ausd_bp_ta_cntrct_evn_core`(
      v_jobkennung,
      v_effective_stichtag,
      v_eintragsnr,
      v_restart_value
    );

    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'I',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
     CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_eintragsnr
      AND job_name = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_log`
    (job_nr, job_name, log_level, message, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'E',
     CONCAT('AppError: Abbruch - ', @@error.message),
     CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_eintragsnr
      AND job_name = v_jobkennung;

    RAISE; -- Re-raise the error to propagate it
  END;
END;
```

## 6. External Dependencies

The initial job analysis (`lineage_assembled_jobs`) indicated no direct external systems at the job level. However, the script itself relies on several implicit "externalities" within its legacy environment:

*   **Environment Initialization (`. $HOME/.dw_init`):** This is a critical dependency for setting up the legacy execution environment. In BigQuery, this will be handled by explicit parameter passing, BigQuery connection configurations, or potentially a dedicated configuration table if global settings are required.
*   **Utility Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These are essentially shared libraries in the shell environment. Their functionalities will need to be re-implemented directly in BigQuery SQL scripting or as BigQuery UDFs if they perform common, reusable logic.
*   **Custom Logging Framework (`DWMSG_*` functions):** This framework is embedded in the shell environment. It will be replaced by inserts into BigQuery `job_log` and `job_control` tables, providing a structured, queryable log.
*   **Data Source (DWH) and Target (FOS-Tabelle):** The script's purpose is to prepare data from a "DWH" for "Forderungsscoring" (FOS-Tabelle). The actual connection and interaction with these data sources/targets are expected to reside within the `k_ausd_bp_ta_cntrct_evn.ksh` script. For the migrated BigQuery solution, these will typically translate to:
    *   **DWH:** Migrated to BigQuery tables, external tables, or federated queries accessing other Google Cloud data sources.
    *   **FOS-Tabelle:** A target BigQuery table where the processed contract event data will be loaded.

## 7. Unresolved / Risks

*   **Core Logic Migration:** The most significant unresolved item is the detailed transformation logic contained within `k_ausd_bp_ta_cntrct_evn.ksh`. This script must be analyzed independently to understand its data sources, transformations, and target systems to design its BigQuery equivalent. This is a critical path item.
*   **Detailed Logic of Sourced Utilities:** While a general approach for utility scripts is outlined, the exact functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be fully understood to ensure accurate and complete migration to BigQuery SQL or UDFs.
*   **Legacy Logging Framework Fidelity:** The custom `DWMSG_*` logging framework might have specific features (e.g., alerting, integration with other legacy monitoring tools) that need careful consideration during migration to ensure no critical functionality is lost or to identify suitable BigQuery/GCP native replacements (e.g., Cloud Logging, Cloud Monitoring).
*   **Performance Considerations:** While BigQuery is highly performant, the shell script's environment setup and parameter passing might have had specific performance characteristics. The BigQuery equivalent, especially the core logic, needs to be designed with performance in mind.
*   **Orchestration Design:** The specific choice and design of the orchestration mechanism (Cloud Composer, Workflows, Cloud Run) will influence monitoring, scheduling, and error recovery. This needs a dedicated design phase.

## 8. Build Plan

1.  **BigQuery Environment Setup (Pre-requisite):**
    *   Create a dedicated BigQuery project and dataset (e.g., `project.dataset`) for the migrated assets.
    *   Define necessary IAM roles and service accounts for BigQuery operations and orchestration.
2.  **Schema Definition:**
    *   Create `project.dataset.job_log` table:
        ```sql
        CREATE TABLE `project.dataset.job_log` (
          job_nr INT64,
          job_name STRING,
          log_level STRING,
          error_nr INT64,
          error_arg STRING,
          message STRING,
          created_at TIMESTAMP
        );
        ```
    *   Create `project.dataset.job_control` table:
        ```sql
        CREATE TABLE `project.dataset.job_control` (
          job_nr INT64,
          job_name STRING,
          script_name STRING,
          log_file STRING,
          stichtag_info STRING,
          status STRING,
          created_at TIMESTAMP,
          finished_at TIMESTAMP
        );
        ```
    *   Define target tables (e.g., `project.dataset.fos_contract_events`) for the data processed by the core logic.
3.  **Core Logic Migration (`k_ausd_bp_ta_cntrct_evn.ksh`):**
    *   **Analyze:** Perform a detailed analysis of `k_ausd_bp_ta_cntrct_evn.ksh` to identify data sources, transformations, and target outputs.
    *   **Design:** Design the BigQuery Stored Procedure(s) or other components for the core data processing.
    *   **Develop:** Implement `project.dataset.k_ausd_bp_ta_cntrct_evn_core` (or similar) in BigQuery SQL, converting legacy SQL, shell commands for data manipulation, and file I/O to BigQuery equivalents.
4.  **Wrapper BigQuery Stored Procedure (`r_ausd_bp_ta_cntrct_evn.ksh`):**
    *   **Develop:** Implement `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` in BigQuery SQL using the pseudocode provided in Section 5.
    *   **Language:** BigQuery Standard SQL (for stored procedures).
5.  **Orchestration Implementation:**
    *   Choose a Google Cloud orchestration service (e.g., Cloud Composer).
    *   **Develop:** Create a DAG (for Cloud Composer) or Workflow definition that:
        *   Triggers `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`.
        *   Passes necessary parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Monitors execution and handles retries or failures.
    *   **Language:** Python (for Cloud Composer DAGs) or YAML/JSON (for Cloud Workflows).
6.  **Testing:**
    *   **Unit Tests:** Develop and execute unit tests for each BigQuery Stored Procedure to verify individual logic components.
    *   **Integration Tests:** Test the `ausd_bp_ta_cntrct_evn_wrapper` procedure calling `k_ausd_bp_ta_cntrct_evn_core`.
    *   **End-to-End Tests:** Test the entire workflow through the orchestration service, validating data output, logging, and error handling.
7.  **Deployment:**
    *   Deploy BigQuery Stored Procedures.
    *   Deploy the orchestration configuration (e.g., Cloud Composer DAG).