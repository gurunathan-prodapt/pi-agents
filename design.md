# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This job, `r_ausd_bp_ta_cntrct_dist.ksh`, is an orchestration KornShell script designed to prepare selected basic products for the BERT system. Its primary function is to act as a wrapper for a core business logic script, handling parameter parsing, date determination, error handling, and logging before invoking the main processing. The script generates a snapshot of contract cache data from the Data Warehouse (DWH) and makes it available for the Forderungsscoring (FOS) system. It supports a restart mechanism to process only new or updated contracts based on a `DWH_VERTRAG_ID` threshold.

## 2. Source Inventory
The job is comprised of a single KornShell script.
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`
  - **Technology:** KornShell Script
  - **Tool:** KornShell
  - **Category:** shell
  - **Complexity Tier:** medium
  - **Migration Bucket:** semi_auto
  - **Purpose:** Orchestration, parameter handling, date calculation, error logging, and invocation of a core business logic script.

## 3. Target Architecture
The migration target is Google BigQuery. The KornShell script's functionality will be primarily re-engineered into a BigQuery Stored Procedure, leveraging BigQuery's capabilities for data manipulation, parameter handling, and error management.

The key components in the target architecture will include:
- **Main BigQuery Stored Procedure:** `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` to replace the shell script's orchestration logic.
- **Core BigQuery Stored Procedure:** `project.dataset.ausd_bp_ta_cntrct_dist_core` to encapsulate the core business logic previously handled by `k_ausd_bp_ta_cntrct_dist.ksh` (assumed to contain data selection, deletion, and insertion logic).
- **Logging and Status Tables:** `project.dataset.job_log` for detailed logging and `project.dataset.job_status` for tracking job execution status. These tables will replace the file-based logging mechanisms of the original script.
- **Source Tables:** Existing DWH tables (e.g., `project.dataset.dwh_vertrag_cache`) will serve as input.
- **Target Tables:** A target FOS table (e.g., `project.dataset.fos_tabelle`) will receive the processed data.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_cntrct_dist.ksh` is invoked, possibly by an external scheduler (e.g., UC4).
1.  **Initialization:** The script sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to set up its environment, error handling, parameter parsing, and date functions.
2.  **Parameter Processing:** It parses command-line arguments for a reference date (`-s Stichtag`) and a restart value (`-l Wiederanlaufwert`). Default values are applied if parameters are missing (restart value defaults to 0, reference date defaults to system date).
3.  **Error Handling Setup:** Initializes logging variables and sets up `trap` commands for robust error handling.
4.  **Core Script Invocation:** The wrapper script calls the core business logic script, `k_ausd_bp_ta_cntrct_dist.ksh`, passing the determined parameters. The output of the core script is redirected to the log file.
5.  **Status Reporting:** After the core script completes, the wrapper script logs the job's success or failure and updates a status mechanism.

In BigQuery:
- An external orchestrator (e.g., Cloud Composer, Workflows) will call the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure.
- The wrapper stored procedure will handle parameter validation, date calculation, and then invoke the `ausd_bp_ta_cntrct_dist_core` stored procedure.
- Both procedures will interact with `job_log` and `job_status` tables for logging and status updates.
- The `ausd_bp_ta_cntrct_dist_core` procedure will perform data deletion and insertion based on source tables (e.g., `dwh_vertrag_cache`) and load into target tables (e.g., `fos_tabelle`).

## 5. Transformation Logic
The transformation logic will migrate the shell script's orchestration and parameter handling into BigQuery SQL, specifically using Stored Procedures.

**Parameter Handling:**
- Shell script's `getopts` and parameter variables (`p_stichtag`, `p_wiederanlaufWert`) will be replaced by `IN` parameters in the BigQuery Stored Procedure `ausd_bp_ta_cntrct_dist_wrapper`.
- Defaulting `p_wiederanlaufWert` to `0` and `p_stichtag` to the system date (if not provided) will be handled using `IFNULL` and `CURRENT_DATE()` in BigQuery SQL.

**Date Derivation:**
- The `DWDate_Gib_Zeitraum` function will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', ...)` functions to derive the system date.

**Error Handling and Logging:**
- Shell script's `set -e`, `trap` commands, and `DWMSG_*` functions will be replaced by:
    - BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks for error trapping.
    - `INSERT` statements into a `job_log` table for detailed logging (replacing `print` and `tee -a`).
    - `SIGNAL SQLSTATE '45000'` for custom error signaling.
    - `INSERT` statements into a `job_status` table to track the overall job status.

**Core Business Logic Invocation:**
- The invocation of `Name_Kernskript` (`k_ausd_bp_ta_cntrct_dist.ksh`) will be replaced by a `CALL` statement to the `project.dataset.ausd_bp_ta_cntrct_dist_core` BigQuery Stored Procedure, passing the necessary parameters.

**Restart Logic:**
- The restart value (`p_wiederanlaufWert`) will be used in the `ausd_bp_ta_cntrct_dist_core` procedure to filter records.
- The deletion logic (e.g., `DELETE FROM target_table WHERE DWH_VERTRAG_ID >= @wiederanlaufwert`) will be explicitly implemented in BigQuery DML.
- The selection logic will include `WHERE src.dwh_vertrag_id > v_restart_value`.

**Data Filtering (from script comments):**
- Records will be selected where `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`. This will translate directly into BigQuery SQL `WHERE` clauses using `DATE()` conversions and comparison operators.

## 6. External Dependencies
The original script has the following external dependencies:
- **Sourced Utility Scripts:**
    - `$HOME/.dw_init`: Environment initialization.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling helpers.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helpers.
- **Core Business Logic Script:**
    - `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`: Contains the main data processing logic.
- **External Scheduler:**
    - Potentially UC4, which invokes `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_CNTRCT_DIST.xml` which then invokes the current script.

**Replacement in BigQuery:**
- **Sourced Utility Scripts:** The functionalities of these scripts will be re-implemented as:
    - BigQuery Stored Procedure parameters or constants.
    - Inline BigQuery SQL functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
    - Logic embedded directly within the wrapper stored procedure.
- **Core Business Logic Script:** This script's functionality will be migrated into a dedicated BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_cntrct_dist_core`).
- **External Scheduler:** The UC4 invocation will be replaced by a modern orchestrator like Cloud Composer (Apache Airflow) which will schedule and call the BigQuery Stored Procedure.

## 7. Unresolved / Risks
- **Core Script (`k_ausd_bp_ta_cntrct_dist.ksh`) Logic:** The precise data extraction, transformation, and load (ETL) logic within the invoked core script is not detailed in the current analysis. It is assumed to be migratable to BigQuery SQL, but a separate, in-depth analysis of `k_ausd_bp_ta_cntrct_dist.ksh` is required to ensure a complete and accurate migration.
- **Helper Script Specifics:** While the general functions of the sourced helper scripts are understood, any highly complex or unusual logic within them would need careful re-implementation in BigQuery SQL or external Python UDFs.
- **Orchestration Context:** The interaction with the UC4 scheduler suggests that the entire job stream needs to be considered for migration to a modern orchestrator like Cloud Composer, which might introduce additional complexities depending on the overall UC4 setup.
- **Data Volume and Performance:** Although BigQuery is highly performant, the actual performance characteristics of the migrated core logic will need to be monitored and optimized post-migration.

## 8. Build Plan
The build plan focuses on creating the necessary BigQuery components.

1.  **Create `project.dataset.job_log` Table (BigQuery DDL):**
    - This table will store detailed log messages, replacing the shell script's log file.
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
      entry_no INT64,
      job_name STRING,
      log_level STRING,
      message STRING,
      stichtag STRING,
      sysdate STRING,
      created_at TIMESTAMP
    );
    ```

2.  **Create `project.dataset.job_status` Table (BigQuery DDL):**
    - This table will track the overall status of job runs.
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
      entry_no INT64,
      job_name STRING,
      status STRING,
      updated_at TIMESTAMP
    );
    ```

3.  **Create `project.dataset.ausd_bp_ta_cntrct_dist_core` Stored Procedure (BigQuery SQL):**
    - This procedure will contain the migrated core business logic, including the `DELETE` and `INSERT` statements to process the contract data.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_core`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufwert INT64
    )
    BEGIN
      DECLARE v_stichtag_date DATE;
      DECLARE v_restart_value INT64;

      SET v_restart_value = IFNULL(p_wiederanlaufwert, 0);
      SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

      -- Assumed target table for FOS data
      DELETE FROM `project.dataset.fos_tabelle`
      WHERE dwh_vertrag_id >= v_restart_value;

      -- Assumed source table for contract cache data
      INSERT INTO `project.dataset.fos_tabelle`
      SELECT
        src.* -- Replace with specific column list for production
      FROM `project.dataset.dwh_vertrag_cache` src
      WHERE DATE(src.gueltig_von) <= v_stichtag_date
        AND v_stichtag_date < DATE(src.gueltig_bis)
        AND DATE(src.ladedatum) < v_stichtag_date
        AND src.dwh_vertrag_id > v_restart_value;
    END;
    ```

4.  **Create `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` Stored Procedure (BigQuery SQL):**
    - This procedure will replace the orchestration logic of the original KornShell script.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(
      IN p_stichtag STRING,
      IN p_wiederanlaufwert INT64
    )
    BEGIN
      DECLARE v_progname STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
      DECLARE v_progversion STRING DEFAULT 'V2.0.0';
      DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_dist';
      DECLARE v_sysdate STRING;
      DECLARE v_stichtag STRING;
      DECLARE v_wiederanlaufwert INT64;
      DECLARE v_eintragsnr INT64;

      SET v_wiederanlaufwert = IFNULL(p_wiederanlaufwert, 0);
      SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
      SET v_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

      IF v_stichtag IS NULL OR v_stichtag = '' THEN
        INSERT INTO `project.dataset.job_log`
        (job_name, log_level, message, created_at)
        VALUES
        (v_jobkennung, 'ERROR', 'Stichtag parameter missing', CURRENT_TIMESTAMP());

        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Stichtag parameter missing';
      END IF;

      SET v_eintragsnr = (\n    SELECT IFNULL(MAX(entry_no), 0) + 1\n    FROM `project.dataset.job_log`\n    WHERE job_name = v_jobkennung\n  );\n
      INSERT INTO `project.dataset.job_log`
      (entry_no, job_name, log_level, message, stichtag, sysdate, created_at)
      VALUES
      (v_eintragsnr, v_jobkennung, 'INFO',
       CONCAT('Job started: ', v_progname, ' ', v_progversion),
       v_stichtag, v_sysdate, CURRENT_TIMESTAMP());

      BEGIN
        CALL `project.dataset.ausd_bp_ta_cntrct_dist_core`(
          v_jobkennung,
          v_stichtag,
          v_eintragsnr,
          v_wiederanlaufwert
        );

        INSERT INTO `project.dataset.job_log`
        (entry_no, job_name, log_level, message, stichtag, created_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'INFO',
         'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
         v_stichtag,
         CURRENT_TIMESTAMP());

        INSERT INTO `project.dataset.job_status`
        (entry_no, job_name, status, updated_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'OK', CURRENT_TIMESTAMP());

      EXCEPTION WHEN ERROR THEN
        INSERT INTO `project.dataset.job_log`
        (entry_no, job_name, log_level, message, stichtag, created_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'ERROR',
         CONCAT('AppError: Abbruch - ', @@error.message),
         v_stichtag,
         CURRENT_TIMESTAMP());

        INSERT INTO `project.dataset.job_status`
        (entry_no, job_name, status, updated_at)
        VALUES
        (v_eintragsnr, v_jobkennung, 'FAILED', CURRENT_TIMESTAMP());

        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Job failed';
      END;
    END;
    ```

5.  **Orchestration (Cloud Composer/Workflows):**
    - A new Airflow DAG or Workflow definition will be created to schedule and invoke the `ausd_bp_ta_cntrct_dist_wrapper` BigQuery Stored Procedure, replacing the original UC4 scheduling. This will involve defining BigQuery operators within the orchestrator.