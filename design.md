# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_rn_einzeln.ksh`, is an orchestration job designed for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. Its primary business purpose is to create a snapshot extraction of contract cache data from the Data Warehouse (DWH) and make it available for a downstream Forderungsscoring (FOS) system. The script handles parameter parsing, environment setup, robust error trapping, logging, and then delegates the core business logic to a kernel script, `k_ausd_bp_ta_rn_einzeln.ksh`. It supports a restart/resume mechanism via a "Wiederanlaufwert".

The scope of this migration is to convert this KornShell orchestration script and its immediate invocation of the kernel script to a BigQuery-native solution, primarily using BigQuery Stored Procedures, while establishing a framework for logging and error handling within BigQuery.

## 2. Source Inventory
The job consists of a single primary source file and several referenced scripts/components:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh`
    *   **Technology:** KornShell
    *   **Summary:** Orchestrates data provisioning for BERT, handles parameters, environment, error trapping, and invokes a core processing script.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** ETL Orchestrator / Wrapper Script
    *   **Key References:**
        *   Sourced Scripts: `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
        *   Invoked Script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh` (Critical)
        *   Referenced Table: `DWH$TA_C_VERTRAG` (Critical - likely source for kernel script)

## 3. Target Architecture
The migration will transform the KornShell script into a BigQuery-native orchestration layer, primarily using BigQuery Stored Procedures.

*   **Main Orchestration:** A BigQuery Stored Procedure, named similar to `project.dataset.ausd_bp_ta_rn_einzeln`, will replace the `r_ausd_bp_ta_rn_einzeln.ksh` wrapper script.
*   **Core Logic:** The logic currently handled by `k_ausd_bp_ta_rn_einzeln.ksh` will also be migrated into a separate BigQuery Stored Procedure, e.g., `project.dataset.k_ausd_bp_ta_rn_einzeln`, or potentially a series of SQL DML statements, depending on its complexity (which is not fully detailed in this analysis).
*   **Logging and Error Handling:** Dedicated BigQuery tables will be created to manage job control, logging, and error reporting.
    *   `project.dataset.job_control`: To track job status, parameters, and timestamps.
    *   `project.dataset.job_log`: For general informational messages.
    *   `project.dataset.job_error_log`: For detailed error information.
*   **Data Sources:** The `DWH$TA_C_VERTRAG` table, and any other data sources accessed by the kernel script, will be migrated to BigQuery tables (e.g., `project.dataset.contract_cache_source`).
*   **Data Targets:** The target FOS table (`project.dataset.fos_target_table`) will reside in BigQuery.
*   **Orchestration (External):** While the script logic is moving to BQ Stored Procedures, an external orchestrator like Cloud Composer (Airflow) or Cloud Workflows might be necessary to schedule the BigQuery stored procedure calls and manage dependencies with other migrated components.

## 4. Data Flow & Lineage
The original data flow involves:
1.  **Parameter Input:** Command-line parameters (`-s` for Stichtag, `-l` for Wiederanlaufwert) are passed to `r_ausd_bp_ta_rn_einzeln.ksh`.
2.  **Environment Setup:** `r_ausd_bp_ta_rn_einzeln.ksh` sources several utility scripts for environment initialization, error handling, parameter parsing, and date functions.
3.  **Date & Parameter Resolution:** The script determines an effective "Stichtag" (cutoff date) and a "Wiederanlaufwert" (restart value), defaulting them if not provided.
4.  **Invocation of Kernel Script:** `r_ausd_bp_ta_rn_einzeln.ksh` then invokes `k_ausd_bp_ta_rn_einzeln.ksh` with the resolved parameters and internal job tracking information.
5.  **Core Processing (by `k_ausd_bp_ta_rn_einzeln.ksh`):** This kernel script is expected to:
    *   Read from `DWH$TA_C_VERTRAG` (or its BigQuery equivalent `project.dataset.contract_cache_source`).
    *   Apply filtering based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `dwh_vertrag_id` (if `p_wiederanlaufWert` is set).
    *   Potentially perform deletion/re-insertion into the FOS target table (`project.dataset.fos_target_table`).
    *   Write results to `project.dataset.fos_target_table`.
6.  **Logging & Status:** Both scripts contribute to log files and update a job status.

**Target Data Flow:**
1.  **External Orchestration:** An Airflow DAG (Cloud Composer) or Cloud Workflow will schedule the main BigQuery Stored Procedure.
2.  **`project.dataset.ausd_bp_ta_rn_einzeln` Procedure:**
    *   Receives `p_stichtag` and `p_wiederanlaufWert` as input parameters.
    *   Initializes job variables and determines `v_sysdate` and `v_effective_stichtag`.
    *   Performs parameter validation.
    *   Records job initiation in `project.dataset.job_control`.
    *   Calls the core processing procedure: `CALL project.dataset.k_ausd_bp_ta_rn_einzeln(...)`.
    *   Records success or failure, updating `project.dataset.job_control` and writing to `project.dataset.job_log` or `project.dataset.job_error_log`.
3.  **`project.dataset.k_ausd_bp_ta_rn_einzeln` Procedure:**
    *   Receives job context and parameters.
    *   Reads from `project.dataset.contract_cache_source`.
    *   Filters data based on `stichtag`, `restart_value`, and date validity.
    *   Inserts/merges processed data into `project.dataset.fos_target_table`.
    *   Inserts audit records into `project.dataset.processing_audit`.

## 5. Transformation Logic

**Original (KornShell `r_ausd_bp_ta_rn_einzeln.ksh`):**
*   **Parameter Parsing:** `getopts` for `-s DDMMYYYY` and `-l value`.
*   **Defaulting:** If `-l` not set, `p_wiederanlaufWert` defaults to `0`. If `-s` not set, `p_stichtag` defaults to `MIN(sysdate, max_load_date)` (though the script comments out `FOSHoleLadedatum` and uses `v_sysdate`).
*   **Date Handling:** Uses `DWDate_Gib_Zeitraum` to get `v_sysdate` in `DDMMYYYY`.
*   **Error Handling:** Extensive use of `set -e` and `trap` for `INT`, `STOP`, `CONT`, `ERR`, calling `DWMSG_Fehlerbehandlung` and `DWMSG_MeldeFehler`.
*   **Logging:** Calls `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` for job control and logging.
*   **Invocation:** Executes `${Name_Kernskript}` (`k_ausd_bp_ta_rn_einzeln.ksh`) with `-j`, `-s`, `-f`, `-l` parameters.

**Target (BigQuery Stored Procedure `project.dataset.ausd_bp_ta_rn_einzeln`):**
*   **Parameter Handling:**
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_einzeln`(
      IN p_stichtag STRING,
      IN p_wiederanlaufWert INT64
    )
    BEGIN
      DECLARE v_restart_value INT64 DEFAULT IFNULL(p_wiederanlaufWert, 0);
      DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
      DECLARE v_effective_stichtag STRING DEFAULT IFNULL(NULLIF(p_stichtag, ''), v_sysdate);
      -- ...
    END;
    ```
*   **Error Handling:** Replaced `trap` with BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks and `SIGNAL SQLSTATE '45000'`. Error details will be inserted into `project.dataset.job_error_log`.
*   **Logging:** Replaced `DWMSG_*` functions with `INSERT` and `UPDATE` statements against `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log` tables.
*   **Date Handling:** `FORMAT_DATE` and `CURRENT_DATE()` for system date. `PARSE_DATE` will be used to convert `DDMMYYYY` strings to `DATE` types for comparisons.
*   **Core Logic Invocation:** Replaced shell execution with `CALL project.dataset.k_ausd_bp_ta_rn_einzeln(...)`.

**Target (BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_rn_einzeln` - Pseudocode):**
*   **Parameter Input:** Receives `p_jobkennung`, `p_stichtag`, `p_eintragsnr`, `p_wiederanlaufWert`.
*   **Date Conversion:** `DECLARE v_stichtag_date DATE; SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);`
*   **Data Filtering (example):**
    ```sql
    CREATE TEMP TABLE tmp_selected_contracts AS
    SELECT *
    FROM `project.dataset.contract_cache_source`
    WHERE DATE(gueltig_von) <= v_stichtag_date
      AND v_stichtag_date < DATE(gueltig_bis)
      AND DATE(ladedatum) < v_stichtag_date
      AND dwh_vertrag_id > p_wiederanlaufWert; -- if p_wiederanlaufWert is not 0
    ```
*   **Target Update:** `DELETE FROM `project.dataset.fos_target_table` WHERE dwh_vertrag_id >= p_wiederanlaufWert;` (optional cleanup based on logic) followed by `INSERT INTO `project.dataset.fos_target_table` SELECT * FROM tmp_selected_contracts;`.
*   **Audit:** `INSERT INTO `project.dataset.processing_audit` ...`.

## 6. External Dependencies
The script has the following external dependencies:

*   **`$HOME/.dw_init`:** An environment initialization script.
    *   **Replacement:** Configuration parameters will be passed directly as stored procedure arguments or managed via external orchestration (e.g., Airflow variables or GCP Secret Manager), or if simple, hardcoded in the BQSP.
*   **`BERT_DIR_ROOT` environment variable:** Used for locating utility and kernel scripts.
    *   **Replacement:** Directory structure becomes irrelevant in BigQuery. The BigQuery Stored Procedures will be directly referenced by their full paths (`project.dataset.procedure_name`).
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** Shell functions for error handling, parameter validation, and date manipulation.
    *   **Replacement:** These functionalities are integrated into the BigQuery Stored Procedure logic using native SQL constructs (e.g., `IF`, `CASE`, `FORMAT_DATE`, `PARSE_DATE`, `CURRENT_DATE()`, and dedicated logging/error tables).
*   **`DWH$TA_C_VERTRAG` (Table):** Critical data source.
    *   **Replacement:** Migrated to a BigQuery table, e.g., `project.dataset.contract_cache_source`.
*   **Kernel Script `k_ausd_bp_ta_rn_einzeln.ksh`:** The main business logic executor.
    *   **Replacement:** Migrated to a BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_rn_einzeln`, called from the main orchestration procedure.

## 7. Unresolved / Risks
*   **Full Scope of `k_ausd_bp_ta_rn_einzeln.ksh`:** The detailed logic of `k_ausd_bp_ta_rn_einzeln.ksh` is currently a placeholder. A separate analysis and design will be required for this kernel script to ensure its complete and accurate migration to BigQuery. This is a critical dependency.
*   **`FOSHoleLadedatum` function:** The commented-out line `FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` suggests there might have been logic to dynamically determine the `Stichtag` based on the maximum load date of `DWH$TA_C_VERTRAG`. The current script defaults to `v_sysdate`. If the original intent was to use `max_load_date`, this will need to be implemented in the BigQuery procedure by querying the source table metadata or data.
*   **Complex Shell Logic:** While the wrapper seems straightforward, if `k_ausd_bp_ta_rn_einzeln.ksh` contains complex file I/O, external system calls, or non-SQL specific logic, these aspects might require alternative solutions like Cloud Functions, Dataflow, or custom Python applications on Cloud Run/GKE, orchestrated by Cloud Composer. The `semi_auto` migration bucket suggests there might be some manual intervention needed here.
*   **Performance:** The migration of logic from a shell script potentially interacting with a traditional database to BigQuery procedures requires careful performance tuning and optimization to leverage BigQuery's columnar storage and distributed query capabilities.

## 8. Build Plan
1.  **Define BigQuery Datasets:** Create the target BigQuery datasets (e.g., `project.dataset`) for the stored procedures and tables.
2.  **Create Logging and Control Tables:**
    *   Create `project.dataset.job_control` table (DDL).
    *   Create `project.dataset.job_log` table (DDL).
    *   Create `project.dataset.job_error_log` table (DDL).
    *   Create `project.dataset.processing_audit` table (DDL).
3.  **Migrate Data Sources:** Create `project.dataset.contract_cache_source` table (DDL) and ingest historical and/or current data into it.
4.  **Create Target FOS Table:** Create `project.dataset.fos_target_table` table (DDL).
5.  **Develop `k_ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure:**
    *   Translate the logic of the original `k_ausd_bp_ta_rn_einzeln.ksh` into BigQuery SQL, incorporating the filtering and data manipulation identified.
    *   **Language:** BigQuery SQL (DDL and DML)
6.  **Develop `r_ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure:**
    *   Translate the wrapper logic into a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_rn_einzeln`).
    *   Include parameter handling, date defaulting, error handling (with `BEGIN...EXCEPTION`), logging, and a `CALL` statement to `project.dataset.k_ausd_bp_ta_rn_einzeln`.
    *   **Language:** BigQuery SQL (DDL and DML)
7.  **Develop External Orchestration (if needed):**
    *   Create an Airflow DAG (e.g., in Cloud Composer) to schedule the `project.dataset.ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure.
    *   **Language:** Python (for Airflow DAG)
8.  **Testing:** Implement unit and integration tests for both BigQuery Stored Procedures and the overall orchestration.
9.  **Deployment:** Deploy BigQuery resources and the orchestration pipeline.