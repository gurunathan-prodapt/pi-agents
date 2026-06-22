# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope

This job, `r_ausd_bp_ta_rn_da_vda_tk.ksh`, is a KornShell script designed to orchestrate the initial provision of selected basic product data for the BERT system. Its primary purpose is to extract contract cache data from a Data Warehouse (DWH) based on a specified snapshot date (`Stichtag`) and make this processed data available for the `Forderungsscoring` system. The script acts as a wrapper, handling parameter parsing, date validation, restart logic, and error handling, before delegating the core data processing and transformation to an external "kernel script" (`k_ausd_bp_ta_rn_da_vda_tk.ksh`).

The scope of this migration design document specifically covers the migration of the `r_ausd_bp_ta_rn_da_vda_tk.ksh` orchestrator script to Google Cloud BigQuery. The design will outline how to replicate the wrapper's functionality (parameter handling, logging, flow control) within BigQuery's ecosystem, while acknowledging that the business logic within the invoked "kernel script" requires separate analysis and migration.

## 2. Source Inventory

The job consists of a single primary source file, which acts as an orchestrator. No complexity or automation rate was directly found for this file in the analysis tables, thus these are inferred.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh`
    *   **Technology:** KornShell Script (shell)
    *   **Summary:** Orchestrates the extraction and provision of contract cache data for the BERT system.
    *   **Inferred Complexity Tier:** Simple (for the orchestrator logic itself)
    *   **Inferred Automation Bucket:** B1 (Automatic for orchestrator logic) - *However, the overall job's migration effort is higher due to the external kernel script.*
    *   **Key Logic:** Parameter parsing (`-s` for `Stichtag` / snapshot date, `-l` for `Wiederanlaufwert` / restart value), environment sourcing, date calculation, logging framework integration, and invocation of a core processing script.

**Internal Dependencies (Sourced/Invoked by the script):**
The primary script sources several utility scripts and invokes a kernel script. Their content is not provided, but their role is inferred.
*   `$HOME/.dw_init` (Environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging framework)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter handling utilities)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling utilities)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh` (The "kernel script" performing core data processing and transformation).

## 3. Target Architecture

The target platform for this job is Google Cloud BigQuery.

*   **Orchestration Component:** The `r_ausd_bp_ta_rn_da_vda_tk.ksh` orchestrator will be migrated to a **BigQuery Stored Procedure**. This stored procedure will handle parameter input, validation, default value assignment, logging, and coordination of the data processing.
*   **Data Processing Component:** The business logic currently residing in `k_ausd_bp_ta_rn_da_vda_tk.ksh` (the "kernel script") will be translated into one or more **BigQuery SQL statements** or additional **BigQuery Stored Procedures**. This will involve `SELECT`, `INSERT`, `UPDATE`, `DELETE` operations on BigQuery tables.
*   **Logging and Auditing:** The existing file-based logging mechanism (`DWMSG_...` functions writing to a log file) will be replaced by dedicated **BigQuery Audit/Log Tables**. These tables will capture job execution status, parameters, errors, and other relevant metadata.
*   **Data Storage:** Source contract cache data and target `Forderungsscoring` data will reside in **BigQuery Tables**.
*   **Scheduling:** The job's execution will be managed by a Google Cloud orchestration service, likely **Cloud Composer (Airflow)**, **Cloud Workflows**, or **Cloud Scheduler**, depending on overall pipeline complexity and trigger mechanisms.

**Proposed BigQuery Tables:**
*   `project.dataset.source_contract_cache`: BigQuery table corresponding to the source DWH contract cache.
*   `project.dataset.target_fos_table`: BigQuery table to store the prepared data for `Forderungsscoring`.
*   `project.dataset.job_audit`: Table to store detailed audit logs for job executions.
    *   Columns: `job_nr` (BIGINT), `job_kennung` (STRING), `progname` (STRING), `progversion` (STRING), `stichtag` (STRING), `restart_value` (INT64), `log_file` (STRING), `status` (STRING), `created_at` (TIMESTAMP), `message` (STRING).
*   `project.dataset.job_log`: Table for general logging messages, including errors.
    *   Columns: `job_kennung` (STRING), `progname` (STRING), `progversion` (STRING), `log_level` (STRING), `message` (STRING), `created_at` (TIMESTAMP).

## 4. Data Flow & Lineage

The original job flow is:
1.  **Orchestrator (`r_ausd_bp_ta_rn_da_vda_tk.ksh`) Start:**
    *   Reads command-line parameters (`-s`, `-l`).
    *   Sources environment configuration (`.dw_init`) and utility scripts (parameter handling, date handling, error logging).
    *   Determines `Stichtag` (snapshot date), defaulting to `sysdate` if not provided.
    *   Initializes `Wiederanlaufwert` (restart value), defaulting to 0 if not provided.
    *   Initializes logging framework variables (`DW_EintragsNr`, `LogDatei`).
    *   Sets up shell `trap` for error handling.
    *   Invokes the "kernel script" (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) with derived parameters.
    *   Logs success or failure messages.
    *   Exits.

2.  **Kernel Script (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) Execution (Inferred):**
    *   Reads contract cache data from the DWH.
    *   Applies filters based on `Stichtag` (`Gueltig_von <= Stichtag < Gueltig_bis` AND `LADEDATUM < Stichtag`).
    *   Applies restart logic: if `Wiederanlaufwert` > 0, deletes existing records for `DWH_VERTRAG_ID >= Wiederanlaufwert` and processes only `DWH_VERTRAG_ID > Wiederanlaufwert`.
    *   Transforms and prepares data as required for `Forderungsscoring`.
    *   Writes processed data to the target `FOS-Tabelle`.

**Target BigQuery Data Flow:**
1.  **BigQuery Stored Procedure (`sp_bereitstellung_basisprodukte_bert`) Invocation:**
    *   Triggered by a scheduler (e.g., Cloud Composer).
    *   Receives `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input parameters.
    *   Initializes internal variables (`v_progname`, `v_progversion`, `v_jobkennung`, `v_sysdate`, `v_effective_stichtag`, `v_effective_restart`, `v_job_nr`, `v_logdatei`, `v_errmsg`).
    *   Defaults `v_effective_restart` to 0 if `p_wiederanlaufWert` is NULL.
    *   Calculates `v_sysdate` using `CURRENT_DATE()` and `FORMAT_DATE`.
    *   Defaults `v_effective_stichtag` to `v_sysdate` if `p_stichtag` is NULL or empty.
    *   Performs parameter validation (e.g., `IF v_effective_stichtag IS NULL`). Raises an error if validation fails, logging to `job_log`.
    *   Generates a `v_job_nr` by incrementing the max existing `job_nr` from `job_audit`.
    *   Inserts a `STARTED` entry into `project.dataset.job_audit`.
    *   **BEGIN...EXCEPTION Block:**
        *   **Restart Cleanup (if `v_effective_restart > 0`):** `DELETE FROM project.dataset.target_fos_table WHERE DWH_VERTRAG_ID >= v_effective_restart;`
        *   **Core Data Load (Equivalent to kernel script):**
            *   `INSERT INTO project.dataset.target_fos_table (...)`
            *   `SELECT ... FROM project.dataset.source_contract_cache s`
            *   Applies `WHERE` clause filters for `GUELTIG_VON`, `GUELTIG_BIS`, `LADEDATUM` using `PARSE_DATE` on `v_effective_stichtag`.
            *   Applies `DWH_VERTRAG_ID > v_effective_restart` filter.
        *   **Success Logging:** Inserts an `OK` entry into `project.dataset.job_audit`.
    *   **Error Handling (WHEN ERROR THEN):**
        *   Captures `@@error.message`.
        *   Inserts an `ERROR` entry into `project.dataset.job_audit` with the error message.
        *   Raises the error using `RAISE USING MESSAGE`.

## 5. Transformation Logic

The KornShell script itself primarily handles orchestration and parameter processing. The actual data transformation logic resides in the external "kernel script" (`k_ausd_bp_ta_rn_da_vda_tk.ksh`). Without the content of this kernel script, the exact column-level transformations cannot be detailed. However, the wrapper's logic will be transformed as follows:

*   **Parameter Initialization:**
    *   `p_wiederanlaufWert` (shell variable) -> `p_wiederanlaufWert` (INT64 stored procedure parameter), defaulted to `0` using `IFNULL(p_wiederanlaufWert, 0)`.
    *   `p_stichtag` (shell variable) -> `p_stichtag` (STRING stored procedure parameter), defaulted to `v_sysdate` using `IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate)`.
*   **Date Determination:**
    *   Shell's `DWDate_Gib_Zeitraum` and system date -> BigQuery's `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation:**
    *   Shell's `pruefeParameterGesetzt` -> BigQuery `IF ... IS NULL OR TRIM(...) = '' THEN RAISE ... END IF;`
*   **Logging/Error Handling:**
    *   Shell's `DWMSG_...` functions, `trap`, and redirection to a log file -> `INSERT` statements into BigQuery `job_audit` and `job_log` tables. Error handling will utilize BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.
*   **Restart Logic:**
    *   Shell's conditional deletion and filtering for `Wiederanlaufwert` -> BigQuery `DELETE FROM target_fos_table WHERE DWH_VERTRAG_ID >= v_effective_restart;` followed by `INSERT ... WHERE DWH_VERTRAG_ID > v_effective_restart;`.
*   **Core Logic Delegation:** The invocation of `k_ausd_bp_ta_rn_da_vda_tk.ksh` will be replaced by the direct inclusion of its migrated BigQuery SQL/Stored Procedure logic within or as a call from the main `sp_bereitstellung_basisprodukte_bert` procedure.
    *   **Inferred Kernel Script Logic:**
        *   Reads from a source DWH table (e.g., `DWH$TA_C_VERTRAG` implied from commented code) which will map to `project.dataset.source_contract_cache`.
        *   Filters based on date ranges: `s.GUELTIG_VON <= PARSE_DATE('%d%m%Y', v_effective_stichtag) AND PARSE_DATE('%d%m%Y', v_effective_stichtag) < s.GUELTIG_BIS AND s.LADEDATUM < PARSE_DATE('%d%m%Y', v_effective_stichtag)`.
        *   Filters based on `DWH_VERTRAG_ID > v_effective_restart`.
        *   Writes to a target table, which will map to `project.dataset.target_fos_table`.

## 6. External Dependencies

Based on the script content, summary, and `lineage_external_systems` analysis:

*   **Source Data Warehouse (DWH):** The script extracts "contract cache data from the DWH". This is likely an Oracle Data Warehouse, indicated by `EXT:DATABASE`, `EXT:PCRS1`, `EXT:TDG` (all Oracle DB link targets) found in external systems.
    *   **Replacement in BigQuery:** Data from the legacy Oracle DWH will be ingested into BigQuery tables (e.g., `project.dataset.source_contract_cache`) via continuous data replication (e.g., Datastream for Oracle to BigQuery) or periodic batch loads. The BigQuery Stored Procedure will then read directly from these BigQuery tables.
*   **Target System (Forderungsscoring):** The processed data is made available for `Forderungsscoring`. This implies an external consumer system.
    *   **Replacement in BigQuery:** The `project.dataset.target_fos_table` will serve as the output for `Forderungsscoring`. Access for this system could be provided directly through BigQuery APIs, BigQuery Data Transfer Service, or exported to a GCS bucket for consumption, depending on the `Forderungsscoring` system's integration capabilities.
*   **Logging/Error Messaging System (`DWMSG_...` functions):** The script heavily uses an internal logging framework, potentially involving a mail server (`EXT:dwmsg_ermittlenr`).
    *   **Replacement in BigQuery:** This functionality will be replaced by writing audit and log entries to dedicated BigQuery tables (`project.dataset.job_audit`, `project.dataset.job_log`). For notifications (e.g., error emails), Cloud Logging integration with Cloud Monitoring alerts or Cloud Functions triggered by log entries could be implemented.
*   **Operating System Environment/Utilities (`.dw_init`, `ksh`, `getopts`, `print`, `tee`, `trap`):** These are core shell functionalities.
    *   **Replacement in BigQuery:** These will be replaced by BigQuery SQL procedural constructs, parameters, and audit tables as detailed in the Transformation Logic section. Environment variables will be translated into BigQuery stored procedure parameters or configuration table values.

## 7. Unresolved / Risks

*   **Kernel Script Logic:** The most significant unresolved item is the detailed business logic within `k_ausd_bp_ta_rn_da_vda_tk.ksh`. This script contains the actual data extraction, transformation, and loading (ETL) logic, which is critical for the job's functionality. Without its content, its migration to BigQuery SQL/Stored Procedures cannot be fully designed. This introduces a **significant redesign risk (B4)** for the overall job.
    *   **Mitigation:** The content of `k_ausd_bp_ta_rn_da_vda_tk.ksh` must be obtained and analyzed in a subsequent step to complete the migration design for the entire job.
*   **`BERT_DIR_ROOT` Resolution:** The script relies on the `BERT_DIR_ROOT` environment variable. While inferred for relative paths, its exact definition and how it's set in the legacy environment are not explicitly known from the provided analysis.
    *   **Risk:** Incorrect resolution could lead to misidentification of internal script paths.
    *   **Mitigation:** Confirm the resolution of `BERT_DIR_ROOT` in the legacy environment.
*   **Legacy DWH Schema Details:** While the source is identified as a DWH, specific table and column schemas for "contract cache data" are not fully available. The design uses placeholder table names.
    *   **Risk:** Inaccurate schema assumptions for `source_contract_cache` and `target_fos_table`.
    *   **Mitigation:** Obtain full DDL and data dictionaries for all relevant DWH tables.
*   **Performance Tuning:** Initial BigQuery SQL for complex transformations may require performance tuning.
    *   **Risk:** Suboptimal query performance.
    *   **Mitigation:** Implement best practices for BigQuery SQL, partitioning, clustering, and review query plans during development.

## 8. Build Plan

The build plan focuses on implementing the BigQuery components based on this design.

1.  **Define BigQuery Datasets:**
    *   Create a dedicated BigQuery dataset for the migrated job (e.g., `my_project.isbert_data_processing`).
2.  **Schema Definition for Target Tables:**
    *   Create the `project.dataset.job_audit` and `project.dataset.job_log` tables in BigQuery.
    *   Define schemas for `project.dataset.source_contract_cache` (based on legacy DWH source) and `project.dataset.target_fos_table` (based on `Forderungsscoring` requirements and kernel script output).
3.  **Data Ingestion for Source Data:**
    *   Set up a data pipeline (e.g., Datastream + Dataflow or manual batch load) to continuously or periodically ingest data from the legacy DWH's contract cache into `project.dataset.source_contract_cache`.
4.  **Develop BigQuery Stored Procedure (Orchestrator):**
    *   **File:** `sp_bereitstellung_basisprodukte_bert.sql`
    *   **Language:** BigQuery SQL
    *   **Content:**
        *   `CREATE OR REPLACE PROCEDURE ...`
        *   Parameter declarations (`p_stichtag`, `p_wiederanlaufWert`).
        *   Variable declarations (`v_progname`, `v_jobkennung`, `v_effective_stichtag`, `v_effective_restart`, `v_job_nr`, `v_logdatei`, `v_errmsg`).
        *   Logic for defaulting `p_wiederanlaufWert` and `p_stichtag`.
        *   BigQuery date functions (`CURRENT_DATE()`, `FORMAT_DATE`).
        *   Parameter validation (`IF ... THEN RAISE ...`).
        *   `job_audit` and `job_log` table inserts for status and errors.
        *   `BEGIN...EXCEPTION WHEN ERROR THEN...END` block for robust error handling.
        *   **Implement restart cleanup:** `DELETE FROM project.dataset.target_fos_table WHERE DWH_VERTRAG_ID >= v_effective_restart;`
        *   **Integrate kernel script logic:** Replace the shell call to `k_ausd_bp_ta_rn_da_vda_tk.ksh` with the translated BigQuery SQL. This might be a direct `INSERT INTO ... SELECT FROM ...` statement or a call to another BigQuery Stored Procedure if the kernel logic is complex.
5.  **Develop BigQuery Stored Procedure (Kernel Script Logic - Pending):**
    *   **File:** `sp_process_contract_cache_data.sql` (or similar, depending on kernel script analysis)
    *   **Language:** BigQuery SQL
    *   **Content:** This component is *pending the analysis of `k_ausd_bp_ta_rn_da_vda_tk.ksh`*. It will contain the core `SELECT`, `TRANSFORM`, `INSERT` logic.
6.  **Orchestration and Scheduling:**
    *   Create a Cloud Composer DAG (or Cloud Workflows/Cloud Scheduler job) to invoke the `sp_bereitstellung_basisprodukte_bert` stored procedure at the required frequency.
    *   Configure parameters for the stored procedure call as needed.
7.  **Testing:**
    *   Develop unit tests for the BigQuery Stored Procedure logic.
    *   Perform integration testing with realistic data volumes.
    *   Conduct user acceptance testing (UAT) to ensure functional parity.
8.  **Monitoring and Alerting:**
    *   Set up Cloud Monitoring alerts based on BigQuery `job_audit` and `job_log` tables for failures or unexpected behavior.