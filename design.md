# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

## 1. Purpose & Scope
This KornShell script (`r_ausd_bp_ta_bpr_evn.ksh`) serves as an orchestration wrapper for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. Its primary purpose is to prepare parameters, establish error handling, and then invoke a core script to generate a snapshot of the Data Warehouse (DWH) contract cache. This snapshot is then made available for demand scoring (Forderungsscoring - FOS-Tabelle). The job handles parameter parsing for a reference date (`Stichtag`) and a restart value (`Wiederanlaufwert`). It also contains logic for deleting already provisioned data under certain conditions and for filtering records based on date criteria (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`).

## 2. Source Inventory
The job is comprised of a single identified component file.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh`
    *   **Technology:** KornShell (KSH)
    *   **Purpose:** Orchestration, Parameter Handling, Error Logging Wrapper
    *   **Complexity Tier:** Not Available (information not found in `file_complexity` table)
    *   **Automation Bucket:** Not Available (information not found in `automation_rate` table)
    *   **Summary:** This KSH script orchestrates the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. It prepares parameters and calls a core script to generate a snapshot of the DWH contract cache for demand scoring (FOS-Tabelle).

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedures:** The main orchestration logic of `r_ausd_bp_ta_bpr_evn.ksh` will be migrated into a BigQuery Stored Procedure. This procedure will handle parameter parsing, validation, date defaulting, job auditing, and the invocation of the core data transformation logic.
*   **BigQuery Tables:**
    *   **`job_audit` table:** To capture job execution status, parameters, log messages, and error information, replacing the file-based logging.
    *   **Source Tables:** The DWH contract cache tables (e.g., `source_contract_cache` in the pseudocode) will be present in BigQuery.
    *   **Target Tables:** The FOS table (`fos_target_table` in the pseudocode) will be the destination for the processed data.
*   **BigQuery SQL:** The actual data extraction, transformation, and loading (ETL) logic, which is currently encapsulated in the `k_ausd_bp_ta_bpr_evn.ksh` (core script) and likely involves SQL, will be translated into optimized BigQuery SQL statements, potentially within another BigQuery Stored Procedure or as part of a series of views.
*   **Orchestration Layer (Optional):** For external scheduling, dependency management, and more complex environmental interactions, Google Cloud Composer (Airflow), Cloud Workflows, or Cloud Run could be used to trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_evn.ksh` acts as an orchestrator.
1.  **Initialization:** The script sources several utility KornShell scripts for environment setup (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), and date utilities (`h_alis_date.ksh`). These are helper functions.
2.  **Parameter Acquisition:** It parses command-line arguments for `Stichtag (-s)` and `Wiederanlaufwert (-l)`. It defaults `Wiederanlaufwert` to 0 if not provided and `Stichtag` to the system date if not provided (the commented-out logic for `MAX(ladedatum)` suggests an alternative date source that is currently inactive).
3.  **Validation:** It performs basic validation on required parameters.
4.  **Logging Setup:** It initializes job tracking information (`DW_EintragsNr`, `LogDatei`, `JobKennung`) and sets up shell `trap` commands for error handling and logging.
5.  **Core Script Invocation:** The script then invokes a "core script" identified as `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_evn.ksh`, passing the parsed parameters. This core script is responsible for the actual data processing.
6.  **Post-Processing/Status:** After the core script completes, the wrapper logs the success status.

**Migrated Data Flow:**
In BigQuery, this flow will be:
1.  **BigQuery Stored Procedure Call:** An external orchestrator (or manual invocation) will call the main BigQuery Stored Procedure (`sp_ausd_bp_ta_bpr_evn`).
2.  **Parameter Handling & Auditing:** The BigQuery Stored Procedure will receive parameters, perform validation, determine default values, and log job initiation and parameters into the `job_audit` table.
3.  **Data Transformation (Core Logic):** Inside the main stored procedure, or by calling a nested stored procedure, the logic equivalent to `k_ausd_bp_ta_bpr_evn.ksh` will be executed. This will involve:
    *   Reading from BigQuery source tables (e.g., `project.dataset.source_contract_cache`).
    *   Applying filters based on `Stichtag`, `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM`.
    *   Conditional deletion of records from the target table based on `Wiederanlaufwert` (`dwh_vertrag_id >= v_wiederanlaufWert`).
    *   Insertion of processed data into the BigQuery target table (e.g., `project.dataset.fos_target_table`).
4.  **Error Handling & Auditing:** Any errors during the BigQuery SQL execution will be caught by `EXCEPTION WHEN ERROR` blocks, logged to the `job_audit` table, and re-raised.
5.  **Final Status Update:** Upon successful completion, the `job_audit` table will be updated with a success status.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_evn.ksh` wrapper script implements the following key transformation/orchestration logic:

*   **Parameter Parsing:** Uses `getopts` to parse `-s <Stichtag>` (reference date in DDMMYYYY format) and `-l <Wiederanlaufwert>` (restart value, an integer representing `DWH_VERTRAG_ID`).
*   **Defaulting `Wiederanlaufwert`:** If `-l` is not provided, `p_wiederanlaufWert` defaults to 0.
*   **Defaulting `Stichtag`:** If `-s` is not provided, `p_stichtag` defaults to the current system date (`v_sysdate`). The original script has commented-out logic (`FOSHoleLadedatum`) that would fetch the `MAX(ladedatum)` from a source table, but this is not active.
*   **Parameter Validation:** Calls `pruefeParameterGesetzt Stichtag p_stichtag` to ensure the `Stichtag` is set. If not, an error (ErrNr 193) is triggered, and the script exits.
*   **Error Handling (Traps):** Sets `trap` commands to catch `INT`, `STOP`, `CONT`, and `ERR` signals, directing error messages and handling to `DWMSG_Fehlerbehandlung`.
*   **Job Status Logging:** Uses a custom `DWMSG_` framework for logging job start, parameters, and final status (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`).
*   **Core Logic Invocation:** Executes the `k_ausd_bp_ta_bpr_evn.ksh` script with the collected parameters.
*   **Data Filtering (in core script, orchestrated by wrapper):** The summary indicates records are selected where:
    *   `Gueltig_von <= Stichtag < Gueltig_bis`
    *   `LADEDATUM < Stichtag`
*   **Restart Logic (in core script, orchestrated by wrapper):** If `p_wiederanlaufWert` is set (i.e., `> 0`), the core script is expected to filter `DWH_VERTRAG_ID > p_wiederanlaufWert` and delete existing entries with `DWH_VERTRAG_ID >= p_wiederanlaufWert` before inserting.

**BigQuery SQL Equivalents (Pseudocode from MCP output):**
*   **Parameters:** BigQuery Stored Procedure parameters (e.g., `IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`).
*   **Defaulting:** `IF p_wiederanlaufWert IS NULL THEN SET v_wiederanlaufWert = 0; END IF;` and similar for `p_stichtag`.
*   **Validation:** `IF v_stichtag IS NULL OR v_stichtag = '' THEN SET v_errnr = 193; RAISE; END IF;`
*   **Logging:** `INSERT INTO `project.dataset.job_audit` (...) VALUES (...)`
*   **Date Operations:** `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `PARSE_DATE('%d%m%Y', v_stichtag)`.
*   **Conditional Deletion:** `DELETE FROM `project.dataset.fos_target_table` WHERE dwh_vertrag_id >= v_wiederanlaufWert;`
*   **Conditional Insertion:** `INSERT INTO `project.dataset.fos_target_table` SELECT * FROM `project.dataset.source_contract_cache` WHERE gueltig_von <= PARSE_DATE(...) AND PARSE_DATE(...) < gueltig_bis AND ladedatum < PARSE_DATE(...) AND dwh_vertrag_id > v_wiederanlaufWert;`

## 6. External Dependencies
The `lineage_assembled_jobs` and `lineage_edges` queries did not identify explicit external systems (like Oracle, SFTP, S3) directly referenced by `r_ausd_bp_ta_bpr_evn.ksh`. However, based on the script content and common ETL patterns, the following are inferred:

*   **Legacy Data Warehouse (DWH):** The source of the contract cache data. This will be replaced by equivalent BigQuery source tables.
*   **File System / Shell Environment:**
    *   `$HOME/.dw_init`: Environment initialization. Replaced by BigQuery project/dataset configuration, procedure variables, or an external orchestrator's environment.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework. Replaced by BigQuery `BEGIN...EXCEPTION` blocks and logging to the `job_audit` table.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper. Replaced by BigQuery Stored Procedure parameters.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper. Replaced by BigQuery date/time functions (`CURRENT_DATE()`, `FORMAT_DATE`, `PARSE_DATE`).
    *   Log files (`LogDatei`): Replaced by BigQuery `job_audit` table.
*   **Core Processing Script:** `k_ausd_bp_ta_bpr_evn.ksh`: This is the most significant dependency. Its internal logic (likely SQL or other data manipulation) must be fully analyzed and migrated to BigQuery SQL/Stored Procedures.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_bp_ta_bpr_evn.ksh`) Logic:** The content of the core processing script (`k_ausd_bp_ta_bpr_evn.ksh`) was not directly analyzed in this step. This script is crucial as it contains the actual data transformation logic. Its migration complexity is currently unknown.
*   **Missing Metadata:** The `complexity_tier` and `migration_bucket` for `r_ausd_bp_ta_bpr_evn.ksh` were not available, which can impact effort estimation.
*   **Dynamic `MAX(ladedatum)`:** The commented-out logic for `FOSHoleLadedatum` suggests that the `Stichtag` could dynamically be determined from data. If this functionality is intended to be reactivated, it would require a SQL query to find the maximum load date from the relevant DWH source table in BigQuery.
*   **Shell Traps:** The intricate shell `trap` mechanism for signal handling needs to be carefully translated into BigQuery's `BEGIN...EXCEPTION` blocks and potentially integrated with external orchestration error handling.
*   **Idempotency and Restartability:** The existing `Wiederanlaufwert` logic suggests a form of restartability. This needs to be preserved and tested thoroughly in the BigQuery implementation to ensure idempotency and correct data handling upon re-runs.

## 8. Build Plan
The migration will follow these steps:

1.  **Analyze `k_ausd_bp_ta_bpr_evn.ksh`:** Perform a detailed analysis of the core KornShell script (`k_ausd_bp_ta_bpr_evn.ksh`) to extract its data processing logic, source tables, target tables, and transformations. This will likely involve another iteration of reading the source file and potentially using MCP tools for SQL extraction if the core script contains embedded SQL.
2.  **Create BigQuery Audit Table:**
    *   **Language:** BigQuery DDL
    *   **File:** `bq_ddl/job_audit_table.sql`
    *   **Content:** Define `project.dataset.job_audit` with columns like `job_id`, `job_name`, `status`, `stichtag`, `restart_value`, `error_code`, `error_arg`, `message`, `created_at`.
3.  **Develop BigQuery Core Logic Stored Procedure:**
    *   **Language:** BigQuery SQL (Stored Procedure)
    *   **File:** `bq_sp/sp_k_ausd_bp_ta_bpr_evn.sql`
    *   **Content:** Implement the data transformation logic identified in step 1, including the filtering, conditional delete, and insert operations into `project.dataset.fos_target_table`. This procedure will accept `p_stichtag` and `p_wiederanlaufWert` as parameters.
4.  **Develop BigQuery Orchestration Stored Procedure:**
    *   **Language:** BigQuery SQL (Stored Procedure)
    *   **File:** `bq_sp/sp_r_ausd_bp_ta_bpr_evn.sql`
    *   **Content:** Based on the provided pseudocode, create `project.dataset.sp_ausd_bp_ta_bpr_evn`. This procedure will:
        *   Accept `p_stichtag` and `p_wiederanlaufWert`.
        *   Implement parameter defaulting and validation.
        *   Log job status to `job_audit`.
        *   Call `project.dataset.sp_k_ausd_bp_ta_bpr_evn` (from step 3).
        *   Implement BigQuery `EXCEPTION WHEN ERROR` for robust error handling.
5.  **Identify/Create BigQuery Source and Target Tables:**
    *   Ensure the source tables (DWH contract cache) and the target FOS table are properly defined and loaded in BigQuery. This might involve separate data ingestion pipelines.
6.  **Develop Orchestration Wrapper (Optional):**
    *   **Language:** Python (for Cloud Composer/Workflows)
    *   **File:** `airflow_dags/dag_r_ausd_bp_ta_bpr_evn.py` (example for Airflow)
    *   **Content:** A minimal wrapper to trigger the `project.dataset.sp_ausd_bp_ta_bpr_evn` BigQuery Stored Procedure, passing parameters if needed, and handling scheduling. This would only be necessary if the BigQuery Stored Procedure cannot be triggered directly by the chosen scheduler or if external integrations are required.
7.  **Unit and Integration Testing:** Develop comprehensive test cases for the BigQuery Stored Procedures, covering various parameter combinations, edge cases, and restart scenarios.