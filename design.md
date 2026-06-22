# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_instance.ksh`, serves as an orchestration wrapper for an ETL process. Its primary purpose is to prepare and initiate the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. The job generates a snapshot extraction of contract cache data from the Data Warehouse (DWH) and makes it available to the Forderungsscoring (FOS) system. It supports flexible execution through parameters like a processing date (`Stichtag`) and a restart value (`Wiederanlaufwert`). The script itself handles parameter parsing, date determination, environment setup, error handling, and logging, then delegates the core data processing logic to an external "kernel script."

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** Orchestration, parameter handling, error management, logging, and invocation of a core data processing script.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve:
*   **BigQuery Stored Procedure:** The orchestration logic of `r_ausd_bp_ta_bpr_instance.ksh` will be migrated to a BigQuery Stored Procedure. This procedure will handle parameter validation, date calculations, and logging.
*   **BigQuery Tables:**
    *   **Source Contract Cache Table:** This will be the BigQuery equivalent of the `DWH$TA_C_VERTRAG` mentioned in the script's comments, representing the source data for contract cache.
    *   **Target Table (`target_table`):** The final destination for the processed data, likely corresponding to the FOS-Tabelle mentioned in the original script's description.
    *   **Job Log Table (`project.dataset.job_log`):** To replace the file-based logging (`DWMSG_*` functions), a dedicated BigQuery table will store job execution details, status, and custom messages.
    *   **Job Error Log Table (`project.dataset.job_error_log`):** To capture detailed error information, replacing the script's error handling mechanism.
*   **BigQuery DML/SQL:** The core data extraction, transformation, and loading logic (currently in the "kernel script" `k_ausd_bp_ta_bpr_instance.ksh`) will be implemented using BigQuery SQL DML statements (e.g., `MERGE INTO`, `DELETE`, `SELECT`).
*   **Cloud Composer (or equivalent orchestrator):** An Airflow DAG will be used to schedule and orchestrate the BigQuery Stored Procedure execution, passing necessary parameters.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_instance.ksh` acts as an orchestrator. The actual data flow is primarily handled by the invoked "kernel script" (`k_ausd_bp_ta_bpr_instance.ksh`).

**Legacy Data Flow:**
1.  `r_ausd_bp_ta_bpr_instance.ksh` starts.
2.  Environment variables and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are sourced.
3.  Command-line parameters (`-s` for Stichtag, `-l` for Wiederanlaufwert) are parsed.
4.  System date is determined, and `Stichtag` is set if not provided.
5.  Parameter validation occurs.
6.  Job logging is initialized, and error traps are set.
7.  The "kernel script" `k_ausd_bp_ta_bpr_instance.ksh` is invoked with parameters.
8.  The kernel script is expected to read from source tables (e.g., `DWH$TA_C_VERTRAG`), apply filtering based on `Stichtag` and `Wiederanlaufwert`, and write to a target FOS table.
9.  Upon completion of the kernel script, `r_ausd_bp_ta_bpr_instance.ksh` updates job status in its log.

**Target BigQuery Data Flow:**
1.  An Airflow DAG (orchestrator) triggers the `project.dataset.ausd_bp_ta_bpr_instance` BigQuery Stored Procedure.
2.  The Stored Procedure receives `p_stichtag` and `p_wiederanlaufWert` as input.
3.  Inside the procedure:
    *   System date is derived using `CURRENT_DATE()`.
    *   Parameters are validated, defaulting `p_wiederanlaufWert` to 0 and `p_stichtag` to the system date if not provided.
    *   Job start and parameter information is recorded in `project.dataset.job_log`.
    *   If `p_wiederanlaufWert` is greater than 0, a `DELETE` statement is executed on the `project.dataset.target_table` to remove records with `DWH_VERTRAG_ID >= p_wiederanlaufWert`.
    *   A `MERGE INTO` statement processes data from `project.dataset.source_contract_cache`. It selects records based on:
        *   `Gueltig_von <= PARSE_DATE('%d%m%Y', v_stichtag)`
        *   `PARSE_DATE('%d%m%Y', v_stichtag) < Gueltig_bis`
        *   `LADEDATUM < PARSE_DATE('%d%m%Y', v_stichtag)`
        *   An optional filter for `DWH_VERTRAG_ID > p_wiederanlaufWert`.
    *   Matching records in `target_table` are updated; new records are inserted.
    *   Job success status is recorded in `project.dataset.job_log`.
    *   Error handling (`EXCEPTION WHEN ERROR`) catches any SQL errors and logs them to `project.dataset.job_error_log`, then raises an error.

## 5. Transformation Logic
The transformation logic described pertains to the shell script's orchestration, assuming the actual data manipulation is in `k_ausd_bp_ta_bpr_instance.ksh`.

**Wrapper Script (`r_ausd_bp_ta_bpr_instance.ksh`) Logic:**
*   **Parameter Handling:** `getopts` parses `-s` (Stichtag, DDMMYYYY) and `-l` (Wiederanlaufwert).
*   **Defaulting:** `p_wiederanlaufWert` defaults to 0. `p_stichtag` defaults to `v_sysdate` (current date in DDMMYYYY) if not provided.
*   **Date Derivation:** `DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy` fetches the current system date.
*   **Validation:** `pruefeParameterGesetzt Stichtag p_stichtag` ensures `Stichtag` is set.
*   **Error Handling & Logging:** Uses `f_alis_msgerr.ksh` for error messaging (`DWMSG_MeldeFehler`), `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` for logging, and `trap` for error interruption handling.
*   **Core Logic Invocation:** Executes `${Name_Kernskript}` (which resolves to `k_ausd_bp_ta_bpr_instance.ksh`) with derived parameters.

**BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_instance`) Logic:**
*   **Parameter Handling:** `IN p_stichtag STRING`, `IN p_wiederanlaufWert INT64`.
*   **Defaulting:** `v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0)`. `v_stichtag = IFNULL(p_stichtag, v_sysdate)`.
*   **Date Derivation:** `SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());`.
*   **Validation:** `IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN ... SIGNAL SQLSTATE '45000'`.
*   **Error Handling & Logging:**
    *   `INSERT INTO project.dataset.job_log` for job start, success, and completion.
    *   `INSERT INTO project.dataset.job_error_log` for parameter validation errors.
    *   `BEGIN...EXCEPTION WHEN ERROR...END` block for robust error handling during data processing, recording error messages in `job_log`.
*   **Restart Logic (DELETE):**
    ```sql
    IF v_wiederanlaufWert > 0 THEN
      DELETE FROM `project.dataset.target_table`
      WHERE DWH_VERTRAG_ID >= v_wiederanlaufWert;
    END IF;
    ```
*   **Core Data Logic (MERGE):** Assumed to be based on the kernel script's intent, using BigQuery SQL. Example provided:
    ```sql
    MERGE INTO `project.dataset.target_table` T
    USING (
      SELECT src.*
      FROM `project.dataset.source_contract_cache` src
      WHERE src.Gueltig_von <= PARSE_DATE('%d%m%Y', v_stichtag)
        AND PARSE_DATE('%d%m%Y', v_stichtag) < src.Gueltig_bis
        AND src.LADEDATUM < PARSE_DATE('%d%m%Y', v_stichtag)
        AND (v_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > v_wiederanlaufWert)
    ) S
    ON T.DWH_VERTRAG_ID = S.DWH_VERTRAG_ID
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...
    ```
    *Note: Column mapping for `UPDATE SET` and `INSERT` must be detailed once the schema of `source_contract_cache` and `target_table` are known, and the exact logic of `k_ausd_bp_ta_bpr_instance.ksh` is analyzed.*

## 6. External Dependencies
The current script is a self-contained shell script with no direct external database calls or system integrations beyond what its internal logic (or the invoked kernel script) handles.
*   **External Kernel Script:** `k_ausd_bp_ta_bpr_instance.ksh` - This is a critical dependency. Its content must be analyzed to fully understand the data processing logic and any further external interactions (e.g., database connections, file I/O). The migration strategy for this kernel script needs to be determined; likely, its SQL parts will be converted to BigQuery SQL, and any non-SQL logic to Python or another suitable BigQuery-compatible language.

## 7. Unresolved / Risks
*   **Kernel Script Logic:** The content and complexity of `k_ausd_bp_ta_bpr_instance.ksh` are currently unknown. This script is identified as the "core script" and likely contains the actual data manipulation logic. Without its analysis, the full scope of transformation cannot be completely defined. It may introduce additional external dependencies (databases, APIs, files) that are not apparent from the wrapper script.
*   **Exact Data Schemas:** The specific schemas for `DWH$TA_C_VERTRAG` and the target FOS table are not available. This prevents precise column mapping in the BigQuery `MERGE` statement.
*   **Proprietary Utilities:** The sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) and custom functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_*` are proprietary. While their functionality can be replicated in BigQuery SQL or Python (e.g., logging to tables, using BigQuery date functions), the exact behavior of all edge cases must be ensured.
*   **Error Code Mapping:** The original script uses specific error codes (`ErrNr=193`, `ErrNr=192`). While BigQuery Stored Procedures support error handling, a mapping or a new error handling strategy for these legacy codes might be needed.

## 8. Build Plan
1.  **Analyze `k_ausd_bp_ta_bpr_instance.ksh`:** Obtain and analyze the content of the "kernel script" to identify its specific data sources, target tables, and transformation logic. Determine if it contains only SQL or also non-SQL logic.
2.  **Define BigQuery Schemas:**
    *   Create `project.dataset.job_log` table (columns: `entry_nr`, `job_name`, `script_name`, `log_name`, `stichtag`, `status`, `created_at`, `error_message`).
    *   Create `project.dataset.job_error_log` table (columns: `job_name`, `error_nr`, `error_arg`, `created_at`).
    *   Define schemas for `project.dataset.source_contract_cache` and `project.dataset.target_table` based on the detailed analysis of the kernel script.
3.  **Develop BigQuery Stored Procedure (Orchestration):**
    *   Write the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_instance` based on the provided pseudocode, incorporating parameter handling, date logic, validation, and logging.
    *   Implement the `DELETE` statement for restart logic.
    *   Implement the `MERGE INTO` statement with precise column mappings and transformations identified from the kernel script analysis.
4.  **Develop BigQuery SQL (Core Logic):** Translate any specific SQL queries or DML statements from `k_ausd_bp_ta_bpr_instance.ksh` into BigQuery SQL, to be embedded within the stored procedure or called as separate scripts/views.
5.  **Develop Python (if necessary):** If `k_ausd_bp_ta_bpr_instance.ksh` contains non-SQL logic that cannot be directly translated to BigQuery SQL, implement that logic in Python.
6.  **Create Airflow DAG:** Develop an Airflow DAG to orchestrate the execution of the BigQuery Stored Procedure, passing required parameters.
7.  **Testing:** Implement unit and integration tests for the BigQuery Stored Procedure and the Airflow DAG to ensure functional equivalence with the legacy system.
8.  **Deployment:** Deploy the BigQuery resources (tables, stored procedure) and the Airflow DAG to the production environment.