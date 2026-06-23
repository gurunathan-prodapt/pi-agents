# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_cntrct_evn.ksh`, is an initial provisioning job responsible for preparing selected base products (Basisprodukte) for the BERT system, specifically dealing with contract events. Its primary function is to extract a snapshot of contract cache data from the Data Warehouse (DWH) and make it available for demand scoring (Forderungsscoring). The script handles date determination and supports restart functionality using a configurable restart value (`DWH_VERTRAG_ID`), ensuring that only new or unprocessed contracts are considered. The overall job was assembled from 1 component(s); stage distribution indicates medium complexity for 1 component.

## 2. Source Inventory
The job consists of a single KornShell script: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh`.
*   **Technology:** KornShell (shell script)
*   **Complexity Tier:** medium
*   **Automation Bucket:** semi_auto
*   **Summary:** This KornShell script orchestrates the preparation of selected base products (Basisprodukte) for the BERT system, specifically contract events. It extracts data from the DWH and makes it available for demand scoring (Forderungsscoring), handling date determination and restart values.

## 3. Target Architecture
The migration target is Google BigQuery. The existing KornShell script's functionality will be replicated using BigQuery Stored Procedures for the main wrapper logic and the core business logic.
*   **Main Wrapper:** A BigQuery Stored Procedure, `project.dataset.ausd_bp_ta_cntrct_evn`, will handle parameter parsing, defaulting, validation, logging, and orchestration of the core business logic.
*   **Core Business Logic:** A separate BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_cntrct_evn`, will encapsulate the data extraction, filtering, and insertion operations into the target FOS table.
*   **Logging/Auditing:** Dedicated BigQuery tables, such as `project.dataset.job_log`, will replace file-based logging for tracking job status, parameters, and errors.
*   **Orchestration:** While the core logic will be in BigQuery Stored Procedures, an external orchestrator like Cloud Composer (Airflow) or Cloud Workflows can be used to schedule and manage the execution of these procedures, providing parameters and monitoring.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_cntrct_evn.ksh` script acts as a wrapper that prepares parameters and then invokes a core business logic script.
1.  **Parameter Input:** The script accepts a key date (`-s <DDMMYYYY>`) and an optional restart value (`-l <restart_value>`).
2.  **Initialization:** It sources several utility KornShell scripts for environment setup, error handling, parameter parsing, and date functions.
3.  **Date Determination:** If the key date is not provided, it defaults to the current system date.
4.  **Restart Value Handling:** The restart value is initialized to `0` if not explicitly provided.
5.  **Error Handling Setup:** The script sets up error traps and initializes a custom logging framework (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, etc.).
6.  **Core Logic Invocation:** The wrapper script explicitly invokes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_evn.ksh` (referenced as `Name_Kernskript`) with the processed parameters and logging details.
7.  **Logging & Exit:** It logs the job's start, status updates, and exit status.

The `lineage_edges` analysis did not explicitly capture the invocation of `k_ausd_bp_ta_cntrct_evn.ksh` or any direct table reads/writes by the main script `r_ausd_bp_ta_cntrct_evn.ksh`. However, based on code analysis, the data flow is:
*   Input parameters (`-s`, `-l`) -> `r_ausd_bp_ta_cntrct_evn.ksh`
*   `r_ausd_bp_ta_cntrct_evn.ksh` invokes `k_ausd_bp_ta_cntrct_evn.ksh` with derived parameters.
*   The `k_ausd_bp_ta_cntrct_evn.ksh` (core logic) is expected to read from a `contract_cache` equivalent and write to a `fos_table` equivalent, applying date and restart value filters.

## 5. Transformation Logic
The transformation will involve converting KornShell constructs and control flow into BigQuery SQL Stored Procedures.

**Parameter Handling:**
*   KornShell `getopts` for command-line arguments will be replaced by `IN` parameters in BigQuery Stored Procedures (`p_stichtag STRING`, `p_wiederanlaufWert INT64`).
*   Defaulting logic (`if [[ -z "$p_wiederanlaufWert" ]]`) will be mapped to `IFNULL` functions or `IF...THEN` blocks in BigQuery.

**Control Flow:**
*   Conditional statements (`if/then/else`) will be translated to BigQuery SQL `IF...THEN...ELSE END IF` structures.
*   Error handling via `set -e` and `trap` will be replaced by `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error management within the stored procedure. `SIGNAL SQLSTATE` will be used for raising errors.

**Data Operations (within the core logic, `k_ausd_bp_ta_cntrct_evn`):**
*   **Deletion (restart mechanism):** If a restart value (`v_restart_value`) is provided and greater than 0, a `DELETE` statement will remove records from the target `fos_table` where `DWH_VERTRAG_ID >= v_restart_value`.
*   **Insertion (data provisioning):** An `INSERT INTO ... SELECT` statement will select records from a source `contract_cache` table (e.g., `project.dataset.contract_cache`) into the target `fos_table` (e.g., `project.dataset.fos_table`).
*   **Filtering:**
    *   Date filtering: `DATE(src.Gueltig_von) <= v_stichtag_date AND v_stichtag_date < DATE(src.Gueltig_bis) AND DATE(src.LADEDATUM) < v_stichtag_date`.
    *   Restart value filtering: `src.DWH_VERTRAG_ID > v_restart_value`.
*   Date conversion: `DDMMYYYY` string dates will be converted to `DATE` types using `PARSE_DATE('%d%m%Y', p_stichtag)`.

**Logging:**
*   All `print` statements and redirection to log files (`>> $LogDatei`) will be replaced by `INSERT` statements into a BigQuery `job_log` table, capturing `job_nr`, `job_name`, `job_status`, `log_ts`, and `message`.

## 6. External Dependencies
The original KornShell script has several dependencies:
*   **Sourced Environment Files:**
    *   `$HOME/.dw_init`: Likely sets environment variables. In BigQuery, this will be handled by procedure parameters, constants, or configuration tables.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework. Replaced by BigQuery's `EXCEPTION` handling and `job_log` table inserts.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper. Replaced by BigQuery stored procedure input parameters.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities. Replaced by BigQuery's native date functions (`CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`).
*   **Invoked Core Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_evn.ksh`: This core script containing the actual data processing logic will be migrated into a separate BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_cntrct_evn`, invoked by the wrapper procedure using `CALL`.
*   **Filesystem for Logging:** The original script writes logs to a dynamic file. This will be replaced by inserts into the `project.dataset.job_log` BigQuery table.

No explicit external systems (like Oracle, SFTP, S3) were identified by the lineage analysis for this specific job. All dependencies are internal shell scripts or inferred data sources (DWH tables).

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_bp_ta_cntrct_evn.ksh`) Content:** The detailed business logic within the core script was not provided. The current design assumes standard SQL operations for data selection, filtering, and insertion. If `k_ausd_bp_ta_cntrct_evn.ksh` contains complex, procedural logic not easily translatable to set-based SQL, it may require Python UDFs or external processing (e.g., via Dataproc or Cloud Functions orchestrated by Cloud Composer).
*   **`usage()` Functionality:** The detailed help text generated by the `usage()` function is not directly replicable in BigQuery SQL procedures. This documentation will be externalized (e.g., in `README` files or procedure comments).
*   **Shell Traps:** Advanced `trap`-based signal handling is not available in BigQuery SQL and will be managed by the `BEGIN...EXCEPTION` blocks.
*   **Dynamic Log File Creation:** The dynamic nature of log file naming is replaced by structured logging into a BigQuery table, which offers better querying and management capabilities.
*   **Absence of Lineage Edges:** The lineage analysis did not explicitly capture the invocation relationship between `r_ausd_bp_ta_cntrct_evn.ksh` and `k_ausd_bp_ta_cntrct_evn.ksh`, nor the specific tables read/written by the core script. This required manual inference from the code. Future lineage runs should be checked for more comprehensive coverage.

## 8. Build Plan

The migration will involve creating the following BigQuery components:

1.  **BigQuery `job_log` Table:**
    *   **Purpose:** To store job execution details, parameters, and logging messages.
    *   **Schema (example):**
        ```sql
        CREATE TABLE `project.dataset.job_log` (
            job_nr INT64,
            job_name STRING,
            job_status STRING,
            log_ts TIMESTAMP,
            stichtag STRING,
            restart_value INT64,
            message STRING
        );
        ```

2.  **BigQuery Stored Procedure: `k_ausd_bp_ta_cntrct_evn` (Core Logic)**
    *   **Purpose:** To encapsulate the main data transformation and loading logic.
    *   **Language:** BigQuery SQL
    *   **DDL (Pseudocode):**
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_cntrct_evn`(
            IN p_jobkennung STRING,
            IN p_stichtag STRING,
            IN p_job_nr INT64,
            IN p_wiederanlaufWert INT64
        )
        BEGIN
            -- Logic for deletion of records based on restart value
            -- Logic for insertion of filtered data from contract_cache to fos_table
            -- (Detailed SQL will depend on the actual k_ausd_bp_ta_cntrct_evn.ksh content)
        END;
        ```

3.  **BigQuery Stored Procedure: `ausd_bp_ta_cntrct_evn` (Wrapper Logic)**
    *   **Purpose:** To serve as the entry point, handling parameter validation, defaulting, and orchestrating the call to the core logic.
    *   **Language:** BigQuery SQL
    *   **DDL (Pseudocode from tool output):**
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn`(
            IN p_stichtag STRING,
            IN p_wiederanlaufWert INT64
        )
        BEGIN
            -- Parameter initialization and defaulting
            -- System date determination
            -- Parameter validation (e.g., Stichtag check)
            -- Job logging (start, status updates)
            -- CALL `project.dataset.k_ausd_bp_ta_cntrct_evn`(...)
            -- Exception handling
        END;
        ```

**Configuration Files:**
*   Orchestration configuration (e.g., Cloud Composer DAG in Python) to schedule `project.dataset.ausd_bp_ta_cntrct_evn` and pass parameters.
*   IAM roles and permissions for BigQuery access.