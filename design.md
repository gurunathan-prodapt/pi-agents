# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `k_ausd_v_ta_p_vertrag.ksh`. The primary purpose of this script is to orchestrate the execution of a SQL script (`d_ausd_v_ta_p_vertrag.sql`) responsible for processing contract data. This involves:
*   Ignoring currently active jobs to prevent conflicts.
*   Invoking the SQL script with necessary parameters.
*   Registering and updating job execution status in a job management table.
*   Deactivating older active jobs.
*   Handling parameters and reporting errors.
The migration aims to re-implement this functionality within Google Cloud's BigQuery ecosystem, leveraging BigQuery Stored Procedures for the orchestration logic and BigQuery SQL for data transformations.

## 2. Source Inventory
The job consists of a single KornShell script with several sourced helper scripts and an invoked SQL script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
    *   **Technology:** KornShell
    *   **Summary:** KornShell script for controlling the execution of a SQL script (`d_ausd_v_ta_p_vertrag.sql`) to process contract data, including parameter parsing, error handling, and job status management.
    *   **Complexity Tier:** Unknown (no data from `file_complexity` table, assume simple for orchestration)
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The legacy KornShell script and its dependencies will be migrated to BigQuery components:

*   **Orchestration Logic:** The parameter parsing, error handling, job status management, and SQL script invocation logic from `k_ausd_v_ta_p_vertrag.ksh` will be re-implemented as a BigQuery Stored Procedure, named `project.dataset.r_ausd_vertrag`.
*   **Data Transformation Logic:** The core data processing logic from `d_ausd_v_ta_p_vertrag.sql` will be converted into a separate BigQuery Stored Procedure or a series of SQL statements/views, designed to be invoked by the main orchestration stored procedure. It will operate on BigQuery tables.
*   **Job Management:** A BigQuery control table (`project.dataset.job_table`) will store job execution status, replacing the implicit job tracking in the shell environment and any legacy job table interactions.
*   **Error Logging:** A BigQuery error log table (`project.dataset.error_log`) will capture and store error messages, replacing the shell script's `DWMSG_MeldeFehler` mechanism.
*   **Temporary Data:** The temporary file usage (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp`) for record counting will be replaced by BigQuery scripting variables (`DECLARE`) and direct `COUNT(*)` queries on the target tables.
*   **Helper Utilities:** The functionality of the sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be either integrated into the main stored procedure, replaced by standard BigQuery SQL functions, or refactored into separate BigQuery user-defined functions (UDFs) or stored procedures if their logic is complex and reusable.
*   **Scheduling:** The migrated BigQuery Stored Procedure will be scheduled using Google Cloud Composer (Airflow) or Cloud Workflows to maintain the existing job scheduling patterns.

## 4. Data Flow & Lineage
The original data flow involves the KornShell script orchestrating the execution of a SQL script, which reads from source tables and writes to target tables.

**Legacy Flow:**
1.  `k_ausd_v_ta_p_vertrag.ksh` (KornShell)
    *   Reads parameters (`p_JobKennung`, `p_EintragsNr`).
    *   Sourced scripts: `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`
    *   Executes `d_ausd_v_ta_p_vertrag.sql` via `starteSQLSkript` (from `h_alis_sqlplus.ksh`).
    *   Reads record count from temporary file.
2.  `d_ausd_v_ta_p_vertrag.sql` (SQL script, likely Oracle)
    *   Reads data from `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_VERTRAG_TMP`.
    *   Writes data to `TABLE:SOF$TA_P_VERTRAG`, `TABLE:VIA`.
    *   Uses `PACKAGE:DWPA_UTIL_SKRIPT`, `PACKAGE:PV`.

**Migrated BigQuery Flow:**
1.  **Cloud Scheduler/Composer/Workflows:** Triggers the main BigQuery Stored Procedure.
2.  **`project.dataset.r_ausd_vertrag` (BigQuery Stored Procedure):**
    *   Receives `p_JobKennung` and `p_EintragsNr` as input parameters.
    *   Performs parameter validation and error logging to `project.dataset.error_log`.
    *   Updates job status in `project.dataset.job_table`.
    *   Calls `project.dataset.d_ausd_v_ta_p_vertrag` (BigQuery Stored Procedure) for data transformation.
    *   Queries `project.dataset.output_table` (or equivalent target table) to get record count.
    *   Records output metrics.
3.  **`project.dataset.d_ausd_v_ta_p_vertrag` (BigQuery Stored Procedure):**
    *   Reads from migrated BigQuery tables (e.g., `project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_VERTRAG_TMP`).
    *   Performs data transformations and writes to migrated BigQuery tables (e.g., `project.dataset.SOF_TA_P_VERTRAG`, `project.dataset.VIA`).
    *   Replaces legacy package calls (DWPA_UTIL_SKRIPT, PV) with equivalent BigQuery SQL logic or UDFs.

## 5. Transformation Logic

**`k_ausd_v_ta_p_vertrag.ksh` (Orchestration):**
*   **Parameter Handling:** The `getopts` logic will be replaced by named parameters for the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag`.
*   **Environment Initialization (`. $HOME/.dw_init`):** This will be replaced by explicit variable declarations within the BigQuery Stored Procedure, or by configuration managed outside the procedure (e.g., in Composer environment variables).
*   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`):** Will be re-implemented using `IF` conditions, `ASSERT`, `RAISE`, and `INSERT` statements into a dedicated BigQuery error logging table (`project.dataset.error_log`).
*   **Job Management:** `UPDATE` and `INSERT` statements against `project.dataset.job_table` will handle job status updates, activation, and deactivation.
*   **SQL Script Execution (`starteSQLSkript`):** The invocation of `d_ausd_v_ta_p_vertrag.sql` will be a direct `CALL` to the corresponding BigQuery Stored Procedure `project.dataset.d_ausd_v_ta_p_vertrag`.
*   **Temporary File Record Count:** The `eval "v_records=`cat $tmpFile`"` will be replaced by a `DECLARE` statement and a `SET` assignment using `SELECT COUNT(*)` from the target table after the data transformation.

**`d_ausd_v_ta_p_vertrag.sql` (Data Transformation):**
*   **SQL Dialect Conversion:** The SQL content will need to be converted from its current dialect (likely Oracle SQL, given `DUAL` and `PACKAGE` usage) to BigQuery Standard SQL. This includes:
    *   Data types conversion.
    *   Function mapping (e.g., `STR_TO_DATE` to `PARSE_DATE`).
    *   `MERGE` statements syntax adjustment.
*   **Table References:** All table references (e.g., `DWTK_MELDUNGEN`, `SOF$TA_VERTRAG_TMP`, `SOF$TA_P_VERTRAG`, `VIA`) will be updated to their fully qualified BigQuery names (e.g., `project.dataset.DWTK_MELDUNGEN`).
*   **Package Calls:** Calls to `DWPA_UTIL_SKRIPT.runstatement` and `pv.vertrag_id_carmen` will need to be analyzed. If they perform simple SQL operations, they can be inlined or converted to BigQuery UDFs. If they encapsulate complex procedural logic, they will be migrated to BigQuery Stored Procedures.

## 6. External Dependencies
The current job does not explicitly list external systems in the `external_systems` field. However, based on the code analysis:

*   **Oracle Database (Inferred):** The presence of `DUAL` and package calls in the SQL script strongly suggests an Oracle database as the source/target for the `d_ausd_v_ta_p_vertrag.sql` script.
    *   **Replacement:** All Oracle tables and packages will be migrated to BigQuery tables and BigQuery Stored Procedures/UDFs, respectively. Data will be ingested into BigQuery using appropriate data migration services (e.g., Database Migration Service, Dataflow, or custom ETL).
*   **Filesystem (`$HOME/.dw_init`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** The script relies on environment variables and filesystem paths for configuration and temporary files.
    *   **Replacement:** Environment variables and configuration paths will be replaced by BigQuery Stored Procedure parameters, BigQuery constants, or external configuration management (e.g., Cloud Secret Manager, Composer environment variables). Temporary file usage will be replaced by BigQuery scripting variables or temporary tables.

## 7. Unresolved / Risks
*   **Complexity of `d_ausd_v_ta_p_vertrag.sql`:** The actual complexity of the SQL script is unknown without its content. A thorough analysis is needed to accurately estimate the effort for BigQuery SQL conversion and package migration.
*   **Logic within Sourced Helper Scripts:** The content and full functionality of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` are not fully known. Any non-trivial logic within these scripts will require separate migration into BigQuery UDFs or Stored Procedures. The `starteSQLSkript` function in `h_alis_sqlplus.ksh` in particular might contain complex error handling or connection logic that needs careful re-implementation.
*   **"Deactivate older active jobs":** The exact mechanism and business rules for deactivating older active jobs are implicitly handled by the shell script. This logic needs to be fully understood and precisely replicated in BigQuery SQL, possibly involving status columns and timestamp comparisons in the `job_table`.
*   **`v_TabName='ta_p_vertrag'`:** This variable is set but its full usage is not explicitly clear from the provided shell script fragment, though it is likely used in job management. Its role needs to be fully mapped during migration.
*   **Data Volume and Performance:** The performance characteristics of the migrated BigQuery SQL will need to be thoroughly tested, especially for large data volumes that `SOF$TA_VERTRAG_TMP` or other tables might represent.

## 8. Build Plan

1.  **Define BigQuery Schema:**
    *   Create `project.dataset.job_table` (schema: `job_kennung STRING, eintrags_nr STRING, tab_name STRING, status STRING, created_at TIMESTAMP, updated_at TIMESTAMP`).
    *   Create `project.dataset.error_log` (schema: `error_timestamp TIMESTAMP, procedure_name STRING, error_message STRING, job_kennung STRING, eintrags_nr STRING`).
    *   Create target tables corresponding to `SOF$TA_P_VERTRAG`, `VIA`, `DWTK_MELDUNGEN`, `SOF$TA_VERTRAG_TMP` in BigQuery.
    *   **Language:** BigQuery DDL
2.  **Migrate `d_ausd_v_ta_p_vertrag.sql`:**
    *   Convert the Oracle SQL to BigQuery Standard SQL, including data type mapping, function replacement, and `MERGE` syntax.
    *   Replace package calls (`DWPA_UTIL_SKRIPT`, `PV`) with equivalent BigQuery logic or UDFs/Stored Procedures.
    *   Wrap the converted SQL into a BigQuery Stored Procedure: `project.dataset.d_ausd_v_ta_p_vertrag`.
    *   **Language:** BigQuery SQL
3.  **Migrate `k_ausd_v_ta_p_vertrag.ksh` (Orchestration):**
    *   Create BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` with input parameters `p_JobKennung STRING, p_EintragsNr STRING`.
    *   Implement parameter validation and error logging.
    *   Implement job status updates (`INSERT/UPDATE` into `project.dataset.job_table`) and job deactivation logic.
    *   Add `CALL project.dataset.d_ausd_v_ta_p_vertrag(p_EintragsNr, p_JobKennung);`
    *   Implement record counting using `DECLARE` and `SET` with `SELECT COUNT(*)`.
    *   **Language:** BigQuery SQL (Stored Procedure)
4.  **Migrate Helper Script Logic (as needed):**
    *   Analyze `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` for any reusable, complex logic.
    *   Create BigQuery UDFs or additional Stored Procedures if specific functions (e.g., advanced date manipulation, custom error formatting) are required and not covered by standard BigQuery functions.
    *   **Language:** BigQuery SQL (UDFs/Stored Procedures)
5.  **Develop Scheduling Mechanism:**
    *   Create a Cloud Composer DAG or Cloud Workflow to schedule the execution of `project.dataset.r_ausd_vertrag`, passing required parameters.
    *   **Language:** Python (for Composer DAG), YAML (for Cloud Workflow)
6.  **Data Ingestion (if not already handled):**
    *   Set up a data pipeline to ingest source data from the legacy Oracle database into the newly created BigQuery tables. This is usually a prerequisite for the transformation step.
    *   **Language:** Varies (e.g., Dataflow, DMS, custom ETL)
7.  **Testing and Validation:** Thoroughly test the migrated BigQuery components, comparing results and performance against the legacy system.