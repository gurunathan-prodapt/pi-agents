# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_vvl_dwh.ksh`, acts as a control and orchestration script for a data processing job. Its primary purpose is to:
- Ignore currently active jobs to prevent conflicts.
- Parse and validate input parameters (`Jobkennung` and `EintragsNr`).
- Execute a core SQL script (`d_ausd_v_ta_vvl_dwh.sql`) responsible for the actual data transformation.
- Record the job's status and processed record counts.
- Deactivate old active jobs (though the explicit logic for deactivation is not detailed in the provided script content).

The script relies on several utility scripts for environment setup, error handling, date functions, parameter parsing, and SQL execution. The business transformation logic is primarily contained within the `d_ausd_v_ta_vvl_dwh.sql` script that this shell script invokes.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh`
  - **Technology:** KornShell
  - **Category:** Shell script
  - **Tool:** KornShell
  - **Purpose:** Orchestration, Control Script
  - **Summary:** Orchestrates the execution of a SQL script for data processing, handling job identification and parameter validation.
  - **Complexity Tier:** Not available from analysis (no rows returned from `file_complexity`).
  - **Automation Bucket:** Not available from analysis (no rows returned from `automation_rate`).

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily BigQuery for data storage and processing, and potentially Cloud Composer (Airflow) or Cloud Workflows for orchestration.

- **Orchestration:** The shell script's orchestration logic will be migrated to a BigQuery Stored Procedure, or if external dependencies become complex, an Airflow DAG on Cloud Composer.
- **Data Processing:** The SQL logic from `d_ausd_v_ta_vvl_dwh.sql` (which this script invokes) will be directly migrated to BigQuery SQL, potentially as part of the stored procedure or as separate BigQuery SQL scripts executed by the orchestrator.
- **Parameter Handling:** Command-line parameters will be replaced by BigQuery Stored Procedure parameters.
- **Logging & Monitoring:** Custom shell-based error logging (`DWMSG_MeldeFehler`) and job tracking will be replaced by BigQuery tables for logging and job status.
- **Temporary Files:** The temporary file mechanism for record counting (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_dwh_$$.tmp`) will be replaced by BigQuery variables or direct inserts into a logging/metrics table.

**BigQuery Components:**
- **Stored Procedure:** `project.dataset.r_ausd_vertrag_control` (or similar) to encapsulate the orchestration logic.
- **Target Table:** `project.dataset.target_table` (representing `ta_vvl_dwh`).
- **Job Tracking Table:** `project.dataset.job_table` to store job status, parameters, and record counts.
- **Error Log Table:** `project.dataset.job_error_log` for detailed error messages.

## 4. Data Flow & Lineage
The original script's data flow involves:
1. Sourcing environment variables and utility functions.
2. Parsing `p_JobKennung` and `p_EintragsNr` from command-line arguments.
3. Passing these parameters to the `starteSQLSkript` function.
4. `starteSQLSkript` then executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_vvl_dwh.sql` with the provided parameters.
5. The SQL script performs data transformations and likely writes to database tables.
6. The record count from the SQL script's execution is written to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_dwh_$$.tmp`).
7. The shell script reads this temporary file to retrieve `v_records`.

**Target Data Flow:**
1. **Orchestration Layer (BigQuery Stored Procedure or Airflow DAG):**
   - Receives `p_JobKennung` and `p_EintragsNr` as input parameters.
   - Performs parameter validation.
   - Logs job start into `project.dataset.job_table`.
   - Executes the core BigQuery SQL logic (migrated from `d_ausd_v_ta_vvl_dwh.sql`). This SQL logic will read from source tables and write to `project.dataset.target_table`.
   - Captures the number of processed records internally (e.g., via `ROW_COUNT()`, `COUNT(*)`, or a temporary variable).
   - Updates `project.dataset.job_table` with job completion status and record count.
   - Logs any errors to `project.dataset.job_error_log`.

## 5. Transformation Logic
The shell script itself contains primarily orchestration and control logic, not direct data transformation. The actual data transformation is delegated to the SQL script `d_ausd_v_ta_vvl_dwh.sql`.

**Shell Script Transformation to BigQuery Stored Procedure Logic:**
- **Environment Initialization (`. $HOME/.dw_init`):** Replaced by BigQuery environment configuration (e.g., project/dataset context, explicit parameters).
- **Utility Script Sourcing:**
  - `f_alis_msgerr.ksh`: Error handling will be converted to BigQuery's `ASSERT` statements, `RAISE` errors, and inserts into `project.dataset.job_error_log`.
  - `h_alis_date.ksh`: Date-related functions will be replaced by BigQuery's built-in date functions (e.g., `CURRENT_TIMESTAMP()`, `FORMAT_DATE()`).
  - `h_alis_parameter.ksh`: Parameter validation (`pruefeParameterGesetzt`) will be converted to `IF` conditions within the stored procedure.
  - `h_alis_sqlplus.ksh`: The `starteSQLSkript` function will be replaced by `EXECUTE IMMEDIATE` for dynamic SQL or direct BigQuery SQL execution within the stored procedure.
- **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`p_JobKennung`, `p_EintragsNr`).
- **Error Handling (`set +e`, `set -eu`, `ErrNr`, `ErrArg`):** Replaced by BigQuery `IF` conditions, `ASSERT`, `RAISE`, and logging to `project.dataset.job_error_log`.
- **Temporary File (`tmpFile`):** Replaced by a `DECLARE` variable in BigQuery SQL to hold the record count, or directly captured from the DML statement.
- **SQL Script Execution (`starteSQLSkript`):** The content of `d_ausd_v_ta_vvl_dwh.sql` will be inlined into the BigQuery Stored Procedure or called as a separate BigQuery script.

**SQL Script `d_ausd_v_ta_vvl_dwh.sql` Transformation:**
- This script, which is executed by the shell wrapper, will need to be fully converted from its original SQL dialect (likely Oracle PL/SQL given `sqlplus` helper) to BigQuery SQL. This will involve:
  - Syntax conversion for DDL/DML statements.
  - Function and operator mapping.
  - Handling of any procedural logic (loops, cursors) by converting them to BigQuery SQL procedural constructs or rewriting using set-based operations.
  - Optimizing for BigQuery's columnar storage and distributed query execution.

## 6. External Dependencies
- **Original Script:**
  - **Environment Files:** `$HOME/.dw_init` and various utility scripts under `${BERT_DIR_ROOT}`. These are internal system dependencies, not external systems like databases or SFTP.
  - **SQL Database:** Implied by the execution of a `.sql` script via `sqlplus` helper, likely Oracle.
- **Target System (BigQuery):**
  - **Environment Files:** These will be replaced by explicit BigQuery parameterization or runtime configuration.
  - **Oracle Database:** The data sources currently residing in Oracle (if any are read by `d_ausd_v_ta_vvl_dwh.sql`) will need to be migrated to BigQuery. This may involve:
    - **One-time data migration:** Using tools like Database Migration Service (DMS) or custom ETL to move historical data.
    - **Continuous data ingestion:** Setting up CDC (Change Data Capture) from Oracle to BigQuery using tools like Datastream.
  - **Temporary Files (local filesystem):** The temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_dwh_$$.tmp` will be eliminated and replaced by in-memory variables or logging tables in BigQuery.

There were no `external_systems` or `unresolved_targets` found in the `lineage_assembled_jobs` record for this specific job, indicating that the lineage system did not identify direct external system interactions beyond the implicit database calls handled by the SQL script.

## 7. Unresolved / Risks
- **SQL Script `d_ausd_v_ta_vvl_dwh.sql` Complexity:** The content of the SQL script invoked by this shell wrapper is unknown. If it contains complex procedural logic (PL/SQL), unsupported functions, or large numbers of transformations, its migration to BigQuery SQL will be the most significant part of this job. Without its content, the full scope of transformation logic is not clear.
- **Dependency on `r_ausd_vertrag.ksh`:** The script mentions "Kontrollscript zu r_ausd_vertrag.ksh". It's unclear if this implies `r_ausd_vertrag.ksh` is a parent script that invokes this one, or if this script is an alternative/related control script. Understanding the full job hierarchy is crucial for proper scheduling and execution order.
- **Job Deactivation Logic:** The purpose states "alte aktive Jobs werden einfach dekativiert" (old active jobs are simply deactivated). The explicit code for this deactivation is not visible in the provided shell script. This logic needs to be identified and migrated.
- **Error Reporting (`DWMSG_MeldeFehler`):** This is a custom function. Its exact implementation and the logging system it interacts with need to be understood to ensure equivalent functionality in BigQuery.
- **Unidentified Utility Scripts:** The functionality within the sourced utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) needs to be fully understood to accurately re-implement their capabilities in BigQuery.
- **No Complexity/Automation Rate:** The absence of `file_complexity` and `automation_rate` data suggests a lack of prior analysis for this file, increasing the risk of underestimating migration effort.

## 8. Build Plan
The migration will involve two main components: the orchestration logic (from the shell script) and the data transformation logic (from the SQL script).

**Phase 1: Analysis and Design (Current State)**
- Review `d_ausd_v_ta_vvl_dwh.sql` to understand its full transformation logic and identify any proprietary SQL constructs or procedural elements.
- Analyze the utility scripts sourced by `k_ausd_v_ta_vvl_dwh.ksh` to document their functionality and dependencies.
- Document any database objects (tables, views, sequences, etc.) that `d_ausd_v_ta_vvl_dwh.sql` reads from or writes to.
- Identify the logic for deactivating old active jobs.

**Phase 2: Migration Implementation**

1.  **BigQuery Schema Creation (SQL DDL):**
    *   Create `project.dataset.job_table` with columns like `job_kennung`, `eintrags_nr`, `tab_name`, `status`, `record_count`, `created_ts`, `finished_ts`.
    *   Create `project.dataset.job_error_log` with columns like `job_kennung`, `eintrags_nr`, `err_nr`, `err_arg`, `created_ts`, `message`.
    *   Create/verify `project.dataset.target_table` (representing `ta_vvl_dwh`) and any source tables.

2.  **Migrate Core SQL Logic (BigQuery SQL):**
    *   Translate `d_ausd_v_ta_vvl_dwh.sql` to BigQuery SQL. This may result in one or more BigQuery SQL scripts or statements.

3.  **Migrate Orchestration Logic (BigQuery Stored Procedure / Python Airflow DAG):**
    *   **Option A: BigQuery Stored Procedure (Recommended for simplicity):**
        *   Generate `project.dataset.r_ausd_vertrag_control.sql` (BigQuery SQL) containing the pseudocode provided in the design document.
        *   This stored procedure will:
            *   Accept `p_JobKennung` and `p_EintragsNr` as parameters.
            *   Implement parameter validation.
            *   Insert job start record into `project.dataset.job_table`.
            *   Execute the migrated core BigQuery SQL logic (from `d_ausd_v_ta_vvl_dwh.sql`).
            *   Update `project.dataset.job_table` with completion status and record count.
            *   Handle error logging to `project.dataset.job_error_log`.
    *   **Option B: Python Airflow DAG (If more complex orchestration is needed):**
        *   Generate `k_ausd_v_ta_vvl_dwh_dag.py` (Python).
        *   This DAG will contain `BigQueryOperator` tasks to:
            *   Validate parameters (can be done within the DAG or by a separate Python function).
            *   Insert job start record.
            *   Execute the migrated core BigQuery SQL script(s).
            *   Update job completion status and record count.
            *   Manage error handling and logging.

4.  **Configuration & Deployment:**
    *   Define BigQuery project, dataset, and table names.
    *   If using Airflow, configure the DAG and its schedule.
    *   Implement any necessary Cloud IAM roles and permissions.

5.  **Testing:**
    *   Unit tests for the BigQuery SQL transformation logic.
    *   Integration tests for the stored procedure or Airflow DAG, including parameter validation, job tracking, and error handling.

**Build Artifacts:**
- `target_table_ddl.sql` (BigQuery DDL for `ta_vvl_dwh` and any other tables)
- `job_table_ddl.sql` (BigQuery DDL for `job_table`)
- `job_error_log_ddl.sql` (BigQuery DDL for `job_error_log`)
- `d_ausd_v_ta_vvl_dwh_migrated.sql` (BigQuery SQL for the core transformation logic)
- `r_ausd_vertrag_control_sp.sql` (BigQuery Stored Procedure for orchestration, if chosen)
- OR `k_ausd_v_ta_vvl_dwh_dag.py` (Python for Airflow DAG, if chosen)