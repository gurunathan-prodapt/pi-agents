# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope

The purpose of this job, `k_ausd_v_ta_barrier_zusgf.ksh`, is to act as a control script for `r_ausd_vertrag.ksh`. Its primary functions include:
- Ignoring jobs that are already active.
- Invoking an SQL script (`d_ausd_v_ta_barrier_zusgf.sql`) for data processing.
- Registering job execution entries in a job table.
- Deactivating older active jobs.
The script handles parameter reading, validation, execution of the core SQL logic, and retrieval of a record count from a temporary file.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh`
- **Technology:** KornShell (shell script)
- **Category:** shell
- **Complexity Tier:** medium
- **Automation Bucket:** semi_auto
- **Summary:** This is a control script for `r_ausd_vertrag.ksh`, responsible for managing job execution, calling an SQL script for data processing, and handling job entries.
- **Purpose:** ETL

## 3. Target Architecture

The target architecture in Google BigQuery will replace the existing KornShell script with a BigQuery Stored Procedure, orchestrating the data processing.

- **BigQuery Stored Procedure:**
    - `project.dataset.proc_ausd_v_ta_barrier_zusgf`: This main stored procedure will encapsulate the control flow, parameter validation, job management logic (activating/deactivating jobs), and invocation of the core data transformation logic.
    - `project.dataset.proc_d_ausd_v_ta_barrier_zusgf`: This stored procedure will house the migrated SQL logic from `d_ausd_v_ta_barrier_zusgf.sql`.
- **BigQuery Tables:**
    - `project.dataset.job_error_log`: For logging error messages.
    - `project.dataset.job_table`: To manage job activation/deactivation status.
    - `project.dataset.target_result_table`: The table where the `proc_d_ausd_v_ta_barrier_zusgf` would write its results, from which the record count is derived.
    - `project.dataset.job_run_log`: For logging successful job runs and record counts.
- **Orchestration (Optional):** Cloud Composer (Airflow) or Cloud Run could be used to trigger the main BigQuery Stored Procedure, passing the necessary parameters.

## 4. Data Flow & Lineage

The original script's lineage involves execution control and invocation of an SQL script. The `lineage_edges` query returned no explicit edges for this file, so the following is inferred from code analysis.

**Original Flow:**
1. **Initialization:** The `k_ausd_v_ta_barrier_zusgf.ksh` script starts, sources environment variables (`$HOME/.dw_init`) and several utility KornShell scripts for error handling, date functions, parameter parsing, and SQL*Plus interaction.
2. **Parameter Processing:** Command-line parameters `j` (JobKennung) and `f` (EintragsNr) are parsed and validated.
3. **Job Management:** The script implicitly interacts with a job control mechanism, likely via the `starteSQLSkript` function, to manage active jobs and deactivate old ones.
4. **SQL Execution:** The script invokes `d_ausd_v_ta_barrier_zusgf.sql` via `starteSQLSkript`, which is responsible for the core data manipulation in a database (likely Oracle given the `sqlplus` utility).
5. **Record Count:** After SQL execution, a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_zusgf_$$.tmp`) is read to retrieve the count of processed records. This count is assigned to `v_records`.

**Target BigQuery Flow:**
1. **Orchestration (e.g., Cloud Composer):** Triggers `proc_ausd_v_ta_barrier_zusgf` with `p_JobKennung` and `p_EintragsNr` parameters.
2. **`proc_ausd_v_ta_barrier_zusgf` execution:**
    a. **Parameter Validation:** Validates `p_JobKennung` and `p_EintragsNr`. Logs errors to `job_error_log` if validation fails and exits.
    b. **Job Management:** Updates `job_table` to deactivate older active jobs and marks the current job as active.
    c. **SQL Logic Invocation:** Calls `proc_d_ausd_v_ta_barrier_zusgf` to perform the main data processing. `proc_d_ausd_v_ta_barrier_zusgf` will transform and load data into `target_result_table`.
    d. **Record Count:** Queries `target_result_table` to get the count of records processed by `proc_d_ausd_v_ta_barrier_zusgf`.
    e. **Logging:** Inserts job run details and record count into `job_run_log`.
    f. **Output:** Returns a success message and the processed record count.

## 5. Transformation Logic

The KornShell script itself acts as an orchestrator and parameter handler. The core transformation logic is expected to reside within the SQL script it calls (`d_ausd_v_ta_barrier_zusgf.sql`).

**Key Transformations in `k_ausd_v_ta_barrier_zusgf.ksh` (and their BigQuery equivalents):**
- **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`).
- **Parameter Validation (`pruefeParameterGesetzt`):** Replaced by `IF` conditions and `ASSERT` statements within the BigQuery Stored Procedure. Error logging will be directed to `job_error_log`.
- **Job Control (activating/deactivating jobs):** Replaced by `UPDATE` and `MERGE` statements on a BigQuery `job_table` to manage job status based on `p_JobKennung` and `p_EintragsNr`.
- **SQL Script Execution (`starteSQLSkript`):** Replaced by a `CALL` statement to `proc_d_ausd_v_ta_barrier_zusgf`, which will contain the migrated logic of the original SQL script.
- **Record Count Retrieval (from `tmpFile`):** Replaced by `SELECT COUNT(*)` from the BigQuery target table (`target_result_table`) where the SQL processing logic deposits its results. This count will then be logged to `job_run_log`.
- **Environment Sourcing (`. $HOME/.dw_init`, etc.):** The equivalent environment variables and utility functions will be replaced by direct declarations within the BigQuery Stored Procedure, or by parameters passed during orchestration.
- **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`):** Replaced by BigQuery's error handling constructs like `RAISE USING MESSAGE` or logging to a dedicated `job_error_log` table.

The actual data transformation logic within `d_ausd_v_ta_barrier_zusgf.sql` needs to be separately analyzed and converted into BigQuery SQL, potentially as another stored procedure or a series of SQL statements within `proc_d_ausd_v_ta_barrier_zusgf`. The target table name for the main processing logic is inferred to be `ta_barrier_zusgf`.

## 6. External Dependencies

Based on the analysis, there were no external systems explicitly identified in the `lineage_assembled_jobs` output for this job. However, the script itself indicates implicit dependencies:

- **Legacy Database (implicit in `starteSQLSkript` and `.sql` extension):** The original script interacts with a database (likely Oracle) through `sqlplus` or a similar utility (`h_alis_sqlplus.ksh`).
    - **Replacement:** This will be replaced by native BigQuery SQL operations within BigQuery Stored Procedures.
- **Filesystem-based Temporary Files (`tmpFile`):** The script uses a temporary file to pass the record count.
    - **Replacement:** This will be replaced by BigQuery's internal variable handling, `OUT` parameters for stored procedures, or by querying the resulting BigQuery table directly for the record count.
- **Shell Utilities:** The script sources several utility KornShell scripts.
    - **Replacement:** These functionalities (error handling, date operations, parameter parsing) will be directly implemented in BigQuery SQL using native functions or embedded within the stored procedure logic. If complex, they could be migrated to Python UDFs, but simple validation and date functions are well-supported in BigQuery SQL.

## 7. Unresolved / Risks

- **Unresolved Targets:** None explicitly identified by lineage analysis.
- **Complexity of `d_ausd_v_ta_barrier_zusgf.sql`:** The actual business logic resides in the SQL script. Its complexity, usage of specific database features (e.g., PL/SQL stored procedures, vendor-specific functions), and data sources/targets will dictate the migration complexity of `proc_d_ausd_v_ta_barrier_zusgf`. This SQL script requires a separate, detailed migration analysis.
- **Job Table Schema:** The schema and exact logic of the `job_table` (and any related tables for job management) are not fully detailed in the provided script. A clear understanding of this will be critical for accurate BigQuery table and stored procedure implementation.
- **`DW_DIR_UTL` variable:** The exact path and purpose of `$DW_DIR_UTL` for temporary files will need to be replaced with a BigQuery-native approach.
- **`BERT_DIR_ROOT` variable:** All paths using this variable will need to be mapped to the corresponding BigQuery datasets and table names or other BigQuery resources.
- **`r_ausd_vertrag.ksh` relationship:** While `k_ausd_v_ta_barrier_zusgf.ksh` is a control script *for* `r_ausd_vertrag.ksh`, the exact nature of this control and its implications for migration need to be fully understood. It might suggest `k_ausd_v_ta_barrier_zusgf.ksh` is a sub-component of a larger workflow.

## 8. Build Plan

The migration involves converting the KornShell script into a BigQuery Stored Procedure, and the called SQL script into another BigQuery Stored Procedure.

1. **Design and Create BigQuery Schemas and Tables:**
    - `job_error_log` (BigQuery Table)
    - `job_table` (BigQuery Table)
    - `target_result_table` (BigQuery Table - schema needs to be derived from `d_ausd_v_ta_barrier_zusgf.sql`)
    - `job_run_log` (BigQuery Table)
    - Language: BigQuery DDL (SQL)

2. **Migrate `d_ausd_v_ta_barrier_zusgf.sql` to BigQuery Stored Procedure:**
    - Analyze `d_ausd_v_ta_barrier_zusgf.sql` to identify its exact logic, input/output tables, and transformations.
    - Convert this SQL into `proc_d_ausd_v_ta_barrier_zusgf`.
    - Language: BigQuery Stored Procedure (SQL)

3. **Migrate `k_ausd_v_ta_barrier_zusgf.ksh` to BigQuery Stored Procedure:**
    - Implement parameter handling, validation, and error logging using BigQuery SQL.
    - Implement job activation/deactivation logic using `UPDATE` and `MERGE` on `job_table`.
    - Call `proc_d_ausd_v_ta_barrier_zusgf` for the core data processing.
    - Implement record count retrieval and logging.
    - Output: `proc_ausd_v_ta_barrier_zusgf`
    - Language: BigQuery Stored Procedure (SQL)

4. **Develop Orchestration (Optional but Recommended):**
    - Create a Cloud Composer DAG or Cloud Run service to trigger `proc_ausd_v_ta_barrier_zusgf` with dynamic parameters as needed.
    - Language: Python (for Airflow DAGs) or other appropriate orchestration technology.