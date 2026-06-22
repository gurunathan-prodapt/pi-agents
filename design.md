# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

## 1. Purpose & Scope
This migration targets an ETL workflow responsible for the reconciliation and aggregation of contract discount data, populating the `sof$ta_disc_zusgf` table. The workflow is primarily composed of an Oracle PL/SQL script that performs the core data transformation, orchestrated by two KornShell scripts.

The scope of this migration includes:
- Converting the KornShell orchestration logic to a BigQuery-compatible stored procedure or an Airflow DAG.
- Translating the Oracle PL/SQL script, including its object types, package, and pipelined table function, into BigQuery SQL, leveraging BigQuery's `STRING_AGG` and `WITH` clause capabilities.
- Identifying and addressing external system dependencies, specifically the Oracle database and potential DB-links.
- Replicating logging and error handling mechanisms within the BigQuery environment.

The overall job is considered to have "medium" complexity.

## 2. Source Inventory

### 2.1. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`
- **Technology**: KornShell
- **Category**: Shell Script
- **Tool**: KornShell
- **Purpose**: Wrapper/orchestration script for contract data reconciliation. Handles environment initialization, parameter parsing, job/log metadata creation, error trapping and logging, and status marking. Its primary function is to invoke the core script `k_ausd_v_ta_disc_zusgf.ksh`.
- **Complexity Tier**: Medium
- **Migration Automation Bucket**: Semi-auto (B2)

### 2.2. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`
- **Technology**: KornShell
- **Category**: Shell Script
- **Tool**: KornShell
- **Purpose**: Control script that manages job execution. It receives parameters from the wrapper script, performs additional parameter validation, and crucially invokes the Oracle SQL script `d_ausd_v_ta_disc_zusgf.sql`. It also handles job registration and deactivation of older active jobs within a job control mechanism.
- **Complexity Tier**: Medium
- **Migration Automation Bucket**: Semi-auto (B2)

### 2.3. `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql`
- **Technology**: Oracle PL/SQL
- **Category**: SQL
- **Tool**: Oracle PL/SQL
- **Purpose**: This script defines custom Oracle object types (`sof$ty_o_discount`, `sof$ty_t_discount`) and a PL/SQL package (`sof$sp_discount_functions`) containing a pipelined table function (`concat_discounts`). This function concatenates discount information for specific contract IDs and versions. The script then truncates and populates the target table `sof$ta_disc_zusgf` by reading from `sof$ta_discount` and `isbert_schema.dwtk_meldungen`, utilizing the custom pipelined function for discount aggregation.
- **Complexity Tier**: Medium
- **Migration Automation Bucket**: Semi-auto (B2)

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data storage and SQL processing, with an optional orchestration layer such as Cloud Composer (Airflow) for scheduling and managing the workflow.

- **BigQuery Datasets**: Dedicated datasets will be created for source tables, staging tables, and target tables. For this job, a dataset (e.g., `isbert_ds`) will host the migrated tables.
- **BigQuery Tables**:
    - `isbert_ds.dwtk_meldungen` (migrated source table)
    - `isbert_ds.sof_ta_discount` (migrated source table)
    - `isbert_ds.sof_ta_disc_zusgf` (target table, to be created by the migration)
    - `isbert_ds.job_control` (BigQuery table for job status and logging, replacing shell-based logging)
    - `isbert_ds.error_log` (BigQuery table for detailed error logging)
    - `isbert_ds.job_message_log` (BigQuery table for general job messages)
    - `isbert_ds.job_result_log` (BigQuery table to store record counts and results)
- **BigQuery Stored Procedures**:
    - `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` (replaces `r_ausd_v_ta_disc_zusgf.ksh`)
    - `isbert_ds.k_ausd_v_ta_disc_zusgf_controller` (replaces `k_ausd_v_ta_disc_zusgf.ksh`)
    - `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` (replaces `d_ausd_v_ta_disc_zusgf.sql`)
- **Orchestration**: If cross-job dependencies or complex scheduling are required, an Airflow DAG in Cloud Composer can be used to sequence the execution of the BigQuery stored procedures.

## 4. Data Flow & Lineage

The current legacy data flow is as follows:

1.  **`r_ausd_v_ta_disc_zusgf.ksh` (Wrapper Script)**
    *   **Invokes**: `k_ausd_v_ta_disc_zusgf.ksh`
    *   **Purpose**: Environment setup, parameter parsing (limited for itself), logging initialization, and calling the main controller script.

2.  **`k_ausd_v_ta_disc_zusgf.ksh` (Controller Script)**
    *   **Invokes**: `d_ausd_v_ta_disc_zusgf.sql`
    *   **Reads**: Parameters passed from the wrapper (`JobKennung`, `EintragsNr`). Implicitly reads environment variables set by `$HOME/.dw_init`. Reads temporary file for record count.
    *   **Writes**: Updates job control tables (implicitly via `starteSQLSkript` helper function), writes to a temporary file.
    *   **Purpose**: Validates parameters, manages job status, and executes the core Oracle SQL logic.

3.  **`d_ausd_v_ta_disc_zusgf.sql` (Oracle PL/SQL Script)**
    *   **Reads**:
        *   `isbert_schema.dwtk_meldungen` (to determine `v_datum`)
        *   `sof$ta_discount` (main source for contract and discount details, including `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, `rabatt`, `rabatthoehe`)
    *   **Writes**:
        *   `sof$ta_disc_zusgf` (truncates and inserts aggregated discount data)
    *   **Purpose**: Defines Oracle object types and a pipelined table function (`concat_discounts`) to concatenate discount descriptions. It then uses this function to populate `sof$ta_disc_zusgf` with processed contract discount data.

**Target Data Flow in BigQuery:**

The migrated flow will consist of chained BigQuery stored procedures, potentially orchestrated by an external scheduler:

1.  **`CALL isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper(p_h, p_s, p_l)`**:
    *   Acts as the entry point.
    *   Handles top-level parameter validation and logging setup, similar to the original wrapper.
    *   **Invokes**: `isbert_ds.k_ausd_v_ta_disc_zusgf_controller` with relevant parameters.
    *   **Writes**: `isbert_ds.job_control`, `isbert_ds.job_message_log`, `isbert_ds.error_log`.

2.  **`CALL isbert_ds.k_ausd_v_ta_disc_zusgf_controller(p_JobKennung, p_EintragsNr)`**:
    *   Receives job parameters.
    *   Updates `isbert_ds.job_control` for job registration and deactivation of older active jobs.
    *   **Invokes**: `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` with necessary parameters.
    *   **Reads**: `isbert_ds.job_control` (for checking and updating job status). Reads `isbert_ds.sof_ta_disc_zusgf` for record count.
    *   **Writes**: `isbert_ds.job_control`, `isbert_ds.job_result_log`.

3.  **`CALL isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic()`**:
    *   **Reads**:
        *   `isbert_ds.dwtk_meldungen`
        *   `isbert_ds.sof_ta_discount`
    *   **Writes**:
        *   `isbert_ds.sof_ta_disc_zusgf` (truncates and inserts data)
    *   **Purpose**: Performs the core data transformation.

## 5. Transformation Logic

### 5.1. `r_ausd_v_ta_disc_zusgf.ksh` (Wrapper) to `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` (BigQuery Stored Procedure)
-   **Parameter Handling**: `getopts` replaced by BigQuery stored procedure parameters.
-   **Environment Setup (`. $HOME/.dw_init`)**: The equivalent environment variables will be passed as procedure parameters or fetched from a BigQuery configuration table/UDFs.
-   **Logging (`DWMSG_*`)**: Replaced by `INSERT` statements into BigQuery logging tables (`isbert_ds.job_control`, `isbert_ds.job_message_log`, `isbert_ds.error_log`).
-   **Error Handling (`trap`)**: Replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block for robust error management within the stored procedure.
-   **Core Script Invocation (`${Name_Kernskript}`)**: Replaced by a `CALL` statement to the `isbert_ds.k_ausd_v_ta_disc_zusgf_controller` BigQuery stored procedure.

### 5.2. `k_ausd_v_ta_disc_zusgf.ksh` (Controller) to `isbert_ds.k_ausd_v_ta_disc_zusgf_controller` (BigQuery Stored Procedure)
-   **Parameter Handling**: `getopts` logic replaced by BigQuery stored procedure parameters (`p_JobKennung`, `p_EintragsNr`).
-   **Parameter Validation**: Shell `if` conditions and `pruefeParameterGesetzt` replaced by BigQuery `IF` statements and `SIGNAL SQLSTATE` for error handling.
-   **Job Control (`h_alis_job.ksh`)**: Functionality for activating/deactivating jobs and checking for active jobs will be translated into `UPDATE` and `INSERT` statements against a BigQuery `isbert_ds.job_control` table.
-   **SQL Script Execution (`starteSQLSkript`)**: Replaced by a `CALL` statement to the `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` BigQuery stored procedure.
-   **Temporary File (`tmpFile`) and Record Count**: The temporary file for record count will be replaced by directly querying the target table (`isbert_ds.sof_ta_disc_zusgf`) using `SELECT COUNT(*)` and storing the result in a BigQuery variable, then logging it to `isbert_ds.job_result_log`.

### 5.3. `d_ausd_v_ta_disc_zusgf.sql` (Oracle PL/SQL) to `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` (BigQuery Stored Procedure)
-   **Variable Definitions (`DEFINE`)**: `v_carmen` and `v_datum` will be handled as BigQuery `DECLARE` variables or procedure parameters. The date calculation from `isbert_schema.dwtk_meldungen` will be translated to BigQuery SQL.
-   **Tracing and Settings (`START ../trace.sql.cfg`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING`, `SET SERVEROUTPUT`)**: These Oracle-specific commands will be removed. BigQuery provides its own execution logs, and tracing/profiling are handled by BigQuery's query plans and Cloud Logging. Error handling will be managed by the calling stored procedure's `EXCEPTION` blocks.
-   **Oracle Object Types (`CREATE TYPE ... AS OBJECT`, `CREATE TYPE ... AS TABLE OF`)**: These will not be directly migrated. BigQuery handles complex data types differently.
-   **Oracle PL/SQL Package with Pipelined Table Function (`sof$sp_discount_functions.concat_discounts`)**: This complex logic, which concatenates discount strings based on contract ID and version, will be replaced by BigQuery's `STRING_AGG` aggregate function in combination with subqueries or CTEs. The `PIPELINED` and `PARALLEL_ENABLE` hints are Oracle-specific and will be removed, as BigQuery automatically optimizes query execution.
-   **Truncation (`TRUNCATE TABLE sof$ta_disc_zusgf`)**: This will be replaced by BigQuery's `TRUNCATE TABLE` statement or by recreating the table if using `CREATE OR REPLACE TABLE AS SELECT ...`.
-   **`INSERT ... SELECT`**: The core `INSERT` statement will be translated directly to BigQuery SQL, with specific attention to:
    -   Oracle's `(+)` outer join syntax converted to `LEFT JOIN`.
    -   Casting `NUMBER` types to `INT64` and `VARCHAR2` types to `STRING`.
    -   Handling `rabatt||' ('||rabatthoehe||'%)'` concatenation using BigQuery's `CONCAT` function.
    -   Oracle `PARALLEL` hints will be removed, as BigQuery manages parallelism automatically.
-   **`COMMIT`**: BigQuery DML operations are transactional by default within a single statement, or managed through BigQuery scripting transactions if multiple DMLs are involved. An explicit `COMMIT` is not typically needed or directly translatable.
-   **`ANALYZE TABLE`**: Oracle-specific command for statistics collection, not needed in BigQuery, which automatically manages table statistics.

## 6. External Dependencies
The source system relies on an Oracle Database.
-   **Oracle Database (`isbert_schema.dwtk_meldungen`, `sof$ta_discount`, `sof$ta_disc_zusgf`)**: These tables currently reside in an Oracle database.
    -   **Replacement**: All source and target tables will be migrated to Google BigQuery. `isbert_schema.dwtk_meldungen` will become `isbert_ds.dwtk_meldungen`, `sof$ta_discount` will become `isbert_ds.sof_ta_discount`, and `sof$ta_disc_zusgf` will become `isbert_ds.sof_ta_disc_zusgf`. Data will need to be ingested into BigQuery via a data transfer service (e.g., Cloud Data Fusion, DMS, or custom ETL).
-   **DB-Link (`@pcrs1`)**: The `DEFINE v_carmen = "@pcrs1"` suggests a potential DB-link to a CARMEN database.
    -   **Replacement**: If `sof$ta_discount` or `isbert_schema.dwtk_meldungen` are sourced from this linked database, then data from the CARMEN database will also need to be migrated or ingested into BigQuery. This could involve direct ingestion of CARMEN data or establishing a federated query if real-time access to an external system is critical and feasible. For this migration, it's assumed the data will be landed in BigQuery.

## 7. Unresolved / Risks
-   **Exact Data Type Mismatch**: While `NUMBER(10)` maps well to `INT64`, precise `VARCHAR2(500)` to `STRING` mapping needs to ensure length constraints are respected in BigQuery, potentially using `QUALIFY ROW_NUMBER()` or similar for truncation if source data exceeds limits.
-   **Oracle Helper Scripts**: The content of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` are not fully known. Their functionalities (e.g., custom error codes, date manipulations, SQL*Plus settings) need to be fully understood and replicated as BigQuery UDFs, stored procedures, or part of the orchestration logic.
-   **Job Control Logic**: The `h_alis_job.ksh` and the "ignore active jobs" / "deactivate old jobs" logic needs careful migration to BigQuery control tables to ensure consistent job state management.
-   **Performance Considerations**: The Oracle script uses `PARALLEL` hints. While BigQuery handles parallelism automatically, performance tuning might be required for the BigQuery SQL to match or exceed the legacy system's performance, especially for large datasets.
-   **`BERT_DROP_TEMP_TABLE` Logic**: The exact function of `isbert_schema.dwtk_meldungen WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'` in determining `v_datum` needs to be confirmed and accurately replicated.

## 8. Build Plan

The migration will be implemented by creating BigQuery stored procedures and tables.

1.  **Create Target Tables in BigQuery**:
    *   `isbert_ds.sof_ta_disc_zusgf` (target table)
    *   `isbert_ds.job_control` (logging/control table)
    *   `isbert_ds.error_log` (logging table)
    *   `isbert_ds.job_message_log` (logging table)
    *   `isbert_ds.job_result_log` (logging table)
    *   **(Language: DDL)**

2.  **Ingest Source Data**:
    *   Migrate data from `isbert_schema.dwtk_meldungen` to `isbert_ds.dwtk_meldungen`.
    *   Migrate data from `sof$ta_discount` to `isbert_ds.sof_ta_discount`.
    *   **(Tool: Data Migration Service, Cloud Data Fusion, or custom ETL)**

3.  **Create BigQuery Stored Procedure for Core SQL Logic**:
    *   Create `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic` from the translated Oracle PL/SQL, replacing object types and pipelined function with BigQuery SQL constructs (e.g., `STRING_AGG`, CTEs).
    *   **(Language: BigQuery SQL)**

4.  **Create BigQuery Stored Procedure for Controller Script**:
    *   Create `isbert_ds.k_ausd_v_ta_disc_zusgf_controller` from the translated KornShell logic, handling parameters, job control table updates, and calling `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`.
    *   **(Language: BigQuery Scripting / SQL)**

5.  **Create BigQuery Stored Procedure for Wrapper Script**:
    *   Create `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper` from the translated KornShell logic, handling top-level parameters, logging, and calling `isbert_ds.k_ausd_v_ta_disc_zusgf_controller`.
    *   **(Language: BigQuery Scripting / SQL)**

6.  **Create Orchestration (Optional but Recommended)**:
    *   If complex scheduling or external dependencies exist, create an Airflow DAG in Cloud Composer to orchestrate the call to `isbert_ds.r_ausd_v_ta_disc_zusgf_wrapper`.
    *   **(Language: Python (Airflow DAG))**