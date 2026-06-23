# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

## 1. Purpose & Scope

This document outlines the migration design for the `k_ausd_adressen.ksh` KornShell script and its associated components to Google Cloud's BigQuery platform.

The primary purpose of `k_ausd_adressen.ksh` is to act as a control script orchestrating the execution of an SQL script (`d_ausd_adressen.sql`) for processing address-related data. It handles environment setup, parameter parsing and validation, date validation, and the invocation of the core SQL logic. This script is intended to be invoked by another script, `r_ausd_adressen.ksh`.

The `d_ausd_adressen.sql` script (an Oracle SQL*Plus script) is responsible for preparing and loading address-related data for various business partner roles (contract partners, invoice recipients, EVN recipients, service users, regulators). It reads from source tables `cds$ta_`, `glv$ta_`, and `bpd$ta_`, performs joins, and then populates intermediate `sof$ta_` tables, eventually populating `sof$ta_e_` tables.

The overall job workflow can be summarized as:
1. `r_ausd_adressen.ksh` (invoker)
2. `k_ausd_adressen.ksh` (orchestrator):
    - Initializes environment and loads utility functions.
    - Parses and validates input parameters (`Jobkennung`, `EintragsNr`, `Stichtag`, `Wiederanlaufwert`).
    - Validates `Stichtag` format.
    - Determines `yesterday` and `today` dates.
    - Executes `d_ausd_adressen.sql` with collected parameters.
    - Captures the record count from the SQL script output.
    - (Intended, but commented out) Creates an entry in a job-tracking table.
3. `d_ausd_adressen.sql` (data processing):
    - Reads data from source tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`).
    - Performs transformations and joins.
    - Inserts processed data into target tables (`sof$ta_`, `sof$ta_e_`).

The scope of this migration design covers the translation of both the KornShell orchestration logic and the Oracle SQL*Plus data processing logic into a BigQuery-compatible solution.

## 2. Source Inventory

| File Path                                                               | Category | Tool         | Primary Type | Complexity Tier | Migration Bucket |
| :---------------------------------------------------------------------- | :------- | :----------- | :----------- | :-------------- | :--------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh` | shell    | KornShell    | ksh          | (Not available) | semi_auto        |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql` | sql      | Oracle SQL*Plus | sql_script   | (Not available) | (Not available)  |

**Utility Scripts Sourced by `k_ausd_adressen.ksh`:**
* `$HOME/.dw_init` (environment initialization)
* `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
* `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utility, includes `DWDate_Datum_Check`)
* `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing utility, includes `pruefeParameterGesetzt`)
* `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper, includes `starteSQLSkript`)
* `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (external script to determine yesterday/today dates)

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform services, with BigQuery as the core data processing engine.

*   **Data Processing**: The logic from `d_ausd_adressen.sql` will be translated into standard BigQuery SQL. This could reside within:
    *   A BigQuery Stored Procedure, enabling parameterization and modularity.
    *   Individual SQL scripts executed sequentially by an orchestrator.
*   **Orchestration**: The control flow logic from `k_ausd_adressen.ksh` will be migrated to:
    *   A BigQuery Stored Procedure (as demonstrated in the pseudocode, consolidating the shell logic and SQL logic).
    *   Alternatively, an Airflow DAG (managed by Cloud Composer) written in Python, which would call BigQuery SQL scripts or stored procedures.
*   **Parameter Management**: Shell script parameters will be converted to BigQuery Stored Procedure input parameters or Airflow DAG parameters. Environment variables will be replaced by BigQuery dataset/project configurations or Airflow variables.
*   **Error Handling & Logging**: Shell-based error handling (`f_alis_msgerr.ksh`) will be replaced by BigQuery SQL error handling constructs (`RAISE ERROR`) and/or integrated with Cloud Logging. Console outputs will be redirected to BigQuery procedure logs or Cloud Logging.
*   **Job Tracking**: The `PoolVertrag` job-table entry creation (currently commented out) will be implemented as an `INSERT` statement into a dedicated BigQuery logging/metadata table.
*   **Temporary Files**: The use of temporary files (`$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp`) will be replaced by BigQuery variables (`DECLARE`) or temporary tables.

## 4. Data Flow & Lineage

The overall data flow involves:

1.  **Invocation**: An upstream job (`r_ausd_adressen.ksh`) invokes the main job `k_ausd_adressen.ksh`. In the BigQuery environment, this suggests `k_ausd_adressen.ksh` might be wrapped into a BigQuery Stored Procedure, and `r_ausd_adressen.ksh` would be another orchestration component or a schedule.
2.  **Orchestration (`k_ausd_adressen.ksh` -> BQ Stored Procedure/Airflow DAG)**:
    *   Reads input parameters (Job ID, Entry Number, Reference Date, Restart Value).
    *   Calls utility functions (date checks, parameter checks).
    *   Prepares execution context for the core SQL logic.
3.  **Data Processing (`d_ausd_adressen.sql` -> BQ SQL)**:
    *   **Sources**: Reads from legacy Oracle tables: `cds$ta_`, `glv$ta_`, `bpd$ta_`. These will need to be ingested into BigQuery (e.g., via Cloud Data Fusion, Dataflow, or a direct Oracle to BigQuery migration service) before this job runs.
    *   **Transformations**: Performs joins and data manipulation as defined in `d_ausd_adressen.sql`.
    *   **Targets**: Writes transformed data into `sof$ta_` and `sof$ta_e_` tables within BigQuery.
4.  **Post-Processing (`k_ausd_adressen.ksh` -> BQ Stored Procedure/Airflow DAG)**:
    *   Captures the number of records processed.
    *   (If activated) Inserts a record into a BigQuery job logging/metadata table (e.g., `project.dataset.job_table`).

The data flow highlights a clear ETL pattern: Extract (from `cds$ta_`, `glv$ta_`, `bpd$ta_`), Transform (within `d_ausd_adressen.sql`), and Load (into `sof$ta_`, `sof$ta_e_`).

## 5. Transformation Logic

**`k_ausd_adressen.ksh` (Orchestration Logic - Migrated to BQ Stored Procedure Pseudocode)**:

*   **Parameter Handling**: Shell `getopts` for `j`, `f`, `s`, `l` parameters will be replaced by explicit `IN` parameters in the BigQuery Stored Procedure definition.
*   **Variable Declarations**: Shell variables like `p_JobKennung`, `v_TabName`, `tmpFile`, `v_records` will become BigQuery `DECLARE` variables.
*   **Conditional Logic**: Shell `if` statements (`[ ! $ErrNr -eq 0 ]`, `[[ -z "$p_wiederanlaufWert" ]]`) will be directly translated to BigQuery `IF THEN END IF;` constructs.
*   **Date Operations**: `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` and the external `gestern.ksh` script will be replaced by BigQuery date functions such as `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`, `CURRENT_DATE()`, and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Execution**: The `starteSQLSkript` function, which wraps `d_ausd_adressen.sql`, will be replaced by embedding the translated BigQuery SQL code directly within the stored procedure, or by calling a separate BigQuery stored procedure for `d_ausd_adressen.sql`.
*   **Record Count**: Reading from `$tmpFile` via `cat` and `eval` will be replaced by directly capturing the `COUNT(*)` from the target table after the SQL processing.
*   **Error Reporting**: `DWMSG_MeldeFehler` will be replaced by `SELECT` statements for logging messages, or `RAISE ERROR` for critical failures.

**`d_ausd_adressen.sql` (Data Transformation Logic - Migrated to BQ SQL)**:

The core logic of `d_ausd_adressen.sql` will be translated from Oracle SQL*Plus to BigQuery Standard SQL. This involves:

*   **Schema and Table Mapping**: Oracle table names (e.g., `cds$ta_`, `glv$ta_`, `bpd$ta_`, `sof$ta_`, `sof$ta_e_`) will be mapped to their corresponding BigQuery dataset.table names (e.g., ``project.dataset.cds_ta_``).
*   **Data Type Conversion**: Oracle data types will be mapped to BigQuery data types.
*   **Function Translation**: Oracle SQL functions will be converted to equivalent BigQuery SQL functions (e.g., `TO_DATE`, `NVL`, `DECODE`, etc., will have their BigQuery counterparts).
*   **Join Operations**: Joins between tables will be preserved.
*   **DML Statements**: `INSERT INTO ... SELECT FROM ...` statements will be directly translated.
*   **Parameter Usage**: Parameters passed from `k_ausd_adressen.ksh` (e.g., `p_Stichtag`) will be used within the BigQuery SQL, either as direct substitutions if generated via an orchestrator, or as explicit parameters if part of a BigQuery Stored Procedure.

## 6. External Dependencies

*   **Oracle Database Tables (`cds$ta_`, `glv$ta_`, `bpd$ta_`)**:
    *   **Replacement**: These source tables must be migrated to BigQuery. This typically involves a one-time historical load and then ongoing incremental data replication using services like Database Migration Service, Cloud Data Fusion, or custom Dataflow jobs. The corresponding BigQuery tables will serve as the new data sources.
*   **Shell Environment (`$HOME/.dw_init`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`)**:
    *   **Replacement**: These environment variables and initialization scripts define paths and potentially configuration. In BigQuery, these will be replaced by:
        *   BigQuery project/dataset IDs.
        *   Procedure parameters for dynamic paths or settings.
        *   Configuration tables within BigQuery itself.
        *   Airflow Variables or XComs if Cloud Composer is used for orchestration.
*   **Utility KornShell Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`)**:
    *   **Replacement**: The functionalities of these scripts will be re-implemented directly in BigQuery SQL (as part of the stored procedure for `k_ausd_adressen.ksh`) or Python functions within an Airflow DAG. For example, date validation and parameter checks will become BigQuery SQL statements. The logic of `gestern.ksh` (returning yesterday's and today's dates) will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **Filesystem (`$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp`)**:
    *   **Replacement**: Temporary file usage for record counts will be replaced by BigQuery `DECLARE` variables or by inserting data into a logging/metrics table.

## 7. Unresolved / Risks

*   **Commented-Out Job Management**: The original `k_ausd_adressen.ksh` contains commented-out lines for `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. It is currently assumed these functionalities are not active. If they are required for future operations, their logic will need to be re-enabled and implemented in BigQuery (e.g., as inserts/updates to a job metadata table). This constitutes a potential scope change if activation is needed.
*   **Complexity of `d_ausd_adressen.sql`**: While the summary indicates standard SQL operations, the actual complexity of the Oracle SQL*Plus script is unknown without full analysis. Any advanced Oracle-specific features (e.g., hierarchical queries, specific PL/SQL packages, complex stored procedures invoked by the SQL*Plus script) would require careful translation and potentially different migration strategies (e.g., custom Python logic in Dataflow/Dataproc if direct BigQuery SQL translation is not feasible).
*   **`r_ausd_adressen.ksh`**: The invoking script `r_ausd_adressen.ksh` is outside the immediate scope of this job's detailed analysis. Its migration strategy will need to ensure it correctly invokes the migrated `k_ausd_adressen.ksh` component.
*   **Performance Tuning**: Initial migration may not be fully optimized. Post-migration performance tuning in BigQuery will be required, especially for large datasets.

## 8. Build Plan

The build plan involves a staged approach:

1.  **Ingest Source Data**:
    *   Migrate `cds$ta_`, `glv$ta_`, `bpd$ta_` tables from Oracle to BigQuery.
        *   **Tool**: Database Migration Service, Cloud Data Fusion, or custom Dataflow.
        *   **Language**: N/A (service configuration) or Python/Java (Dataflow).
        *   **Deliverable**: Populated BigQuery tables.
2.  **Translate `d_ausd_adressen.sql` to BigQuery SQL**:
    *   Convert all Oracle SQL*Plus constructs, functions, and DML into BigQuery Standard SQL.
        *   **Tool**: Manual conversion assisted by `hql_sql_to_bqsql_design` (or similar for Oracle SQL) from CM MCP, or automated SQL translation tools.
        *   **Language**: BigQuery SQL.
        *   **Deliverable**: `d_ausd_adressen.bq.sql` script or a BigQuery Stored Procedure DDL.
3.  **Translate `k_ausd_adressen.ksh` Orchestration Logic**:
    *   Implement the parameter handling, validation, date logic, and execution flow of `k_ausd_adressen.ksh` into a BigQuery Stored Procedure, embedding the translated `d_ausd_adressen.bq.sql`.
        *   **Tool**: Manual conversion following the provided BQ SQL Pseudocode from `shellscript_to_bqsql_design`.
        *   **Language**: BigQuery SQL (Stored Procedure).
        *   **Deliverable**: `k_ausd_adressen_control.bq.sql` (BQ Stored Procedure DDL).
    *   *Alternative (if Airflow is chosen for orchestration)*: Create a Python Airflow DAG that orchestrates the execution of the BigQuery SQL for `d_ausd_adressen.sql` (potentially wrapped in another BQ Stored Procedure).
        *   **Tool**: Manual Python development.
        *   **Language**: Python (for Airflow DAG).
        *   **Deliverable**: `k_ausd_adressen_dag.py`.
4.  **Implement Job Logging (if needed)**:
    *   Create the `project.dataset.job_table` in BigQuery.
    *   Integrate `INSERT` statements into the `k_ausd_adressen_control` stored procedure to log job execution details.
        *   **Tool**: Manual SQL development.
        *   **Language**: BigQuery SQL.
        *   **Deliverable**: `job_table_ddl.sql`, modifications to `k_ausd_adressen_control.bq.sql`.
5.  **Testing**:
    *   Develop unit tests for the BigQuery SQL components.
    *   Develop integration tests for the entire job flow.
        *   **Tool**: Custom SQL/Python testing frameworks.
        *   **Language**: BigQuery SQL, Python.
        *   **Deliverable**: Test scripts and reports.