# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

## 1. Purpose & Scope
The shell script `k_ausd_adressen.ksh` serves as a control script responsible for the preparation and execution of an address-related data processing workflow. Its primary functions include:
- Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
- Performing error checking and date validation.
- Orchestrating the execution of the SQL script `d_ausd_adressen.sql`.
- Calculating "yesterday" and "today" dates.
- Capturing the number of records processed by the SQL script.
- (Commented out functionality) Managing job table entries, including deactivating old jobs and creating new ones.

The scope of this migration is to re-platform this KornShell script and its associated SQL processing to Google Cloud Platform, specifically utilizing BigQuery for data processing and Cloud Composer (Apache Airflow) for orchestration.

## 2. Source Inventory
The job consists of a single primary source file and several implicit dependencies:

**Primary File:**
- **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh`
- **Technology:** KornShell Script
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-automatic (B2)
- **Summary:** This script acts as an orchestrator, handling parameter validation, date checks, and invoking an SQL script (`d_ausd_adressen.sql`) for data processing. It also includes commented-out logic for job table management.

**Dependencies (identified from source code analysis):**
- **Executes:** `SQL_SCRIPT:D_AUSD_ADRESSEN.SQL` (an Oracle SQL script, as indicated by `h_alis_sqlplus.ksh` sourcing)
- **Invokes (shell script):** `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (for date calculation)
- **Sources (utility scripts):**
    - `$HOME/.dw_init` (environment initialization)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling framework)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation functions, e.g., `DWDate_Datum_Check`)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing functions, e.g., `pruefeParameterGesetzt`)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus wrapper functions, e.g., `starteSQLSkript`)
- **Temporary Files:** `$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp` (used to store record counts from SQL execution)

**External Systems:** No direct external systems were explicitly identified by lineage analysis for this specific job, beyond the implicit Oracle database interaction through SQL*Plus and the `.sql` script.

## 3. Target Architecture
The migration will transition the job to Google Cloud Platform, leveraging the following components:
- **Orchestration:** Cloud Composer (managed Apache Airflow) will manage the workflow execution.
- **Data Processing:** Google BigQuery will be the target for all SQL-based data transformations.
- **Script Logic:** KornShell script logic will be re-implemented in Python within Airflow DAGs and supporting modules.
- **Persistent Storage:** Cloud Storage for any transient files if necessary, although BigQuery tables are preferred for structured data.

## 4. Data Flow & Lineage

**Current Data Flow:**
1. `k_ausd_adressen.ksh` starts.
2. Initializes environment variables and sources various utility shell scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3. Parses input parameters (`-j`, `-f`, `-s`, `-l`) using `getopts`.
4. Validates mandatory parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`) using `pruefeParameterGesetzt`.
5. Performs date format validation on `p_Stichtag` using `DWDate_Datum_Check`.
6. Executes `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`.
7. Initializes `p_wiederanlaufWert` if not provided.
8. (Commented out) `FOSJobDeaktivate $v_TabName`.
9. Executes `d_ausd_adressen.sql` via `starteSQLSkript` with collected parameters. The SQL script presumably reads data from source tables, performs transformations, and writes to target tables.
10. Reads the processed record count from `$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp`.
11. (Commented out) `FOSJobErzeugeEintrag` to update a job table.
12. Script completes.

**Target Data Flow (Cloud Composer Airflow DAG):**
1. **`start_task` (PythonOperator):** Initiates the DAG, sets up context, and potentially retrieves parameters from Airflow variables or runtime configuration.
2. **`parse_and_validate_parameters_task` (PythonOperator):** Re-implements the `getopts` and `pruefeParameterGesetzt` logic. Parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) will be passed via Airflow DAG parameters or configuration.
3. **`calculate_dates_task` (PythonOperator):** Replaces `gestern.ksh` and `DWDate_Datum_Check` functionality. Uses Python's `datetime` module to calculate `p_datum_heute` and `p_datum_gestern` and validate `p_Stichtag`. Results are passed via Airflow XComs.
4. **`execute_d_ausd_adressen_sql_task` (BigQueryOperator):** Executes the BigQuery-converted `d_ausd_adressen.sql`. Parameters from previous tasks will be passed as Jinja templates to the BigQuery query. The task will run the SQL script, potentially within a BigQuery stored procedure.
5. **`log_record_count_task` (PythonOperator):** Replaces the `eval "v_records=\`cat $tmpFile\`"` step. This task will query BigQuery to get the number of records affected by `d_ausd_adressen.sql` or read from a designated output table.
6. **(Optional) `manage_job_table_task` (BigQueryOperator):** If the job table management functionality is re-enabled, this task will execute BigQuery DML to update the job tracking table (`PoolVertrag`).

## 5. Transformation Logic

- **Shell Script to Airflow DAG (Python):**
    - The overall orchestration logic of `k_ausd_adressen.ksh` will be translated into an Airflow DAG.
    - Parameter parsing (`getopts`) will be re-implemented using Python's `argparse` or by defining DAG parameters directly.
    - Error handling and logging (`f_alis_msgerr.ksh`) will leverage Airflow's native logging and error handling mechanisms, with custom Python functions for specific error messaging if needed.
    - Date validation (`h_alis_date.ksh`'s `DWDate_Datum_Check`) and date calculation (`gestern.ksh`) will be implemented using Python's `datetime` module.
    - The `h_alis_sqlplus.ksh` and `starteSQLSkript` functionality will be replaced by direct interaction with BigQuery using the `BigQueryOperator` in Airflow.
    - Temporary file (`tmpFile`) usage will be eliminated by directly querying BigQuery for record counts after `d_ausd_adressen.sql` execution.
    - The commented-out job table management logic will be translated into BigQuery DML statements if activated.

- **`d_ausd_adressen.sql` to BigQuery SQL:**
    - This SQL script, currently designed for an Oracle environment (inferred from `h_alis_sqlplus.ksh`), will be fully converted to BigQuery SQL syntax. This will involve:
        - **Data Type Mapping:** Converting Oracle-specific data types to their BigQuery equivalents.
        - **Function Conversion:** Translating Oracle SQL functions (e.g., `NVL`, `DECODE`, date functions) to BigQuery standard SQL functions.
        - **Syntax Adjustments:** Adapting any proprietary SQL syntax or constructs to BigQuery's SQL dialect.
        - **Table/View References:** Ensuring all tables and views referenced within `d_ausd_adressen.sql` are migrated and available in BigQuery with correct schemas and permissions.
        - **Performance Optimization:** Reviewing and optimizing the SQL for BigQuery's columnar storage and distributed query engine.

## 6. External Dependencies
- **Oracle Database (implicit):** The original script, through `h_alis_sqlplus.ksh` and `d_ausd_adressen.sql`, interacts with an Oracle database. This Oracle database will be migrated to BigQuery. All data sources and target tables for `d_ausd_adressen.sql` will reside within BigQuery.
- **Unix/KornShell Environment:** The shell-specific environment variables and utilities (e.g., `$HOME`, `BERT_DIR_ROOT`, `getopts`, `set -e`, `print`, `eval`) will be replaced by Cloud Composer's environment, Airflow variables, and Python constructs.

## 7. Unresolved / Risks
- **`d_ausd_adressen.sql` Details:** The full content and complexity of `d_ausd_adressen.sql` were not directly analyzed. A detailed analysis and conversion plan for this SQL script are critical and currently unresolved. This will dictate the specifics of BigQuery table creation, data loading, and SQL transformation.
- **`starteSQLSkript` Implementation:** The exact implementation of `starteSQLSkript` in `h_alis_sqlplus.ksh` (e.g., how it handles errors, commits, or captures output) is not fully known. This needs to be understood to ensure faithful replication in the Airflow BigQueryOperator task.
- **Job Table Management Logic:** The `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` functions are commented out. Confirmation is required if this functionality is still business-critical. If so, their specific logic and the `PoolVertrag` table structure need to be understood for migration.
- **Source of Parameters:** The script expects parameters like `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`. The upstream system that calls `k_ausd_adressen.ksh` and provides these parameters needs to be identified and its parameter passing mechanism needs to be integrated with the Airflow DAG.

## 8. Build Plan

1.  **Detailed Analysis of `d_ausd_adressen.sql`:**
    *   Extract the content of `d_ausd_adressen.sql`.
    *   Identify all source tables, target tables, views, and SQL functions used.
    *   Document the SQL logic and data transformations performed.

2.  **BigQuery Schema Design and DDL Generation:**
    *   Based on the analysis of `d_ausd_adressen.sql`, design the target BigQuery schemas and generate DDL for all required tables and views.

3.  **BigQuery SQL Conversion for `d_ausd_adressen.sql`:**
    *   Convert `d_ausd_adressen.sql` into optimized BigQuery Standard SQL, handling syntax, function, and data type conversions.

4.  **Python Utility Development:**
    *   Create Python modules or functions to replicate the logic of `f_alis_msgerr.ksh`, `h_alis_date.ksh` (e.g., `DWDate_Datum_Check`), `h_alis_parameter.ksh` (e.g., `pruefeParameterGesetzt`), and `gestern.ksh`. These should be reusable and designed for Airflow.

5.  **Airflow DAG Development:**
    *   Create a new Python file for the Airflow DAG (`k_ausd_adressen_dag.py`).
    *   Define DAG parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    *   Integrate the Python utility functions for parameter parsing, validation, and date calculations into Airflow `PythonOperator` tasks.
    *   Add a `BigQueryOperator` task to execute the converted `d_ausd_adressen.sql` in BigQuery, passing parameters from the DAG.
    *   Implement tasks for logging the record count and (if required) for managing the job table in BigQuery.

6.  **Testing Plan:**
    *   **Unit Tests:** For individual Python utility functions and BigQuery SQL components.
    *   **Integration Tests:** Verify the Airflow DAG's flow, parameter passing, and successful execution of BigQuery jobs.
    *   **Data Validation:** Compare output data between the legacy system and BigQuery for a representative dataset.

7.  **Deployment:**
    *   Deploy the BigQuery DDL (schemas, tables, views).
    *   Deploy the converted `d_ausd_adressen.sql` (e.g., as a BigQuery stored procedure or a script to be executed by `BigQueryOperator`).
    *   Deploy the Airflow DAG and any custom Python modules to Cloud Composer.