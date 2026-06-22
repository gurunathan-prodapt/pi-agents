# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope

This document outlines the migration design for the legacy KornShell script `k_ausd_bp_ta_msisdn.ksh`. The script serves as a control and orchestration wrapper for executing a core SQL script, `d_ausd_bp_ta_msisdn.sql`. Its primary functions include parsing command-line parameters, initializing the execution environment by sourcing utility scripts, validating input dates and parameters, executing the SQL data processing script, and capturing processing metrics (record counts). The scope of this migration is to transition this orchestration and data processing logic from the legacy KornShell/Oracle environment to Google Cloud Platform, utilizing Cloud Composer (Apache Airflow) for orchestration and BigQuery for data warehousing and SQL execution.

The job `5af228f1` is a single-component assembled job, with the `k_ausd_bp_ta_msisdn.ksh` script being the sole identified component.

## 2. Source Inventory

The primary source component for this migration is a KornShell script:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh`
    *   **Technology**: KornShell (ksh) Script
    *   **Category**: Shell
    *   **Tool**: KornShell
    *   **Summary**: A control and orchestration wrapper that handles parameter parsing, environment setup, error handling, date validation, and the execution of `d_ausd_bp_ta_msisdn.sql`.
    *   **Complexity Tier**: Medium (inferred, due to orchestration logic, parameter handling, and external script dependencies)
    *   **Automation Bucket**: Semi-Auto / Redesign (inferred, as it requires significant refactoring to a new orchestration paradigm like Airflow).

## 3. Target Architecture

The migrated solution will leverage Google Cloud Platform services:

*   **Orchestration Layer**: Google Cloud Composer (Apache Airflow) will manage the workflow, replacing the KornShell script's control flow, parameter handling, and sequential execution of tasks.
*   **Data Processing Layer**: Google BigQuery will serve as the data warehouse. All SQL transformations currently performed by `d_ausd_bp_ta_msisdn.sql` will be re-implemented and executed as BigQuery SQL queries.
*   **Utility & Helper Functions**: Common utilities (date calculations, parameter validation) currently in separate `.ksh` files will be re-implemented as Python functions within the Airflow DAG or as shared Python modules. Error handling will utilize Airflow's native mechanisms.
*   **Data Storage**: Temporary files (like `$tmpFile`) will be eliminated, with data and metrics being directly managed within BigQuery.

## 4. Data Flow & Lineage

**Current Data Flow (Legacy)**:
1.  The `k_ausd_bp_ta_msisdn.ksh` script is invoked with parameters (`-j`, `-f`, `-s`, `-l`).
2.  Environment configuration is loaded from `$HOME/.dw_init`.
3.  Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are sourced for error handling, date validation, parameter validation, and SQL execution.
4.  Parameters are parsed using `getopts` and validated using `pruefeParameterGesetzt`.
5.  The reference date (`p_Stichtag`) is validated using `DWDate_Datum_Check`.
6.  `gestern.ksh` is executed to derive `p_datum_heute` and `p_datum_gestern`.
7.  The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) executes `d_ausd_bp_ta_msisdn.sql`, likely via SQL*Plus against an Oracle database. The SQL script is expected to read data from and write to the `PoolBasisprodukt` table, or intermediate tables.
8.  The SQL script's output (e.g., record count) is captured into `$tmpFile`.
9.  The script reads `$tmpFile` to populate the `v_records` variable.
10. Error messages are reported using `DWMSG_MeldeFehler`.

**Target Data Flow (BigQuery / Airflow)**:
1.  An Airflow DAG, `dag_k_ausd_bp_ta_msisdn`, is triggered (either on schedule or manually with parameters).
2.  **Parameter Handling Task**: A PythonOperator within the DAG will receive and validate input parameters, similar to the `getopts` and `pruefeParameterGesetzt` logic.
3.  **Date Derivation Task**: A PythonOperator will replace the `gestern.ksh` functionality to calculate current and previous dates (`today`, `yesterday`).
4.  **BigQuery SQL Execution Task**: A `BigQueryOperator` will execute the migrated `d_ausd_bp_ta_msisdn.sql` logic (now in BigQuery SQL format). This SQL will read from and write to BigQuery tables, including `project.dataset.PoolBasisprodukt`.
5.  **Record Count & Logging Task**: A subsequent PythonOperator or BigQueryOperator will query the target BigQuery table(s) to obtain record counts, eliminating the need for temporary files.
6.  **Error Handling & Monitoring**: Airflow's native logging, alerting, and retry mechanisms will manage operational aspects. Custom Python functions can be integrated for specific error reporting logic that was previously in `f_alis_msgerr.ksh`.

## 5. Transformation Logic

The migration will involve transforming the shell script's imperative control flow and its embedded SQL execution into a declarative Airflow DAG and BigQuery SQL.

*   **KornShell Control Flow (e.g., `getopts`, `if-else`, variable assignments)**: Will be re-implemented using Python logic within an Airflow DAG. This includes parameter parsing, validation, conditional execution paths, and variable management.
*   **Environment Setup (`. $HOME/.dw_init`, etc.)**: Replaced by Airflow's environment configuration, Python package imports, or explicit task-level environment variables.
*   **Utility Script Calls (`pruefeParameterGesetzt`, `DWDate_Datum_Check`, `starteSQLSkript`, `gestern.ksh`)**:
    *   `gestern.ksh` functionality will be replaced by standard Python `datetime` operations.
    *   `pruefeParameterGesetzt` and `DWDate_Datum_Check` will be re-implemented as Python functions for parameter and date validation.
    *   `starteSQLSkript` (responsible for executing the SQL script) will be replaced by the `BigQueryOperator` in Airflow, which directly executes SQL against BigQuery.
*   **SQL Script (`d_ausd_bp_ta_msisdn.sql`)**: This script's content needs to be fully converted from its current SQL dialect (likely Oracle SQL, given SQL*Plus context) to BigQuery Standard SQL. This involves:
    *   Syntax adjustments for data types, functions, and query constructs.
    *   Optimizations for BigQuery's columnar storage and distributed query engine.
    *   Potential re-architecture of temporary tables or staging areas to BigQuery best practices.
*   **Temporary File Handling (`tmpFile`, `cat`, `eval`)**: The use of temporary files for capturing record counts will be eliminated. Post-execution, record counts can be directly queried from the BigQuery target tables using a `BigQueryOperator` or `PythonOperator` with BigQuery client library.
*   **Commented-out `sed`, `sort`, `join` commands**: These inactive file processing steps are noted but will not be migrated unless explicitly required in a future phase. If needed, they would be reimplemented in BigQuery SQL (for set operations) or Python/PySpark on Dataproc/Dataflow for more complex file manipulation.

## 6. External Dependencies

*   **Oracle Database**: The current source database is assumed to be Oracle, based on the SQL*Plus context of the original script. This will be migrated to Google BigQuery. Data will be ingested into BigQuery from its source system.
*   **Operating System Utilities (ksh, getopts, cat, print, eval)**: These are intrinsic to the legacy environment and will be replaced by their Python and Airflow counterparts.
*   **Filesystem-based Utilities (sourced .ksh scripts)**: The functionality embedded in `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `gestern.ksh` will be re-implemented as Python functions or modules within the Cloud Composer environment. No direct migration of these shell scripts is planned; rather, their core logic will be translated.
*   **Environment Variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`)**: These will be replaced by Airflow variables, connections, or constants defined within the Python DAG, aligned with GCP resource paths (e.g., GCS buckets).

## 7. Unresolved / Risks

*   **`d_ausd_bp_ta_msisdn.sql` content is unknown**: The specific SQL logic of `d_ausd_bp_ta_msisdn.sql` has not been analyzed. Its complexity, dependencies on specific Oracle features (e.g., PL/SQL, proprietary functions), and performance characteristics will directly impact the effort required for BigQuery migration and potential re-optimization. This is the primary technical risk.
*   **Detailed logic of sourced utility scripts**: While the purpose of utility scripts like `f_alis_msgerr.ksh` is known, their full internal implementation is not. Any complex logic within these scripts must be carefully understood and re-implemented in Python to ensure functional equivalence.
*   **`PoolBasisprodukt` table schema**: The exact schema definition, data types, and volume of the `PoolBasisprodukt` table are not available. This information is critical for designing the target BigQuery table and ensuring proper data type mapping.
*   **Performance Characteristics**: The performance profile of the original job in the Oracle environment is unknown. Benchmarking will be required post-migration to ensure BigQuery performance meets or exceeds legacy SLAs.
*   **Error Handling Parity**: Replicating the exact error handling behavior of `DWMSG_MeldeFehler` and other custom logic in Airflow/Python needs careful consideration to maintain operational consistency.

## 8. Build Plan

1.  **Phase 1: Discovery & Analysis (Manual)**
    *   Obtain the source code for `d_ausd_bp_ta_msisdn.sql`.
    *   Thoroughly analyze `d_ausd_bp_ta_msisdn.sql` to understand its logic, input/output tables, joins, filters, and any specific Oracle SQL features used.
    *   Obtain schemas for `PoolBasisprodukt` and any other tables accessed by `d_ausd_bp_ta_msisdn.sql`.
    *   Obtain source code for `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` to understand their precise logic for re-implementation.

2.  **Phase 2: Data Migration & BigQuery Schema (BQ DDL)**
    *   Create `PoolBasisprodukt` table and any other required source/staging tables in BigQuery, mapping data types appropriately.
    *   Establish a data ingestion pipeline to bring data from the source Oracle database into the new BigQuery tables.

3.  **Phase 3: SQL Conversion (BigQuery SQL)**
    *   Translate `d_ausd_bp_ta_msisdn.sql` into optimized BigQuery Standard SQL, ensuring functional equivalence and performance.

4.  **Phase 4: Utility Function Re-implementation (Python)**
    *   Develop Python modules/functions for the logic previously handled by:
        *   `gestern.ksh` (date calculations).
        *   `h_alis_parameter.ksh` (parameter validation logic).
        *   `h_alis_date.ksh` (date format validation logic).
        *   `f_alis_msgerr.ksh` (custom error messaging/logging logic).
        *   `h_alis_sqlplus.ksh` (any non-execution-related logic, execution replaced by `BigQueryOperator`).

5.  **Phase 5: Airflow DAG Development (Python)**
    *   Create a new Airflow DAG `dag_k_ausd_bp_ta_msisdn.py`.
    *   **Task 1: `validate_parameters` (PythonOperator)**: Implement parameter parsing and validation logic.
    *   **Task 2: `derive_dates` (PythonOperator)**: Call the Python function that replaces `gestern.ksh`.
    *   **Task 3: `execute_bigquery_sql` (BigQueryOperator)**: Execute the converted BigQuery SQL for `d_ausd_bp_ta_msisdn.sql`.
    *   **Task 4: `capture_record_count` (PythonOperator/BigQueryOperator)**: Query BigQuery table to get processed record counts.
    *   Configure dependencies between tasks.
    *   Implement robust error handling, retries, and logging using Airflow's native capabilities.

6.  **Phase 6: Testing & Deployment**
    *   Unit testing of individual Python functions and BigQuery SQL.
    *   Integration testing of the Airflow DAG on Cloud Composer.
    *   Performance testing against legacy benchmarks.
    *   Deployment to production environment.