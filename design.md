# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_bp_ta_p_basisprod.ksh`, serves as a control wrapper for an Oracle SQL script, `d_ausd_bp_ta_p_basisprod.sql`. Its primary purpose is to:
1.  Initialize the environment by sourcing common utility scripts for error handling, date validation, parameter parsing, and SQL*Plus execution.
2.  Parse command-line parameters including job identifier (`-j`), entry number (`-f`), reference date (`-s`), and restart value (`-l`).
3.  Validate the required parameters and the format of the reference date.
4.  Determine dynamic date variables (yesterday and today).
5.  Execute the core SQL logic contained in `d_ausd_bp_ta_p_basisprod.sql`, passing several parameters.
6.  Optionally, it includes commented-out sections for post-processing of flat files using `sed`, `sort`, and `join` commands, indicating a potential hybrid data processing approach.
7.  It writes the record count to a temporary file, which is then read back into a variable.
8.  There are commented-out sections referencing FOS job management, suggesting integration with an older job scheduling or monitoring system.

The business purpose of this job is to prepare/process basis product data (`PoolBasisprodukt`) based on a given reference date, ultimately writing results into the `SOF$TA_P_BASISPROD` table.

## 2. Source Inventory
The job consists of a single primary KornShell script that orchestrates an Oracle SQL script.

| File Path                                                                   | Technology  | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                               |
| :-------------------------------------------------------------------------- | :---------- | :----- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh` | KornShell   | `medium` | `semi_auto`       | This ksh script acts as a control wrapper for an SQL script, handling parameter parsing, environment setup, date validation, and execution of the main SQL logic. It also includes commented-out sections for file-based data processing (sed, sort, join). |
| `d_ausd_bp_ta_p_basisprod.sql` (invoked by ksh)                             | Oracle SQL  | N/A    | N/A               | This SQL script reads from various source tables (`TABLE:3`, `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_DIST`, `TABLE:SOF$TA_BCP_ICCID`) and writes to `TABLE:SOF$TA_P_BASISPROD`. It also utilizes several Oracle PL/SQL packages.                                                                |
| Helper scripts (sourced by ksh)                                             | KornShell   | N/A    | N/A               | `. $HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`, `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` |

## 3. Target Architecture
The target platform is Google BigQuery.
The migration will involve transforming the KornShell orchestration into a Python-based workflow, likely managed by Airflow, and the Oracle SQL logic into BigQuery Standard SQL.

*   **Orchestration Layer**: Apache Airflow DAG written in Python. This DAG will replace the `k_ausd_bp_ta_p_basisprod.ksh` script's parameter handling, environment setup, and sequential execution.
*   **Data Processing Layer**:
    *   **BigQuery Tables**: All source Oracle tables (`TABLE:3`, `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_DIST`, `TABLE:SOF$TA_BCP_ICCID`) will be migrated to corresponding BigQuery tables. The target table `SOF$TA_P_BASISPROD` will also be a BigQuery table.
    *   **BigQuery SQL**: The `d_ausd_bp_ta_p_basisprod.sql` script will be translated into BigQuery Standard SQL.
    *   **Python/PySpark (Optional)**: If the commented-out `sed`, `sort`, `join` logic needs to be reactivated or if complex file manipulations are involved, this could be implemented using Python or PySpark for efficiency. However, based on the current active code, BigQuery SQL is the primary target for data transformations.
*   **Logging and Error Handling**: Google Cloud Logging and Monitoring will replace the custom KornShell error handling (`f_alis_msgerr.ksh`).
*   **Date Utilities**: Python's `datetime` module will replace `h_alis_date.ksh` and `gestern.ksh`.
*   **Parameter Management**: Airflow's native parameter passing or a more robust configuration management system (e.g., Google Cloud Secret Manager for sensitive values) will replace `h_alis_parameter.ksh`.

## 4. Data Flow & Lineage
The current data flow is:
1.  `k_ausd_bp_ta_p_basisprod.ksh` (KornShell script)
    *   Sourced utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Calls `gestern.ksh` to get today's and yesterday's date.
    *   Invokes `starteSQLSkript` (a function from `h_alis_sqlplus.ksh`) which executes `d_ausd_bp_ta_p_basisprod.sql` via SQL*Plus.
2.  `d_ausd_bp_ta_p_basisprod.sql` (Oracle SQL script)
    *   **Reads from**: `TABLE:3`, `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_DIST`, `TABLE:SOF$TA_BCP_ICCID`.
    *   **Uses**: Oracle packages `DWPA_UTIL_SKRIPT`, `EV`, `ICC`, `MSI`, `OPT`, `AV`, `MSD`, `BCCM`.
    *   **Writes to**: `TABLE:SOF$TA_P_BASISPROD`.

In BigQuery, this flow will be:
1.  **Airflow DAG (Python)**:
    *   Handles parameter input and validation.
    *   Orchestrates the execution of BigQuery SQL tasks.
    *   Manages logging and error handling.
    *   Python `datetime` for date calculations.
2.  **BigQuery SQL (derived from `d_ausd_bp_ta_p_basisprod.sql`)**:
    *   **Reads from**: `PROJECT.DATASET.TABLE_3`, `PROJECT.DATASET.DWTK_MELDUNGEN`, `PROJECT.DATASET.SOF_TA_CNTRCT_DIST`, `PROJECT.DATASET.SOF_TA_BCP_ICCID`.
    *   **Writes to**: `PROJECT.DATASET.SOF_TA_P_BASISPROD`.

## 5. Transformation Logic
**From `k_ausd_bp_ta_p_basisprod.ksh` to Airflow DAG (Python):**

*   **Shell Script Structure**: The sequential execution and conditional logic of the ksh script will be translated into Airflow tasks and dependencies.
*   **Parameter Parsing (`getopts`)**: Airflow DAG parameters or Python `argparse` will replace `getopts`.
    *   `-j` (JobKennung) -> Airflow DAG parameter `job_kennung`
    *   `-f` (EintragsNr) -> Airflow DAG parameter `eintrags_nr`
    *   `-s` (Stichtag) -> Airflow DAG parameter `stichtag` (reference date, e.g., 'DDMMYYYY')
    *   `-l` (wiederanlaufWert) -> Airflow DAG parameter `wiederanlauf_wert`
*   **Environment Variables**: `$HOME/.dw_init` and other `BERT_DIR_ROOT` references will be replaced by environment variables in the Airflow environment or explicit paths within the Python code.
*   **Error Handling (`f_alis_msgerr.ksh`)**: Replaced by standard Python exception handling and Airflow's retry mechanisms and logging to Google Cloud Logging.
*   **Date Checks (`h_alis_date.ksh`, `gestern.ksh`)**: Python's `datetime` module will handle date parsing and calculations.
*   **SQL Execution (`h_alis_sqlplus.ksh`, `starteSQLSkript`)**: Replaced by `BigQueryOperator` in Airflow, executing the translated BigQuery SQL. The `$tmpFile` for record count will be handled by BigQuery's `COUNT(*)` or similar post-query.
*   **File Post-processing (commented `sed`, `sort`, `join`)**: If this logic is reactivated, it will be implemented in Python using libraries like Pandas or potentially PySpark if data volumes require distributed processing. Otherwise, it will be omitted.

**From `d_ausd_bp_ta_p_basisprod.sql` (Oracle) to BigQuery Standard SQL:**

*   **Syntax Conversion**: All Oracle SQL syntax (e.g., `DUAL`, specific function calls, `(+)` for outer joins) will be converted to BigQuery Standard SQL equivalents.
*   **Data Types**: Oracle data types will be mapped to appropriate BigQuery data types.
*   **PL/SQL Packages (`DWPA_UTIL_SKRIPT`, etc.)**: These Oracle packages likely contain custom logic or functions. Each function/procedure within these packages that is used by `d_ausd_bp_ta_p_basisprod.sql` will need to be re-implemented in BigQuery (as user-defined functions or views) or in Python/PySpark, depending on complexity. This is a critical area for detailed analysis.
*   **Table References**: Oracle table names like `TABLE:3` (anonymized, requires identification), `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_DIST`, `TABLE:SOF$TA_BCP_ICCID`, `TABLE:SOF$TA_P_BASISPROD` will be mapped to BigQuery table references (e.g., `project.dataset.table_name`).

## 6. External Dependencies
The current job has the following external dependencies:

*   **Oracle Database**: The `d_ausd_bp_ta_p_basisprod.sql` script interacts directly with an Oracle database for reading source tables and writing to the target table `SOF$TA_P_BASISPROD`. It also uses Oracle PL/SQL packages.
    *   **Replacement**: The Oracle database will be replaced by Google BigQuery. All relevant source tables (`TABLE:3`, `TABLE:DWTK_MELDUNGEN`, `TABLE:SOF$TA_CNTRCT_DIST`, `TABLE:SOF$TA_BCP_ICCID`) and the target table (`SOF$TA_P_BASISPROD`) will be migrated to BigQuery. The logic in Oracle packages will be re-implemented in BigQuery UDFs or Python, as described above.
*   **Local File System**: The script interacts with the local file system for temporary files (`$tmpFile`) and potentially for the commented-out `sed`/`sort`/`join` operations on `.dat` files.
    *   **Replacement**: Google Cloud Storage (GCS) will replace local file system operations. Temporary data or intermediate files can be stored in GCS buckets. BigQuery operations typically do not require explicit temporary files for intermediate results.
*   **Scheduler / Job Management (FOSJobDeaktivate, FOSJobErzeugeEintrag)**: The commented-out FOS job management calls suggest integration with a legacy job scheduler.
    *   **Replacement**: Apache Airflow will manage the scheduling and monitoring of the migrated job. Its built-in logging and monitoring capabilities will replace the FOS system.

## 7. Unresolved / Risks
*   **`TABLE:3` Identification**: The table name `TABLE:3` is highly anonymized. Its actual name and schema within the Oracle database need to be identified to ensure correct migration to BigQuery.
*   **Oracle PL/SQL Package Logic**: The logic encapsulated within the Oracle packages (`DWPA_UTIL_SKRIPT`, `EV`, `ICC`, `MSI`, `OPT`, `AV`, `MSD`, `BCCM`) is a significant unknown. These packages must be thoroughly analyzed to understand their functionality and accurately translate them to BigQuery SQL UDFs or Python functions. This is the highest risk area due to potential complexity and reliance on specific Oracle features.
*   **Commented-out Code**: The `sed`, `sort`, `join` sections are commented out but indicate a potential need for file-based processing. It needs to be clarified if this logic is still relevant or if it can be safely ignored. If needed, this will add complexity and require Python/PySpark implementation.
*   **FOS Job Management**: The commented-out FOS calls (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) need to be investigated. If the functionality is still required, it needs to be mapped to Airflow's capabilities or other Google Cloud services (e.g., Cloud Functions, Pub/Sub for custom events).
*   **Performance**: The performance of the migrated BigQuery SQL and Python logic needs to be benchmarked against the original Oracle execution, especially considering the potential for large data volumes with "Basisprodukt" data.
*   **Dynamic `BERT_DIR_ROOT`**: The script relies heavily on `$BERT_DIR_ROOT` for path resolution. A robust configuration management strategy is needed in Airflow to manage these paths effectively.

## 8. Build Plan

1.  **Data Migration (Oracle to BigQuery)**
    *   Identify the concrete schema and content of `TABLE:3` in Oracle.
    *   Migrate `TABLE:3`, `DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_DIST`, `SOF$TA_BCP_ICCID`, and `SOF$TA_P_BASISPROD` from Oracle to BigQuery. This will likely involve a one-time historical load followed by incremental loads.
2.  **Oracle Package Analysis & Re-implementation**
    *   Conduct a detailed analysis of all functions/procedures within `DWPA_UTIL_SKRIPT`, `EV`, `ICC`, `MSI`, `OPT`, `AV`, `MSD`, `BCCM` used by `d_ausd_bp_ta_p_basisprod.sql`.
    *   Re-implement the logic of these packages as BigQuery UDFs (if stateless and SQL-compatible) or Python functions/modules (if procedural logic or external interactions are involved).
3.  **SQL Translation (`d_ausd_bp_ta_p_basisprod.sql` to BigQuery SQL)**
    *   Translate the Oracle SQL script `d_ausd_bp_ta_p_basisprod.sql` into BigQuery Standard SQL, incorporating the re-implemented package logic.
    *   Test the translated SQL for functional equivalence and performance.
4.  **Airflow DAG Development (Python)**
    *   Create a new Airflow DAG in Python (e.g., `k_ausd_bp_ta_p_basisprod_dag.py`).
    *   Implement parameter parsing and validation using Airflow DAG parameters.
    *   Replace environment variable lookups with Airflow configurations or Python constants.
    *   Integrate Python `datetime` for date calculations.
    *   Develop an Airflow task (using `BigQueryOperator` or a custom PythonOperator) to execute the translated BigQuery SQL.
    *   Implement logging to Google Cloud Logging.
    *   Address the commented-out `sed`/`sort`/`join` logic: either remove if no longer needed, or implement in Python.
    *   Review and potentially replace FOS job management calls with Airflow native features.
5.  **Testing**
    *   Unit tests for individual Python components and BigQuery SQL transformations.
    *   Integration tests for the entire Airflow DAG, ensuring correct parameter passing, execution order, and data output.
    *   Regression testing against historical data to ensure data consistency between legacy and migrated systems.
    *   Performance testing.
6.  **Deployment**
    *   Deploy Airflow DAG to a Google Cloud Composer environment.
    *   Configure necessary BigQuery datasets and tables.
    *   Set up monitoring and alerting in Google Cloud Monitoring.