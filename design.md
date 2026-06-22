# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Purpose & Scope
This job, originally defined in UC4 as `DW.BERT_AUSD_V_TA_CNTRCT_CRS3`, is responsible for synchronizing and updating contract data within the data warehouse. Specifically, it populates the `ta_cntrct_crs3` table with contract information, including details related to "twin-bill" contracts. The overall business purpose is to maintain an up-to-date view of contract relationships and attributes for reporting and analytical consumption.

The scope of this migration covers the conversion of the UC4 scheduler object, its associated KornShell wrapper scripts, and the core Oracle SQL transformation logic to a Google Cloud Platform (GCP) native environment, primarily utilizing Airflow for orchestration and BigQuery for data storage and transformation.

## 2. Source Inventory

The job consists of four primary components:

*   **`DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`**
    *   **Relative Path**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`
    *   **Technology**: UC4/Automic Job Definition (JOBS_UNIX)
    *   **Description**: The top-level scheduler object that initiates the contract data update process. It calls a KornShell script on a Unix host.
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto

*   **`r_ausd_v_ta_cntrct_crs3.ksh`**
    *   **Relative Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh`
    *   **Technology**: KornShell Script
    *   **Description**: A wrapper script that handles parameter parsing, environment setup, error logging, and then orchestrates the execution of a core data processing script (`k_ausd_v_ta_cntrct_crs3.ksh`).
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto

*   **`k_ausd_v_ta_cntrct_crs3.ksh`**
    *   **Relative Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh`
    *   **Technology**: KornShell Script
    *   **Description**: A control script called by `r_ausd_v_ta_cntrct_crs3.ksh`. It further sets up the environment and executes the primary Oracle SQL script (`d_ausd_v_ta_cntrct_crs3.sql`). It also manages job activation/deactivation.
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto

*   **`d_ausd_v_ta_cntrct_crs3.sql`**
    *   **Relative Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql`
    *   **Technology**: Oracle SQL
    *   **Description**: The core data transformation script. It truncates the target table `sof$ta_cntrct_crs3` and populates it with contract data, including twin-bill information, by joining and unioning data from `sof$ta_cntrct_crs2` and retrieving a date from `isbert_schema.dwtk_meldungen`.
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto

## 3. Target Architecture
The target architecture on GCP will leverage managed services for scheduling, computation, and data warehousing:

*   **Orchestration**: Apache Airflow (Cloud Composer) will replace UC4 for scheduling and workflow management.
*   **Computation**: The KornShell script logic will be re-implemented in Python. If significant data manipulation beyond SQL execution is required, this will run on Dataproc as a PySpark job. If the Python logic is primarily for orchestration and running SQL, it could be executed directly via an Airflow PythonOperator or BashOperator interacting with BigQuery. For this design, a DataprocSubmitJobOperator is assumed based on the UC4 conversion tool output.
*   **Data Warehousing**: Google BigQuery will serve as the target data store, replacing the Oracle database tables.
*   **Logging & Monitoring**: Cloud Logging and Cloud Monitoring will replace existing error logging mechanisms.
*   **Storage**: Google Cloud Storage (GCS) will be used for storing Python/PySpark scripts and any temporary files, replacing Unix file system dependencies.

## 4. Data Flow & Lineage

The current data flow in the legacy system is:
1.  **UC4 Job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`)**: Triggers the execution.
2.  **Wrapper KornShell Script (`r_ausd_v_ta_cntrct_crs3.ksh`)**: Executed by the UC4 job. It sets up the environment and calls the control script.
3.  **Control KornShell Script (`k_ausd_v_ta_cntrct_crs3.ksh`)**: Executed by the wrapper script. It further prepares the environment and executes the Oracle SQL script.
4.  **Oracle SQL Script (`d_ausd_v_ta_cntrct_crs3.sql`)**:
    *   **Reads from**: `isbert_schema.dwtk_meldungen` (Oracle Table)
    *   **Reads from**: `sof$ta_cntrct_crs2` (Oracle Table)
    *   **Writes to**: `sof$ta_cntrct_crs3` (Oracle Table)

The proposed target data flow in GCP BigQuery will be:
1.  **Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3`)**: The primary scheduler.
2.  **Airflow Task (e.g., `DataprocSubmitJobOperator` executing `r_ausd_v_ta_cntrct_crs3.py`)**: This task will encapsulate the combined logic of the wrapper and control KornShell scripts, executing the BigQuery SQL.
3.  **BigQuery SQL (derived from `d_ausd_v_ta_cntrct_crs3.sql`)**:
    *   **Reads from**: `isbert_schema.dwtk_meldungen` (BigQuery Table)
    *   **Reads from**: `sof.ta_cntrct_crs2` (BigQuery Table)
    *   **Writes to**: `sof.ta_cntrct_crs3` (BigQuery Table)

The execution order will be sequential: Airflow DAG triggers the Python/PySpark task, which then executes the BigQuery SQL.

## 5. Transformation Logic

*   **UC4 XML (`DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`)**:
    *   **Legacy**: Defines a `JOBS_UNIX` object triggering a shell script. Contains basic job properties, scheduling (implicit, as no explicit scheduler object was provided), and execution host/login.
    *   **Target**: Converted to an Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3.py`). This DAG will have a single task, `bert_ausd_v_ta_cntrct_crs3`, implemented using a `DataprocSubmitJobOperator`. The DAG will run on a manual trigger initially (`schedule=None`) with no retries or retry delays by default, as per the UC4 design tool analysis.

*   **KornShell Scripts (`r_ausd_v_ta_cntrct_crs3.ksh`, `k_ausd_v_ta_cntrct_crs3.ksh`)**:
    *   **Legacy**: These scripts primarily handle environment sourcing (`. $HOME/.dw_init`), error handling (`f_alis_msgerr.ksh`), parameter parsing, and executing the Oracle SQL script via `h_alis_sqlplus.ksh`. They also involve temporary file creation (`tmpFile`) and spooling.
    *   **Target**: The functionality of these scripts will be re-implemented in a Python/PySpark script (e.g., `r_ausd_v_ta_cntrct_crs3.py`). This script will:
        *   Parse parameters passed from Airflow (e.g., `JobKennung`, `EintragsNr`).
        *   Handle error logging using Python's logging module, integrating with Cloud Logging.
        *   Execute the converted BigQuery SQL using a BigQuery client library (e.g., `google-cloud-bigquery`).
        *   Manage any temporary data in GCS if needed, replacing local file system operations.

*   **Oracle SQL (`d_ausd_v_ta_cntrct_crs3.sql`)**:
    *   **Legacy**:
        *   Uses Oracle-specific `DEFINE`, `COLUMN new_value`, `NVL`, `TO_CHAR` for date formatting, `/*+ PARALLEL(...) */` hints, `WHENEVER SQLERROR`, `START ../trace.sql.cfg`, `SPOOL`, `begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs3'); end; /`, and Oracle's implicit outer join `(+)`.
        *   The core logic involves an `INSERT` statement with a `UNION` of two `SELECT` statements, joining `sof$ta_cntrct_crs2` with itself to identify twin-bill contracts and filtering by `cntrct_ty`.
    *   **Target (BigQuery SQL)**:
        *   `DEFINE v_carmen` and `v_datum` logic will be converted to BigQuery `DECLARE` and `SET` statements.
        *   `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` will become `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')` assuming `timecreated` is a timestamp type.
        *   `TRUNCATE TABLE sof$ta_cntrct_crs3` will be directly translated to `TRUNCATE TABLE \`sof.ta_cntrct_crs3\``. The Oracle stored procedure call for truncation will be replaced.
        *   Oracle outer join `(+)` will be explicitly converted to `LEFT JOIN`.
        *   `UNION` will be translated to `UNION DISTINCT` (BigQuery's default `UNION` behavior).
        *   Oracle `/*+ PARALLEL(...) */` hints will be removed as BigQuery manages query parallelism automatically.
        *   `SPOOL`, `START`, `WHENEVER SQLERROR`, `COMMIT` will be removed as they are client-side or specific to Oracle SQLPlus. Error handling and transaction management will be handled by the orchestrating Python/Airflow code.
        *   All table names will be updated to BigQuery's `project.dataset.table` format (e.g., `isbert_schema.dwtk_meldungen` becomes `isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2` becomes `sof.ta_cntrct_crs2`, and `sof$ta_cntrct_crs3` becomes `sof.ta_cntrct_crs3` assuming `isbert_schema` and `sof` are the target BigQuery datasets).

## 6. External Dependencies

*   **Oracle Database**: The entire Oracle database hosting `isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2`, and `sof$ta_cntrct_crs3` is an external dependency. These tables need to be migrated to BigQuery.
    *   **Replacement**: The tables will be ingested into Google BigQuery.
*   **Unix Host (`|DWHDWH1P|HOST`)**: The environment where the UC4 job executes the KornShell scripts.
    *   **Replacement**: Replaced by Google Cloud Composer (Airflow) for scheduling and potentially Google Dataproc for executing the refactored KornShell logic as a PySpark job.
*   **UC4 Login (`DW.UNIX.ISBERT`)**: The user context under which the scripts execute.
    *   **Replacement**: Replaced by GCP service accounts with appropriate IAM roles for BigQuery, Dataproc, and GCS access.
*   **Helper KornShell Scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`)**: These are implicit external dependencies as they are sourced by the main KornShell scripts.
    *   **Replacement**: Their functionalities will be re-implemented in Python as part of the refactored script. For instance, parameter handling will use Python's `argparse`, date utilities will use Python's `datetime` module, and BigQuery SQL execution will use the `google-cloud-bigquery` library. Logging will integrate with Cloud Logging.
*   **Oracle Stored Procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`)**: Used for truncating the table.
    *   **Replacement**: Direct `TRUNCATE TABLE` statement in BigQuery SQL.

## 7. Unresolved / Risks

*   **Unresolved References**:
    *   **`v_carmen` DB Link**: The Oracle SQL defines `DEFINE v_carmen = "@pcrs1"`. While not directly used in the provided SQL, if `sof$ta_cntrct_crs2` or `isbert_schema.dwtk_meldungen` are actually views or tables that rely on this DB link to Carmen, then the Carmen database itself is an upstream dependency that needs to be migrated or established with a connection to GCP.
    *   **`DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`**: These were included in the UC4 XML as `:inc` directives but their content or exact functionality was not retrieved. It's assumed they are environment or logging related and will be addressed in the Python refactoring.
*   **Risks**:
    *   **Data Type Mismatches**: Subtle differences in Oracle and BigQuery data type handling (e.g., date formats, numeric precision) could lead to data inconsistencies if not carefully mapped and validated during migration.
    *   **Performance Differences**: While BigQuery is generally performant, the `PARALLEL` hints in Oracle suggest large data volumes. Performance testing will be crucial to ensure the migrated BigQuery SQL queries perform as expected or better.
    *   **Complexity of Helper Script Translation**: The helper `.ksh` scripts need detailed analysis. If they contain complex logic beyond basic utility functions, their translation to Python might be more involved than anticipated.
    *   **Testing and Validation**: Comprehensive testing of the migrated SQL and orchestration will be essential to ensure functional equivalence and data integrity.

## 8. Build Plan

The migration will follow these ordered steps:

1.  **BigQuery Table Schema Definition (Target)**:
    *   Define the BigQuery schemas for `isbert_schema.dwtk_meldungen`, `sof.ta_cntrct_crs2`, and `sof.ta_cntrct_crs3` based on their Oracle counterparts.
    *   **Language**: BigQuery DDL.

2.  **Data Ingestion (Legacy Oracle to BigQuery)**:
    *   Migrate historical and ongoing data from Oracle tables `isbert_schema.dwtk_meldungen` and `sof$ta_cntrct_crs2` into their respective BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_cntrct_crs2`).
    *   **Language**: Data Migration Tools (e.g., Cloud Data Transfer Service, Dataflow, or manual export/import).

3.  **BigQuery SQL Transformation Script Development**:
    *   Convert the Oracle SQL script `d_ausd_v_ta_cntrct_crs3.sql` into a BigQuery-compatible SQL script (`d_ausd_v_ta_cntrct_crs3.bqsql`). This will include adapting syntax for date functions, `TRUNCATE`, outer joins, and removing Oracle-specific hints and scripting elements.
    *   **Language**: BigQuery SQL.

4.  **Python Script Refactoring (KornShell Logic)**:
    *   Develop a Python script (e.g., `r_ausd_v_ta_cntrct_crs3.py`) to encapsulate the logic from `r_ausd_v_ta_cntrct_crs3.ksh` and `k_ausd_v_ta_cntrct_crs3.ksh`. This script will:
        *   Handle parameter parsing.
        *   Implement error handling and logging.
        *   Execute the `d_ausd_v_ta_cntrct_crs3.bqsql` script in BigQuery.
        *   Address functionality of sourced helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) in Python.
    *   **Language**: Python.

5.  **Airflow DAG Development**:
    *   Create an Airflow DAG file (`dw_bert_ausd_v_ta_cntrct_crs3.py`) that uses a `DataprocSubmitJobOperator` (or BigQueryOperator if the Python script directly runs BQ SQL) to invoke the `r_ausd_v_ta_cntrct_crs3.py` script.
    *   Define DAG properties such as `dag_id`, `schedule`, `start_date`, `default_args` as per the Airflow design.
    *   **Language**: Python.

6.  **Deployment and Testing**:
    *   Deploy the BigQuery tables, Python scripts, and Airflow DAG to their respective GCP environments.
    *   Conduct thorough unit, integration, and user acceptance testing to ensure data accuracy and functional equivalence with the legacy system.
    *   Validate performance and resource utilization in BigQuery.
    *   **Language**: GCP Deployment Tools, Testing Frameworks.