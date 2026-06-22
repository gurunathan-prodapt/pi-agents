# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh

## 1. Purpose & Scope
This job, rooted in `r_ausd_v_ta_notice.ksh`, is an ETL workflow designed for the reconciliation of contract data related to the `ta_notice` table. It operates by orchestrating the execution of a core SQL script responsible for data transformation.

The main purpose of the `r_ausd_v_ta_notice.ksh` script is to:
*   Set up the execution environment.
*   Parse command-line parameters.
*   Handle job-specific logging and error management.
*   Invoke a control script (`k_ausd_v_ta_notice.ksh`) which, in turn, executes the core data processing SQL script (`d_ausd_v_ta_notice.sql`).

The scope of this migration is to translate the existing KornShell orchestration and Oracle SQL*Plus data manipulation logic to a BigQuery-native solution, leveraging BigQuery's data warehousing and SQL scripting capabilities, potentially with Cloud Composer for orchestration.

## 2. Source Inventory
This job comprises three main components:

| File Name                                                               | Technology       | Category | Summary                                                                                                                                                                                                                                                                | Assessed Tier | Migration Bucket |
| :---------------------------------------------------------------------- | :--------------- | :------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------ | :--------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh` | KornShell Script | Shell    | Wrapper/orchestrator for a core data processing script, handling parameter parsing, error logging, and job status management for the `ta_notice` table. Invokes `k_ausd_v_ta_notice.ksh`.                                                                              | Medium        | Semi-Auto (B2)   |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` | KornShell Script | Shell    | Control script, invoked by `r_ausd_v_ta_notice.ksh`, handling further environment setup, parameter passing, and orchestrating the execution of the SQL script `d_ausd_v_ta_notice.sql`. It relies on external shell libraries for SQL execution.                  | Medium        | Semi-Auto (B2)   |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql` | Oracle SQL*Plus  | SQL      | Core data transformation script. It truncates a target table (`sof$ta_notice`) and then inserts data into it from a source table (`cds$ta_notice`), applying filters based on dates and a production flag. It utilizes a database link and an Oracle package. | Medium        | Semi-Auto (B2)   |

*(Note: `file_complexity` and `automation_rate` tables returned no rows for this job. Tier and Migration Bucket are assessed based on the complexity revealed by `file_analysis` summaries and code content analysis, suggesting semi-automated translation for shell orchestration and SQL dialect conversion.)*

## 3. Target Architecture
The migration target platform is Google Cloud BigQuery.

*   **Orchestration:** The KornShell scripts (`r_ausd_v_ta_notice.ksh`, `k_ausd_v_ta_notice.ksh`) will be migrated to a Cloud Composer (Airflow) DAG. This DAG will handle environment setup, parameter passing, logging, error handling, and the sequential execution of the data transformation logic.
*   **Data Transformation:** The Oracle SQL*Plus script (`d_ausd_v_ta_notice.sql`) will be translated into BigQuery Standard SQL, likely implemented as a BigQuery Stored Procedure.
*   **Data Storage:** All source and target tables (`DWTK_MELDUNGEN`, `CDS$TA_NOTICE`, `SOF$TA_NOTICE`, `VIA`) will reside in BigQuery datasets.
*   **Logging & Monitoring:** Cloud Logging will capture execution logs. Job auditing and status tracking will be managed through dedicated BigQuery audit tables.
*   **External Dependencies:** The Oracle database link and package will be addressed. Data from the `pcrs1` remote database will need to be ingested into BigQuery (e.g., via Cloud Data Fusion, Dataflow, or a direct transfer service) if not already present. The `DWPA_UTIL_SKRIPT` package functionality will be reimplemented in BigQuery SQL or Python within the orchestration layer.

## 4. Data Flow & Lineage

The overall data flow for this job is as follows:

1.  **`r_ausd_v_ta_notice.ksh` (Legacy Wrapper)**:
    *   Acts as the main entry point, setting up the environment, handling parameters, and initializing logging.
    *   **INVOKES** `k_ausd_v_ta_notice.ksh`.

2.  **`k_ausd_v_ta_notice.ksh` (Legacy Control Script)**:
    *   Receives job parameters from the wrapper.
    *   Further initializes the environment and calls an external function (`starteSQLSkript` from `h_alis_sqlplus.ksh`) to execute the core SQL logic.
    *   **INVOKES** `d_ausd_v_ta_notice.sql`.

3.  **`d_ausd_v_ta_notice.sql` (Legacy Core Transformation)**:
    *   **READS_TABLE** `isbert_schema.dwtk_meldungen` to determine the `v_datum` processing date.
    *   **READS_TABLE** `cds$ta_notice@pcrs1` (via DB link) as the primary source for contract notices.
    *   **WRITES_TABLE** `sof$ta_notice` (target table), first truncating it, then inserting filtered data.
    *   **WRITES_TABLE** `VIA` (merge via `DWPA_UTIL_SKRIPT` package - direct DML not explicitly in provided SQL, but identified by lineage analysis). This might be a placeholder in the `DWPA_UTIL_SKRIPT.runstatement` call.
    *   **USES_PACKAGE** `isbert_schema.DWPA_UTIL_SKRIPT` (for statements like `TRUNCATE TABLE sof$ta_notice` and potentially the `VIA` merge).

**Migrated Data Flow (BigQuery):**

*   **Cloud Composer DAG**: Will orchestrate the entire flow.
    *   **Task 1 (Setup & Parameter Handling)**: PythonOperator or BigQueryOperator to derive parameters and initialize BigQuery audit logs, replacing `r_ausd_v_ta_notice.ksh`'s wrapper functions.
    *   **Task 2 (Core Logic Execution)**: BigQueryOperator calling the BigQuery Stored Procedure that replaces `k_ausd_v_ta_notice.ksh` and `d_ausd_v_ta_notice.sql`.
*   **BigQuery Stored Procedure (`sp_process_ta_notice`)**:
    *   **Reads**: `project.dataset.dwtk_meldungen` to derive `v_datum`.
    *   **Reads**: `project.dataset.cds_ta_notice` (after data ingestion from `pcrs1`).
    *   **Writes**: `project.dataset.sof_ta_notice` (full refresh).
    *   **Writes**: `project.dataset.via` (if the `DWPA_UTIL_SKRIPT` functionality involves this table, it will be translated).
*   **BigQuery Audit Tables**: For tracking job execution, status, and errors.

## 5. Transformation Logic
### `r_ausd_v_ta_notice.ksh` (Wrapper)
*   **Original Logic**: Shell environment setup, `getopts` for parameter parsing (`-h`, `-j`, `-f`), logging initialization with `DWMSG_` functions, and a direct call to `k_ausd_v_ta_notice.ksh`. Error traps are set.
*   **BigQuery Migration**: This will be primarily replaced by a Cloud Composer DAG.
    *   Parameters (`-j`, `-f`) will become Airflow DAG parameters or BigQuery Stored Procedure input parameters.
    *   Environment sourcing (`.dw_init`) will be managed by the Composer environment or BigQuery connection settings.
    *   `DWMSG_` logging functions will be replaced by BigQuery audit table inserts and Cloud Logging.
    *   Error trapping (`trap`) will be handled by Airflow's task failure mechanisms and BigQuery's `BEGIN...EXCEPTION` blocks.
    *   The `k_ausd_v_ta_notice.ksh` call will be a BigQuery Stored Procedure invocation within the DAG.

### `k_ausd_v_ta_notice.ksh` (Control Script)
*   **Original Logic**: Parses `j:` and `f:` parameters, validates them, defines `v_TabName='ta_notice'`, and calls `starteSQLSkript` to execute `d_ausd_v_ta_notice.sql`. It also reads the number of records from a temporary file.
*   **BigQuery Migration**: This logic will be integrated into the BigQuery Stored Procedure that contains the translated SQL.
    *   Parameters `p_JobKennung` and `p_EintragsNr` will be input arguments to the BigQuery Stored Procedure.
    *   Parameter validation will be implemented with `IF` conditions and `RAISE` or error logging `INSERT` statements.
    *   The `starteSQLSkript` call will be direct execution of the translated BigQuery SQL within the stored procedure.
    *   Reading from a temporary file for record count will be replaced by `SELECT COUNT(*)` or by retrieving `@@row_count` after the `INSERT` statement.

### `d_ausd_v_ta_notice.sql` (Core Transformation)
*   **Original Logic**:
    1.  `DEFINE v_carmen = "@pcrs1"`: Oracle DB link definition.
    2.  Derive `v_datum` from `MAX(timecreated)` in `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. Falls back to `19000101`.
    3.  `TRUNCATE TABLE sof$ta_notice` via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    4.  `INSERT` into `sof$ta_notice` from `cds$ta_notice@pcrs1` with conditions:
        *   `n.insert_at <= TO_DATE('&v_datum','YYYYMMDD')`
        *   `(n.modified_at IS NULL OR n.modified_at > TO_DATE('&v_datum','YYYYMMDD'))`
        *   `(n.valid_to IS NULL OR n.valid_to > TO_DATE('&v_datum','YYYYMMDD'))`
        *   `n.is_production = 1`
    5.  `commit;`
*   **BigQuery Migration (within a Stored Procedure)**:
    1.  The `@pcrs1` DB link will be replaced by direct table references in BigQuery, assuming `cds$ta_notice` data is ingested.
    2.  `v_datum` derivation:
        ```sql
        DECLARE v_datum DATE DEFAULT (
          SELECT COALESCE(DATE(MAX(timecreated)), DATE '1900-01-01')
          FROM `project.dataset.dwtk_meldungen`
          WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        ```
    3.  `TRUNCATE TABLE sof$ta_notice`:
        ```sql
        TRUNCATE TABLE `project.dataset.sof_ta_notice`;
        ```
    4.  `INSERT` statement translation:
        *   `TO_DATE` functions will be replaced by BigQuery's `DATE()`, `PARSE_DATE()`, or `CAST()`.
        *   `NVL` will become `COALESCE` or `IFNULL`.
        *   The filtering logic remains similar.
        ```sql
        INSERT INTO `project.dataset.sof_ta_notice` (
          cntrct_id, valid_from, valid_to, entry_date_of_notice
        )
        SELECT
          n.cntrct_id, n.valid_from, n.valid_to, n.entry_date_of_notice
        FROM `project.dataset.cds_ta_notice` AS n
        WHERE DATE(n.insert_at) <= v_datum
          AND (n.modified_at IS NULL OR DATE(n.modified_at) > v_datum)
          AND (n.valid_to IS NULL OR DATE(n.valid_to) > v_datum)
          AND n.is_production = 1;
        ```
    5.  `commit;` is implicitly handled by BigQuery's transactional behavior for DML statements.

## 6. External Dependencies
### Original System External Dependencies:
*   **Oracle Database (`pcrs1`)**: The `cds$ta_notice@pcrs1` table is accessed via a database link.
*   **Oracle Package (`isbert_schema.DWPA_UTIL_SKRIPT`)**: Used for executing DDL (like `TRUNCATE TABLE`) and potentially other operations (like `MERGE VIA`).
*   **Filesystem (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/...*.ksh` utilities, temporary files, `trace.sql.cfg`, spool files)**: For environment setup, logging, and intermediate data exchange.

### Target System Replacement:
*   **Oracle Database (`pcrs1`)**: Data from `cds$ta_notice` must be ingested into a BigQuery table (e.g., `project.dataset.cds_ta_notice`) using a suitable data ingestion strategy (e.g., one-time batch load, CDC, or scheduled ETL with Cloud Data Fusion). The DB link concept is not directly transferable.
*   **Oracle Package (`isbert_schema.DWPA_UTIL_SKRIPT`)**: The specific functionalities used from this package (e.g., `runstatement`) will be reimplemented using BigQuery DDL statements directly in the stored procedure (e.g., `TRUNCATE TABLE`). If `DWPA_UTIL_SKRIPT` performs complex logic for `VIA` merges, that logic will need to be translated to BigQuery SQL as well.
*   **Filesystem Dependencies**:
    *   Environment variables (`$HOME/.dw_init`, `BERT_DIR_ROOT`) will be replaced by BigQuery Stored Procedure parameters, Cloud Composer environment variables, or BigQuery connection configurations.
    *   KornShell utility scripts will be replaced by Python modules within Cloud Composer or BigQuery SQL scripting constructs.
    *   Temporary files for record counts will be replaced by BigQuery variables or direct query results.
    *   SQL*Plus `SPOOL` and `trace.sql.cfg` will be replaced by Cloud Logging and BigQuery job history, with optional custom audit tables in BigQuery.

## 7. Unresolved / Risks
*   **Empty `file_complexity` and `automation_rate`**: The lack of pre-calculated complexity tiers and automation rates means that the initial assessment of "Medium" complexity and "Semi-Auto" migration bucket is based on manual code review. There might be hidden complexities not immediately apparent.
*   **`DWPA_UTIL_SKRIPT` Package Logic**: The exact implementation of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` and any other operations performed by this package (specifically concerning the `VIA` table mentioned in lineage) needs further investigation to ensure complete and accurate translation.
*   **Data Type Mismatches**: Oracle-specific data types (e.g., `NUMBER`, `DATE` with specific formats) might have subtle differences in BigQuery (e.g., `BIGNUMERIC`, `DATE`, `TIMESTAMP`) that require careful handling during schema migration and SQL translation to avoid data loss or precision issues.
*   **Performance Differences**: Oracle query hints and specific execution plans do not directly translate to BigQuery. The translated BigQuery SQL might require performance tuning and optimization to match or exceed original performance.
*   **`pcrs1` Data Ingestion**: The strategy for ingesting data from the remote Oracle `pcrs1` system into BigQuery needs to be defined and implemented. This could be a significant sub-project if not already addressed.
*   **Parameter `s:` and `l:` in `r_ausd_v_ta_notice.ksh`**: The original shell script accepts these parameters but does not explicitly handle them. Their intended purpose, if any, needs to be clarified to avoid feature loss.

## 8. Build Plan
The migration build plan will proceed as follows:

1.  **Schema Migration & Data Ingestion (Manual/Automated)**
    *   Migrate source tables (`DWTK_MELDUNGEN`, `CDS$TA_NOTICE`) and target tables (`SOF$TA_NOTICE`, `VIA`) schemas from Oracle to BigQuery.
    *   Establish a data ingestion pipeline for `CDS$TA_NOTICE` from `pcrs1` (Oracle) to `project.dataset.cds_ta_notice` (BigQuery).

2.  **BigQuery SQL Stored Procedure Development (`sp_process_ta_notice.sql`) (BQSQL)**
    *   Translate `d_ausd_v_ta_notice.sql` into a BigQuery Standard SQL Stored Procedure.
    *   Implement the `v_datum` derivation using `DECLARE` and `COALESCE`.
    *   Replace `TRUNCATE TABLE` calls and `INSERT ... SELECT` statements with their BigQuery equivalents.
    *   Translate the logic of `DWPA_UTIL_SKRIPT` as it pertains to this job (e.g., `VIA` table interaction).
    *   Include error handling (`BEGIN...EXCEPTION`) and logging to a BigQuery audit table.

3.  **BigQuery Audit Table DDL (`audit_tables.sql`) (BQSQL)**
    *   Create DDL for job execution logging and error tables (e.g., `project.dataset.job_audit`, `project.dataset.job_error_log`).

4.  **Cloud Composer DAG Development (`r_ausd_v_ta_notice_dag.py`) (Python)**
    *   Create an Airflow DAG in Python.
    *   Define tasks for:
        *   Initializing job run (e.g., inserting into `job_audit` table).
        *   Invoking the BigQuery Stored Procedure (`sp_process_ta_notice`) using `BigQueryOperator`.
        *   Handling success/failure logging and updates in `job_audit` table.
        *   Pass parameters (`p_JobKennung`, `p_EintragsNr`) to the stored procedure.

5.  **Configuration Management (`config.yaml`) (YAML/JSON)**
    *   Define configuration parameters such as BigQuery project and dataset IDs, table names, and any other runtime variables for the Airflow DAG and BigQuery Stored Procedures.

6.  **Deployment (GCP Deployment)**
    *   Deploy the BigQuery Stored Procedure.
    *   Deploy the BigQuery audit tables.
    *   Deploy the Cloud Composer DAG to an Airflow environment.

7.  **Testing and Validation (Manual/Automated)**
    *   Conduct thorough unit, integration, and user acceptance testing to ensure data accuracy and functional equivalence with the legacy system.
    *   Verify logging and error handling mechanisms.