# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

## 1. Purpose & Scope
This migration job focuses on a control script (`k_ausd_v_ta_acc_ref.ksh`) that orchestrates the execution of an SQL script (`d_ausd_v_ta_acc_ref.sql`). The primary purpose of the KornShell script is to manage job execution, including parameter parsing, environment setup, error handling, ignoring already active jobs, calling the embedded SQL script, registering the job in a job table, and deactivating older active jobs. The embedded SQL script's core function is to create and maintain a local copy of `ta_acc_ref` data in the `sof$ta_acc_ref` table, sourced from `cds$ta_acc_ref` located in a remote Carmen database via a database link. The overall scope is to re-platform this existing KornShell and Oracle SQL-based ETL process to Google BigQuery.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh`
*   **Technology:** KornShell Script
*   **Complexity Tier:** Unknown (Migration notes indicate overall job complexity as medium).
*   **Automation Bucket:** semi_auto
*   **Description:** An orchestration script responsible for environment initialization, parameter validation, invoking an SQL script, and managing job states within a job control framework.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_acc_ref.sql`
*   **Technology:** Oracle SQL
*   **Complexity Tier:** Unknown (Inherits complexity from the orchestrating script, likely medium).
*   **Automation Bucket:** Assumed semi_auto (as it's a core component of the `semi_auto` job).
*   **Description:** An SQL script executed by the KornShell wrapper. It queries a system table (`dwtk_meldungen`) for a processing date, truncates a target table (`sof$ta_acc_ref`), and then populates it with filtered data from `cds$ta_acc_ref` (via a database link). It also utilizes an Oracle package (`DWPA_UTIL_SKRIPT`).

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily:
*   **Google BigQuery:** For data warehousing, storing both source data replicas, the transformed `sof_ta_acc_ref` table, and job control/logging tables.
*   **BigQuery Stored Procedures:** To encapsulate the orchestration logic previously handled by the KornShell script and to execute the core SQL transformations.
*   **Data Ingestion Service (e.g., Cloud Data Fusion, Datastream, or custom Dataflow job):** To continuously or periodically ingest data from the source Oracle Carmen database into BigQuery tables, replacing the database link functionality.
*   **Cloud Logging/Monitoring:** For centralized logging and operational oversight, replacing shell-based logging.

## 4. Data Flow & Lineage
The original data flow involves the KornShell script invoking an SQL script which reads from source tables and writes to a target table within an Oracle environment.

**Original Flow:**
1.  **Input Parameters:** `p_JobKennung`, `p_EintragsNr` are passed to `k_ausd_v_ta_acc_ref.ksh`.
2.  **KornShell Execution (`k_ausd_v_ta_acc_ref.ksh`):**
    *   Initializes environment, sources utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Validates input parameters.
    *   Manages job states (ignores active jobs, deactivates old jobs).
    *   Invokes `starteSQLSkript` function, which in turn executes `d_ausd_v_ta_acc_ref.sql`.
    *   Reads record count from a temporary file.
3.  **SQL Script Execution (`d_ausd_v_ta_acc_ref.sql`):**
    *   Reads `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen` (Oracle table) to determine a processing date (`v_datum`).
    *   Executes `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to `TRUNCATE TABLE sof$ta_acc_ref`.
    *   Reads from `cds$ta_acc_ref@pcrs1` (Oracle table via DB link) using `v_datum` for filtering on `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production = 1`.
    *   Writes/Inserts selected data into `sof$ta_acc_ref` (Oracle table).
    *   Commits the transaction.
4.  **Output:** Updated `sof$ta_acc_ref` table, console messages, and a temporary file containing record count.

**Target BigQuery Flow:**
1.  **Data Ingestion:** Source Oracle tables (`dwtk_meldungen`, `cds$ta_acc_ref`) are continuously or periodically ingested into corresponding BigQuery tables (`bq_dataset.dwtk_meldungen`, `bq_dataset.cds_ta_acc_ref`).
2.  **BigQuery Stored Procedure Execution:** A BigQuery Stored Procedure (e.g., `bq_dataset.usp_k_ausd_v_ta_acc_ref`) is called with input parameters for job identification.
    *   **Orchestration Logic (within Stored Procedure):**
        *   Receives input parameters.
        *   Performs parameter validation using BigQuery scripting.
        *   Interacts with BigQuery job control tables (`bq_dataset.job_control`, `bq_dataset.job_log`) to manage job states (e.g., updating active flags, inserting run logs).
        *   Calls the core data transformation logic.
        *   Captures and logs the number of processed records.
    *   **Data Transformation Logic (within Stored Procedure or separate SP):**
        *   Queries `bq_dataset.dwtk_meldungen` to derive the processing date.
        *   Truncates `bq_dataset.sof_ta_acc_ref`.
        *   Performs an `INSERT INTO ... SELECT FROM` operation, reading from `bq_dataset.cds_ta_acc_ref` and applying the original date and production flag filters.
        *   Handles error conditions with BigQuery `EXCEPTION WHEN ERROR` blocks.
3.  **Output:** Updated `bq_dataset.sof_ta_acc_ref` table, entries in BigQuery job control/logging tables, and BigQuery job execution logs.

## 5. Transformation Logic
**KornShell Script (`k_ausd_v_ta_acc_ref.ksh`) to BigQuery Stored Procedure:**
*   **Parameter Parsing:** The `getopts` logic for `j` (Job ID) and `f` (Entry Number) will be directly translated into BigQuery Stored Procedure input parameters (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`).
*   **Environment Variables:** Shell environment variables (`$HOME/.dw_init`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by BigQuery scripting variables, parameters, or values retrieved from a BigQuery configuration table.
*   **Error Handling:** Shell-based error checks (`if [ ! $ErrNr -eq 0 ]`) and error messaging (`DWMSG_MeldeFehler`, `echo "FEHLER: ..."`) will be replaced with BigQuery `IF/THEN/ELSE` control flow, `INSERT` statements into a BigQuery error logging table, and `SIGNAL SQLSTATE` for procedure termination.
*   **Job Management:** The logic for activating/deactivating jobs and managing their state will be re-implemented as DML operations (UPDATE/INSERT) against dedicated BigQuery job control tables.
*   **SQL Script Execution Wrapper:** The `starteSQLSkript` function call will be eliminated. Its purpose (executing the SQL) will be directly achieved by embedding the translated SQL logic within the BigQuery Stored Procedure.
*   **Temporary File Record Count:** The `eval "v_records=\`cat $tmpFile\`"` will be replaced by assigning the `COUNT(*)` result from the BigQuery `INSERT` operation or a subsequent `SELECT COUNT(*)` to a BigQuery `DECLARE` variable.

**Oracle SQL Script (`d_ausd_v_ta_acc_ref.sql`) to BigQuery SQL:**
*   **Date Determination:** The Oracle SQL `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'` will be translated to BigQuery SQL using appropriate date/timestamp functions and `IFNULL` (or `COALESCE`) for `NVL` and `FORMAT_DATE` (or `FORMAT_TIMESTAMP`) for `TO_CHAR`.
*   **Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_acc_ref')` will be translated to a direct BigQuery `TRUNCATE TABLE bq_dataset.sof_ta_acc_ref;` command. The `DWPA_UTIL_SKRIPT` package call will be analyzed, and its specific `runstatement` functionality (likely dynamic SQL execution) will be either replicated or absorbed into the direct DDL/DML.
*   **Core Data Transformation (INSERT/SELECT):** The Oracle `INSERT INTO sof$ta_acc_ref (...) SELECT ... FROM cds$ta_acc_ref &v_carmen ar WHERE ...` statement will be converted to BigQuery SQL syntax.
    *   The database link (`&v_carmen`) will be replaced by direct referencing of the `bq_dataset.cds_ta_acc_ref` table, which will be populated via a separate ingestion pipeline.
    *   Oracle `TO_DATE` calls will be replaced with BigQuery `PARSE_DATE` or equivalent.
    *   `IS NULL` and other filtering conditions will remain largely similar.
    *   `COMMIT` is implicitly handled by BigQuery transactions or removed if not required per BigQuery's auto-commit behavior for DML.

## 6. External Dependencies
*   **Oracle Database (Carmen DB):** The source `cds$ta_acc_ref` table resides in an external Oracle database, accessed via a DB link (`@pcrs1`). This dependency will be resolved by implementing a dedicated data ingestion pipeline (e.g., using Datastream for CDC, or scheduled Dataflow/Fivetran jobs for batch loading) to bring `cds$ta_acc_ref` and `dwtk_meldungen` into BigQuery.
*   **Oracle Package `isbert_schema.DWPA_UTIL_SKRIPT`:** This package is used for dynamic SQL (TRUNCATE). The specific functions called need to be understood. If simple, they will be replaced by direct BigQuery DDL/DML. If complex, they may need to be re-implemented as BigQuery functions or procedures.
*   **KornShell Utility Scripts:** Scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` provide common shell functionalities. Their equivalents will be built into the BigQuery Stored Procedure scripting logic or removed if BigQuery provides native equivalents.
*   **Temporary Filesystem:** The use of temporary files (`$DW_DIR_UTL/bert_k_ausd_v_ta_acc_ref_$$.tmp`) will be eliminated, with record counts being held in BigQuery scripting variables or logged directly to BigQuery tables.

## 7. Unresolved / Risks
*   **Unknown Complexity Tier:** The system was unable to determine a complexity tier for the individual files. This may lead to an underestimation of migration effort.
*   **Oracle-Specific `DWPA_UTIL_SKRIPT` Functionality:** The exact implementation and criticality of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` needs further investigation to ensure accurate re-implementation or replacement in BigQuery.
*   **Data Ingestion Pipeline for Oracle Sources:** The successful and timely establishment of robust data ingestion pipelines for `cds$ta_acc_ref` and `dwtk_meldungen` from Oracle to BigQuery is critical and needs to be treated as a prerequisite.
*   **Oracle SQL Dialect Conversion:** While many SQL constructs are standard, subtle differences in functions, data types, and implicit conversions between Oracle and BigQuery SQL need careful handling during translation.
*   **Job Control Table Logic:** The implicit structure and behavior of the current "job table" for `p_JobKennung` and `p_EintragsNr` for active job management needs to be explicitly defined and migrated to BigQuery.
*   **Error Handling Fidelity:** Ensuring the BigQuery error handling mimics the original KornShell/Oracle behavior (e.g., exit codes, specific messages) for downstream consumers or monitoring systems.

## 8. Build Plan
1.  **Define Target BigQuery Dataset(s):**
    *   Create a dedicated BigQuery dataset (e.g., `isbert_rpt_staging`) to host the migrated tables and stored procedures.
2.  **Schema Definition for Target Tables in BigQuery:**
    *   Create DDL for `sof_ta_acc_ref` table in BigQuery.
    *   Create DDL for `dwtk_meldungen` and `cds_ta_acc_ref` staging tables in BigQuery based on source Oracle schemas.
    *   Create DDL for job control and logging tables (e.g., `job_control`, `job_error_log`, `job_run_log`) in BigQuery.
    *(Language: BigQuery DDL)*
3.  **Implement Oracle-to-BigQuery Data Ingestion:**
    *   Design and implement a data pipeline (e.g., using Datastream for real-time CDC or Dataflow for batch ELT) to ingest data from Oracle `isbert_schema.dwtk_meldungen` and `cds$ta_acc_ref` into the respective BigQuery staging tables.
    *(Language: Python/Java for Dataflow, Datastream configuration)*
4.  **Develop BigQuery SQL for `d_ausd_v_ta_acc_ref.sql` Logic:**
    *   Translate the core `TRUNCATE` and `INSERT INTO ... SELECT FROM` logic from Oracle SQL to BigQuery SQL.
    *   Convert Oracle-specific functions (e.g., `NVL`, `TO_CHAR`, `TO_DATE`) to BigQuery equivalents.
    *   Address the `DWPA_UTIL_SKRIPT` package call by direct BigQuery DDL/DML.
    *(Language: BigQuery SQL)*
5.  **Develop BigQuery Stored Procedure for `k_ausd_v_ta_acc_ref.ksh` Orchestration:**
    *   Create a BigQuery Stored Procedure (e.g., `bq_dataset.usp_k_ausd_v_ta_acc_ref`).
    *   Implement input parameters, validation, and error handling using BigQuery scripting.
    *   Integrate the BigQuery SQL transformation logic developed in step 4 into this stored procedure.
    *   Implement logic for job control (reading/writing to `job_control`, `job_run_log` tables).
    *(Language: BigQuery SQL (Stored Procedure))*.
6.  **Develop Orchestration (Optional, if external scheduling is needed):**
    *   If the original script was part of a larger scheduler (e.g., UC4), create a Cloud Composer DAG or Google Cloud Workflow to schedule and execute the BigQuery Stored Procedure.
    *(Language: Python (for Airflow DAGs) or YAML (for Workflows))*.
7.  **Testing and Validation:**
    *   Unit test the BigQuery SQL transformation.
    *   Integration test the full BigQuery Stored Procedure.
    *   Perform data validation and reconciliation between the Oracle source and BigQuery target.
    *(Language: SQL, Python for test scripts)*.