# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS2

## 1. Purpose & Scope

This migration design document outlines the plan to port the `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job from its legacy UC4/KornShell/Oracle environment to Google Cloud Platform, specifically utilizing BigQuery for data processing and Airflow for orchestration.

The job's primary purpose is to update contract information in the `sof$ta_cntrct_crs2` table. It specifically excludes frame contract parents from this update, enriching existing contract records with information about their parent contracts where applicable.

## 2. Source Inventory

The `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job is composed of the following source files:

1.  **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`**
    *   **Technology:** UC4/Automic Job Definition
    *   **Summary:** UC4 UNIX job definition for updating contracts, excluding frame contract parents, by executing a ksh script.
    *   **Migration Bucket:** semi_auto

2.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** KornShell script serving as a wrapper for data reconciliation of the `ta_cntrct_crs2` table. It handles parameter parsing, environment setup, logging, error trapping, and then calls a core processing script.
    *   **Migration Bucket:** semi_auto

3.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** This KornShell script acts as a control script for data extraction, parsing parameters, sourcing utility functions, executing a SQL script, and handling errors. It manages the execution of a SQL script that likely processes data for the 'ta_cntrct_crs2' table.
    *   **Migration Bucket:** semi_auto

4.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql`**
    *   **Technology:** Oracle SQL Script
    *   **Summary:** This SQL script truncates a contract table (`sof$ta_cntrct_crs2`) and then re-populates it by selecting and joining data from another contract table (`sof$ta_cntrct_crs`), filtering out parent contracts and enriching with parent contract numbers.
    *   **Migration Bucket:** semi_auto

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform services:

*   **Orchestration:** Apache Airflow on Cloud Composer. The UC4 job definition will be translated into an Airflow DAG.
*   **Data Storage & Processing:** Google BigQuery. All relational data operations will be performed within BigQuery.
*   **Data Landing:** Cloud Storage for any intermediate or external data landing zones, if required for data ingestion from external systems.

**Target BigQuery Components:**
*   **Dataset:** `dw_bert_staging` (or similar, configurable)
*   **Tables:**
    *   `dw_bert_staging.sof_ta_cntrct_crs2` (Target table)
    *   `dw_bert_staging.sof_ta_cntrct_crs` (Source table, assuming it's also migrated to BigQuery)
    *   `dw_bert_staging.dwtk_meldungen` (Source table, assuming it's also migrated to BigQuery)

## 4. Data Flow & Lineage

The legacy job follows a sequential flow:

1.  **UC4 (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`)**: Serves as the scheduler and orchestrator. It initiates the process by invoking the `r_ausd_v_ta_cntrct_crs2.ksh` KornShell wrapper script.
2.  **KornShell Wrapper (`r_ausd_v_ta_cntrct_crs2.ksh`)**: This script handles environment setup, logging, and parameter passing. Its primary action is to call the core KornShell script, `k_ausd_v_ta_cntrct_crs2.ksh`.
3.  **KornShell Core (`k_ausd_v_ta_cntrct_crs2.ksh`)**: This script further processes parameters and executes the `d_ausd_v_ta_cntrct_crs2.sql` Oracle SQL script.
4.  **Oracle SQL (`d_ausd_v_ta_cntrct_crs2.sql`)**:
    *   **Reads From:**
        *   `sof$ta_cntrct_crs`: The primary source table containing contract data.
        *   `isbert_schema.dwtk_meldungen`: Used to determine a reference date (`v_datum`).
        *   `@pcrs1` (Carmen DB): An external Oracle database accessed via DB link, likely for additional lookup or joining data.
    *   **Writes To:** `sof$ta_cntrct_crs2`: The target table, which is truncated and then re-populated with processed contract data.

**Migrated Data Flow (Airflow DAG):**

1.  **Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs2_dag.py`)**:
    *   **Task 1 (Setup & Parameter Parsing)**: PythonOperator to mimic the parameter handling and environment setup from `r_ausd_v_ta_cntrct_crs2.ksh` and `k_ausd_v_ta_cntrct_crs2.ksh`.
    *   **Task 2 (Execute BigQuery SQL)**: BigQueryOperator to execute the translated `d_ausd_v_ta_cntrct_crs2.sql` logic. This task will involve:
        *   Truncating `dw_bert_staging.sof_ta_cntrct_crs2`.
        *   Inserting data into `dw_bert_staging.sof_ta_cntrct_crs2` from `dw_bert_staging.sof_ta_cntrct_crs` and other sources.
    *   **Task 3 (Logging & Error Handling)**: PythonOperator or built-in Airflow mechanisms for logging success/failure, similar to `r_ausd_v_ta_cntrct_crs2.ksh`'s error concept.

## 5. Transformation Logic

Each component's transformation logic will be re-engineered for the BigQuery/Airflow environment:

*   **UC4 Job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`)**:
    *   **Legacy Logic:** Orchestrates the execution of a KornShell script, passing environment variables and handling basic job properties like host, login, and error handling.
    *   **Target Logic:** Will be translated into a Python-based Airflow DAG. The DAG will define the sequence of tasks corresponding to the shell script execution and SQL processing. UC4-specific variables like `&DWH_JOB_KENNUNG` will be replaced with Airflow variables or passed as XComs.

*   **KornShell Scripts (`r_ausd_v_ta_cntrct_crs2.ksh`, `k_ausd_v_ta_cntrct_crs2.ksh`)**:
    *   **Legacy Logic:** These scripts manage the execution flow, handle command-line parameters, source common utility functions (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`), and invoke the Oracle SQL script. They also implement basic error trapping and logging.
    *   **Target Logic:** The core orchestration logic will be embedded within the Airflow DAG's Python code using PythonOperators.
        *   Parameter parsing (`getopts`) will be re-implemented in Python.
        *   Environment initialization (`. $HOME/.dw_init`) will be replaced by Airflow connection management and task-specific environment variables.
        *   Utility functions for error handling, date manipulation, and SQLPlus interaction will either be re-implemented as Python functions/modules or replaced by Airflow's native logging and error handling mechanisms (e.g., `on_failure_callback`).
        *   The invocation of the SQL script will be handled by a BigQueryOperator.

*   **Oracle SQL Script (`d_ausd_v_ta_cntrct_crs2.sql`)**:
    *   **Legacy Logic:**
        *   `TRUNCATE TABLE sof$ta_cntrct_crs2;`: Clears the target table.
        *   `INSERT INTO sof$ta_cntrct_crs2 (...) SELECT ... FROM sof$ta_cntrct_crs c, sof$ta_cntrct_crs cr WHERE c.cntrct_parent = cr.cntrct_id (+) AND cr.cntrct_ty (+) = 10 AND c.cntrct_ty <> 10;`: Populates the target table. This logic performs a self-join on `sof$ta_cntrct_crs` to identify parent contracts and filters out frame contract parents (`c.cntrct_ty <> 10`). The `(+)` indicates an Oracle-specific outer join syntax.
        *   `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`: Determines a date variable.
        *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs2');`: Calls an Oracle package to execute DDL.
    *   **Target Logic:** Will be translated into standard BigQuery SQL.
        *   `TRUNCATE TABLE dw_bert_staging.sof_ta_cntrct_crs2;`
        *   `INSERT INTO dw_bert_staging.sof_ta_cntrct_crs2 (...) SELECT ... FROM dw_bert_staging.sof_ta_cntrct_crs c LEFT OUTER JOIN dw_bert_staging.sof_ta_cntrct_crs cr ON c.cntrct_parent = cr.cntrct_id WHERE cr.cntrct_ty = 10 AND c.cntrct_ty <> 10;`: The Oracle outer join syntax `(+)` will be converted to `LEFT OUTER JOIN`. BigQuery's `PARALLEL` hints are not required as BigQuery handles parallelism automatically.
        *   The date determination logic will be converted to BigQuery SQL equivalents for `NVL`, `TO_CHAR`, `MAX`.
        *   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call will be replaced by direct BigQuery DDL.

## 6. External Dependencies

*   **Oracle Database (Carmen DB - `@pcrs1`)**: The SQL script references an external Oracle database via a DB link.
    *   **Migration Strategy:** Data from Carmen DB accessed via `@pcrs1` will need to be either:
        *   **Migrated:** If Carmen DB is also being migrated to GCP, the data will reside in BigQuery.
        *   **Federated:** If Carmen DB remains external, BigQuery's Federated Queries (e.g., via Cloud SQL for Oracle, then external table in BigQuery) or a Data Transfer Service will be required to ingest necessary data into BigQuery before the job runs.
*   **Oracle Tables (`sof$ta_cntrct_crs`, `isbert_schema.dwtk_meldungen`)**: These tables are sources for the SQL script.
    *   **Migration Strategy:** These tables should be migrated to BigQuery as `dw_bert_staging.sof_ta_cntrct_crs` and `dw_bert_staging.dwtk_meldungen` respectively, either through a full data migration or continuous data replication.
*   **KornShell Utility Scripts/Packages**: (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)
    *   **Migration Strategy:** These will be re-implemented in Python within the Airflow environment, or their functionalities will be replaced by native Airflow features.
*   **Oracle Package (`isbert_schema.DWPA_UTIL_SKRIPT`)**: Used for DDL execution.
    *   **Migration Strategy:** Will be replaced by direct BigQuery DDL commands within the Airflow BigQueryOperator.

## 7. Unresolved / Risks

*   **Incomplete UC4 Workflow:** The provided UC4 XML was a single job definition, not a complete workflow. The full scheduling details (e.g., cron schedule, dependencies on other jobs) of this `DW.BERT_AUSD_V_TA_CNTRCT_CRS2` job within the broader UC4 ecosystem are not fully captured. This may require manual investigation of the UC4 environment to ensure the migrated Airflow DAG has the correct schedule and external dependencies.
*   **Carmen DB Integration:** The reliance on `Carmen DB` via an Oracle DB link requires a clear strategy. Depending on its sensitivity and size, direct federation might introduce performance overhead or data freshness challenges compared to a full migration or regular data ingestion.
*   **Oracle-Specific SQL:** While the SQL is relatively straightforward, subtle differences in data types, function behavior, and optimization hints between Oracle and BigQuery will need careful review during translation and testing.
*   **KornShell Script Environment**: The `. $HOME/.dw_init` and other sourced scripts might contain complex environment variable settings or custom functions that are not immediately apparent from the provided code snippets. A thorough review of these helper scripts is needed to ensure all relevant logic is captured in the Python migration.
*   **UC4 Host/Login Details:** The UC4 job references `HostDst>|DWHDWH1P|HOST` and `Login>DW.UNIX.ISBERT`. These represent the execution environment and credentials. These will need to be securely configured in Airflow as connections and potentially as Kubernetes secrets if using Cloud Composer with GKE.

## 8. Build Plan

The migration will follow these steps:

1.  **Airflow DAG Construction (Python)**
    *   Create a new Airflow DAG file (`dw_bert_ausd_v_ta_cntrct_crs2_dag.py`).
    *   Define PythonOperators for parameter parsing and error handling, mimicking the KornShell wrapper and core scripts.
    *   Define a BigQueryOperator to execute the translated SQL.
    *   Configure Airflow connections for BigQuery.

2.  **BigQuery SQL Translation (BigQuery SQL)**
    *   Translate `d_ausd_v_ta_cntrct_crs2.sql` into BigQuery-compliant SQL (`d_ausd_v_ta_cntrct_crs2.bqsql`).
    *   Ensure Oracle-specific syntax (e.g., `(+)` outer join, `NVL`, `TO_CHAR`) is converted to BigQuery equivalents.
    *   Address any performance hints that might need re-evaluation for BigQuery's architecture.

3.  **BigQuery DDL (BigQuery SQL)**
    *   Generate DDL for the target table `dw_bert_staging.sof_ta_cntrct_crs2` based on the schema implied by the `INSERT` statement in the original SQL.

4.  **External Data Ingestion/Federation Setup (Varies)**
    *   Establish a mechanism to access `dw_bert_staging.sof_ta_cntrct_crs` and `dw_bert_staging.dwtk_meldungen` in BigQuery (e.g., Cloud Data Fusion, Storage Transfer Service, `bq load`).
    *   Implement the strategy for Carmen DB data access (e.g., Cloud SQL for Oracle with federated tables, or a dedicated data pipeline to ingest Carmen data into BigQuery).

5.  **Python Helper Modules (Python)**
    *   Develop Python modules for any complex logic within the KornShell utility scripts that cannot be directly replaced by Airflow features.

6.  **Testing and Validation:**
    *   Develop unit and integration tests for the Airflow DAG and BigQuery SQL.
    *   Perform data validation to ensure the migrated job produces identical or functionally equivalent results to the legacy job.