# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh

## 1. Purpose & Scope
This job, orchestrated by the KornShell script `k_ausd_bp_ta_bpr_optionen.ksh`, is responsible for preparing and populating data related to "Basisprodukt" (base product) tariff options. The script acts as a control flow, handling environment setup, parameter parsing and validation, and the execution of a core SQL script. The primary function is to extract or derive tariff option data from a source table (`sof$ta_bpr_instance`) and load it into a target table (`sof$ta_bpr_optionen`) after truncating the latter.

The job ensures data consistency by validating input parameters and date formats. It also incorporates error handling and basic logging mechanisms. This process appears to be a daily or scheduled batch process for updating product option master data or similar reference data.

## 2. Source Inventory
The job consists of two primary files:
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh`**
  - **Technology:** KornShell Script
  - **Purpose:** ETL Orchestration, Parameter Handling, Date Validation, SQL Script Execution.
  - **Tier:** (Unknown - `file_complexity` returned no rows, but based on logic, likely Medium due to multiple external script dependencies and parameter handling).
  - **Automation Bucket:** (Unknown - `automation_rate` returned no rows, but with a clear migration path to BQ Stored Procedures and Python orchestration, likely B1: Auto or B2: Semi-Auto).
  - **Summary:** KornShell control script that handles input parameters (`-j` Jobkennung, `-f` EintragsNr, `-s` Stichtag, `-l` Wiederanlaufwert), validates dates, sets up the environment, and invokes the `d_ausd_bp_ta_bpr_optionen.sql` script to perform data manipulation. It sources several utility shell scripts for error handling, date checking, parameter parsing, and SQL*Plus interaction. It also determines "yesterday" and "today" dates.

- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_optionen.sql`**
  - **Technology:** Oracle SQL / SQL*Plus script
  - **Purpose:** Data Transformation and Loading
  - **Tier:** (Unknown - implicitly Medium, as it's a critical data load component.)
  - **Automation Bucket:** (Unknown - implicitly B1: Auto, as it's directly translatable to BQSQL).
  - **Summary:** An Oracle SQL script executed by `k_ausd_bp_ta_bpr_optionen.ksh`. It defines a date variable from `isbert_schema.dwtk_meldungen`, truncates the `sof$ta_bpr_optionen` table, and then inserts all records from `sof$ta_bpr_instance` into `sof$ta_bpr_optionen`. It uses SQL*Plus commands for variable definition, tracing, and error handling.

**Dependencies (sourced/invoked by ksh script):**
- `$HOME/.dw_init` (environment initialization)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date validation)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus interaction helpers)
- `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (determines yesterday's and today's dates)

## 3. Target Architecture
The job will be migrated to Google Cloud Platform, utilizing BigQuery for data storage and SQL execution, and Cloud Composer (Apache Airflow) or Google Cloud Workflows for orchestration.

- **BigQuery:**
    - **Source Tables:** `isbert_schema.dwtk_meldungen` and `sof$ta_bpr_instance` will be ingested into BigQuery as `project.isbert_schema.dwtk_meldungen` and `project.dataset.sof_ta_bpr_instance` respectively. The `project` and `dataset` placeholders will be replaced with actual GCP project ID and BigQuery dataset names.
    - **Target Table:** `sof$ta_bpr_optionen` will be migrated to BigQuery as `project.dataset.sof_ta_bpr_optionen`.
    - **Stored Procedure:** The combined logic of `k_ausd_bp_ta_bpr_optionen.ksh` and `d_ausd_bp_ta_bpr_optionen.sql` will be implemented as a BigQuery SQL stored procedure, e.g., `project.dataset.sp_d_ausd_bp_ta_bpr_optionen`.
    - **Helper Tables:** Dedicated tables for error logging (`project.dataset.error_log`) and job logging (`project.dataset.job_log`) will be created in BigQuery to replace shell-based logging and temporary files.

- **Orchestration:**
    - **Cloud Composer (Airflow) / Google Cloud Workflows:** A Python-based orchestration layer will invoke the BigQuery stored procedure. This layer will be responsible for defining parameters, calling the stored procedure, and potentially handling retry logic or external dependencies not directly managed by BigQuery.

## 4. Data Flow & Lineage
The original data flow is as follows:

1. **`k_ausd_bp_ta_bpr_optionen.ksh` (Shell Script):**
   - Reads parameters: `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
   - Sources utility scripts for environment setup, error handling, date validation, parameter parsing, and SQL*Plus interaction.
   - Invokes `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
   - Executes `d_ausd_bp_ta_bpr_optionen.sql` via a `starteSQLSkript` helper function (implicitly using SQL*Plus).
   - Captures the number of processed records in a temporary file (`tmpFile`).

2. **`d_ausd_bp_ta_bpr_optionen.sql` (SQL Script):**
   - **Reads from:** `isbert_schema.dwtk_meldungen` (to determine `v_datum`).
   - **Reads from:** `sof$ta_bpr_instance`.
   - **Writes to:** `sof$ta_bpr_optionen` (after truncating).
   - Uses Oracle package `isbert_schema.DWPA_UTIL_SKRIPT` for `TRUNCATE TABLE`.

**Migrated Data Flow:**

1. **Orchestration Layer (Python / Cloud Composer / Workflows):**
   - Receives or determines input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
   - Invokes the BigQuery Stored Procedure `project.dataset.sp_d_ausd_bp_ta_bpr_optionen` with these parameters.
   - Handles overall job scheduling and monitoring.

2. **`project.dataset.sp_d_ausd_bp_ta_bpr_optionen` (BigQuery Stored Procedure):**
   - **Input Parameters:** `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
   - **Internal Logic:**
     - Parameter validation and error logging to `project.dataset.error_log`.
     - Date validation for `p_Stichtag`.
     - Derives `v_datum` from `project.isbert_schema.dwtk_meldungen`.
     - `TRUNCATE TABLE project.dataset.sof_ta_bpr_optionen;`
     - `INSERT INTO project.dataset.sof_ta_bpr_optionen SELECT cntrct_id, bpr_id FROM project.dataset.sof_ta_bpr_instance;`
     - Calculates record count and logs job details to `project.dataset.job_log`.

**Lineage:**
`project.isbert_schema.dwtk_meldungen` --> `project.dataset.sp_d_ausd_bp_ta_bpr_optionen`
`project.dataset.sof_ta_bpr_instance` --> `project.dataset.sp_d_ausd_bp_ta_bpr_optionen`
`project.dataset.sp_d_ausd_bp_ta_bpr_optionen` --> `project.dataset.sof_ta_bpr_optionen`
`project.dataset.sp_d_ausd_bp_ta_bpr_optionen` --> `project.dataset.error_log`
`project.dataset.sp_d_ausd_bp_ta_bpr_optionen` --> `project.dataset.job_log`

## 5. Transformation Logic

**Original KornShell Script (`k_ausd_bp_ta_bpr_optionen.ksh`):**
- **Parameter Parsing:** Uses `getopts` for `-j`, `-f`, `-s`, `-l`.
- **Validation:** Calls `pruefeParameterGesetzt` and `DWDate_Datum_Check` for required parameters and date format. Error messages are handled by `DWMSG_MeldeFehler`.
- **Date Derivation:** Executes `gestern.ksh` to get today's and yesterday's dates.
- **SQL Execution:** Calls `starteSQLSkript` which implicitly interacts with Oracle SQL*Plus to run `d_ausd_bp_ta_bpr_optionen.sql`.
- **Record Count:** Reads `v_records` from a temporary file `tmpFile`.
- **Job Logging:** `FOSJobErzeugeEintrag` (commented out).

**Original SQL Script (`d_ausd_bp_ta_bpr_optionen.sql`):**
- **Variable Definition:** `v_carmen` and `v_datum` (derived from `isbert_schema.dwtk_meldungen`).
- **Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_optionen REUSE STORAGE');`
- **Data Load:** `INSERT INTO sof$ta_bpr_optionen (CNTRCT_ID, BPR_ID) SELECT bp.cntrct_id, bp.bpr_id FROM sof$ta_bpr_instance bp;`
- **Transaction Control:** `COMMIT;`

**Target BigQuery SQL Stored Procedure (`project.dataset.sp_d_ausd_bp_ta_bpr_optionen`):**
- **Parameter Mapping:** Shell script parameters `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` become direct `STRING` input parameters to the stored procedure.
- **Validation:**
    - `IF IS NULL OR = ''` checks for required parameters.
    - `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL` for date format validation.
    - Errors are handled by `RAISE USING MESSAGE` and logged to `project.dataset.error_log`.
- **Date Derivation:** `v_datum_heute` from `CURRENT_DATE()`, `v_datum_gestern` from `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`. The `v_datum` derivation from `dwtk_meldungen` remains as `SELECT IFNULL(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101') FROM project.isbert_schema.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'`.
- **Truncation:** `TRUNCATE TABLE project.dataset.sof_ta_bpr_optionen;`
- **Data Load:** `INSERT INTO project.dataset.sof_ta_bpr_optionen (cntrct_id, bpr_id) SELECT bp.cntrct_id, bp.bpr_id FROM project.dataset.sof_ta_bpr_instance bp;`
- **Record Count:** `SET v_records = (SELECT COUNT(*) FROM project.dataset.sof_ta_bpr_optionen);`
- **Job Logging:** `INSERT INTO project.dataset.job_log` with relevant job details.
- **Optimization Hints:** Oracle `/*+ full(bp) parallel(bp,4) */` hints are removed as BigQuery's query optimizer handles execution plan generation automatically.

## 6. External Dependencies
- **Oracle Database:** The source system for `isbert_schema.dwtk_meldungen` and `sof$ta_bpr_instance`.
    - **Replacement:** These tables will be migrated to BigQuery. The `sof_ta_bpr_instance` table is likely an intermediate staging table or a source from another system. This will require initial data ingestion (e.g., via batch loads or CDC) to populate these tables in BigQuery before the stored procedure can run.
- **Shell Utilities:** `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`.
    - **Replacement:** The functionality of these utilities will be absorbed into the BigQuery stored procedure logic (for parameter and date validation, error handling) or managed by the Python orchestration layer (for environment setup, date calculations). Dedicated error and job logging tables in BigQuery will replace shell-based error reporting and temporary files.
- **Oracle Package `DWPA_UTIL_SKRIPT`:** Used for `TRUNCATE TABLE`.
    - **Replacement:** Direct `TRUNCATE TABLE` SQL statement in BigQuery.
- **Commented-out `sed`, `sort`, `join` commands:** These indicate potential post-processing of flat files.
    - **Replacement:** If these become active in the future, they would be handled by BigQuery's capabilities for data manipulation, potentially using external tables or GCS for intermediate file storage, or by integrating with services like Dataflow for complex file processing. For now, they are ignored as they are commented out.

## 7. Unresolved / Risks
- **Missing `file_complexity` and `automation_rate`:** The absence of these details means the initial effort estimation and automation level are based on general assumptions. A manual review of the script's complexity and an assessment of its automation potential are needed.
- **`starteSQLSkript` implementation:** The exact implementation of `starteSQLSkript` in `h_alis_sqlplus.ksh` is not fully known. It is assumed to be a wrapper for executing SQL via SQL*Plus. If it contains complex logic (e.g., dynamic SQL generation beyond simple concatenation), this needs further investigation. The current design assumes direct execution of the SQL script.
- **Oracle Data Types and Constraints:** The migration assumes a direct mapping of Oracle data types to BigQuery data types. Specific handling for `NUMBER` precision, `DATE` formats, and `VARCHAR2` lengths might be required.
- **Data Volume and Performance:** The `FULL` and `PARALLEL` hints in the original SQL suggest large data volumes. Performance of the BigQuery solution should be monitored and optimized using partitioning, clustering, and appropriate indexing if needed.
- **Commented-out Code:** The commented `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, and post-processing `sed`/`sort`/`join` commands are currently ignored. If these functionalities are required in the future, they represent additional scope and design effort.

## 8. Build Plan

The migration will involve the following steps:

1.  **BigQuery Schema Definition (DDL):**
    -   Create BigQuery datasets: `project.isbert_schema` and `project.dataset`.
    -   Define `project.isbert_schema.dwtk_meldungen` table schema.
    -   Define `project.dataset.sof_ta_bpr_instance` table schema.
    -   Define `project.dataset.sof_ta_bpr_optionen` table schema.
    -   Define `project.dataset.error_log` table schema (e.g., `job_name STRING, error_nr INT64, error_arg STRING, created_at TIMESTAMP`).
    -   Define `project.dataset.job_log` table schema (e.g., `tab_name STRING, job_kennung STRING, eintrags_nr STRING, stichtag STRING, wiederanlauf_wert STRING, records INT64, created_at TIMESTAMP`).

2.  **Initial Data Loading:**
    -   Ingest historical and/or initial data from Oracle `isbert_schema.dwtk_meldungen` to BigQuery `project.isbert_schema.dwtk_meldungen`.
    -   Ingest historical and/or initial data from Oracle `sof$ta_bpr_instance` to BigQuery `project.dataset.sof_ta_bpr_instance`.

3.  **BigQuery Stored Procedure Development:**
    -   Translate the combined logic of `k_ausd_bp_ta_bpr_optionen.ksh` and `d_ausd_bp_ta_bpr_optionen.sql` into a BigQuery SQL stored procedure `project.dataset.sp_d_ausd_bp_ta_bpr_optionen` as per the transformation logic outlined above.
    -   Language: BigQuery Standard SQL.

4.  **Orchestration Layer Development:**
    -   Create a Python script (or Cloud Composer DAG / Workflows YAML) to invoke `project.dataset.sp_d_ausd_bp_ta_bpr_optionen`.
    -   Pass parameters to the stored procedure.
    -   Implement error handling, logging, and scheduling.
    -   Language: Python (for Airflow/Workflows).

5.  **Testing and Validation:**
    -   Unit testing for the BigQuery stored procedure.
    -   Integration testing with the orchestration layer.
    -   Data validation and reconciliation between source Oracle and target BigQuery.

6.  **Deployment:**
    -   Deploy BigQuery objects (tables, stored procedure).
    -   Deploy orchestration component to Cloud Composer or Workflows.