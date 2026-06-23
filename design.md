# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_iccid_einzeln.ksh` to Google BigQuery. The original script acts as a control and orchestration component for a batch job. Its primary functions include:
- Parsing and validating runtime parameters (Job ID, Entry Number, Reference Date, Restart Value).
- Initializing the execution environment by sourcing common utility scripts.
- Performing date format validation.
- Executing a core SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) which is expected to contain the primary data extraction and processing logic.
- Recording the number of processed records.
- The job's implicit business purpose is to orchestrate data processing related to the `PoolBasisprodukt` table/entity.

The scope of this migration focuses on transforming the shell script's orchestration, parameter handling, and utility function calls into BigQuery native constructs, with the understanding that the invoked SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) will be migrated separately.

## 2. Source Inventory
The job is primarily composed of a single KornShell script:

| File Path                                                                   | Technology | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------------------- | :--------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh` | KornShell  | medium          | semi_auto         |

## 3. Target Architecture
The migrated solution will primarily leverage Google BigQuery's capabilities, with potential for Cloud Composer (Apache Airflow) for orchestration.

- **Main Processing Logic:** The shell script's orchestration and parameter validation will be reimplemented as a BigQuery Stored Procedure, named `project.dataset.r_ausd_bp_ta_iccid_einzeln`.
- **Core SQL Logic:** The SQL logic currently residing in `d_ausd_bp_ta_iccid_einzeln.sql` will be migrated either directly inline within the main BigQuery stored procedure or as a separate BigQuery SQL script/stored procedure.
- **Logging and Monitoring:** A dedicated BigQuery table (e.g., `project.dataset.job_log`) will be used to store job execution metadata, status, and processed record counts, replacing the temporary file-based record count and any legacy job management system entries.
- **Environment and Utility Functions:** Common utility functions (date checks, error handling) will be converted into BigQuery UDFs (User-Defined Functions) or helper stored procedures, or replaced by BigQuery's built-in functions.
- **Orchestration:** If the original KornShell script was invoked by an external scheduler, Cloud Composer (Airflow) or Cloud Workflows can be used to schedule and manage the execution of the BigQuery stored procedure.

## 4. Data Flow & Lineage
The original data flow involves the shell script orchestrating the execution of an external SQL file.

**Legacy Flow:**
1. **Input Parameters:** The script receives parameters (`-j`, `-f`, `-s`, `-l`) via `getopts`.
2. **Environment Setup:** Sources several utility KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`).
3. **Validation:** Performs parameter and date format validation.
4. **SQL Script Execution:** Invokes an external SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) via the `starteSQLSkript` function (presumably utilizing `sqlplus` or a similar tool).
5. **Record Count:** Reads the count of processed records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`) after the SQL script execution.
6. **Logging:** (Commented out in source) Potentially inserts job execution details into a job table (`FOSJobErzeugeEintrag`).

**Target BigQuery Flow:**
1. **Orchestration Layer (e.g., Cloud Composer):** Triggers the main BigQuery stored procedure, passing necessary parameters.
2. **Main BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_iccid_einzeln`):**
    - Receives parameters (e.g., `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    - Performs parameter and date validation using BigQuery's procedural statements (`IF`, `RAISE`, `SAFE.PARSE_DATE`).
    - Calculates `p_datum_heute` and `p_datum_gestern` using BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`).
    - Executes the migrated core SQL logic (either inline or by calling a nested BigQuery stored procedure that contains the logic from `d_ausd_bp_ta_iccid_einzeln.sql`).
    - Retrieves the count of processed records directly from the target table after the data processing, or as a return value from the nested procedure.
    - Inserts job execution details, including record counts, into `project.dataset.job_log`.

## 5. Transformation Logic
The transformation logic mainly involves migrating control flow and utility calls, as the core data transformations are expected to be in the separate SQL script.

**Original Script Logic:**
- **Parameter Parsing:** `getopts` is used to parse command-line arguments `j`, `f`, `s`, `l`.
- **Parameter Validation:** Calls `pruefeParameterGesetzt` for `Jobkennung`, `Stichtag`, `EintragsNr`.
- **Date Validation:** Calls `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
- **Initialization:** Sets `v_TabName='PoolBasisprodukt'`, initializes `p_wiederanlaufWert`.
- **Date Calculation:** Executes `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
- **SQL Execution:** Calls `starteSQLSkript` with various parameters including `Name_SQLskript` (pointing to `d_ausd_bp_ta_iccid_einzeln.sql`).
- **Record Count Retrieval:** `eval "v_records=`cat $tmpFile`"` reads from a temporary file.
- **Error Handling:** `DWMSG_MeldeFehler`, `echo`, `print`, and `exit` with error codes.

**BigQuery Transformation:**
- **Parameters:** Shell script parameters will be converted to `IN` parameters of the BigQuery stored procedure.
- **Validation:** `pruefeParameterGesetzt` logic will be replaced with `IF ... THEN RAISE USING MESSAGE = '...' END IF;` statements.
- **Date Validation:** `DWDate_Datum_Check` will be replaced by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and a check for `IS NULL`.
- **Date Calculation:** `gestern.ksh` will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
- **SQL Execution:** The content of `d_ausd_bp_ta_iccid_einzeln.sql` will be refactored into BigQuery SQL, potentially as another stored procedure or inline. The `starteSQLSkript` wrapper function will be obsolete.
- **Record Count:** The `tmpFile` mechanism will be replaced by a `SELECT COUNT(*)` on the target table within the stored procedure, or by capturing the row count from the DML statement.
- **Error Handling:** `DWMSG_MeldeFehler` will be replaced by BigQuery's `RAISE` statement or inserts into a dedicated error logging table. `print/echo` for informational messages can be replaced with `SELECT` statements for debugging during development, or removed for production logging.

## 6. External Dependencies
The original script has several external dependencies that need to be addressed in the BigQuery migration:

- **Shell Environment (`.dw_init`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These environment variables and initialization scripts will be replaced by BigQuery stored procedure parameters, configuration tables, or by explicit BigQuery project/dataset references within the code.
- **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`):** The functionality of these scripts will be re-implemented using BigQuery's native capabilities (built-in functions, UDFs, or in-line procedural logic). For example, `h_alis_sqlplus.ksh` implies an Oracle or similar SQL engine, which will be entirely replaced by BigQuery's SQL engine.
- **Temporary File (`$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`):** This file for storing record counts will be replaced by a BigQuery logging table or by capturing the result of the DML operation directly within the stored procedure.
- **FOS Job Management (commented out `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These indicate integration with a legacy job scheduling/management system. This functionality will be replaced by a modern orchestrator like Cloud Composer (Airflow) and logging into a BigQuery job status table.
- **Core SQL Script (`d_ausd_bp_ta_iccid_einzeln.sql`):** This is a significant dependency. Its contents, including any source tables it reads from and target tables it writes to, need to be fully migrated to BigQuery SQL.
- **Oracle Database (implied):** The execution of a `.sql` script via `sqlplus`-like functionality strongly suggests an underlying Oracle database. All Oracle tables and views referenced within `d_ausd_bp_ta_iccid_einzeln.sql` must be migrated to BigQuery tables.

## 7. Unresolved / Risks
- **Content of `d_ausd_bp_ta_iccid_einzeln.sql`:** This is the most critical unknown. The actual data transformation logic is within this SQL script. Without its content, the full migration design (especially data sources, transformations, and target tables) cannot be finalized. It needs to be thoroughly analyzed and designed for BigQuery.
- **Legacy File Processing (Commented `sed`, `sort`, `join`):** The presence of commented-out, complex file manipulation suggests that the original system may have involved flat-file processing that could re-emerge if `d_ausd_bp_ta_iccid_einzeln.sql` itself interacts with flat files. If such operations are necessary, they should be refactored using BigQuery's capabilities (e.g., external tables, `LOAD DATA`, SQL string/array functions) or by using PySpark jobs on Dataproc/Serverless Spark for more complex transformations.
- **Dynamic SQL:** If `d_ausd_bp_ta_iccid_einzeln.sql` generates dynamic SQL based on input parameters, this complexity needs careful handling in BigQuery stored procedures, potentially using `EXECUTE IMMEDIATE`.
- **Error Reporting Complexity:** The `f_alis_msgerr.ksh` script likely implements a specific error reporting framework. Migrating this to BigQuery will require defining a standard error logging mechanism (e.g., `RAISE` and an error log table).
- **Security and Permissions:** The `_init` and other sourced files might contain sensitive information or specific permission settings that need to be replicated or replaced with IAM roles and service accounts in GCP.

## 8. Build Plan
The migration will follow these ordered steps:

1.  **Analyze `d_ausd_bp_ta_iccid_einzeln.sql`:**
    - Identify all source tables, target tables, and views.
    - Extract all SQL statements and understand their logic (joins, filters, aggregations, DDL/DML).
    - Map data types and transformations for BigQuery.
2.  **Data Migration:**
    - Migrate all source and target tables identified in `d_ausd_bp_ta_iccid_einzeln.sql` from the legacy database to BigQuery. This may involve one-time historical loads and setting up CDC for ongoing data synchronization.
3.  **Develop BigQuery Utility Stored Procedures/UDFs (BQSQL):**
    - Create BigQuery UDFs for date validation (e.g., `DWDate_Datum_Check` logic).
    - Create a BigQuery stored procedure for error logging (e.g., `DWMSG_MeldeFehler` replacement).
    - Implement the logic of `gestern.ksh` directly using BigQuery's `CURRENT_DATE()` and `DATE_SUB()`.
4.  **Develop Core Data Processing BigQuery Stored Procedure (BQSQL):**
    - Convert the SQL logic from `d_ausd_bp_ta_iccid_einzeln.sql` into a BigQuery SQL stored procedure. Ensure all transformations, joins, and aggregations are correctly implemented for BigQuery's dialect.
5.  **Develop Main Orchestration BigQuery Stored Procedure (BQSQL):**
    - Create the main stored procedure `project.dataset.r_ausd_bp_ta_iccid_einzeln`.
    - Implement parameter validation using `IF...RAISE`.
    - Call the utility procedures/UDFs for date validation and error logging.
    - Invoke the core data processing stored procedure (from step 4).
    - Implement logic to capture and log the record count into `project.dataset.job_log`.
6.  **Create Job Logging Table (BQSQL DDL):**
    - Define and create `project.dataset.job_log` table with columns for `job_kennung`, `eintrags_nr`, `tab_name`, `stichtag`, `records`, `status`, `created_at`, etc.
7.  **Orchestration (Python for Cloud Composer / YAML for Cloud Workflows):**
    - If needed, create an Airflow DAG or Cloud Workflow definition to schedule and execute the `project.dataset.r_ausd_bp_ta_iccid_einzeln` stored procedure in BigQuery, passing the required parameters.
8.  **Testing:**
    - Unit test each BigQuery stored procedure and UDF.
    - Integration test the entire BigQuery workflow.
    - Perform end-to-end testing with realistic data scenarios.
9.  **Deployment:**
    - Deploy BigQuery DDL (tables, stored procedures, UDFs).
    - Deploy Cloud Composer DAGs or Cloud Workflows.