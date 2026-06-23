# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the `k_ausd_bp_ta_bpr_basis_his.ksh` KornShell script. The script acts as a control and wrapper for a data processing job related to the `PoolBasisprodukt`. Its primary functions include:
*   Parsing and validating input parameters (`JobKennung`, `EintragsNr`, `Stichtag`, `wiederanlaufWert`).
*   Performing date format validation for the `Stichtag`.
*   Setting up the execution environment by sourcing various utility scripts.
*   Executing a core SQL script, `d_ausd_bp_ta_bpr_basis_his.sql`, which contains the main data preparation logic.
*   Retrieving and logging the number of processed records from a temporary file.
*   The script also contains commented-out logic for post-processing temporary output files (sed, sort, join operations) which suggests potential file manipulation requirements.
The overall purpose, as indicated by the lineage analysis, is a medium-complexity job involving 1 component.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`
    *   **Technology:** KornShell
    *   **Category:** Shell Script
    *   **Tool (analysis):** KornShell
    *   **Complexity Tier:** Undetermined (no specific `file_complexity` entry found for this file)
    *   **Migration Bucket:** semi_auto
    *   **File Purpose:** ETL Orchestration/Wrapper script.

## 3. Target Architecture
The migration target is Google Cloud Platform, primarily utilizing BigQuery for data processing and storage.
*   **Orchestration:** An Airflow DAG will replace the KornShell script's orchestration logic. This DAG will manage parameter passing, execution flow, and error handling.
*   **Core Logic:** The business logic from `d_ausd_bp_ta_bpr_basis_his.sql` will be refactored into a BigQuery Stored Procedure or a set of modular BigQuery SQL scripts.
*   **Parameter Handling & Validation:** BigQuery Stored Procedures will handle input parameters, validate them, and manage conditional execution paths.
*   **Data Storage:** All source and target data will reside in BigQuery tables. Intermediate results from file post-processing (if activated) will be handled via BigQuery staging tables.
*   **Logging & Control:** Job execution logs and control entries (e.g., record counts, job status) will be stored in dedicated BigQuery control tables.
*   **File-based Operations:** Legacy file operations (sed, sort, join) will be re-implemented using standard BigQuery SQL transformations (e.g., `REPLACE`, `ROW_NUMBER()`, `FULL OUTER JOIN`, `UNION DISTINCT`) where possible. If any flat-file export is strictly required, BigQuery extract jobs to Cloud Storage, followed by external workflow processing, will be considered.

## 4. Data Flow & Lineage
The `k_ausd_bp_ta_bpr_basis_his.ksh` script acts as an orchestrator.
1.  **Parameter Input:** The script receives parameters (`j`, `f`, `s`, `l`) via command-line arguments.
2.  **Environment Setup:** It sources several utility KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) to establish its execution environment, logging, date utilities, and SQL execution helpers.
3.  **Parameter Validation:** It validates the presence and format of critical parameters, especially the `Stichtag`. Errors lead to script termination.
4.  **SQL Script Execution:** The script invokes the `starteSQLSkript` function (likely from `h_alis_sqlplus.ksh`), passing parameters and the path to `d_ausd_bp_ta_bpr_basis_his.sql`. This SQL script is responsible for the core data transformations.
5.  **Record Count & Logging:** After the SQL script completes, the orchestrator reads a record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis_his.tmp`) and implicitly uses this for logging (though `FOSJobErzeugeEintrag` is commented out).
6.  **Commented Post-Processing:** The script includes commented-out sections for `sed`, `sort`, and `join` operations on `cibasis_data24.dat`, `cibasis_data96.dat`, and `cibasis_fax.dat`, which would typically produce `cibasis_24_96.tmp` and `cibasisprodukt.csv`. This suggests a potential need for post-processing flat files, although not currently active.

Given that `lineage_edges` did not show direct `READS`/`WRITES` for this specific `.ksh` file, its role is confirmed as an orchestrator, with the actual data operations occurring within the invoked `d_ausd_bp_ta_bpr_basis_his.sql` and the (currently commented-out) post-processing steps.

## 5. Transformation Logic
The transformation logic will primarily reside within the migrated BigQuery SQL and Stored Procedures.

*   **Parameter Handling & Validation:**
    *   BigQuery Stored Procedure parameters will replace shell script arguments.
    *   `IF` statements and `SELECT ERROR(...)` or `ASSERT` will be used for parameter validation (e.g., checking for `NULL` or empty strings for `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`).
    *   Date validation will use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` to ensure correct format and raise errors if invalid.
*   **Date Derivations:** `CURRENT_DATE()` and `DATE_SUB()` functions in BigQuery will replace the `gestern.ksh` utility for deriving today's and yesterday's dates.
*   **Core Data Processing:** The contents of `d_ausd_bp_ta_bpr_basis_his.sql` will be translated into BigQuery SQL, forming the central part of a BigQuery Stored Procedure or a series of dependent SQL scripts. This will involve converting source table references, SQL syntax, and any specific functions to their BigQuery equivalents.
*   **Record Count:** Instead of reading from a temporary file, record counts will be obtained directly via `SELECT COUNT(*)` queries on the result sets or intermediate tables, and then inserted into a BigQuery control table.
*   **File Post-Processing (if enabled/required):**
    *   **Whitespace Removal (`sed`):** `REPLACE(column_name, ' ', '')` in BigQuery.
    *   **Deduplication and Sorting (`sort -u -k 1`):** Achieved using `ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY other_column) = 1` to get unique rows, often after splitting the input string into components.
    *   **Joining (`join`):** Replicated using `FULL OUTER JOIN` or `LEFT JOIN` on parsed key columns.
    *   These operations would typically be performed on external tables (if flat files are ingested) or staging tables within BigQuery.

## 6. External Dependencies
The current script has several external dependencies that need to be addressed:
*   **Environment Initialization:** `.dw_init` and `BERT_DIR_ROOT` environmental variables are sourced. These will be replaced by BigQuery Stored Procedure constants, configuration tables, or Airflow DAG parameters.
*   **Utility Scripts:**
    *   `f_alis_msgerr.ksh`: Error handling. This will be replaced by BigQuery's native error handling (`SELECT ERROR(...)`, `ASSERT`) and integrated logging mechanisms.
    *   `h_alis_date.ksh`: Date utilities. BigQuery's built-in date functions will be used.
    *   `h_alis_parameter.ksh`: Parameter parsing. Replaced by BigQuery Stored Procedure parameters.
    *   `h_alis_sqlplus.ksh`: SQL execution wrapper. Replaced by direct BigQuery SQL execution within the stored procedure or via Airflow operators.
    *   `gestern.ksh`: Date calculation. Replaced by BigQuery date functions.
*   **SQL Script:** `d_ausd_bp_ta_bpr_basis_his.sql` is a critical dependency. Its content needs to be fully migrated to BigQuery SQL.
*   **Temporary File:** `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_basis_his.tmp` for record count. This will be replaced by direct SQL queries and BigQuery control tables.
*   **Legacy Data Sources:** The commented `sed`, `sort`, `join` operations suggest potential interaction with `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat` files. If these are still active sources, they will need to be ingested into BigQuery as external tables or native tables, potentially via Cloud Storage.
*   **Job Control System:** The commented `FOSJobErzeugeEintrag` indicates interaction with a job control system. This will be replaced by inserting records into a BigQuery control table.

## 7. Unresolved / Risks
*   **`d_ausd_bp_ta_bpr_basis_his.sql` Content:** The actual logic within `d_ausd_bp_ta_bpr_basis_his.sql` is unknown and requires separate analysis for migration to BigQuery SQL. This is the primary unresolved component for full data transformation.
*   **Complexity Tier:** The `file_complexity` was not found, so a definitive complexity assessment for the script itself is missing. This might impact effort estimation.
*   **Commented-out Code:** The script contains significant commented-out sections for file post-processing (sed, sort, join). It needs clarification if this functionality is still required or can be disregarded. If required, its migration can add complexity, especially if flat-file processing cannot be fully translated to BigQuery SQL and requires Python on Dataflow/Dataproc.
*   **Custom Utilities:** The sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) are custom implementations. Their specific logic needs to be fully understood to ensure accurate replication in BigQuery functions/procedures or Airflow operators.
*   **Job Kennung/EintragsNr/Stichtag:** Understanding the business meaning and usage of these parameters is crucial for correct migration, especially if they map to specific data partitioning or processing logic.
*   **Wiederanlaufwert (`p_wiederanlaufWert`):** The logic for handling restart values needs careful migration to ensure idempotency and correct recovery behavior in the new environment.

## 8. Build Plan
The migration will be executed in phases, focusing on re-platforming and then refining.

1.  **BigQuery Schema Definition:**
    *   Define target BigQuery schemas for all data sources and target tables, including any control/logging tables.
    *   Create external tables or ingestion pipelines for any legacy flat-file inputs (if applicable, e.g., `cibasis_data*.dat`).
2.  **Migrate SQL Script:**
    *   Translate `d_ausd_bp_ta_bpr_basis_his.sql` into BigQuery SQL syntax. This will likely involve converting data types, functions, and potentially query structures to optimize for BigQuery.
3.  **BigQuery Stored Procedure (Core Logic):**
    *   Create a BigQuery Stored Procedure (e.g., `proc_ausd_bp_ta_bpr_basis_his`) that encapsulates the migrated SQL logic from `d_ausd_bp_ta_bpr_basis_his.sql`.
    *   This procedure will accept `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as parameters.
4.  **BigQuery Stored Procedure (Orchestration & Validation):**
    *   Create a wrapper BigQuery Stored Procedure or incorporate into the main procedure that handles:
        *   Parameter validation and error reporting.
        *   Date calculations (`v_datum_heute`, `v_datum_gestern`).
        *   Initialization of `v_restart`.
        *   Invoking the core data transformation logic (from step 3).
        *   Updating the BigQuery control table with record counts and job status.
        *   Implementation of the commented-out file post-processing logic using BigQuery SQL if required.
5.  **Airflow DAG Development:**
    *   Develop an Airflow DAG to orchestrate the execution.
    *   The DAG will define tasks for:
        *   Setting parameters for the BigQuery Stored Procedure.
        *   Executing the BigQuery Stored Procedure using `BigQueryOperator` or `BigQueryExecuteQueryOperator`.
        *   Any pre-processing or post-processing steps that cannot be fully handled within BigQuery SQL (e.g., if external file interactions are still required, use Python `BashOperator` or `PythonOperator` interacting with Cloud Storage/Dataflow).
        *   Error handling and alerting within Airflow.
6.  **Testing and Validation:**
    *   Thorough unit and integration testing of the BigQuery SQL, Stored Procedures, and Airflow DAG.
    *   Data validation to ensure output matches legacy system.
7.  **Deployment:**
    *   Deploy BigQuery objects (tables, procedures) and the Airflow DAG to the target environment.

**Build Artifacts:**
*   BigQuery SQL scripts for table creation and `d_ausd_bp_ta_bpr_basis_his.sql` migration (language: `BigQuery SQL`)
*   BigQuery Stored Procedure definition for `proc_ausd_bp_ta_bpr_basis_his` (language: `BigQuery SQL`)
*   Airflow DAG Python file (language: `Python`)
*   Configuration files for BigQuery connections and environment parameters.