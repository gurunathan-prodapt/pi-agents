# Migration Design — vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_tarifoption.ksh` to Google BigQuery. The original script acts as a control script for a data processing job. Its primary functions include:
*   Parsing and validating runtime parameters such as job identifier, entry number, and a key date (`Stichtag`).
*   Initializing the environment by sourcing various utility scripts.
*   Executing an external SQL script (`d_ausd_bp_ta_tarifoption.sql`) which contains the core data processing logic, likely related to the `PoolBasisprodukt` table.
*   Capturing and reporting the number of records processed by the SQL script.
*   The job is assembled from a single component file.

## 2. Source Inventory
The migration job consists of one primary source file:

| File Path                                                       | Technology | Complexity Tier | Automation Bucket | Notes                                              |
| :-------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh` | KornShell  | Medium          | Semi-Automated    | Orchestrates an external SQL script, performs parameter and date validation. |

*Note: The complexity tier for this file is assessed as 'Medium' due to its role as an orchestrator for an external SQL script and its parameter/date validation logic, requiring careful translation to BigQuery stored procedures and data types. No explicit complexity tier was found in the `file_complexity` table.*

## 3. Target Architecture
The migrated solution will primarily leverage Google BigQuery's capabilities for data processing and orchestration.

*   **BigQuery Stored Procedures:** The core orchestration logic, including parameter parsing, validation, and calling the main business logic, will be implemented as a BigQuery SQL stored procedure (e.g., `project.dataset.r_ausd_bp_ta_tarifoption`). This procedure will accept input parameters and handle control flow.
*   **BigQuery SQL:** The actual data manipulation and business logic currently residing in `d_ausd_bp_ta_tarifoption.sql` will be translated into BigQuery SQL and likely encapsulated within another BigQuery stored procedure (e.g., `project.dataset.d_ausd_bp_ta_tarifoption_core`) or a series of DML statements.
*   **BigQuery Tables:** All source and target data, including the `PoolBasisprodukt` table, will reside in BigQuery tables. Temporary data required during processing will utilize BigQuery temporary tables, Common Table Expressions (CTEs), or BigQuery script variables.
*   **Error and Job Logging:** Dedicated BigQuery tables (e.g., `project.dataset.error_log`, `project.dataset.job_log`) will be used to capture errors, execution status, and record counts, replacing the shell script's `DWMSG_MeldeFehler` and commented-out `FOSJobErzeugeEintrag` functionality.
*   **Orchestration:** While the immediate control logic will be in a BigQuery stored procedure, external scheduling and workflow management could be handled by Cloud Composer (Apache Airflow) or Dataform if complex dependencies or external system interactions are required.

## 4. Data Flow & Lineage
The original KornShell script serves as the entry point and orchestrator for a data processing task.

**Inputs to the Orchestrator (BigQuery Stored Procedure):**
*   `p_JobKennung` (Job identifier)
*   `p_EintragsNr` (Entry number / Run identifier)
*   `p_Stichtag` (Key date, DDMMYYYY format)
*   `p_wiederanlaufWert` (Restart / recovery value, optional)

**Internal Data Flow and Processing Steps:**
1.  **Parameter Validation:** The input parameters `p_JobKennung`, `p_EintragsNr`, and `p_Stichtag` are checked for presence and the `p_Stichtag` is validated for `DDMMYYYY` format.
2.  **Date Derivation:** `p_datum_heute` (current date) and `p_datum_gestern` (yesterday's date) are derived using BigQuery date functions.
3.  **SQL Script Execution:** The equivalent of `d_ausd_bp_ta_tarifoption.sql` (now a BigQuery stored procedure `d_ausd_bp_ta_tarifoption_core`) is invoked, passing the validated parameters and derived dates. This SQL procedure will perform the actual data extraction, transformation, and loading into BigQuery tables.
4.  **Record Count Capture:** After the core SQL logic completes, the number of processed records is obtained, typically by querying the target BigQuery table or capturing a row count from the executed DML.
5.  **Logging:** Job execution details and any errors are logged to BigQuery logging tables.

**Output:**
*   Updated data in BigQuery tables (specifically, effects from the `d_ausd_bp_ta_tarifoption_core` procedure on `PoolBasisprodukt` or other relevant tables).
*   Logging records in `project.dataset.error_log` and `project.dataset.job_log`.

## 5. Transformation Logic
The transformation logic within the KornShell script itself is primarily orchestrational, rather than data-transforming.

*   **Parameter Parsing and Validation:** `getopts`-based parsing will be replaced by direct BigQuery stored procedure input parameters. Validation checks for mandatory parameters will use `IF`/`ASSERT` statements. Date format validation for `p_Stichtag` will use `SAFE.PARSE_DATE` in BigQuery SQL.
*   **Environment Sourcing:** The sourcing of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will be replaced by BigQuery's native capabilities or equivalent SQL functions/procedures.
*   **Date Derivation:** The call to `gestern.ksh` will be replaced by BigQuery functions like `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Script Invocation:** The `starteSQLSkript` function, which executes `d_ausd_bp_ta_tarifoption.sql`, will be replaced by a direct `CALL` to the corresponding BigQuery stored procedure (`d_ausd_bp_ta_tarifoption_core`).
*   **Record Count:** Reading from `$tmpFile` will be replaced by a `SELECT COUNT(*)` query against the target table or by using `@@row_count` if applicable within procedural SQL.
*   **Error Handling:** The custom error framework (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) will be replaced by BigQuery's error handling mechanisms (`ASSERT`, `RAISE ERROR`) and insertion into a dedicated error logging table.
*   **Commented-out Transformations:** The `sed`, `sort`, and `join` commands in the commented-out section (which handled post-processing of temporary output files) suggest a need for data shaping. If these steps become active requirements, they will be translated into BigQuery SQL transformations using functions like `TRIM`, `REGEXP_REPLACE`, `QUALIFY`, `UNION`, `JOIN`, `ARRAY_AGG`, `STRING_AGG` on BigQuery tables.

## 6. External Dependencies
The original script has implied external dependencies that need to be addressed in the migration.

*   **Oracle Database (Implied):** The script sources `h_alis_sqlplus.ksh` and executes a SQL script, strongly suggesting interaction with an Oracle database via SQL*Plus.
    *   **Replacement Strategy:** All data residing in the Oracle database will be migrated to BigQuery tables. The SQL logic (`d_ausd_bp_ta_tarifoption.sql`) will be rewritten into BigQuery SQL, leveraging BigQuery's native DML/DDL capabilities within stored procedures.
*   **Filesystem-based Utilities:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date validation.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helpers.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus wrapper.
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Date calculation.
    *   **Replacement Strategy:** These will be replaced by BigQuery's built-in functions, BigQuery SQL stored procedures or UDFs, or parameters passed from an orchestration layer. Error logging will go to a BigQuery table.
*   **Temporary Files:** `$DW_DIR_UTL/bert_k_ausd_bp_ta_tarifoption.tmp` for record count.
    *   **Replacement Strategy:** BigQuery procedure variables, temporary tables, or direct `SELECT COUNT(*)` queries will replace temporary files.
*   **Commented-out UNIX Utilities:** `sed`, `sort`, `join`
    *   **Replacement Strategy:** If these functions become active, they will be translated to BigQuery SQL equivalents for string manipulation, sorting, and joining on BigQuery tables.

## 7. Unresolved / Risks
*   **`d_ausd_bp_ta_tarifoption.sql` Content:** The most significant unresolved item is the actual content of `d_ausd_bp_ta_tarifoption.sql`. This file holds the core business logic, including specific DML/DDL statements, data sources, and targets. A separate analysis and migration design for this SQL script are essential to complete the overall job migration.
*   **Orchestration Details of `d_ausd_bp_ta_tarifoption.sql`:** The `starteSQLSkript` function is a wrapper. The exact parameters, error handling, and transactional behavior of the original SQL script's execution need to be fully understood from `h_alis_sqlplus.ksh` and `d_ausd_bp_ta_tarifoption.sql` to ensure a faithful BigQuery migration.
*   **Commented-out Logic:** While currently commented out, the `sed`, `sort`, `join` operations suggest historical post-processing. It's a risk if this logic is unexpectedly required later. Clarification on its necessity is needed.
*   **Job Management Functions:** `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` were commented out. If these job management functions are active in other parts of the system and are critical for this job's lifecycle, their functionality will need to be implemented either as BigQuery logging, part of a Cloud Composer/Dataform orchestration, or within a separate application.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` parameter is initialized but not explicitly used in the provided script content. Its intended use in the SQL script (`d_ausd_bp_ta_tarifoption.sql`) needs to be understood.

## 8. Build Plan
**Target Language:** BigQuery SQL (primary)

**Ordered List of Files to Generate:**

1.  **DDL for Target Tables:**
    *   `create_table_poolbasisprodukt.sql`: DDL for the `PoolBasisprodukt` table in BigQuery.
    *   `create_table_error_log.sql`: DDL for a BigQuery error logging table.
    *   `create_table_job_log.sql`: DDL for a BigQuery job logging table.

2.  **Core Business Logic Stored Procedure:**
    *   `d_ausd_bp_ta_tarifoption_core.sql`: This will contain the translated BigQuery SQL DML/DDL from the original `d_ausd_bp_ta_tarifoption.sql`. It will be implemented as a BigQuery stored procedure (e.g., `project.dataset.d_ausd_bp_ta_tarifoption_core`) and will accept parameters like `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `p_datum_heute`, `p_datum_gestern`, `p_wiederanlaufWert`. This is the most critical component and requires full analysis of the original SQL script.

3.  **Orchestration Stored Procedure:**
    *   `r_ausd_bp_ta_tarifoption.sql`: This BigQuery stored procedure (e.g., `project.dataset.r_ausd_bp_ta_tarifoption`) will encapsulate the control flow of the original `k_ausd_bp_ta_tarifoption.ksh` script. It will handle:
        *   Accepting `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` as input.
        *   Parameter and date validation using BigQuery SQL.
        *   Deriving current and previous dates.
        *   Calling the `project.dataset.d_ausd_bp_ta_tarifoption_core` stored procedure.
        *   Capturing and logging record counts.
        *   Error handling and logging.

4.  **Optional: Orchestration Layer (if needed for scheduling or external triggers):**
    *   `k_ausd_bp_ta_tarifoption_dag.py` (Cloud Composer/Airflow DAG): If the job needs to be scheduled or integrated into a broader workflow, a Python DAG will be created to call the `project.dataset.r_ausd_bp_ta_tarifoption` BigQuery stored procedure.

**Execution Order:**
1.  Deploy DDL for all necessary BigQuery tables (`PoolBasisprodukt`, `error_log`, `job_log`).
2.  Deploy the `d_ausd_bp_ta_tarifoption_core` BigQuery stored procedure (business logic).
3.  Deploy the `r_ausd_bp_ta_tarifoption` BigQuery stored procedure (orchestration logic).
4.  If using Cloud Composer/Airflow, deploy the DAG to trigger the `r_ausd_bp_ta_tarifoption` stored procedure. Otherwise, execute the `r_ausd_bp_ta_tarifoption` stored procedure directly (e.g., via `bq query` command, a scheduled query, or a Cloud Function).