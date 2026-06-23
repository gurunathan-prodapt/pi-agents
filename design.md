# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_v_ta_discount.ksh` to Google BigQuery. The original script serves as a control and orchestration component for a data processing job. Its primary responsibilities include:
- Parsing and validating command-line parameters.
- Loading environment variables and utility scripts.
- Orchestrating the execution of a core SQL script (`d_ausd_v_ta_discount.sql`) to process data into the `ta_discount` table.
- Implementing job management logic (e.g., ignoring active jobs, registering job status, deactivating older jobs).
- Handling error logging and reporting.
- Capturing processed record counts.

The scope of this migration is to translate the functionality of this shell script and its direct dependencies into a BigQuery-native solution, ensuring equivalent data processing, job control, and error handling capabilities.

## 2. Source Inventory

**Primary Component:**
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh`
  - **Technology:** KornShell Script
  - **Tier:** medium
  - **Automation Bucket:** semi_auto
  - **Summary:** This ksh script acts as a control script for a data processing job, handling parameter parsing, error logging, and orchestrating the execution of an SQL script to process data into the 'ta_discount' table.

**Inferred Dependent Files (requires further analysis):**
The primary script sources and executes other files. These were not part of the `component_files` for this assembled job and their detailed metadata (tier, bucket) is not available but should be assessed during their migration.
- **SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_discount.sql`
  - **Role:** Contains the core SQL logic for data processing into `ta_discount`.
- **Utility Scripts:**
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility functions.
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper functions.
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution routines, including `starteSQLSkript`.

## 3. Target Architecture
The migration target is Google BigQuery. The current shell-based orchestration and SQL execution pattern will be transformed into:

- **BigQuery Stored Procedure:** The `k_ausd_v_ta_discount.ksh` control script will be refactored into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This stored procedure will:
    - Accept parameters equivalent to the original shell script's command-line arguments.
    - Implement parameter validation, error handling, and logging using BigQuery scripting constructs.
    - Contain or call the translated BigQuery SQL logic from `d_ausd_v_ta_discount.sql`.
    - Manage job state by performing DML operations on dedicated BigQuery job control/metadata tables.
- **BigQuery Tables:**
    - The `ta_discount` table will be migrated to a BigQuery table (`project.dataset.ta_discount`).
    - New BigQuery tables will be created for job control, logging, and auditing purposes, replacing the legacy job tracking mechanisms and temporary file usage.
- **Orchestration (Optional/Recommended):** If external scheduling or complex inter-job dependencies exist, a Cloud Composer (Airflow) DAG can be used to invoke the BigQuery Stored Procedure, pass parameters, and manage the overall workflow.

## 4. Data Flow & Lineage

**Legacy Data Flow:**
1. **Execution Trigger:** The `k_ausd_v_ta_discount.ksh` script is executed, likely by a scheduler, passing `-j <JobKennung>` and `-f <EintragsNr>` parameters.
2. **Environment & Utilities:** The script sources `$HOME/.dw_init` and several utility ksh scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3. **Parameter Parsing & Validation:** Parameters are parsed using `getopts` and validated via `pruefeParameterGesetzt`. Errors result in console output and script exit.
4. **SQL Execution:** The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) is called. This function is responsible for executing the SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_discount.sql` against an underlying RDBMS (presumably Oracle).
5. **Data Processing:** The `d_ausd_v_ta_discount.sql` script processes data, leading to updates in the `ta_discount` table and potentially other job tracking tables. It also likely outputs a record count to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_discount_$$.tmp`).
6. **Record Count Retrieval:** The main script reads the processed record count from the temporary file into the `v_records` variable.
7. **Completion:** Console output indicates data processing completion.

**Target BigQuery Data Flow:**
1. **Execution Trigger:** A Cloud Composer DAG or a direct BigQuery command invokes the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as input arguments.
2. **Parameter Handling & Validation:** The BigQuery Stored Procedure's internal logic handles parameter validation and error checks using BigQuery scripting. Invalid parameters raise exceptions or log errors to a BigQuery logging table.
3. **Job Control Updates:** The stored procedure interacts with BigQuery job control tables to record job start, status, and deactivation of older jobs.
4. **Data Processing:** The core logic from `d_ausd_v_ta_discount.sql` (now translated to BigQuery SQL) is executed directly within the stored procedure or as a separate BigQuery script called by the stored procedure. This updates the `project.dataset.ta_discount` table.
5. **Record Count:** The number of processed records is captured directly into a BigQuery scripting variable (`v_records`) from the SQL execution result, eliminating the temporary file dependency.
6. **Logging & Completion:** Job completion and any output/error messages are logged to BigQuery logging tables.

## 5. Transformation Logic

The migration involves transforming KornShell script constructs and external RDBMS SQL into BigQuery-native equivalents.

-   **Parameter Parsing (`getopts`)**: Will be replaced by BigQuery Stored Procedure `IN` parameters.
    -   **Legacy:** `while getopts ... case $param in j) p_JobKennung=$OPTARG;; f) p_EintragsNr=$OPTARG;; esac`
    -   **Target:** `CREATE OR REPLACE PROCEDURE ... (IN p_JobKennung STRING, IN p_EintragsNr STRING)`
-   **Environment Sourcing (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/...`)**:
    -   These will be replaced by explicit variable declarations within the BigQuery Stored Procedure, or by passing configuration values through the orchestration layer (e.g., Airflow DAG parameters). `BERT_DIR_ROOT` and `DW_DIR_UTL` will map to BigQuery project/dataset/table names or Cloud Storage paths.
-   **Parameter Validation (`pruefeParameterGesetzt`)**: Shell `if` conditions will become BigQuery `IF` statements.
    -   **Legacy:** `if [ ! $ErrNr -eq 0 ] ... DWMSG_MeldeFehler ... exit $ErrNr`
    -   **Target:** `IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN SET v_errnr = 1; SET v_error = 'Jobkennung fehlt'; END IF; IF v_errnr <> 0 THEN SELECT 'FEHLER' ...; LEAVE; END IF;`
-   **SQL Script Execution (`starteSQLSkript $Name_SQLskript`)**: The call to the external SQL script via a shell wrapper will be replaced by embedding the translated SQL logic directly within the BigQuery Stored Procedure or by calling a separate BigQuery script.
    -   **Legacy:** `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
    -   **Target:** The body of `d_ausd_v_ta_discount.sql` (after BigQuery translation) will form the core executable part of the BigQuery Stored Procedure.
-   **Temporary File for Record Count (`tmpFile`, `cat $tmpFile`, `eval "v_records=\`cat $tmpFile\`"`)**: The use of a temporary file will be replaced by directly assigning the result of a `COUNT(*)` query to a BigQuery scripting variable.
    -   **Legacy:** `tmpFile="...$$"` then `eval "v_records=\`cat $tmpFile\`"`
    -   **Target:** `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) FROM \`project.dataset.ta_discount\` WHERE ...);`
-   **Error Handling (`f_alis_msgerr.ksh`, `set -eu`)**: Shell-based error handling will be replaced by BigQuery's `EXCEPTION WHEN ERROR` block, `RAISE`, and `ASSERT` statements for explicit error signaling and control. Logging will be directed to BigQuery audit/log tables.
-   **Job Control Logic**: The implicit job management (ignoring active jobs, registering, deactivating) within `starteSQLSkript` and `d_ausd_v_ta_discount.sql` will be explicitly translated into DML operations (INSERT, UPDATE) on dedicated BigQuery job control tables.

## 6. External Dependencies

**Legacy External Dependencies:**
-   **RDBMS (e.g., Oracle):** The `d_ausd_v_ta_discount.sql` script is executed against an RDBMS via SQL*Plus calls orchestrated by `h_alis_sqlplus.ksh`. This is a critical dependency for data storage and processing.
-   **Operating System/File System:** Used for sourcing environment variables (`.dw_init`), utility scripts, storing temporary files, and reading the SQL script itself.
-   **`SQL*Plus`:** The tool used to interact with the RDBMS.

**Target Replacements in BigQuery:**
-   **RDBMS → BigQuery:** The source RDBMS and SQL*Plus will be entirely replaced by Google BigQuery for data storage and processing. `ta_discount` and any associated job control tables will reside in BigQuery.
-   **File System → BigQuery/Cloud Storage/Scripting Variables:**
    -   Temporary files (`tmpFile`) will be replaced by BigQuery scripting variables or by directly writing to BigQuery logging/audit tables.
    -   Sourced utility script functionalities will be absorbed into BigQuery Stored Procedure logic or standard BigQuery functions.
    -   Environment variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`) will be resolved within the BigQuery environment (e.g., project/dataset IDs) or passed as parameters.

## 7. Unresolved / Risks

-   **`d_ausd_v_ta_discount.sql` Content Analysis:** The actual SQL logic within `d_ausd_v_ta_discount.sql` is currently unknown. This is the most significant unresolved item. A detailed analysis of this SQL file is required to understand its complexity, data sources, transformations, and BigQuery compatibility. Potential risks include:
    -   Use of RDBMS-specific SQL syntax (e.g., Oracle PL/SQL, vendor-specific functions) requiring complex translation.
    -   Dependencies on RDBMS features not directly available in BigQuery (e.g., specific indexing, row-level locking behavior).
-   **Detailed Job Control Logic:** While the shell script indicates job management, the precise logic for "ignoring active jobs" and "deactivating older jobs" resides within the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) and `d_ausd_v_ta_discount.sql`. This logic needs to be fully extracted and understood to ensure accurate replication in BigQuery using DML against new job control tables. The commented-out `h_alis_job.ksh` might also contain relevant logic.
-   **Utility Script Functionality:** The detailed implementations of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` are not available. Their functionalities must be reviewed. Most are likely translatable to BigQuery scripting (e.g., date functions), but a few might require custom BigQuery UDFs or external Cloud Functions if very complex.
-   **Parameter Origin:** The source of `p_JobKennung` and `p_EintragsNr` (i.e., how they are supplied to the original shell script) needs to be identified to ensure proper parameter passing in the BigQuery/orchestration environment.
-   **Semi-Automatic Classification:** The `semi_auto` migration bucket indicates that manual effort will be required. This is primarily due to the need for manual translation and potential re-design of the core SQL logic and job control mechanisms.

## 8. Build Plan

1.  **Phase 1: Discovery & Analysis (Manual)**
    *   **Action:** Retrieve and thoroughly analyze the source code of `d_ausd_v_ta_discount.sql`.
    *   **Action:** Analyze the content of `h_alis_sqlplus.ksh` to understand `starteSQLSkript` and its interaction with the RDBMS.
    *   **Action:** Review `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh` to understand their exact functionalities and identify BigQuery equivalents.
    *   **Output:** Detailed BigQuery SQL migration plan for `d_ausd_v_ta_discount.sql`, functional specifications for utility scripts, and clarified job control logic.

2.  **Phase 2: BigQuery DDL & Schema Migration (BQSQL)**
    *   **Action:** Design and create the BigQuery table DDL for `ta_discount`, including appropriate partitioning and clustering strategies.
    *   **Action:** Design and create BigQuery DDL for new job control, logging, and audit tables to replace legacy job management and temporary file usage.
    *   **Output:** `ta_discount.ddl`, `job_control_table.ddl`, `job_log_table.ddl`.

3.  **Phase 3: Core Logic Translation (BQSQL)**
    *   **Action:** Translate the `d_ausd_v_ta_discount.sql` script into optimized BigQuery SQL.
    *   **Output:** `d_ausd_v_ta_discount_bq.sql` (can be integrated into the stored procedure).

4.  **Phase 4: BigQuery Stored Procedure Development (BQSQL)**
    *   **Action:** Create the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control`.
    *   **Action:** Implement parameter handling and validation within the stored procedure.
    *   **Action:** Integrate the translated `d_ausd_v_ta_discount_bq.sql` logic into the stored procedure.
    *   **Action:** Implement the job control logic (insert/update job status, deactivate old jobs) using BigQuery DML against the new job control tables.
    *   **Action:** Implement error handling, logging, and record count retrieval using BigQuery scripting features.
    *   **Output:** `r_ausd_vertrag_control.bqsql` (stored procedure definition).

5.  **Phase 5: Orchestration (Python/Airflow)**
    *   **Action:** If required, develop a Cloud Composer (Airflow) DAG to schedule and invoke the `r_ausd_vertrag_control` BigQuery Stored Procedure.
    *   **Action:** Configure parameter passing from the DAG to the stored procedure.
    *   **Output:** `airflow_dag_k_ausd_v_ta_discount.py`.

6.  **Phase 6: Testing & Validation (Manual/Automated)**
    *   **Action:** Develop and execute unit tests for the BigQuery Stored Procedure.
    *   **Action:** Develop and execute integration tests to verify end-to-end data processing and job control.
    *   **Output:** Test plan and results.

7.  **Phase 7: Deployment (CI/CD)**
    *   **Action:** Deploy all BigQuery DDLs, Stored Procedures, and (if applicable) Airflow DAGs to the target BigQuery environment via CI/CD pipelines.
    *   **Output:** Deployed BigQuery objects and operational DAG.