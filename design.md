# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_v_ta_cntrct_valid.ksh` to Google BigQuery. The original script acts as a control script for a larger process (`r_ausd_vertrag.ksh`), primarily orchestrating the execution of a SQL script for data processing. Its key functions include:
- Ignoring active jobs to prevent concurrent execution issues.
- Executing a core SQL script responsible for processing contract validity data.
- Registering and managing job execution status within a job tracking table.
- Deactivating older, active jobs as part of its control flow.

The job is considered to be of medium complexity, as indicated by its stage distribution. The primary output is data written to a table named `ta_cntrct_valid`, and updates to job tracking/logging tables.

## 2. Source Inventory

The migration scope is focused on a single primary component:

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`
- **Technology:** KornShell Script
- **Category:** shell
- **Tool:** KornShell
- **Complexity Tier:** Unknown (file_complexity data was not available)
- **Automation Bucket:** semi_auto
- **Purpose:** ETL orchestration, parameter handling, and SQL script invocation.

**Inferred Dependencies (from script content):**
- **Sourced Shell Utilities:**
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing)
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus execution wrapper)
- **External SQL Script:**
    - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql` (Core data processing logic)

## 3. Target Architecture
The migrated solution will leverage Google BigQuery's capabilities for data processing and orchestration.

**Core Component:**
- **BigQuery Stored Procedure:** A BigQuery Stored Procedure, tentatively named `project.dataset.bert_k_ausd_v_ta_cntrct_valid`, will encapsulate the entire logic of the original KornShell script and its invoked SQL component. This procedure will handle parameter validation, error handling, job tracking, and the core data transformations.

**Data Storage:**
- **`ta_cntrct_valid`:** The target table for processed contract validity data, residing in a BigQuery dataset (`project.dataset.ta_cntrct_valid`).
- **`error_log`:** A BigQuery table (`project.dataset.error_log`) for centralized logging of errors encountered during job execution.
- **`job_table`:** A BigQuery table (`project.dataset.job_table`) for tracking the status and metadata of job runs, mirroring the functionality described in the source script's comments.
- **`job_result_log`:** A BigQuery table (`project.dataset.job_result_log`) to store job execution results, such as the count of processed records.

**Orchestration (Conceptual):**
- **Cloud Composer / Cloud Workflows:** If this job is part of a larger scheduled workflow, it will be orchestrated using Cloud Composer (Apache Airflow) or Cloud Workflows to define dependencies, scheduling, and error handling for the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script orchestrates the following flow:
1. **Initialization:** Sources environment variables (`. $HOME/.dw_init`) and helper KornShell scripts for error handling, date utilities, parameter parsing, and SQL execution.
2. **Parameter Intake:** Accepts `p_JobKennung` (job identifier) and `p_EintragsNr` (entry number) via command-line arguments.
3. **Validation:** Validates the presence of required parameters. If validation fails, it reports an error and exits.
4. **Job Registration:** (Inferred from comments and purpose) The script, through its helper functions or the invoked SQL, registers the job's start in a job tracking table.
5. **SQL Execution:** Invokes an external SQL script (`d_ausd_v_ta_cntrct_valid.sql`) via a wrapper function `starteSQLSkript` (from `h_alis_sqlplus.ksh`). This SQL script is responsible for the main data processing and interaction with the `ta_cntrct_valid` table. The shell script's comments suggest that active jobs are ignored and old active jobs are deactivated as part of this process.
6. **Result Logging:** Reads the number of processed records from a temporary file and stores it in a variable `v_records`.
7. **Job Completion:** Logs the job's completion and the number of processed records in the job tracking/logging tables.

**Migrated BigQuery Data Flow:**
1. **Stored Procedure Invocation:** The BigQuery Stored Procedure `bert_k_ausd_v_ta_cntrct_valid` is called with `p_JobKennung` and `p_EintragsNr` as input parameters.
2. **Parameter Validation:** Input parameters are validated within the stored procedure using BigQuery SQL logic (`IF` statements).
3. **Error Logging:** Errors during validation or execution are recorded in the `project.dataset.error_log` table.
4. **Job Status Update:** The `project.dataset.job_table` is updated to reflect the job's start and completion status.
5. **Core Transformation:** The logic from `d_ausd_v_ta_cntrct_valid.sql` is translated to BigQuery SQL and embedded directly into the stored procedure. This logic will perform reads and writes to `project.dataset.ta_cntrct_valid` and potentially other source tables.
6. **Record Count:** The count of processed records is obtained using `SELECT COUNT(*)` queries within the stored procedure.
7. **Result Logging:** The processed record count is stored in the `project.dataset.job_result_log` table.

## 5. Transformation Logic
The transformation logic will be re-implemented directly in BigQuery SQL within the stored procedure.

**Key Transformations:**
- **Parameter Handling:**
    - **Source:** KornShell `getopts` for command-line arguments.
    - **Target:** BigQuery Stored Procedure `IN` parameters.
- **Parameter Validation:**
    - **Source:** Shell `pruefeParameterGesetzt` function and `if` conditions.
    - **Target:** BigQuery SQL `IF` conditions and `ASSERT` statements within the stored procedure.
- **Error Handling:**
    - **Source:** Sourced `f_alis_msgerr.ksh` and custom shell `if` conditions for error code management.
    - **Target:** BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and inserts into `project.dataset.error_log`.
- **Job Control & Logging:**
    - **Source:** Implicit job table interactions (comments indicate "Eintrag in die Job-Tabelle" and "alte aktive Jobs werden einfach dekativiert").
    - **Target:** `INSERT` and `UPDATE` statements against `project.dataset.job_table` and `project.dataset.job_result_log`.
- **Core Data Processing:**
    - **Source:** External SQL script `d_ausd_v_ta_cntrct_valid.sql`, executed via `starteSQLSkript`.
    - **Target:** The complete content of `d_ausd_v_ta_cntrct_valid.sql` must be translated into BigQuery SQL and embedded within the stored procedure. This includes any DML (INSERT, UPDATE, DELETE) and DDL (CREATE, ALTER) statements. Special attention is required for any Oracle-specific SQL constructs.
- **Record Counting:**
    - **Source:** Reading a temporary file (`cat $tmpFile`) generated by the SQL execution wrapper.
    - **Target:** `SELECT COUNT(*)` on the target table (or a staging table) and storing the result in a BigQuery `DECLARE` variable, which is then persisted to `project.dataset.job_result_log`.

## 6. External Dependencies
The original script has dependencies on local file system resources and an underlying database (likely Oracle, given the common use of SQL*Plus wrappers).

- **Environment Initialization (`$HOME/.dw_init`):** This file likely sets environment variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`). In BigQuery, these will be replaced by BigQuery project/dataset IDs, procedure parameters, or configuration tables.
- **Sourced KornShell Libraries (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** The functionality of these utilities will be re-implemented directly in BigQuery SQL as part of the stored procedure. Complex logic might require auxiliary BigQuery functions or even Python Cloud Functions if string/date manipulation is beyond standard SQL capabilities.
- **SQL Script (`d_ausd_v_ta_cntrct_valid.sql`):** The contents of this script represent the core business logic and must be fully migrated to BigQuery SQL syntax.
- **Database Connection (via `h_alis_sqlplus.ksh`):** The original script connects to a database (implicitly Oracle). In the BigQuery target, the stored procedure will directly operate on BigQuery tables, eliminating the need for an external database connection wrapper.

No other external systems (e.g., SFTP, S3) were identified in the `lineage_external_systems` analysis.

## 7. Unresolved / Risks
- **SQL Script Content:** The exact SQL logic within `d_ausd_v_ta_cntrct_valid.sql` is currently unknown. This is the biggest risk, as its complexity, use of proprietary SQL features (e.g., Oracle PL/SQL), or specific performance considerations will dictate the effort required for BigQuery migration. A detailed analysis of this SQL file is critical.
- **Job Table Schema:** The schema and exact usage of the "job table" mentioned in the source script are not fully defined in the provided metadata. This schema needs to be reverse-engineered and designed for BigQuery.
- **Assumed Logic:** The design makes assumptions about the logic for "ignoring active jobs" and "deactivating old active jobs" being embedded in the SQL script or its helper routines. This needs confirmation during the analysis of `d_ausd_v_ta_cntrct_valid.sql`.
- **Empty Complexity and Flags:** The absence of `file_complexity` data means we lack detailed flags for specific migration challenges. The `semi_auto` automation bucket suggests potential areas requiring manual intervention or careful design.

## 8. Build Plan
The migration will follow these steps:

1.  **Analyze `d_ausd_v_ta_cntrct_valid.sql`:**
    *   **Action:** Retrieve and thoroughly analyze the source code of `d_ausd_v_ta_cntrct_valid.sql`.
    *   **Output:** Detailed documentation of its SQL statements, tables read/written, and any proprietary syntax.
    *   **Language:** SQL (source), Analysis Document.

2.  **Design BigQuery Schemas:**
    *   **Action:** Define the BigQuery table schemas for `ta_cntrct_valid`, `error_log`, `job_table`, and `job_result_log`. Ensure data types and partitioning/clustering strategies are optimized for BigQuery.
    *   **Output:** BigQuery DDL scripts.
    *   **Language:** BigQuery SQL (DDL).

3.  **Migrate Utility Logic to BigQuery SQL:**
    *   **Action:** Translate the functionalities found in `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` into BigQuery SQL functions, UDFs, or integrate them directly into the main stored procedure. This includes error handling, date formatting, and parameter validation.
    *   **Output:** BigQuery SQL scripts for UDFs/procedures or integrated logic within the main procedure.
    *   **Language:** BigQuery SQL.

4.  **Develop `bert_k_ausd_v_ta_cntrct_valid` BigQuery Stored Procedure:**
    *   **Action:** Create the main BigQuery Stored Procedure. Embed the migrated SQL logic from `d_ausd_v_ta_cntrct_valid.sql`, implement parameter handling, error logging, job status updates, and record counting logic.
    *   **Output:** BigQuery Stored Procedure script.
    *   **Language:** BigQuery SQL.

5.  **Develop Orchestration (if applicable):**
    *   **Action:** If this job is part of a larger workflow, develop an Airflow DAG in Cloud Composer to schedule and execute the `bert_k_ausd_v_ta_cntrct_valid` BigQuery Stored Procedure.
    *   **Output:** Python script for the Airflow DAG.
    *   **Language:** Python.