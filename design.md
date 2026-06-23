# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_cntrct_valid.ksh`, serves as a control script for a larger data processing workflow related to `ta_cntrct_valid`. Its primary purpose is to orchestrate the execution of an SQL script (`d_ausd_v_ta_cntrct_valid.sql`), manage job states, and handle parameter parsing and environment setup. Specifically, it ensures that active jobs are ignored, registers job execution in a job table, and deactivates older active jobs. The script takes `p_JobKennung` (job identifier) and `p_EintragsNr` (entry number) as input parameters.

## 2. Source Inventory
The job consists of a single KornShell script.
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`
  - **Technology**: KornShell (shell script)
  - **Complexity Tier**: medium
  - **Automation Bucket**: semi_auto
  - **Summary**: This ksh script acts as a control script for `r_ausd_vertrag.ksh`, handling parameter parsing, environment setup, and orchestrating the execution of an SQL script to process data related to `ta_cntrct_valid`.

## 3. Target Architecture
The migration target is Google BigQuery. The existing KornShell script and its associated SQL logic will be converted into a BigQuery Stored Procedure, potentially supplemented by BigQuery SQL scripts for the core data processing. Orchestration, if required for sequencing multiple BigQuery components or external triggers, will leverage Google Cloud Workflows, Cloud Composer, or Cloud Scheduler.

- **Main Component**: BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`)
  - This stored procedure will encapsulate the parameter parsing, validation, job state management (start, complete, error), and the invocation of the core data processing logic.
- **Data Processing Logic**: BigQuery SQL script(s) or another BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_cntrct_valid`)
  - This will contain the actual business logic that was previously within `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql`.
- **Control/Logging Tables**: BigQuery tables for `job_table` and `job_log`.
  - These tables will manage job metadata, status, and error messages, replacing the shell script's implicit job tracking and error reporting.
- **Temporary Data**: BigQuery temporary tables or CTEs will replace the filesystem-based temporary file for record counting.

## 4. Data Flow & Lineage
The original script's data flow is primarily an orchestration pattern:
1. **Environment Setup**: Sourcing of several utility `.ksh` scripts.
2. **Parameter Parsing**: `getopts` for `p_JobKennung` and `p_EintragsNr`.
3. **Parameter Validation**: `pruefeParameterGesetzt` function checks for mandatory parameters.
4. **SQL Script Invocation**: `starteSQLSkript` function executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql`, passing job parameters. This function likely uses `sqlplus` or a similar tool.
5. **Job State Management**: Implicit handling of "active jobs" and deactivation of old jobs, likely within the `starteSQLSkript` or the SQL script itself.
6. **Record Counting**: A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_valid_$$.tmp`) is used to store the count of processed records, which is then read back into `v_records`.

In BigQuery, this flow will be re-engineered:
- **Parameters**: Passed directly to the BigQuery Stored Procedure.
- **Validation**: Replaced with `ASSERT` statements or `IF` conditions within the stored procedure.
- **Job State Management**: `INSERT` and `UPDATE` statements on dedicated BigQuery `job_table` for tracking job status.
- **SQL Script Invocation**: The core SQL logic from `d_ausd_v_ta_cntrct_valid.sql` will be inlined within the stored procedure or called as a separate BigQuery script/procedure.
- **Record Counting**: Replaced with `SELECT COUNT(*)` on the processed data and storing the result in a BigQuery variable (`DECLARE v_records INT64; SET v_records = ...`).
- **Error Handling**: Replaced with BigQuery `EXCEPTION WHEN ERROR` blocks and logging to a `job_log` table.

## 5. Transformation Logic
The `k_ausd_v_ta_cntrct_valid.ksh` script itself contains primarily control and orchestration logic rather than direct data transformations. The actual data transformation logic is assumed to be within the external SQL script, `d_ausd_v_ta_cntrct_valid.sql`.

**Key Transformation Areas for Migration:**
- **Parameter Handling**:
  - **Legacy**: `getopts` in ksh.
  - **Target**: BigQuery Stored Procedure parameters (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`).
- **Conditional Logic**:
  - **Legacy**: `if [ ! $ErrNr -eq 0 ]` and `case` statements in ksh.
  - **Target**: BigQuery `IF` statements, `ASSERT` for parameter validation, and `BEGIN...EXCEPTION WHEN ERROR...END` blocks for error handling.
- **Job State Updates**:
  - **Legacy**: Implicit job tracking and `starteSQLSkript` function.
  - **Target**: Explicit `INSERT` and `UPDATE` statements on a `job_table` in BigQuery.
- **Temporary File for Record Count**:
  - **Legacy**: `tmpFile=\"$DW_DIR_UTL/..._$$.tmp\"` and `eval \"v_records=`cat $tmpFile`\"`.
  - **Target**: BigQuery `DECLARE` and `SET` statements for variables (e.g., `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*)...)`).
- **Sourced Utility Scripts**:
  - **Legacy**: `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/.../f_alis_msgerr.ksh`, etc.
  - **Target**: The functionalities of these helper scripts (error messaging, date handling, parameter validation, SQL execution wrappers) will need to be re-implemented directly in BigQuery Stored Procedure logic or standard BigQuery functions/procedures.
- **SQL Script Execution**:
  - **Legacy**: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`.
  - **Target**: The SQL content of `d_ausd_v_ta_cntrct_valid.sql` will be directly incorporated into the BigQuery stored procedure or called as a separate BigQuery SQL script.

## 6. External Dependencies
The script has several dependencies which need to be addressed in the migration:

- **Environment File**: `$HOME/.dw_init`
  - This file likely sets environment variables crucial for the script's execution.
  - **Migration Strategy**: Any essential configurations from this file will be translated into BigQuery project/dataset configurations, environment variables for Cloud Workflows/Composer, or parameters for the BigQuery stored procedure.
- **Utility Scripts**:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date checks)
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL execution helpers)
  - **Migration Strategy**: The functionality of these scripts will be re-implemented using native BigQuery scripting features, UDFs, or integrated into the main BigQuery stored procedure. Specifically, error handling will use `RAISE` and error logging tables, date checks will use BigQuery date functions, parameter parsing will be handled by stored procedure arguments, and SQL execution will be direct BigQuery SQL.
- **External SQL Script**: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql`
  - This contains the core business logic.
  - **Migration Strategy**: This SQL script must be separately analyzed and converted to BigQuery SQL, potentially becoming another BigQuery stored procedure or an integral part of the main `r_ausd_vertrag_control` procedure.
- **Temporary Filesystem**: Used for record counting.
  - **Migration Strategy**: Replaced by BigQuery temporary tables, CTEs, or variables within stored procedures.
- **SQL*Plus (implied by `h_alis_sqlplus.ksh` and `starteSQLSkript`)**:
  - **Migration Strategy**: Direct BigQuery SQL execution, removing the need for an external SQL client.

## 7. Unresolved / Risks
- **Unresolved Lineage Edges**: The initial `lineage_edges` query returned no explicit INVOKES/READS/WRITES edges for this script. While the script content clearly shows dependencies (sourced ksh files, executed SQL file), the automated lineage did not capture them. This means a manual review of these internal script dependencies is crucial to ensure all components are migrated.
- **Logic within Sourced Scripts**: The detailed logic within the sourced `.ksh` utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is not known. These scripts must be thoroughly analyzed to understand their exact functionality and translate them correctly into BigQuery.
- **Core SQL Script (`d_ausd_v_ta_cntrct_valid.sql`)**: The actual data transformation logic is assumed to be within this script. This script needs to be obtained, analyzed, and migrated to BigQuery SQL or a BigQuery stored procedure. Its complexity and specific transformations are currently unknown.
- **`DW_DIR_UTL` and `BERT_DIR_ROOT` Variables**: The exact values and resolution of these environment variables are not provided by the analysis. These will need to be configured appropriately in the BigQuery environment or through orchestration.
- **`starteSQLSkript` Function Details**: The exact implementation of `starteSQLSkript` within `h_alis_sqlplus.ksh` is unknown. It might contain additional logic (e.g., connection details, error handling) that needs to be replicated.

## 8. Build Plan
The migration will proceed in the following ordered steps:

1.  **Analyze and Migrate Core SQL Script**:
    *   **Action**: Obtain the content of `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql`.
    *   **Action**: Analyze its SQL statements (DML, DDL, DQL) and convert them to BigQuery SQL. This might result in one or more BigQuery SQL scripts or a new BigQuery stored procedure (`project.dataset.d_ausd_v_ta_cntrct_valid`).
    *   **Language**: BigQuery SQL

2.  **Define BigQuery Schema for Control Tables**:
    *   **Action**: Create DDL for `project.dataset.job_table` and `project.dataset.job_log` in BigQuery to track job execution status and errors.
    *   **Language**: BigQuery DDL

3.  **Develop Main BigQuery Stored Procedure**:
    *   **Action**: Create the `project.dataset.r_ausd_vertrag_control` stored procedure in BigQuery.
    *   **Content**:
        *   Accept `p_JobKennung` and `p_EintragsNr` as parameters.
        *   Implement parameter validation using `ASSERT` or `IF` statements.
        *   Implement job start logging using `INSERT` into `job_table`.
        *   Implement logic to deactivate older active jobs (`UPDATE job_table`).
        *   Integrate or call the BigQuery-migrated core SQL logic (from step 1).
        *   Implement record counting using BigQuery `SELECT COUNT(*)` and assign to a variable.
        *   Implement job completion logging and record count update (`UPDATE job_table`).
        *   Implement error handling using `BEGIN...EXCEPTION WHEN ERROR...END` and log errors to `job_log`.
    *   **Language**: BigQuery SQL (Stored Procedure)

4.  **Migrate Utility Script Functionalities**:
    *   **Action**: Extract relevant functionalities from `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh`.
    *   **Content**: Implement these functionalities directly within the `r_ausd_vertrag_control` stored procedure using BigQuery's native features or create small BigQuery UDFs/procedures if appropriate for reusable logic.
    *   **Language**: BigQuery SQL (UDFs/Stored Procedures)

5.  **Develop Orchestration (Optional but Recommended for Production)**:
    *   **Action**: If the job needs to be scheduled or triggered by other events, create a Cloud Scheduler job, a Cloud Workflows definition, or a Cloud Composer DAG to invoke the `r_ausd_vertrag_control` BigQuery stored procedure.
    *   **Language**: YAML (Cloud Workflows), Python (Cloud Composer)

6.  **Testing**:
    *   **Action**: Implement unit and integration tests for the BigQuery stored procedures and any orchestration components.
    *   **Language**: BigQuery SQL (for stored procedure tests), Python (for orchestration/integration tests).