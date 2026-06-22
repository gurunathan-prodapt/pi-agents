# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_inv_assign.ksh`. The primary purpose of this script is to act as a control and orchestration wrapper for a database extraction job related to the `ta_inv_assign` table. It handles environment setup, parameter parsing and validation, and the execution of a core SQL script (`d_ausd_v_ta_inv_assign.sql`). The script also manages job status, including ignoring active jobs and deactivating old ones, and logs the number of processed records. The scope of this migration is to replatform the orchestration logic to Google Cloud Composer (Airflow) and refactor the underlying SQL logic to Google BigQuery.

## 2. Source Inventory
The job consists of a single source file:

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh`
*   **Technology**: KornShell (ksh)
*   **Description**: Orchestration script for a database extraction job. It sources utility scripts for error handling, date checks, parameter parsing, and SQL*Plus interaction. Its main function is to execute an SQL script (`d_ausd_v_ta_inv_assign.sql`) and manage job status within a job table `ta_inv_assign`.
*   **Complexity Tier**: medium
*   **Automation Bucket**: semi_auto

## 3. Target Architecture
The migrated solution will leverage Google Cloud services, primarily:

*   **Google BigQuery**: For data storage and execution of SQL transformation logic. The `ta_inv_assign` table and any intermediate or audit tables will reside here. The logic from `d_ausd_v_ta_inv_assign.sql` will be converted into BigQuery SQL, potentially within a BigQuery Stored Procedure.
*   **Google Cloud Composer (Airflow)**: To orchestrate the execution of the BigQuery components. This will replace the shell script's role as the primary job scheduler and controller.
*   **Cloud Storage**: Potentially for temporary data staging if required by the SQL logic, replacing local temporary files.

The overall architecture will involve an Airflow DAG that calls a BigQuery Stored Procedure (or a series of BigQuery SQL statements) to perform the data processing.

## 4. Data Flow & Lineage
The original script's data flow involves:

1.  **Inputs**:
    *   **Command-line parameters**: `-j <JobKennung>` (job identifier) and `-f <EintragsNr>` (entry number).
    *   **Sourced utility scripts**:
        *   `$HOME/.dw_init`: Environment initialization.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing/validation.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL execution wrapper.
    *   **External SQL script**: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_assign.sql` - This is the core business logic.
    *   **Temporary file**: `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_assign_$$.tmp` (reads record count).

2.  **Processing (Current)**:
    *   Environment variables are set up via `$HOME/.dw_init`.
    *   Parameters are parsed and validated using `getopts` and helper functions.
    *   `starteSQLSkript` (from `h_alis_sqlplus.ksh`) is called to execute `d_ausd_v_ta_inv_assign.sql`, passing `p_EintragsNr` and `p_JobKennung`. This step likely handles job control and writes the record count to the temporary file.
    *   The `v_records` variable is populated by reading the temporary file.

3.  **Outputs**:
    *   Console messages (errors, completion status).
    *   Updates to the `ta_inv_assign` job table (implicit from `starteSQLSkript` and `d_ausd_v_ta_inv_assign.sql`).
    *   Job status updates (e.g., ignoring active jobs, deactivating old ones).

**Migrated Data Flow & Lineage**:
The Airflow DAG will become the orchestrator.
*   The DAG will accept parameters equivalent to `p_JobKennung` and `p_EintragsNr`.
*   It will contain tasks to:
    *   Call a BigQuery Stored Procedure (representing the migrated `d_ausd_v_ta_inv_assign.sql` logic).
    *   Pass the parameters to the BigQuery Stored Procedure.
    *   The Stored Procedure will perform the data processing, including any logic for ignoring/deactivating jobs, and update the `ta_inv_assign` table (or its BigQuery equivalent).
    *   The Stored Procedure can return the record count, which can be logged by the Airflow DAG if needed.
    *   Audit/job logging will be handled by DML operations within the BigQuery Stored Procedure into dedicated audit tables.

## 5. Transformation Logic
The transformation logic will primarily involve converting the KornShell orchestration into an Airflow DAG and the Oracle SQL within `d_ausd_v_ta_inv_assign.sql` into BigQuery SQL.

*   **Parameter Handling**: Shell's `getopts` will be replaced by Airflow DAG parameters (e.g., `dag_run.conf`). These parameters will then be passed to the BigQuery Stored Procedure.
*   **Environment Setup**: Explicit environment sourcing will be replaced by Airflow's environment configuration or BigQuery connection settings.
*   **Utility Scripts**: Shell utility scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` will be replaced by:
    *   Error handling mechanisms built into Airflow and BigQuery (e.g., `try-catch` blocks in stored procedures, Airflow task failure handling).
    *   BigQuery's native date/time functions.
    *   Parameter validation within the BigQuery Stored Procedure (using `IF` conditions and `RAISE` statements).
    *   Direct BigQuery SQL execution using BigQuery operators in Airflow.
*   **SQL Execution (`d_ausd_v_ta_inv_assign.sql`)**: The content of this SQL file will be translated into BigQuery SQL. This might involve:
    *   Converting Oracle-specific SQL syntax to BigQuery's dialect.
    *   Encapsulating the logic within a BigQuery Stored Procedure for reusability and parameterization.
    *   Implementing the logic for ignoring/deactivating jobs and updating `ta_inv_assign` within this Stored Procedure.
*   **Record Counting**: The `eval "v_records=\`cat $tmpFile\`"` mechanism will be replaced by:
    *   Returning the affected row count from the BigQuery Stored Procedure.
    *   Using `COUNT(*)` in the BigQuery SQL if the count needs to be derived after the operation.
    *   Storing the count directly in a BigQuery audit/job table.

## 6. External Dependencies
The original script explicitly refers to several external `.ksh` scripts and one `.sql` file. There are no other external systems mentioned (like Oracle, SFTP, S3, etc.) in `external_systems` output.

*   **Utility KornShell Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: These will be replaced by native BigQuery features, Airflow task dependencies, and BigQuery's SQL capabilities as described in Section 5. They will not be migrated as separate scripts.
*   **Core SQL Script (`d_ausd_v_ta_inv_assign.sql`)**: This is the primary business logic. It will be converted into BigQuery SQL and integrated into a BigQuery Stored Procedure or directly executed via Airflow.
*   **Job Table (`ta_inv_assign`)**: This table will be migrated to BigQuery. All DML operations against it will be converted to BigQuery SQL.

## 7. Unresolved / Risks
*   **Missing `d_ausd_v_ta_inv_assign.sql` content**: The exact business logic in the SQL script is unknown without its content. This is the biggest gap in the current design. A full migration requires this content to be analyzed and converted to BigQuery SQL.
*   **Oracle-specific SQL**: If `d_ausd_v_ta_inv_assign.sql` contains Oracle-specific SQL constructs (e.g., PL/SQL, specific functions), these will require careful refactoring for BigQuery compatibility.
*   **Implicit Job Control Logic**: The `starteSQLSkript` function and the SQL script itself likely contain implicit job control logic (e.g., for `a) aktive Jobs werden ignoriert`, `c) alte aktive Jobs werden einfach dekativiert`). This logic needs to be fully understood from the SQL script to be accurately replicated in BigQuery.
*   **`tmpFile` Usage**: The use of a temporary file for record counts is a shell-specific pattern. While easily replaced by a variable or direct return from a stored procedure in BigQuery, the exact significance of this temporary file in downstream processes needs to be confirmed.

## 8. Build Plan
The build plan will proceed in an iterative fashion:

1.  **Analyze `d_ausd_v_ta_inv_assign.sql`**: Obtain and analyze the content of the SQL script to understand its exact data transformations, DML operations, and any job control logic.
2.  **Design BigQuery Schema**: Create the target BigQuery schema, including the `ta_inv_assign` table and any necessary audit/job tables.
3.  **Convert SQL to BigQuery Stored Procedure**: Translate the logic from `d_ausd_v_ta_inv_assign.sql` into a BigQuery Stored Procedure. This includes parameterization, error handling, and DML operations.
4.  **Develop Airflow DAG**:
    *   Create an Airflow DAG with appropriate BigQuery operators to call the newly created BigQuery Stored Procedure.
    *   Implement parameter passing from the DAG to the Stored Procedure.
    *   Integrate monitoring and alerting mechanisms within the DAG.
5.  **Refactor Job Control Logic**: Ensure all job control aspects (ignoring/deactivating jobs) are correctly implemented within the BigQuery Stored Procedure or the Airflow DAG, replacing the original shell script's logic.
6.  **Testing**: Develop and execute unit, integration, and end-to-end tests to ensure functional equivalence and performance.
7.  **Deployment**: Deploy the BigQuery Stored Procedure and Airflow DAG to the target GCP environment.

**Generated artifacts**:
*   BigQuery SQL DDL for `ta_inv_assign` and audit/job tables.
*   BigQuery SQL DDL for the Stored Procedure implementing `d_ausd_v_ta_inv_assign.sql`'s logic.
*   Python code for the Airflow DAG.