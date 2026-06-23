# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_c_bfc.ksh` to Google Cloud Platform, targeting BigQuery for data processing.

The original script `k_ausd_v_ta_c_bfc.ksh` acts as a control and orchestration script. Its primary purposes are:
*   To manage the execution of a core SQL script (`d_ausd_v_ta_c_bfc.sql`) for data processing.
*   To ensure active jobs are ignored during execution, preventing conflicts.
*   To register job execution details in a job tracking mechanism.
*   To deactivate previously active jobs as part of its workflow.
*   To handle parameter validation and basic error reporting for the overall job.

The scope of this migration is to re-implement the functionality of this KornShell orchestration, including its parameter handling, job control, and SQL script invocation, into a BigQuery-native solution, potentially augmented by an orchestration layer if necessary.

## 2. Source Inventory
The job is primarily composed of one KornShell script and its dependencies.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto (indicating some manual effort required)
    *   **Summary:** This is a control script that orchestrates the execution of a SQL script (`d_ausd_v_ta_c_bfc.sql`) for data processing. It handles parameter validation, job status checks, and error reporting, ensuring active jobs are managed and old ones deactivated.

**Dependencies:**
The `k_ausd_v_ta_c_bfc.ksh` script has several internal dependencies, primarily sourced utility scripts and an invoked SQL script:
*   `. $HOME/.dw_init`: Environment initialization script.
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Custom error messaging utility.
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus wrapper/helper script.
*   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql`: The main SQL script containing the data processing logic. This is the primary data transformation component.
*   `$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`: A temporary file used to store the count of processed records.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, with BigQuery as the central data processing engine.

*   **Core Logic Execution:** The primary orchestration and data processing logic will be encapsulated within a BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_ta_c_bfc`. This procedure will directly replace the `k_ausd_v_ta_c_bfc.ksh` script.
*   **Data Processing:** The SQL logic from `d_ausd_v_ta_c_bfc.sql` will be migrated to BigQuery Standard SQL and executed within the BigQuery Stored Procedure, likely via `EXECUTE IMMEDIATE`.
*   **Job Control & Logging:** Dedicated BigQuery tables will be established for job tracking and error logging:
    *   `project.dataset.job_table`: To record job status, start/end times, and other metadata, replacing the legacy job management.
    *   `project.dataset.error_log`: To capture and store error messages, replacing the custom `DWMSG_MeldeFehler` mechanism.
*   **Orchestration (Optional):** For highly complex job control patterns or external system interactions, Cloud Composer (managed Apache Airflow) might be used as an external orchestrator to invoke the BigQuery Stored Procedure and manage broader workflow dependencies. However, for this specific job, a self-contained BigQuery Stored Procedure is the primary target.

## 4. Data Flow & Lineage
The data flow describes the execution sequence and dependencies:

1.  **Input Parameters:** The migrated BigQuery Stored Procedure will accept `p_JobKennung` and `p_EintragsNr` as input parameters. These replace the `getopts` parsed parameters from the KornShell script.
2.  **Environment Variables:** Legacy environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by:
    *   Direct references to BigQuery project/dataset IDs.
    *   Constants within the BigQuery Stored Procedure.
    *   Configuration parameters passed to the BigQuery Stored Procedure or managed by an orchestrator.
3.  **Parameter Validation:** The procedure will perform validation of `p_JobKennung` and `p_EintragsNr`.
4.  **Job Status Management:** Before executing the core data logic, the BigQuery Stored Procedure will interact with `project.dataset.job_table` to check for active jobs and update job statuses as per the original script's intent ("aktive Jobs werden ignoriert", "alte aktive Jobs werden einfach dekativiert").
5.  **Core Data Transformation:** The migrated SQL from `d_ausd_v_ta_c_bfc.sql` will be executed within the BigQuery Stored Procedure. This SQL will perform the actual data processing, reading from source tables (to be identified from `d_ausd_v_ta_c_bfc.sql`) and writing to target tables within BigQuery.
6.  **Record Count:** Instead of a temporary file, the count of processed records will be obtained via a `SELECT COUNT(*)` query on the target table(s) or as an output parameter of the data transformation logic.
7.  **Error Handling & Logging:** Any errors encountered during parameter validation or SQL execution will be caught and logged to `project.dataset.error_log`.

## 5. Transformation Logic
The transformation from the KornShell script to BigQuery Stored Procedure will involve mapping shell constructs to their BigQuery equivalents:

*   **Parameter Parsing (`getopts`):** Replaced by `IN` parameters in the BigQuery Stored Procedure (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`).
*   **Variable Declarations & Assignments:** Shell variables (`v_TabName`, `ErrNr`, `ErrArg`, `v_records`) will be replaced by `DECLARE` statements and `SET` assignments in BigQuery SQL.
*   **Conditional Logic (`if [ ! $ErrNr -eq 0 ]`):** Translated to `IF ... THEN ... END IF;` blocks in BigQuery SQL for parameter validation and error handling.
*   **Error Reporting (`DWMSG_MeldeFehler`, `echo`):** Replaced by `INSERT` statements into the `project.dataset.error_log` table and `SELECT` statements for output messages. The procedure will use `LEAVE` to exit on critical errors.
*   **Sourced Utility Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):**
    *   Environment setup (`.dw_init`) will be managed by BigQuery project/dataset settings or configuration parameters.
    *   Error messaging (`f_alis_msgerr.ksh`) will be replaced by the BigQuery `error_log` table.
    *   Date utilities (`h_alis_date.ksh`) will be replaced by BigQuery's built-in date/time functions (e.g., `CURRENT_TIMESTAMP()`).
    *   Parameter utilities (`h_alis_parameter.ksh`) are replaced by the BigQuery Stored Procedure's `IN` parameters.
    *   SQL*Plus wrapper (`h_alis_sqlplus.ksh`) and the `starteSQLSkript` function will be replaced by direct `EXECUTE IMMEDIATE` calls to BigQuery Standard SQL, managing the job control logic (ignoring active jobs, deactivating old ones) directly within the procedure.
*   **SQL Script Invocation (`d_ausd_v_ta_c_bfc.sql`):** The content of this SQL script will be migrated to BigQuery Standard SQL and embedded or called as a separate sub-procedure within the main `project.dataset.r_ausd_ta_c_bfc` stored procedure.
*   **Temporary File (`$tmpFile`) and Record Count (`eval "v_records=\`cat $tmpFile\`"`):** This will be replaced by a `SELECT COUNT(*)` query on the target table(s) or a `DECLARE` variable that captures the row count from the executed data transformation logic.

## 6. External Dependencies
The original job interacts with an Oracle database (implied by SQL*Plus utilities and SQL script execution).

*   **Legacy System:** Oracle Database
    *   **Replacement:** BigQuery. All database interactions (data reads, writes, and DDL/DML operations) will be translated to BigQuery Standard SQL and executed against BigQuery datasets and tables.
*   **Legacy Job Control System:** The shell script interacts with an implicit job control system (active jobs, deactivation).
    *   **Replacement:** A dedicated BigQuery job control table (`project.dataset.job_table`) will be used to manage and track job executions and statuses.

## 7. Unresolved / Risks
*   **`d_ausd_v_ta_c_bfc.sql` Analysis:** The content and complexity of the SQL script `d_ausd_v_ta_c_bfc.sql` are critical and require separate detailed analysis and migration effort. Its specific tables, columns, and transformation logic will dictate the final BigQuery SQL implementation.
*   **Job Deactivation Logic:** The exact logic for "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert" from the `starteSQLSkript` function and surrounding code needs to be fully reverse-engineered and precisely re-implemented in BigQuery SQL to maintain the original job's integrity.
*   **Utility Script Functionality:** A thorough review of all sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is required to ensure all relevant functionality is captured and correctly translated to BigQuery SQL or Python.
*   **Data Type and Function Mapping:** Potential risks exist in accurately mapping all legacy SQL data types and functions (likely Oracle-specific) to BigQuery Standard SQL.
*   **`semi_auto` Bucket:** The "semi_auto" migration bucket indicates that some manual intervention and custom development will be necessary beyond automated translation tools. This is anticipated due to the orchestration nature and custom shell utilities.

## 8. Build Plan
The migration will proceed in the following ordered steps:

1.  **Analyze and Migrate Core SQL (`d_ausd_v_ta_c_bfc.sql`):**
    *   **Description:** Perform a detailed analysis of `d_ausd_v_ta_c_bfc.sql` to identify all tables, columns, data types, and transformation logic. Translate this SQL to BigQuery Standard SQL, ensuring functional equivalence. This will likely result in a separate BigQuery SQL script or a sub-procedure.
    *   **Language:** BigQuery SQL
2.  **Define BigQuery Control Schemas:**
    *   **Description:** Create Data Definition Language (DDL) for the `project.dataset.job_table` and `project.dataset.error_log` tables in BigQuery.
    *   **Language:** BigQuery DDL
3.  **Re-implement Utility Logic:**
    *   **Description:** Translate the essential functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` into BigQuery SQL functions or procedures. This includes error logging, date manipulations, and any database connection/execution wrappers.
    *   **Language:** BigQuery SQL
4.  **Develop Main BigQuery Stored Procedure (`project.dataset.r_ausd_ta_c_bfc`):**
    *   **Description:** Create the primary BigQuery Stored Procedure. This procedure will:
        *   Accept `p_JobKennung` and `p_EintragsNr` as input parameters.
        *   Implement parameter validation.
        *   Integrate job control logic (checking active jobs, deactivating old jobs) using `project.dataset.job_table`.
        *   Invoke the migrated core SQL logic from `d_ausd_v_ta_c_bfc.sql` via `EXECUTE IMMEDIATE`.
        *   Capture and report the record count.
        *   Log errors to `project.dataset.error_log`.
    *   **Language:** BigQuery SQL
5.  **Implement External Orchestration (if needed):**
    *   **Description:** If complex external dependencies or scheduling requirements necessitate it, create an Apache Airflow DAG in Cloud Composer to invoke the BigQuery Stored Procedure, manage its parameters, and coordinate with other jobs.
    *   **Language:** Python
6.  **Testing and Validation:**
    *   **Description:** Conduct comprehensive unit, integration, and user acceptance testing to ensure the migrated solution functions correctly and produces accurate results compared to the legacy system.
    *   **Language:** BigQuery SQL, Python (for orchestration testing)