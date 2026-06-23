# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_geschaeftspartner.ksh` to Google Cloud's BigQuery platform. The script serves as a control and orchestration layer for data processing. Its primary business purpose is to manage the execution of an underlying SQL script (`d_ausd_geschaeftspartner.sql`), including parameter parsing, job status management, and handling of active jobs. Specifically, it ignores already active jobs, executes an SQL script, records job entries, and deactivates old active jobs. This job is assessed as having medium complexity.

## 2. Source Inventory
The job is primarily composed of one KornShell script and its dependencies:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`
    *   **Technology:** KornShell (shell script)
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Semi-automatic (B2)
    *   **Summary:** This ksh script acts as a control script, parsing parameters, sourcing utility functions, and orchestrating the execution of an SQL script for data processing, including job status management.
    *   **Dependencies (identified from source code):**
        *   **Sourced Environment/Utility Scripts:**
            *   `. $HOME/.dw_init` (environment initialization)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utility functions)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus execution wrapper)
        *   **Invoked External Script:**
            *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (date calculation: yesterday/today)
        *   **Executed SQL Script:**
            *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_geschaeftspartner.sql` (core data transformation logic)
        *   **Implicit System Interaction:**
            *   Job management system (functions like `FOSJobDeaktivate`, `FOSJobErzeugeEintrag` are commented out but indicate intended interaction).

## 3. Target Architecture
The target architecture on BigQuery will leverage native Google Cloud services:

*   **Orchestration:** The control flow logic of the shell script will be migrated to BigQuery scripting within a BigQuery Stored Procedure, and potentially orchestrated by Cloud Composer (managed Airflow) or Google Cloud Workflows/Cloud Run for scheduling and external calls if any.
*   **Data Processing:** The SQL logic from `d_ausd_geschaeftspartner.sql` will be converted to native BigQuery SQL (DML/DDL) and integrated into the BigQuery Stored Procedure or called as a separate BigQuery script.
*   **Job Management:** A dedicated BigQuery table will replace the legacy "Job-Tabelle" for tracking job status and metadata.
*   **Parameter Handling:** Parameters will be passed as explicit arguments to BigQuery Stored Procedures.
*   **Date Operations:** BigQuery's native date functions will handle date calculations and validations.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring will be used for error reporting and operational oversight.

## 4. Data Flow & Lineage
The original data flow involves the shell script orchestrating an SQL script. In the migrated BigQuery environment, this flow will be consolidated:

1.  **Input Parameters:** The main BigQuery Stored Procedure will receive parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2.  **Validation & Derivation:** Inside the stored procedure, parameters will be validated (e.g., date format for `p_Stichtag`) and auxiliary dates (yesterday, today) will be derived using BigQuery functions.
3.  **SQL Execution:** The core data transformation logic (migrated from `d_ausd_geschaeftspartner.sql`) will be executed as part of the BigQuery Stored Procedure or as a separate BigQuery SQL statement called by the procedure. This logic will likely:
    *   READ from source tables (e.g., `project.dataset.source_table`).
    *   WRITE to target tables (e.g., `project.dataset.target_table`).
4.  **Record Count & Job Tracking:** After SQL execution, the count of processed records will be captured. This count, along with other job metadata, will be inserted into a BigQuery job tracking table.
5.  **Output:** The stored procedure may return status codes or log messages via Cloud Logging.

**Lineage in Target:**
*   `r_ausd_vertrag_control_sp` (BigQuery Stored Procedure) INVOKES `d_ausd_geschaeftspartner_sql` (BigQuery SQL DML).
*   `d_ausd_geschaeftspartner_sql` READS `source_tables` and WRITES `target_tables`.
*   `r_ausd_vertrag_control_sp` WRITES `job_tracking_table`.

## 5. Transformation Logic
The transformation logic consists of both orchestration and data manipulation:

**Legacy Logic (k_ausd_geschaeftspartner.ksh):**
*   **Parameter Parsing:** Uses `getopts` to parse `j`, `f`, `s`, `l` parameters.
*   **Parameter Validation:** Checks if required parameters are set and validates date format (`DDMMYYYY`) of `p_Stichtag`.
*   **Environment Setup:** Sources `$HOME/.dw_init` and various utility scripts for error handling, date functions, and SQL*Plus execution.
*   **Date Derivation:** Calls `gestern.ksh` to get yesterday's and today's dates.
*   **SQL Execution:** Invokes `starteSQLSkript` to run `d_ausd_geschaeftspartner.sql` with collected parameters.
*   **Record Count:** Reads record count from a temporary file (`tmpFile`) after SQL execution.
*   **Job Management:** Intended to update/insert into a job tracking table.

**Migrated Logic (BigQuery Stored Procedure/Scripting):**
*   **BigQuery Stored Procedure (`r_ausd_vertrag_control_sp`):**
    *   Will declare input parameters for `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
    *   **Parameter Validation:** Implement `IF` conditions to check for `NULL` or empty string parameters and `RAISE` errors if validation fails.
    *   **Date Validation:** Use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and check for `NULL` to validate the date format.
    *   **Date Derivation:** Use BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for yesterday's and today's dates.
    *   **SQL Execution:** The migrated `d_ausd_geschaeftspartner.sql` (now BigQuery DML/DDL) will be executed directly within the stored procedure or as a separate BigQuery script called via `EXECUTE IMMEDIATE` if dynamic.
    *   **Record Count:** The `INSERT` or `UPDATE` statement for the data transformation will directly return the number of affected rows, which can be stored in a variable (`v_records`).
    *   **Job Management:** `INSERT` statements into the `project.dataset.job_tracking_table` will replace the `FOSJobErzeugeEintrag` logic.

## 6. External Dependencies
The following external dependencies need to be addressed during migration:

*   **Sourced KornShell Utility Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):**
    *   **Replacement:** The functionalities provided by these scripts (e.g., environment setup, error handling, date manipulation, parameter parsing, SQL execution wrapper) will be reimplemented using native BigQuery scripting capabilities, built-in functions, and robust error handling mechanisms (e.g., `RAISE` and Cloud Logging). Environment variables will be replaced by BigQuery script variables or procedure parameters. The `h_alis_sqlplus.ksh` wrapper behavior will be made redundant as SQL will be executed natively in BigQuery.
*   **`gestern.ksh`:**
    *   **Replacement:** BigQuery's `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will directly provide today's and yesterday's dates, making a separate script unnecessary.
*   **`d_ausd_geschaeftspartner.sql`:**
    *   **Replacement:** This Oracle/legacy SQL script must be fully analyzed and converted to BigQuery SQL, ensuring all syntax, functions, and data types are compatible. It will form the core data processing logic within the BigQuery Stored Procedure.
*   **Job Management System (implied by `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):**
    *   **Replacement:** A dedicated BigQuery table, e.g., `project.dataset.job_tracking_table`, will be created to store job metadata and status. `INSERT` and `UPDATE` DML operations within the BigQuery Stored Procedure will manage job entries.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_geschaeftspartner_$$.tmp`):**
    *   **Replacement:** BigQuery Stored Procedures can return values via `OUT` parameters or directly log information. The record count can be captured in a variable and directly inserted into the job tracking table without intermediate files.

## 7. Unresolved / Risks
*   **`d_ausd_geschaeftspartner.sql` Details:** The content of the main SQL script is currently unknown. Its specific DDL, DML, functions, and performance characteristics are critical for an accurate BigQuery conversion. This is the largest unknown in the data transformation.
*   **`starteSQLSkript` Implementation:** The exact logic and behavior of the `starteSQLSkript` function within `h_alis_sqlplus.ksh` needs to be fully understood. If it involves complex pre/post-processing or error handling specific to Oracle SQL*Plus, these details must be carefully re-engineered into the BigQuery environment.
*   **Commented-Out Logic:** The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` lines imply a legacy job management system. While the intent is clear, the full scope and exact interaction with this system might require further investigation if they represent critical business logic. It's assumed the provided pseudocode for job tracking covers the original intent.
*   **Environment Variable (`.dw_init`):** The variables set in `.dw_init` need to be cataloged and mapped to appropriate BigQuery procedure parameters, BigQuery script variables, or Google Cloud environment variables.
*   **Error Handling (`f_alis_msgerr.ksh`):** The specifics of the legacy error handling and messaging system need to be understood to ensure equivalent functionality in Cloud Logging.

## 8. Build Plan
1.  **Phase 1: Analysis and Design (Current Phase)**
    *   **Complete SQL Analysis:** Thoroughly analyze `d_ausd_geschaeftspartner.sql` to understand its full functionality, dependencies (source/target tables), and conversion requirements to BigQuery SQL.
    *   **Detail Utility Script Functionality:** Document the precise functionality of all sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) to ensure all aspects are covered in the BigQuery migration.
    *   **BigQuery Job Tracking Table DDL:** Design and create the DDL for `project.dataset.job_tracking_table` to replicate the legacy job management functionality.

2.  **Phase 2: BigQuery Implementation**
    *   **Migrate `d_ausd_geschaeftspartner.sql`:** Convert `d_ausd_geschaeftspartner.sql` into optimized BigQuery SQL (DML/DDL or a separate BigQuery Stored Procedure). (Language: BigQuery SQL)
    *   **Develop BigQuery Control Stored Procedure:** Create the main BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control_sp`) encapsulating parameter handling, validation, date derivation, and job tracking logic. This procedure will invoke or integrate the migrated `d_ausd_geschaeftspartner.sql` logic. (Language: BigQuery SQL)
    *   **Implement Utility Logic:** Embed or create BigQuery UDFs/procedures for any complex logic from the sourced utility scripts (e.g., specialized date calculations if not covered by native functions). (Language: BigQuery SQL)

3.  **Phase 3: Orchestration and Testing**
    *   **Cloud Composer DAG (Optional but Recommended):** Develop a Python-based Airflow DAG in Cloud Composer to schedule and invoke the `project.dataset.r_ausd_vertrag_control_sp` stored procedure, handling any external dependencies or orchestrating multiple steps if needed. (Language: Python)
    *   **Unit and Integration Testing:** Write comprehensive test cases for the BigQuery Stored Procedure and any associated DAGs to ensure functional parity with the legacy system. This includes parameter edge cases, date validations, and data transformation outputs. (Language: SQL, Python)
    *   **Data Validation:** Perform data validation against legacy outputs to ensure accuracy of the migrated data processing.

4.  **Phase 4: Deployment and Monitoring**
    *   **Deployment:** Deploy the BigQuery DDL, Stored Procedures, and Cloud Composer DAG (if used) to the target BigQuery and Cloud Composer environments.
    *   **Monitoring and Alerting:** Configure Cloud Monitoring and Alerting for the BigQuery jobs and Cloud Composer DAGs.