# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_apn_vertrag.ksh`, serves as a control script for `r_ausd_bp_ta_apn_vertrag.ksh` (likely an associated report or process). Its primary purpose is to orchestrate the execution of a data preparation task. This involves initializing the environment, parsing and validating input parameters (Job ID, Entry Number, Reference Date, Restart Value), performing date format validation, and crucially, executing an external SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) that carries out the core data extraction and transformation logic. Finally, it records the number of processed records. The original `purpose_note` from lineage analysis indicated "Job assembled from 1 component(s); stage dist: medium=1", aligning with its orchestrational role.

## 2. Source Inventory
The job consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic
    *   **Purpose:** Orchestration, parameter handling, error management, and SQL script execution.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve:

*   **BigQuery Stored Procedure:** The core logic of the KornShell script (parameter parsing, validation, date calculations, and orchestration of the SQL script) will be translated into a BigQuery Stored Procedure. This procedure will accept input parameters, perform validations, and call other BigQuery procedures or execute DML/DDL statements.
*   **BigQuery SQL Procedures/Statements:** The external SQL script `d_ausd_bp_ta_apn_vertrag.sql` will be fully rewritten as a native BigQuery SQL stored procedure or a series of SQL statements executed within the main orchestration procedure.
*   **BigQuery Tables:** Source and target data will reside in BigQuery tables. Audit/error logging will be directed to dedicated BigQuery logging tables.
*   **Cloud Composer / Cloud Workflows (Optional):** If there are complex scheduling or external system integrations required beyond what a single BigQuery stored procedure can manage, Cloud Composer or Cloud Workflows can be used for overall job orchestration. This job appears to be primarily shell-driven for a single SQL task, so a single BigQuery Stored Procedure might be sufficient.

## 4. Data Flow & Lineage
The original script's lineage indicated no direct edges from database analysis, so we derive the flow from the script's content.

**Legacy Flow:**
1.  **Environment Setup:** The `k_ausd_bp_ta_apn_vertrag.ksh` script sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Input:** Parameters `j`, `f`, `s`, `l` (Job ID, Entry Number, Reference Date, Restart Value) are passed via command-line arguments and parsed using `getopts`.
3.  **Validation:** Parameters are validated (`pruefeParameterGesetzt`), and the reference date (`p_Stichtag`) is checked for `DDMMYYYY` format (`DWDate_Datum_Check`). Error messages are handled by `DWMSG_MeldeFehler`.
4.  **Date Calculation:** The `gestern.ksh` script is invoked to derive yesterday's and today's dates.
5.  **SQL Script Execution:** The script calls the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) to execute `d_ausd_bp_ta_apn_vertrag.sql`, passing various parameters including the entry number, job ID, reference date, temporary file path, and derived dates.
6.  **Record Count:** After SQL execution, the script reads the record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`).
7.  **Job Tracking (Commented Out):** There are commented-out sections for `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` which suggest interaction with an external job management system.

**Target BigQuery Flow:**
1.  **Main Orchestration Procedure (`r_ausd_bp_ta_apn_vertrag_proc`):**
    *   Accepts `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (as STRING for initial parsing), `p_wiederanlaufWert` as input parameters.
    *   **Parameter Validation:** Implements validation checks for required parameters and logs errors to a `project.dataset.error_log` table.
    *   **Date Validation:** Converts `p_Stichtag` to `DATE` using `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`.
    *   **Date Calculation:** Uses `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` for `v_datum_heute` and `v_datum_gestern`.
    *   **Core SQL Logic Execution:** Calls a separate BigQuery Stored Procedure, `d_ausd_bp_ta_apn_vertrag_proc`, passing all necessary parameters.
    *   **Record Count:** Retrieves the count of processed records from the target table of `d_ausd_bp_ta_apn_vertrag_proc` or from an audit table updated by it.
    *   **Job Audit:** Inserts an entry into a `project.dataset.job_audit` table with job name, table name (`PoolBasisprodukt`), reference date, and record count.

2.  **Core Data Transformation Procedure (`d_ausd_bp_ta_apn_vertrag_proc`):**
    *   Accepts parameters needed for the SQL logic.
    *   Executes the translated DML/DDL operations, potentially involving temporary tables or direct inserts/updates into target tables.
    *   Updates an execution log or returns the processed record count.

## 5. Transformation Logic

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`**

*   **Parameter Parsing:** The `getopts` logic will be replaced by BigQuery Stored Procedure input parameters.
*   **Environment Sourcing:** The sourcing of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be replaced by direct BigQuery SQL functions, stored procedures, or Python functions within an orchestration layer (e.g., error logging to a BigQuery table, date functions for date calculations and validations).
*   **Date Calculation (`gestern.ksh`):** Replaced with BigQuery date functions like `CURRENT_DATE()` and `DATE_SUB()`.
*   **Error Handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`):** Replaced by `IF` statements, `SIGNAL SQLSTATE`, and `INSERT` statements into a dedicated BigQuery error log table.
*   **Temporary File (`tmpFile` for record count):** Replaced by querying the count directly from the target table or from an audit table after the SQL execution.
*   **SQL Execution Wrapper (`starteSQLSkript`):** The functionality of this wrapper will be integrated directly into the BigQuery Stored Procedure, calling the translated `d_ausd_bp_ta_apn_vertrag_proc`.
*   **Commented-out `sed`, `sort`, `join`:** These operations are not active in the source script and will be ignored during migration, assuming they are not required for the current business logic. If they become active, they would be translated to BigQuery SQL DML or Dataflow/Spark transformations.
*   **Job Tracking (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These commented-out functions, if reactivated, would require migration to a BigQuery audit log table or integration with a BigQuery-native job scheduling/monitoring system.

**File: `d_ausd_bp_ta_apn_vertrag.sql` (Inferred)**

*   This SQL script will be translated into a native BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_apn_vertrag_proc`). The current design assumes this script performs core ETL operations.
*   All DDL and DML operations within this script will be converted to BigQuery-compatible syntax.
*   Parameters originally passed from the shell script will become input parameters to this BigQuery Stored Procedure.

## 6. External Dependencies
The original script has several implicit external dependencies through sourced KornShell scripts and a job management system:

*   **Shell Utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`):**
    *   **Replacement:** The functionalities provided by these scripts will be re-implemented using BigQuery SQL's native capabilities (e.g., date functions, string manipulation, error handling logic in stored procedures). Environment variable handling (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by explicit BigQuery dataset/table paths or configuration parameters.
*   **External SQL Script (`d_ausd_bp_ta_apn_vertrag.sql`):**
    *   **Replacement:** This will be converted into a BigQuery Stored Procedure that is called directly by the main orchestration procedure.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`):**
    *   **Replacement:** The record count will be derived directly within BigQuery SQL (e.g., `SELECT COUNT(*) FROM target_table`) and stored in an audit log table rather than a file.
*   **Job Management System (implied by `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):**
    *   **Replacement:** If active, this would be replaced by inserts into a BigQuery audit/job status table, or by integrating with GCP's native scheduling and monitoring tools like Cloud Composer, Cloud Workflows, or Cloud Monitoring. Since these are commented out, a simple audit log in BigQuery is a suitable initial replacement.

No explicit external systems like Oracle, SFTP, or S3 were identified in the `lineage_assembled_jobs` output or the script content.

## 7. Unresolved / Risks

*   **Unresolved References:** The `lineage_unresolved` from the initial query was empty.
*   **Dynamic SQL:** The script executes an external SQL file. The exact content of `d_ausd_bp_ta_apn_vertrag.sql` was not available for direct analysis. The complexity of its translation will depend on its contents (e.g., specific database functions, proprietary SQL constructs). This is the biggest unknown.
*   **Environment Variables:** The script relies heavily on environment variables like `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`. The values and purpose of these variables need to be fully understood to correctly map file paths and configurations in the BigQuery environment.
*   **`starteSQLSkript` Functionality:** The precise implementation of `starteSQLSkript` (within `h_alis_sqlplus.ksh`) is unknown. It likely handles database connections, error handling during SQL execution, and potentially dynamic SQL generation. This functionality will need to be carefully translated to BigQuery's stored procedure execution model.
*   **Character Encoding:** The comments in the script contain German special characters (e.g., `Lbbers`, `temporren`). Ensuring correct character encoding during migration is important.
*   **Audit/Logging Framework:** The original `DWMSG_MeldeFehler` and job tracking mechanisms need a robust BigQuery-native replacement for consistent error reporting and operational visibility.

## 8. Build Plan
The migration will result in BigQuery SQL objects.

1.  **Define BigQuery Audit and Error Log Tables:**
    *   `project.dataset.error_log` (for validation and runtime errors)
    *   `project.dataset.job_audit` (for tracking job execution status and record counts)
    *   **Language:** BigQuery DDL

2.  **Translate `d_ausd_bp_ta_apn_vertrag.sql` to BigQuery Stored Procedure:**
    *   Create `project.dataset.d_ausd_bp_ta_apn_vertrag_proc`
    *   This procedure will contain the core data manipulation logic (CREATE/MERGE/INSERT/UPDATE statements).
    *   **Language:** BigQuery SQL (Stored Procedure)

3.  **Translate `k_ausd_bp_ta_apn_vertrag.ksh` to BigQuery Stored Procedure:**
    *   Create `project.dataset.r_ausd_bp_ta_apn_vertrag_proc`
    *   This procedure will encapsulate parameter validation, date calculations, and call `d_ausd_bp_ta_apn_vertrag_proc`.
    *   **Language:** BigQuery SQL (Stored Procedure)

4.  **Develop Orchestration Layer (if needed):**
    *   If external scheduling or more complex inter-job dependencies exist, implement a Cloud Composer DAG or Cloud Workflows definition to invoke `r_ausd_bp_ta_apn_vertrag_proc`.
    *   **Language:** Python (for Cloud Composer) or YAML (for Cloud Workflows)