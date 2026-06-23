# Migration Design — vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh

## 1. Purpose & Scope
The KornShell script `k_ausd_bp_ta_tarifoption.ksh` serves as a control script for a data processing job related to 'PoolBasisprodukt'. Its primary purpose is to:
*   Parse and validate runtime parameters (Job ID, Entry Number, As-of Date, Restart Value).
*   Check the format of the provided as-of date.
*   Prepare the execution environment by sourcing several utility scripts.
*   Orchestrate the execution of a core SQL script, `d_ausd_bp_ta_tarifoption.sql`, passing relevant parameters.
*   Capture the number of records processed by the SQL script.
*   Log the job's execution status and record count (currently commented out).

The scope of this migration covers transforming the shell script's control flow, parameter handling, and SQL script execution into a BigQuery-native solution, likely a stored procedure, while preserving its business logic and data processing intent. The commented-out file processing steps suggest a potential future scope for additional data transformations if they become active.

## 2. Source Inventory
The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh`
    *   **Technology:** KornShell
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** medium
    *   **Migration Flags:** []
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** This KornShell script acts as a control script for a data processing job, handling parameter parsing, date validation, and orchestrating the execution of a SQL script to process data related to 'PoolBasisprodukt'.

## 3. Target Architecture
The migration target platform is BigQuery. The existing KornShell script's orchestration logic and parameter handling will be re-implemented as a BigQuery Stored Procedure. The core SQL logic currently residing in `d_ausd_bp_ta_tarifoption.sql` will be directly migrated into this BigQuery Stored Procedure or called as a separate BigQuery SQL script/view/UDF, depending on its complexity and reusability.

Key BigQuery components will include:
*   **BigQuery Stored Procedure:** To encapsulate the parameter validation, date checks, dynamic variable setting, and execution flow. This procedure will accept parameters equivalent to the shell script's command-line arguments.
*   **BigQuery Tables:**
    *   **Target Data Tables:** Where the output of the migrated `d_ausd_bp_ta_tarifoption.sql` logic will be stored (e.g., `project.dataset.target_table`).
    *   **Audit/Log Table:** To capture job execution details, parameter values, status, and record counts (e.g., `project.dataset.job_audit_table`). This replaces the temporary file and the commented-out FOSJobErzeugeEintrag call.
    *   **Staging Tables:** If the original `d_ausd_bp_ta_tarifoption.sql` involves intermediate data sets, these will be converted to BigQuery staging tables.
    *   **Configuration Table (Optional):** To manage job-specific configurations, such as job key mappings or restart values, replacing shell environment variables or sourced configuration files.
*   **BigQuery Date Functions:** For date validation and derivation of 'today' and 'yesterday' dates, replacing shell script utilities like `h_alis_date.ksh` and `gestern.ksh`.
*   **Orchestration (Cloud Composer/Workflows/Cloud Scheduler):** An external orchestrator might be necessary to schedule and trigger the BigQuery Stored Procedure, especially if the original job was part of a larger workflow.

## 4. Data Flow & Lineage
The original script's data flow involves:

1.  **Parameter Input:** Command-line arguments (`-j`, `-f`, `-s`, `-l`) define the scope and context of the data processing.
2.  **Environment Setup:** Sourcing of `dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and execution of `gestern.ksh` to set up environment variables and utility functions.
3.  **Validation:**
    *   Mandatory parameter checks (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`).
    *   Date format validation for `p_Stichtag` (DDMMYYYY).
4.  **SQL Script Execution:** The script dynamically constructs parameters and executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_tarifoption.sql` via `starteSQLSkript` (which likely wraps `sqlplus`). This SQL script is assumed to perform the core data extraction and transformation, possibly writing to a table named 'PoolBasisprodukt' or an output file.
5.  **Record Count Capture:** The number of records processed is written to a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_tarifoption.tmp`) and then read back into a shell variable `v_records`.
6.  **Job Logging (Commented):** An entry is intended to be created in a job table via `FOSJobErzeugeEintrag`.
7.  **Commented-out Post-processing:** Sections for `sed`, `sort`, and `join` operations on `cibasis_data24.dat`, `cibasis_data96.dat`, and `cibasis_fax.dat` suggest potential file-based transformations.

**Migrated Data Flow (BigQuery):**

1.  **Input Parameters:** The BigQuery Stored Procedure will accept `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as input arguments.
2.  **Parameter and Date Validation:** These checks will be implemented using BigQuery's `IF` statements and `RAISE` for errors, along with `SAFE.PARSE_DATE` for date format validation.
3.  **Date Derivation:** BigQuery functions like `CURRENT_DATE()` and `DATE_SUB()` will derive 'today' and 'yesterday' dates.
4.  **Core Data Logic:** The logic from `d_ausd_bp_ta_tarifoption.sql` will be directly translated into BigQuery SQL within the stored procedure, performing data extraction, transformation, and loading into designated BigQuery target tables (e.g., `project.dataset.target_table`).
5.  **Record Count:** The record count will be captured directly using `SELECT COUNT(*)` on the target table after the data load, storing it in a BigQuery variable.
6.  **Audit Logging:** An `INSERT` statement into a BigQuery audit table (`project.dataset.job_audit_table`) will log the job's execution details, status, and processed record count.
7.  **Optional File Transformations (if activated):** If the commented `sed`, `sort`, `join` logic needs to be implemented, it will be translated into BigQuery SQL transformations using `REPLACE`, `DISTINCT`, `ORDER BY`, `SPLIT`, `COALESCE`, and `JOIN` operations on BigQuery tables.

**Execution Order:**
The BigQuery Stored Procedure will execute sequentially: parameter declaration, validation, date derivation, core SQL logic execution, record count capture, and audit logging.

## 5. Transformation Logic

**Original (KornShell) -> Target (BigQuery SQL Stored Procedure)**

*   **Parameter Parsing (`getopts`):**
    *   **Original:** `getopts` loop to read `-j`, `-f`, `-s`, `-l` from command line.
    *   **Target:** Directly accept parameters `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` as arguments to the BigQuery Stored Procedure.
*   **Environment Sourcing (`. $HOME/.dw_init`, etc.):**
    *   **Original:** Sources multiple utility scripts for error handling, date functions, parameter parsing, and SQLPlus wrappers.
    *   **Target:** Most of these utilities will be replaced by native BigQuery functionality.
        *   Error handling: `IF/RAISE` statements.
        *   Date validation: `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`.
        *   Parameter handling: Handled by stored procedure arguments.
        *   SQL execution wrapper: Replaced by direct BigQuery SQL execution within the procedure.
*   **Parameter Validation (`pruefeParameterGesetzt`):**
    *   **Original:** Calls `pruefeParameterGesetzt` function.
    *   **Target:** Implemented using `IF p_Param IS NULL THEN RAISE USING MESSAGE = '...'; END IF;` constructs.
*   **Date Validation (`DWDate_Datum_Check`):**
    *   **Original:** Calls `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'`.
    *   **Target:** `SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag); IF v_stichtag_date IS NULL THEN RAISE ... END IF;`.
*   **Date Derivation (`gestern.ksh`):**
    *   **Original:** `set \`\`${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh\`\``.
    *   **Target:** `SET v_datum_heute = CURRENT_DATE(); SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);`.
*   **SQL Script Execution (`starteSQLSkript`):**
    *   **Original:** `starteSQLSkript $p_EintragsNr $Name_SQLskript ...`. This executes `d_ausd_bp_ta_tarifoption.sql` (assumed Oracle SQL).
    *   **Target:** The SQL code within `d_ausd_bp_ta_tarifoption.sql` needs to be analyzed and rewritten into BigQuery SQL. This BigQuery SQL will then be embedded directly into the stored procedure (e.g., `INSERT INTO ... SELECT ... FROM ...`). Any dynamic SQL generation in the original script would be handled by `EXECUTE IMMEDIATE` in BigQuery, if necessary.
*   **Temporary File (`tmpFile`) for Record Count:**
    *   **Original:** `tmpFile="$DW_DIR_UTL/bert_k_ausd_bp_ta_tarifoption.tmp"`; `eval "v_records=\`cat $tmpFile\`"`.
    *   **Target:** After the core SQL logic execution, use `SET v_records = (SELECT COUNT(*) FROM `project.dataset.target_table` WHERE ...);`.
*   **Job Logging (`FOSJobErzeugeEintrag` - commented):**
    *   **Original:** `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_job.ksh`; `FOSJobErzeugeEintrag $v_TabName 'A' 'I' $p_Stichtag ...`.
    *   **Target:** An `INSERT` statement into a dedicated BigQuery audit/log table (`project.dataset.job_audit_table`).
*   **Commented File Post-processing (`sed`, `sort`, `join`):**
    *   **Original:** Shell commands for string replacement, sorting, and joining text files.
    *   **Target:** If this logic becomes active, it should be translated into BigQuery SQL transformations:
        *   `sed s/\\ //g`: `REPLACE(column, ' ', '')`.
        *   `sort -u -k 1 -t ';'`: `SELECT DISTINCT ... FROM ... ORDER BY SPLIT(column, ';')[OFFSET(0)]`.
        *   `join`: BigQuery `JOIN` operations (e.g., `FULL OUTER JOIN`, `LEFT JOIN`).

## 6. External Dependencies
The current analysis identified no external systems listed in `lineage_assembled_jobs`. However, based on the script content, the following external interactions/dependencies exist:

*   **Legacy Database (implied by SQLPlus/SQL script):** The script executes an SQL script `d_ausd_bp_ta_tarifoption.sql` via a `starteSQLSkript` function, which strongly suggests interaction with an RDBMS, likely Oracle given the typical enterprise landscape.
    *   **Replacement in BigQuery:** The SQL logic within `d_ausd_bp_ta_tarifoption.sql` must be re-written to BigQuery SQL syntax. Data currently residing in the legacy database (e.g., source tables for `PoolBasisprodukt`) will need to be migrated or replicated to BigQuery. This could involve batch data migration, data replication services (e.g., Datastream), or federated queries if the source data remains external for a transitional period.
*   **Filesystem (for temporary files):** The script uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_tarifoption.tmp`) to store record counts.
    *   **Replacement in BigQuery:** This will be replaced by BigQuery variables (`DECLARE v_records INT64;`) and/or by storing audit information directly in BigQuery tables.
*   **Shell Environment/System Utilities:** The script relies on various sourced shell scripts and standard Unix utilities (`getopts`, `cat`, `print`, potentially `sed`, `sort`, `join` if uncommented).
    *   **Replacement in BigQuery:** These functionalities are replaced by BigQuery Stored Procedure features:
        *   Parameter parsing -> Stored Procedure arguments.
        *   Date utilities -> BigQuery `DATE` functions.
        *   File manipulation (`cat`, `sed`, `sort`, `join`) -> BigQuery SQL functions and table operations.
        *   Error handling -> BigQuery `IF/RAISE` constructs.

## 7. Unresolved / Risks

*   **Content of `d_ausd_bp_ta_tarifoption.sql`:** This is the most significant unknown. The actual business logic and specific SQL operations for 'PoolBasisprodukt' reside in this file. Its complexity (e.g., use of proprietary SQL functions, complex joins, stored procedures, temporary tables within SQLPlus) will dictate the effort required for BigQuery SQL translation. This file should be analyzed separately.
*   **Purpose of commented-out file processing (`sed`, `sort`, `join`):** While currently inactive, if these operations become necessary, their precise logic and source/target file formats need to be fully understood to implement them effectively in BigQuery SQL.
*   **FOS Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** The commented-out calls to these functions indicate integration with a job management system. While a BigQuery audit table can replace the logging, the "deactivate" functionality might imply external control or status management that needs to be replicated or integrated with a new orchestrator (e.g., Cloud Composer).
*   **Error Reporting (`DWMSG_MeldeFehler`):** The exact functionality of this error reporting utility needs to be understood. BigQuery `RAISE` statements provide basic error messages, but if it integrates with a broader alerting system, that integration will need to be re-established.
*   **Security Context and Credentials:** The original script relies on `sqlplus` and `DW_DIR_UTL`. The migration needs to define how BigQuery connections are managed, including service accounts, IAM roles, and secure storage of any necessary credentials.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Analyze `d_ausd_bp_ta_tarifoption.sql`:**
    *   **Language:** Oracle SQL (assumed), BigQuery SQL
    *   **Output:** Detailed BigQuery SQL migration plan for the core logic.
2.  **Design BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL (DDL, DML, Control Flow)
    *   **Output:** `k_ausd_bp_ta_tarifoption.sql` (BQ Stored Procedure definition). This will contain:
        *   Parameter declarations.
        *   Parameter validation logic.
        *   Date derivation.
        *   The migrated BigQuery SQL logic from `d_ausd_bp_ta_tarifoption.sql`.
        *   Record count capture.
        *   Audit table insertion.
3.  **Define BigQuery Audit Table:**
    *   **Language:** BigQuery SQL (DDL)
    *   **Output:** `job_audit_table.sql` (Table creation script).
4.  **Define Target Data Tables:**
    *   **Language:** BigQuery SQL (DDL)
    *   **Output:** `target_table_ddl.sql` (DDL for tables populated by the core logic).
5.  **Implement Optional File Transformation Logic (if activated):**
    *   **Language:** BigQuery SQL (DML, potentially staging table DDL)
    *   **Output:** Separate BigQuery SQL scripts or functions for each transformation.
6.  **Develop Orchestration (e.g., Cloud Composer DAG):**
    *   **Language:** Python
    *   **Output:** `k_ausd_bp_ta_tarifoption_dag.py` (Airflow DAG to schedule and execute the BigQuery Stored Procedure).
7.  **IAM and Access Control Configuration:**
    *   **Language:** gcloud CLI, Terraform (if IaC)
    *   **Output:** IAM policies and service account definitions.