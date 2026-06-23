# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This KornShell script (`k_ausd_bp_ta_bpr_instance.ksh`) acts as an orchestrator for data preparation. Its primary purpose is to parse and validate input parameters (Job ID, Entry Number, Reference Date, Restart Value), set up the execution environment by sourcing helper scripts, and then execute a core SQL script (`d_ausd_bp_ta_bpr_instance.sql`) with the gathered parameters. It also handles basic error reporting and records counting post-execution. The business purpose is to prepare and load data related to 'PoolBasisprodukt' instances, likely for reporting or further processing, based on a specific reference date.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`
    *   **Technology:** KornShell (orchestration)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Orchestrates the execution of an SQL script for data preparation, handling parameter parsing, validation, and environment setup.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve:
*   **BigQuery Stored Procedure:** The core logic of the KornShell script, including parameter parsing, validation, date derivation, and the invocation of the SQL logic, will be encapsulated within a BigQuery stored procedure. This procedure will handle the flow control and error handling.
*   **BigQuery SQL Script/Procedure:** The SQL logic originally in `d_ausd_bp_ta_bpr_instance.sql` (which is invoked by the ksh script) will be migrated into a BigQuery SQL script or a separate stored procedure.
*   **BigQuery Tables:**
    *   Source tables for the SQL script (implicitly defined in `d_ausd_bp_ta_bpr_instance.sql`).
    *   Target tables for the processed data (implicitly defined in `d_ausd_bp_ta_bpr_instance.sql`).
    *   `project.dataset.error_log`: A new audit table to record error messages and details, replacing the shell script's `DWMSG_MeldeFehler`.
    *   `project.dataset.job_audit`: A new audit table to store execution metrics, such as record counts, replacing the file-based temporary record count.
    *   Optional `project.dataset.job_control`: If commented-out job management logic is to be reactivated, a job control table will be created.
*   **BigQuery Functions/UDFs:** Small, reusable utility functions (e.g., date formatting checks) could be implemented as BigQuery UDFs.
*   **Orchestration:** BigQuery scripting provides the necessary flow control. External orchestration (e.g., Airflow) might be used for scheduling and managing dependencies, mapping environment variables to BigQuery project/dataset settings.

## 4. Data Flow & Lineage
The original script's data flow:
1.  **Environment Setup**: Sources `.dw_init` and helper ksh scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Parsing**: `getopts` reads `j`, `f`, `s`, `l` parameters.
3.  **Validation**: Checks if required parameters (`Jobkennung`, `Stichtag`, `EintragsNr`) are set and `Stichtag` has the correct `DDMMYYYY` format. Exits on error.
4.  **Date Derivation**: Invokes `gestern.ksh` to get `p_datum_heute` and `p_datum_gestern`.
5.  **SQL Execution**: Calls `starteSQLSkript` to execute `d_ausd_bp_ta_bpr_instance.sql` with all collected parameters. This SQL script is where the actual data reads, transformations, and writes occur.
6.  **Record Count**: Reads the number of processed records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_instance.tmp`).
7.  **Job Table Entry (Commented)**: Intended to create an entry in a job table.

**Target BigQuery Data Flow:**
1.  **Stored Procedure Invocation**: The BigQuery stored procedure `project.dataset.r_ausd_bp_ta_bpr_instance` will be called with the input parameters.
2.  **Parameter Validation**: Within the stored procedure, the `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` parameters will be validated for presence and `p_Stichtag` for format. Errors will be logged to `project.dataset.error_log` and raise an exception.
3.  **Date Derivation**: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will derive `p_datum_heute` and `p_datum_gestern`.
4.  **SQL Logic Execution**: The core logic from `d_ausd_bp_ta_bpr_instance.sql` will be embedded or called as a separate BigQuery SQL script/procedure. This will perform the necessary `SELECT` (reads from source tables) and `INSERT`/`UPDATE` (writes to target tables).
5.  **Record Count**: After the SQL logic, `COUNT(*)` on the target table (or a temporary table) will derive the record count.
6.  **Audit Logging**: The record count and other execution details will be inserted into `project.dataset.job_audit`.
7.  **Job Control (Optional)**: If reactivated, an `INSERT` into `project.dataset.job_control` will manage job status.

## 5. Transformation Logic
The `k_ausd_bp_ta_bpr_instance.ksh` script itself does not contain direct data transformation logic; it orchestrates the execution of an external SQL script (`d_ausd_bp_ta_bpr_instance.sql`). The transformation logic will therefore reside primarily in the migrated BigQuery SQL script/procedure corresponding to `d_ausd_bp_ta_bpr_instance.sql`.

The script does implement:
*   **Parameter Handling:**
    *   `j:f:s:l:` parameters parsed by `getopts` will become direct arguments to the BigQuery stored procedure.
    *   `p_wiederanlaufWert` default to `0` will be handled by a conditional assignment within the BigQuery procedure.
*   **Date Derivation:**
    *   `gestern.ksh` functionality (`p_datum_heute`, `p_datum_gestern`) will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` in BigQuery SQL.
*   **Temporary Record Count:**
    *   Reading from `tmpFile` will be replaced by direct `COUNT(*)` operations on BigQuery tables or procedure `OUT` parameters.

## 6. External Dependencies
The script explicitly references several external components:

*   **Helper Scripts (`.ksh` files):**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date validation (`DWDate_Datum_Check`).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter validation (`pruefeParameterGesetzt`).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus wrapper (`starteSQLSkript`).
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Derives yesterday's and today's dates.
    **Replacement:**
        *   Error messaging (`f_alis_msgerr.ksh`): Replaced by `INSERT` statements into an `error_log` BigQuery table and `RAISE` statements for critical errors within the BigQuery stored procedure.
        *   Date and Parameter Validation (`h_alis_date.ksh`, `h_alis_parameter.ksh`): Replaced by BigQuery scripting `IF` conditions, `REGEXP_CONTAINS`, and `PARSE_DATE` functions. Custom validation logic can be implemented directly within the stored procedure.
        *   SQL*Plus wrapper (`h_alis_sqlplus.ksh`): Not directly applicable as BigQuery SQL is the target. The SQL script it executes will become native BigQuery SQL.
        *   Date derivation (`gestern.ksh`): Replaced by BigQuery `CURRENT_DATE()` and `DATE_SUB()`.

*   **SQL Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql`: Contains the core data manipulation logic.
    **Replacement:** This SQL script needs to be migrated independently into a BigQuery SQL script or a dedicated BigQuery stored procedure. The current orchestrator will call this migrated BigQuery artifact.

*   **Temporary File:**
    *   `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_instance.tmp`: Used to store a record count.
    **Replacement:** Replaced by directly querying BigQuery tables for record counts or using stored procedure `OUT` parameters to return the count. An audit table (`project.dataset.job_audit`) will permanently store these metrics.

*   **Environment File:**
    *   `$HOME/.dw_init`: Sources environment variables.
    **Replacement:** Environment variables should be managed through BigQuery connection configurations, external orchestration (e.g., Airflow variables), or dedicated BigQuery configuration tables for dynamic values. Project and dataset IDs will be explicitly managed.

## 7. Unresolved / Risks

*   **External SQL Script (`d_ausd_bp_ta_bpr_instance.sql`):** The details of this script are unknown and represent the most significant part of the data transformation. Its migration complexity and potential external database dependencies are critical unknowns. It is assumed to be migratable to BigQuery SQL.
*   **Commented-out Job Management (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):** These lines suggest an existing job management framework. If this functionality is required, it needs to be explicitly designed and implemented in BigQuery using dedicated job control tables and potentially BigQuery's built-in scheduling capabilities or external orchestrators. The current design document includes placeholders for this.
*   **Commented-out Post-processing (`sed`, `sort`, `join`):** The commented blocks for `sed`, `sort`, and `join` on files like `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`, and `cibasisprodukt.csv` indicate a potential file-based post-processing step that is currently inactive. If these become active or if similar logic is required, these transformations must be re-implemented using BigQuery SQL (e.g., `STRING_REPLACE`, `ARRAY_AGG`, `ORDER BY`, `JOIN` operations) or potentially Cloud Dataflow if data volumes are very high and require distributed processing.
*   **`BERT_DIR_ROOT`, `DW_DIR_UTL`:** These environment variables imply a structured directory layout for scripts and utilities. In BigQuery, these paths translate to specific project, dataset, and table names or logical organization within Cloud Storage for intermediate files if any.

## 8. Build Plan

The migration will involve building the following components:

1.  **`project.dataset.r_ausd_bp_ta_bpr_instance` (BigQuery Stored Procedure):**
    *   **Language:** BigQuery SQL
    *   **Content:**
        *   Definition of input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Parameter validation logic (presence and date format), including `RAISE` and `INSERT` into `error_log`.
        *   Date derivation for `p_datum_heute` and `p_datum_gestern`.
        *   Placeholder for the migrated SQL logic from `d_ausd_bp_ta_bpr_instance.sql` (either embedded or as a call to another stored procedure).
        *   Logic to calculate `v_records` by counting rows in the target table.
        *   `INSERT` statement to `project.dataset.job_audit` for logging execution metrics.
        *   (Optional) `INSERT` statement to `project.dataset.job_control` if job management needs to be reactivated.

2.  **`d_ausd_bp_ta_bpr_instance` (BigQuery SQL Script/Procedure):**
    *   **Language:** BigQuery SQL
    *   **Content:** The full translation of the original Oracle/legacy SQL script, `d_ausd_bp_ta_bpr_instance.sql`, into BigQuery-compatible SQL, including source table reads, transformation logic, and target table writes. This is a separate, critical migration task.

3.  **`project.dataset.error_log` (BigQuery Table DDL):**
    *   **Language:** BigQuery DDL
    *   **Content:** Schema definition for `job_audit` table, e.g., `(process_name STRING, error_nr INT64, error_arg STRING, created_at TIMESTAMP)`.

4.  **`project.dataset.job_audit` (BigQuery Table DDL):**
    *   **Language:** BigQuery DDL
    *   **Content:** Schema definition for `job_audit` table, e.g., `(job_kennung STRING, eintrags_nr STRING, stichtag STRING, records INT64, created_at TIMESTAMP, tab_name STRING)`.

5.  **`project.dataset.job_control` (BigQuery Table DDL - Optional):**
    *   **Language:** BigQuery DDL
    *   **Content:** Schema definition for job control table, if commented-out job management is reactivated, e.g., `(tab_name STRING, status STRING, mode STRING, from_date DATE, to_date DATE, job_type STRING, restart_flag STRING, records INT64, description STRING)`.

6.  **Orchestration Configuration (e.g., Airflow DAG):**
    *   **Language:** Python (Airflow)
    *   **Content:** If an external orchestrator is used, a DAG will be created to schedule the BigQuery stored procedure, passing parameters and handling environment configuration.