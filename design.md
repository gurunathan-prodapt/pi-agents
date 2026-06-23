# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope
This job, `r_ausd_bp_ta_rn_vertrag.ksh`, is a KornShell script acting as an orchestration wrapper. Its primary purpose is the initial provisioning run for selected base products (e.g., FAX, Data24) for the BERT system. Specifically, it creates a snapshot (Stichtags-Abzug) of the contract cache (`Vertrags-Cache`) within the Data Warehouse (DWH) and makes it available for credit scoring (`Forderungsscoring`).

The script handles runtime parameter parsing, determines a processing date (`Stichtag`), initializes logging and error handling, and then invokes a core "kernel" script, `k_ausd_bp_ta_rn_vertrag.ksh`, to perform the actual data processing. It passes job identifiers, the processing date, an entry number, and a restart value to the kernel script. Upon successful completion of the kernel script, it marks the job as successful.

The scope of this migration design document covers the transformation of this KornShell wrapper script and its inferred kernel logic to BigQuery SQL stored procedures and potentially external orchestration.

## 2. Source Inventory
The primary component of this job is a single KornShell script.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh`
    *   **Technology**: KornShell
    *   **Complexity**: Medium (Inferred from `lineage_assembled_jobs` `stage dist: medium=1` and its orchestration role)
    *   **Automation Bucket**: `semi_auto`

**Inferred Dependent Files (from script content):**
*   **Sourced Environment File**: `$HOME/.dw_init`
*   **Sourced Helper Scripts**:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error/Logging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter Handling)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date Handling)
*   **Invoked Kernel Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh` (Contains core data processing logic)

## 3. Target Architecture
The target architecture will leverage BigQuery's capabilities for data processing and storage, with orchestration managed via BigQuery stored procedures and potentially external tools like Cloud Composer or Workflows.

*   **Main Wrapper Logic**: A BigQuery SQL Stored Procedure, `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`, will encapsulate the parameter handling, date determination, and job orchestration aspects of the original `r_ausd_bp_ta_rn_vertrag.ksh` script.
*   **Kernel Logic**: A separate BigQuery SQL Stored Procedure, `project.dataset.k_ausd_bp_ta_rn_vertrag`, will implement the core data manipulation logic previously contained in `k_ausd_bp_ta_rn_vertrag.ksh`.
*   **Logging and Auditing**: A BigQuery table, `project.dataset.job_audit`, will replace the file-based logging, storing job status, errors, and metadata.
*   **Source Data**: The `DWH$TA_C_VERTRAG` (Contract Cache) and related tables will be migrated to BigQuery tables, e.g., `project.dataset.ta_vertrag_cache`.
*   **Target Data**: The output table for credit scoring will be a BigQuery table, e.g., `project.dataset.fos_vertrag`.
*   **Orchestration**: If external job scheduling or complex dependencies are present in the wider ecosystem, Cloud Composer (Airflow DAG) or Workflows could be used to trigger the main BigQuery stored procedure.

## 4. Data Flow & Lineage
The original script's flow involves receiving parameters, preparing for execution, and invoking a kernel script.

**Source System Flow:**
1.  **Input**:
    *   Command-line parameters: `-s DDMMYYYY` (Stichtag/processing date), `-l <restart_value>` (Wiederanlaufwert).
    *   Environment variables via `. $HOME/.dw_init`.
2.  **Processing by `r_ausd_bp_ta_rn_vertrag.ksh`**:
    *   Sources helper scripts for error handling, parameter parsing, and date functions.
    *   Parses command-line arguments using `getopts`.
    *   Initializes `p_wiederanlaufWert` to `0` if not provided.
    *   Determines `v_sysdate` (system date) and defaults `p_stichtag` to `v_sysdate` if not supplied.
    *   Validates parameters. If errors, logs and exits.
    *   Initializes job entry (`DW_EintragsNr`) and log file (`LogDatei`) using `DWMSG_*` functions.
    *   Sets shell traps for error handling.
    *   Invokes `k_ausd_bp_ta_rn_vertrag.ksh` with parameters: `-j <JobKennung> -s <p_stichtag> -f <DW_EintragsNr> -l <p_wiederanlaufWert>`.
    *   Appends script output to `LogDatei`.
    *   On successful completion, updates job status to OK via `DWMSG_SetzeStatusOK`.
3.  **Processing by `k_ausd_bp_ta_rn_vertrag.ksh` (Inferred)**:
    *   Reads contract data from a source like `DWH$TA_C_VERTRAG` (e.g., `project.dataset.ta_vertrag_cache`).
    *   Performs a conditional delete operation on the target `FOS-Tabelle` (e.g., `project.dataset.fos_vertrag`) based on `DWH_VERTRAG_ID >= p_wiederanlaufWert`.
    *   Inserts records into the target `FOS-Tabelle` where `DWH_VERTRAG_ID > p_wiederanlaufWert` and date conditions (`Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`) are met.
4.  **Output**:
    *   Updated `FOS-Tabelle` (target for credit scoring).
    *   Log file containing execution details and status.

**Target BigQuery Flow:**
1.  **Orchestration Layer (Optional Cloud Composer/Workflows):** Triggers `project.dataset.ausd_bp_ta_rn_vertrag_wrapper` stored procedure, passing `p_stichtag` and `p_wiederanlaufWert`.
2.  **`project.dataset.ausd_bp_ta_rn_vertrag_wrapper` (BQ Stored Procedure)**:
    *   Receives `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input parameters.
    *   Determines system date using `CURRENT_DATE()` and `FORMAT_DATE`.
    *   Defaults `v_wiederanlaufWert` to `0` and `v_stichtag` to system date if inputs are null.
    *   Performs parameter validation and raises an `EXCEPTION` if invalid, logging to `job_audit`.
    *   Inserts job start entry into `project.dataset.job_audit`.
    *   Calls `project.dataset.k_ausd_bp_ta_rn_vertrag` stored procedure with relevant parameters.
    *   Inserts job success entry into `project.dataset.job_audit`.
3.  **`project.dataset.k_ausd_bp_ta_rn_vertrag` (BQ Stored Procedure)**:
    *   Receives job identifier, `p_stichtag`, `p_eintragsnr`, and `p_wiederanlaufWert`.
    *   Converts `p_stichtag` to `DATE`.
    *   **Deletes** from `project.dataset.fos_vertrag` records where `dwh_vertrag_id >= p_wiederanlaufWert`.
    *   **Inserts** into `project.dataset.fos_vertrag` by selecting from `project.dataset.ta_vertrag_cache` where `dwh_vertrag_id > p_wiederanlaufWert` and the date conditions (`src.gueltig_von <= v_stichtag_date`, `v_stichtag_date < src.gueltig_bis`, `src.ladedatum < v_stichtag_date`) are met.

## 5. Transformation Logic
**`r_ausd_bp_ta_rn_vertrag.ksh` (Wrapper Script) to BigQuery Stored Procedure:**

*   **Parameter Handling**:
    *   The `getopts` logic will be replaced by direct input parameters of the BigQuery stored procedure `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`.
    *   Defaulting `p_wiederanlaufWert` to `0` and `p_stichtag` to the system date if not provided will be handled using `IFNULL` or `IF` statements within the stored procedure.
*   **Date Determination**: `DWDate_Gib_Zeitraum` and `v_sysdate` logic will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Error Handling and Logging**:
    *   The `DWMSG_*` functions and shell `trap` commands will be replaced by `INSERT` statements into a `project.dataset.job_audit` table.
    *   Parameter validation will use `IF ... THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ...` to simulate script termination with an error, with error details logged to `job_audit`.
*   **Script Invocation**: The call to `k_ausd_bp_ta_rn_vertrag.ksh` will be replaced by a `CALL` statement to the BigQuery stored procedure `project.dataset.k_ausd_bp_ta_rn_vertrag`.

**`k_ausd_bp_ta_rn_vertrag.ksh` (Kernel Script) to BigQuery Stored Procedure:**

*   **Restart Logic**: The logic involving `p_wiederanlaufWert` will be translated directly into BigQuery SQL:
    *   `DELETE FROM project.dataset.fos_vertrag WHERE dwh_vertrag_id >= p_wiederanlaufWert;`
    *   `INSERT INTO project.dataset.fos_vertrag SELECT ... FROM project.dataset.ta_vertrag_cache WHERE dwh_vertrag_id > p_wiederanlaufWert AND <date_conditions>;`
*   **Date Conditions**: The date filtering conditions (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) will be translated using BigQuery's date functions and comparisons, e.g., `PARSE_DATE('%d%m%Y', p_stichtag)` to convert the input `Stichtag` string to a `DATE` type.
*   **Data Sources**: Reads will be from `project.dataset.ta_vertrag_cache`.
*   **Data Targets**: Writes/deletes will be to `project.dataset.fos_vertrag`.

## 6. External Dependencies
*   **Oracle Data Warehouse**: The original script implicitly interacts with an Oracle DWH (e.g., `DWH$TA_C_VERTRAG`). In BigQuery, this data will be present in equivalent BigQuery tables such as `project.dataset.ta_vertrag_cache` and `project.dataset.fos_vertrag`. This requires a prior data migration of these Oracle tables to BigQuery.
*   **Shell Environment Variables**: Variables like `$HOME` and `BERT_DIR_ROOT` used for path construction will be replaced by explicit string literals or configuration parameters within the BigQuery stored procedures or the orchestration layer.
*   **Helper Scripts (`.ksh`)**: The functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` will be absorbed into the BigQuery stored procedures' logic, using native BigQuery SQL functions and procedural constructs.
*   **File-based Logging**: The log files (`LogDatei`) will be replaced by an audit/logging table in BigQuery (`project.dataset.job_audit`), centralizing logging within the BigQuery environment.

## 7. Unresolved / Risks
*   **Missing `file_complexity` details**: While `lineage_assembled_jobs` indicated "medium" complexity and `automation_rate` is `semi_auto`, the specific `tier` and `migration_flags` from `file_complexity` were not available. This suggests a need for manual review of the inferred complexities and potential challenges not captured by the automated analysis.
*   **Full `k_ausd_bp_ta_rn_vertrag.ksh` analysis**: The exact content of the kernel script `k_ausd_bp_ta_rn_vertrag.ksh` was not provided, but its assumed data operations (delete/insert with specific conditions) were inferred by the MCP. A detailed analysis of this kernel script is crucial to ensure accurate migration of its specific business logic, especially if it contains complex SQL, joins, or additional transformations beyond the restart logic.
*   **Error Message Translation**: The specific error codes (`ErrNr=192`, `ErrNr=193`) and messages from the original shell script need to be carefully replicated or mapped to an appropriate BigQuery error handling and logging scheme for consistent operational behavior.
*   **Idempotency and Performance of Restart Logic**: The "delete then insert" pattern for restart functionality in BigQuery needs careful consideration to ensure idempotency and optimal performance, especially for very large tables. Using `MERGE` statements might be a more efficient and robust alternative.

## 8. Build Plan
1.  **Define BigQuery Schemas (Language: BQ DDL)**
    *   Create `project.dataset.job_audit` table:
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
          job_entry_nr INT64,
          job_kennung STRING,
          status STRING,
          error_nr INT64,
          error_arg STRING,
          log_ts TIMESTAMP,
          message STRING,
          stichtag STRING,
          sysdate_value STRING,
          restart_value INT64,
          log_file_name STRING
        );
        ```
    *   Ensure `project.dataset.ta_vertrag_cache` and `project.dataset.fos_vertrag` schemas are defined, replicating the structure of the source DWH tables.

2.  **Develop BigQuery Stored Procedure for `k_ausd_bp_ta_rn_vertrag` (Language: BQSQL)**
    *   Translate the core data manipulation logic for delete and insert operations based on the restart value and date conditions.
    *   Example pseudocode from MCP:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_vertrag`(
          IN p_job_kennung STRING,
          IN p_stichtag STRING,
          IN p_eintragsnr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          DECLARE v_stichtag_date DATE;
          DECLARE v_restart_value INT64 DEFAULT IFNULL(p_wiederanlaufWert, 0);

          SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

          DELETE FROM `project.dataset.fos_vertrag`
          WHERE dwh_vertrag_id >= v_restart_value;

          INSERT INTO `project.dataset.fos_vertrag`
          SELECT src.*
          FROM `project.dataset.ta_vertrag_cache` src
          WHERE src.dwh_vertrag_id > v_restart_value
            AND src.gueltig_von <= v_stichtag_date
            AND v_stichtag_date < src.gueltig_bis
            AND src.ladedatum < v_stichtag_date;
        END;
        ```

3.  **Develop BigQuery Stored Procedure for `r_ausd_bp_ta_rn_vertrag_wrapper` (Language: BQSQL)**
    *   Implement parameter validation, defaulting, and logging mechanisms.
    *   Integrate the call to `project.dataset.k_ausd_bp_ta_rn_vertrag`.
    *   Example pseudocode from MCP:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          -- ... (declarations and logic as per MCP output for wrapper) ...
          CALL `project.dataset.k_ausd_bp_ta_rn_vertrag`(
            JobKennung, v_stichtag, DW_EintragsNr, v_wiederanlaufWert
          );
          -- ... (success logging) ...
        END;
        ```

4.  **Develop Orchestration Layer (Language: Python for Airflow/Cloud Composer or YAML for Workflows)**
    *   Create an Airflow DAG or Workflows definition to schedule and execute the `project.dataset.ausd_bp_ta_rn_vertrag_wrapper` stored procedure. This layer will pass the necessary parameters (e.g., `stichtag`, `wiederanlaufWert`) to the BigQuery procedure.

5.  **Unit and Integration Testing (Language: SQL/Python)**
    *   Write unit tests for each BigQuery stored procedure to verify logic and error handling.
    *   Conduct integration tests to confirm the entire flow, including orchestration and data interactions, works as expected in the BigQuery environment.

6.  **Deployment (Tools: gcloud CLI, CI/CD pipelines)**
    *   Deploy BigQuery table schemas and stored procedures.
    *   Deploy the orchestration layer (e.g., Airflow DAG to Cloud Composer).