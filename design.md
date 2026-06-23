# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_beschr.ksh`, serves as a wrapper for an ETL job. Its primary purpose is the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. It handles parameter parsing, environment setup, logging, and error handling, before invoking a core script, `k_ausd_bp_ta_bpr_beschr.ksh`, to perform the actual data processing. The job extracts a snapshot of contract cache data from the Data Warehouse (DWH) and makes it available for the Forderungsscoring (FOS) system. It supports a restart mechanism and manages the deletion of already provisioned table content when no active, uncollected contract cache exists.

The scope of this migration design document covers the transformation of this KornShell wrapper script and its orchestrational logic to Google BigQuery. The actual data transformation logic, presumed to be within `k_ausd_bp_ta_bpr_beschr.ksh`, is not explicitly detailed here but is assumed to be integrated into the BigQuery stored procedure's core logic.

## 2. Source Inventory
This job consists of a single KornShell script.

*   **File Name**: `r_ausd_bp_ta_bpr_beschr.ksh`
*   **Relative Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh`
*   **Technology**: KornShell
*   **Category**: shell
*   **Tool**: KornShell
*   **Complexity Tier**: medium
*   **Migration Bucket**: semi_auto
*   **Purpose Note**: Job assembled from 1 component(s); stage dist: medium=1

## 3. Target Architecture
The migration target is Google BigQuery. The KornShell wrapper script will be re-engineered into a BigQuery Stored Procedure. This stored procedure will encapsulate the parameter handling, date logic, logging, and error handling previously managed by the shell script.

*   **Main Component**: BigQuery Stored Procedure (e.g., `project.dataset.ausd_bp_ta_bpr_beschr`)
*   **Auxiliary Tables**:
    *   `project.dataset.job_audit`: For storing job execution metadata, start/end times, status, parameters, and error messages.
    *   `project.dataset.job_log`: For detailed logging of job execution steps, similar to the original log file.
*   **Data Tables (Presumed)**:
    *   Source: `project.dataset.ta_vertrag_cache` (equivalent of `DWH$TA_C_VERTRAG` if `FOSHoleLadedatum` was active).
    *   Target: `project.dataset.fos_tabelle`.
*   **Orchestration**: Cloud Composer (Apache Airflow) could be used to schedule and trigger the BigQuery Stored Procedure, replacing the `ksh` execution.

## 4. Data Flow & Lineage
The original script's data flow is primarily orchestrational, delegating the core data logic to an invoked script.

*   **Legacy Flow**:
    1.  `r_ausd_bp_ta_bpr_beschr.ksh` starts.
    2.  Reads environment variables and sources helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    3.  Parses command-line parameters (`-s` for Stichtag, `-l` for restart value).
    4.  Initializes `p_wiederanlaufWert` to 0 if not provided.
    5.  Determines `v_sysdate` and defaults `p_stichtag` to `v_sysdate` if not provided.
    6.  Validates `p_stichtag`. If invalid, logs error and exits.
    7.  Initializes logging (obtains `DW_EintragsNr`, `LogDatei`).
    8.  Sets up `trap` handlers for error management.
    9.  Invokes `k_ausd_bp_ta_bpr_beschr.ksh` with parameters (`-j`, `-s`, `-f`, `-l`).
    10. Logs successful completion and sets job status if `k_ausd_bp_ta_bpr_beschr.ksh` completes without error.
    11. Exits.

*   **Migrated BigQuery Flow**:
    1.  A Cloud Composer DAG or external scheduler invokes the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_beschr`.
    2.  The stored procedure receives `p_stichtag_string` and `p_wiederanlaufWert` as input parameters.
    3.  Initializes internal variables and normalizes `p_stichtag` to a `DATE` type.
    4.  Logs the job start and parameters into `project.dataset.job_audit` and `project.dataset.job_log`.
    5.  Executes the core business logic (derived from `k_ausd_bp_ta_bpr_beschr.ksh`), which is expected to:
        *   Select records from `project.dataset.ta_vertrag_cache` based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` (using `p_stichtag` and `v_restart_value`).
        *   Optionally delete records from `project.dataset.fos_tabelle` based on `v_restart_value`.
        *   Insert the selected records into `project.dataset.fos_tabelle`.
    6.  Updates the job status to 'OK' in `project.dataset.job_audit` and logs success in `project.dataset.job_log`.
    7.  If any error occurs, the `EXCEPTION WHEN ERROR` block captures it, updates the job status to 'ERROR', logs the error, and raises an error to the caller.

## 5. Transformation Logic
The transformation logic focuses on converting shell script constructs to BigQuery SQL equivalents within a stored procedure.

*   **Parameter Handling**: `getopts` logic will be replaced by direct Stored Procedure input parameters (`IN p_stichtag_string STRING`, `IN p_wiederanlaufWert INT64`).
*   **Defaulting Logic**: Shell `if [[ -z "$var" ]]` conditions will translate to `IF ... IS NULL OR TRIM(...) = '' THEN ... END IF;` statements in BigQuery SQL.
*   **Date Operations**: `DWDate_Gib_Zeitraum` and other date helpers will be replaced by BigQuery native date functions like `CURRENT_DATE()`, `PARSE_DATE('%d%m%Y', ...)`.
*   **Logging and Error Handling**:
    *   Shell `print` statements to log files will be converted to `INSERT INTO project.dataset.job_log (...)` statements.
    *   `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler` calls will be replaced by inserts/updates to `job_audit` and `job_log` tables.
    *   Shell `trap` error handling will be superseded by BigQuery Stored Procedure `EXCEPTION WHEN ERROR THEN ... END` blocks, which can capture `@@error.message`.
*   **Script Invocation**: The invocation of `k_ausd_bp_ta_bpr_beschr.ksh` will be replaced by the direct inclusion of its data transformation logic within the BigQuery Stored Procedure. This will involve:
    *   Selecting data from a source table (`project.dataset.ta_vertrag_cache`).
    *   Filtering based on date ranges (`gueltig_von`, `gueltig_bis`, `ladedatum`) and the `p_stichtag`.
    *   Applying restart logic filter (`dwh_vertrag_id > v_restart_value`).
    *   Potentially performing `DELETE` operations on the target table (`project.dataset.fos_tabelle`) for restartability.
    *   `INSERT`ing processed data into `project.dataset.fos_tabelle`.

## 6. External Dependencies
The original script has minimal direct external dependencies beyond its local filesystem.

*   **Environment Variables & Helper Scripts**:
    *   `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: These provide environment setup, error handling functions, parameter parsing, and date utilities.
    *   **Replacement**: In BigQuery, these will be replaced by the Stored Procedure's internal logic, BigQuery's native functions, and potentially configuration tables or parameters for environment-specific settings.
*   **Core Script Invocation**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`: This is the crucial external dependency, as it contains the actual business logic.
    *   **Replacement**: The logic from `k_ausd_bp_ta_bpr_beschr.ksh` needs to be extracted and translated into BigQuery SQL, forming the core of the new Stored Procedure's data manipulation statements.
*   **Logging (Filesystem)**:
    *   Log files (`>> $LogDatei`): The script writes output and errors to a dynamically named log file.
    *   **Replacement**: All logging will be redirected to the `project.dataset.job_log` BigQuery table, providing structured and queryable logs.

## 7. Unresolved / Risks
*   **Missing `k_ausd_bp_ta_bpr_beschr.ksh` Logic**: The exact SQL logic and tables used within `k_ausd_bp_ta_bpr_beschr.ksh` are not available in this analysis. This is the primary unresolved item. A detailed analysis of this core script is required to complete the transformation logic for the BigQuery Stored Procedure. The pseudocode provided by the tool makes an assumption about the source and target tables (`ta_vertrag_cache`, `fos_tabelle`) and the filtering conditions, which needs to be verified against the actual `k_ausd_bp_ta_bpr_beschr.ksh` content.
*   **Complexity of Helper Scripts**: The content of helper scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` was not analyzed in detail. While the MCP tool inferred replacements, complex logic within these helpers might require specific BigQuery UDFs or further logic within the stored procedure.
*   **Performance Implications**: Migrating shell-based ETL to BigQuery stored procedures might have performance implications that need to be evaluated during implementation, especially concerning large data volumes and complex SQL transformations.
*   **Restart Semantics**: The script mentions a `Wiederanlaufwert` (restart value) and corresponding delete logic. The exact implications of this restart mechanism on data consistency and idempotency in BigQuery need careful consideration and testing.
*   **Typo**: The script contains `end esac` instead of `esac`. This should be corrected during migration.

## 8. Build Plan
1.  **Analyze `k_ausd_bp_ta_bpr_beschr.ksh`**: Obtain and analyze the content of the core script to extract its data extraction, transformation, and loading logic. This is the critical first step.
2.  **Define BigQuery Schema**:
    *   Create `project.dataset.job_audit` table.
    *   Create `project.dataset.job_log` table.
    *   Confirm and define schemas for source (`project.dataset.ta_vertrag_cache`) and target (`project.dataset.fos_tabelle`) tables, including data types and partitioning/clustering strategies.
3.  **Develop BigQuery Stored Procedure**:
    *   Translate the parameter parsing, defaulting, and validation logic into BigQuery SQL.
    *   Implement date calculation using BigQuery date functions.
    *   Integrate the core data transformation logic from `k_ausd_bp_ta_bpr_beschr.ksh` into the stored procedure, including `SELECT`, `DELETE` (if applicable for restart), and `INSERT` statements.
    *   Implement logging into `job_audit` and `job_log` tables.
    *   Implement robust error handling using `EXCEPTION WHEN ERROR`.
    *   **Language**: BigQuery SQL
4.  **Develop Orchestration (Optional but Recommended)**:
    *   Create an Apache Airflow DAG in Cloud Composer to schedule and trigger the BigQuery Stored Procedure.
    *   Define DAG parameters that map to the Stored Procedure's inputs.
    *   **Language**: Python (for Airflow DAG)
5.  **Testing**:
    *   Unit test the BigQuery Stored Procedure with various parameter combinations (including null/missing parameters, different `Stichtag` and `Wiederanlaufwert`).
    *   Integrate testing to ensure the entire pipeline (orchestrator -> stored procedure) functions correctly.
    *   Validate data output against legacy system results.
6.  **Deployment**:
    *   Deploy the BigQuery tables and Stored Procedure.
    *   Deploy the Cloud Composer DAG (if used).