# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

## 1. Purpose & Scope
This job, named "Bereitstellung Basisprodukte BERT" (Provisioning of Basic Products for BERT) version V2.0.0, is designed to prepare and provide selected base products (e.g., FAX, Data24) for the BERT system. Its primary function is to create a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and make it available for demand scoring. The job acts as an orchestrator, handling parameter parsing, date determination, and logging, while delegating the core business data processing to a downstream kernel script: `k_ausd_bp_ta_cntrct_evn.ksh`. The job is assembled from a single component and is considered of medium complexity.

## 2. Source Inventory
The job consists of one primary source file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh`
    *   **Technology**: KornShell Script
    *   **Tool**: KornShell
    *   **Purpose**: Local orchestration script
    *   **Complexity Tier**: Not available in analysis.
    *   **Automation Bucket**: Not available in analysis.

## 3. Target Architecture
The migration target platform is BigQuery. The existing KornShell script's functionality will be primarily translated into a BigQuery Stored Procedure, complemented by BigQuery tables for logging and orchestration through a suitable GCP service (e.g., Cloud Composer, Cloud Workflows, or Scheduled Queries).

*   **BigQuery Stored Procedure**: A `CREATE OR REPLACE PROCEDURE` will encapsulate the parameter handling, date defaulting logic, validation, and invocation of the core data processing. This procedure will accept input parameters for the cutoff date (`Stichtag`) and restart value (`Wiederanlaufwert`).
*   **BigQuery Audit/Log Table**: A dedicated BigQuery table (e.g., `project.dataset.job_log`) will replace the shell script's custom `DWMSG_*` logging framework for recording job start/end, errors, and status messages.
*   **Core Data Processing**: The business logic currently residing in `k_ausd_bp_ta_cntrct_evn.ksh` (which this wrapper script calls) will be migrated into a separate BigQuery Stored Procedure or a series of SQL DML statements within the wrapper procedure. This will handle the selection, filtering, and insertion of contract data, including any deletion logic based on the `Wiederanlaufwert`.
*   **Orchestration**: A GCP orchestration service will be used to schedule and invoke the BigQuery Stored Procedure. This will manage the overall workflow, including error handling, retries, and parameter passing.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_cntrct_evn.ksh` script orchestrates the following flow:

1.  **Environment Setup**:
    *   Sources `$HOME/.dw_init` to initialize the environment.
    *   Sources common utility scripts: `f_alis_msgerr.ksh` (error handling), `h_alis_parameter.ksh` (parameter parsing helpers), `h_alis_date.ksh` (date handling helpers).
2.  **Parameter Processing**:
    *   Parses command-line arguments `-s` (cutoff date) and `-l` (restart value).
    *   Defaults `Wiederanlaufwert` to `0` if not provided.
    *   Determines `sysdate`.
    *   Defaults `Stichtag` to `sysdate` if not provided.
    *   Validates the `Stichtag` parameter.
3.  **Logging & Error Handling**:
    *   Initializes job metadata and logging via custom `DWMSG_*` functions.
    *   Sets `trap` commands for `INT`, `STOP`, `CONT`, and `ERR` signals for robust error handling.
    *   Prints job information to standard output/log.
4.  **Core Business Logic Invocation**:
    *   Invokes the kernel script `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_evn.ksh` with the parsed parameters (`JobKennung`, `Stichtag`, `DW_EintragsNr`, `Wiederanlaufwert`). This kernel script is responsible for the actual data extraction, transformation, and loading of contract data to the FOS table.
5.  **Completion**:
    *   Upon successful completion of the kernel script, logs a success message.
    *   Sets the job status to OK using `DWMSG_SetzeStatusOK`.
    *   Clears traps and exits with status `0`.

There are no direct `lineage_edges` explicitly recorded for this script within the provided `run_id`, meaning the specific data read/write dependencies were not explicitly captured by the automated lineage tools. However, the script's content clearly indicates its role as an orchestrator and its reliance on the kernel script for data operations.

## 5. Transformation Logic
The transformation logic in `r_ausd_bp_ta_cntrct_evn.ksh` primarily involves:

*   **Parameter Normalization**:
    *   The restart value (`p_wiederanlaufWert`) is initialized to `0` if not provided. This translates to `IFNULL(p_wiederanlaufWert, 0)` in BigQuery SQL.
    *   The cutoff date (`p_stichtag`) is defaulted to the system date (`v_sysdate`) if not explicitly set. This translates to `IFNULL(NULLIF(p_stichtag, ''), v_sysdate)` in BigQuery.
*   **Date Handling**: The script uses a custom function `DWDate_Gib_Zeitraum` to get the system date. In BigQuery, this will be replaced by `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Validation**: The script includes a `pruefeParameterGesetzt Stichtag p_stichtag` call. This will be replaced by an `IF ... THEN RAISE` or `ASSERT` statement in BigQuery SQL to ensure required parameters are present.
*   **Orchestration**: The script's main "transformation" is the sequential execution of setup, parameter processing, and then the call to the kernel script. This will be replicated by the BigQuery Stored Procedure calling other procedures (if the kernel script is broken down) or containing the migrated DML directly.

The actual data transformations, such as data filtering based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID`, are expected to be contained within the `k_ausd_bp_ta_cntrct_evn.ksh` kernel script. These DML operations will need to be explicitly translated into BigQuery SQL within the target stored procedure.

## 6. External Dependencies
The legacy script has the following external dependencies and their proposed BigQuery/GCP replacements:

*   **Sourced Environment File (`. $HOME/.dw_init`)**:
    *   **Replacement**: Environment variables specific to the BigQuery project and dataset can be passed as parameters to the BigQuery Stored Procedure, managed via an orchestration tool like Cloud Composer, or stored in a BigQuery configuration table.
*   **Sourced Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**:
    *   **Replacement**:
        *   Error handling (`f_alis_msgerr.ksh`): Replaced by BigQuery's `RAISE` statements, `EXCEPTION WHEN ERROR` blocks, and a BigQuery audit/log table.
        *   Parameter handling (`h_alis_parameter.ksh`): Replaced by native BigQuery Stored Procedure parameter declarations and `IFNULL`/`CASE` logic.
        *   Date handling (`h_alis_date.ksh`): Replaced by BigQuery's built-in date functions like `CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`.
*   **Downstream Kernel Script (`k_ausd_bp_ta_cntrct_evn.ksh`)**:
    *   **Replacement**: This script contains the core business logic. Its functionality will be fully migrated into BigQuery SQL, likely as a separate BigQuery Stored Procedure, which will then be called from the main orchestration procedure.
*   **File-based Logging (`>> $LogDatei`)**:
    *   **Replacement**: All logging will be redirected to an audit/log table in BigQuery. GCP's Cloud Logging can also ingest logs from BigQuery for centralized monitoring.
*   **OS Signal Traps (`trap`)**:
    *   **Replacement**: Specific OS signal handling is not directly replicable in BigQuery SQL. This functionality will be managed at the orchestration layer (e.g., Cloud Composer's retry mechanisms, task dependencies, and failure handling).

## 7. Unresolved / Risks
*   **Missing Complexity and Automation Rate**: The absence of `tier`, `migration_flags`, and `migration_bucket` from `file_complexity` and `automation_rate` tables means a definitive assessment of migration effort and automation potential is not available. This introduces a risk of underestimating the effort required.
*   **Kernel Script Logic**: The design assumes the core business logic within `k_ausd_bp_ta_cntrct_evn.ksh` will be fully translated into BigQuery SQL. Any complex, non-SQL logic within that kernel script (e.g., external calls, file manipulations not related to data, complex string processing) could pose additional challenges and may require alternative solutions (e.g., UDFs, Cloud Functions, Python scripts in Cloud Composer).
*   **Exact Date Synchronization (`MIN(sysdate, maxladedatum)`)**: While the current script defaults to `sysdate`, the commented-out section indicates a potential historical requirement for `MIN(sysdate, maxladedatum)`. If this logic is reactivated or implicitly used elsewhere, it needs to be explicitly handled in BigQuery SQL using `LEAST()` and appropriate table lookups.
*   **`DWH_VERTRAG_ID` Data Type**: The `Wiederanlaufwert` parameter is used in a comparison with `DWH_VERTRAG_ID`. The data type of `DWH_VERTRAG_ID` in the source system needs to be confirmed to ensure correct type casting and comparison in BigQuery.
*   **Security Context of Init Files**: The `. $HOME/.dw_init` script might set up specific security contexts or environment variables that need careful consideration for translation into GCP's IAM and secrets management.

## 8. Build Plan
The build plan involves creating BigQuery resources and configuring an orchestration workflow.

1.  **Define BigQuery Audit/Log Table DDL**:
    *   Create a DDL for a `job_log` table to capture job execution details, status, and messages.
    *   Language: BigQuery DDL.
2.  **Migrate Kernel Script `k_ausd_bp_ta_cntrct_evn.ksh` to BigQuery Stored Procedure**:
    *   Analyze the content of `k_ausd_bp_ta_cntrct_evn.ksh`.
    *   Translate all data extraction, filtering, transformation, and loading logic into BigQuery SQL DML statements.
    *   Encapsulate this logic into a `CREATE OR REPLACE PROCEDURE` (e.g., `project.dataset.process_contract_data`).
    *   Language: BigQuery SQL.
3.  **Create Main Orchestration BigQuery Stored Procedure**:
    *   Implement the logic of `r_ausd_bp_ta_cntrct_evn.ksh` as a BigQuery Stored Procedure (e.g., `project.dataset.ausd_bp_ta_cntrct_evn`).
    *   This procedure will handle:
        *   Declaration and defaulting of parameters (`p_stichtag`, `p_wiederanlaufWert`).
        *   Date determination using `CURRENT_DATE()` and `FORMAT_DATE()`.
        *   Parameter validation using `IF ... THEN RAISE`.
        *   Logging of job start/end and errors to the `job_log` table.
        *   Invocation of the migrated kernel stored procedure (`project.dataset.process_contract_data`).
    *   Language: BigQuery SQL.
    *   **Pseudocode Example (from CM MCP tool):**
        ```sql
        -- BigQuery Script / Stored Procedure Pseudocode
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
          DECLARE v_sysdate STRING;
          DECLARE v_stichtag STRING;
          DECLARE v_wiederanlaufWert INT64;
          DECLARE v_eintragsnr INT64;
          DECLARE v_log_message STRING;

          -- Initialize restart value
          SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

          -- System date in DDMMYYYY
          SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

          -- Default cutoff date
          SET v_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

          -- Validate required parameter
          IF v_stichtag IS NULL OR v_stichtag = '' THEN
            INSERT INTO `project.dataset.job_log`
            (job_name, log_level, message, created_at)
            VALUES
            (v_job_kennung, 'ERROR', 'Stichtag parameter missing', CURRENT_TIMESTAMP());

            RAISE USING MESSAGE = 'Stichtag parameter missing';
          END IF;

          -- Create job entry number and Log start
          -- (Logic to determine v_eintragsnr and insert into job_log)

          -- Core processing placeholder:
          -- Call the migrated kernel stored procedure here
          -- CALL `project.dataset.process_contract_data`(v_stichtag, v_wiederanlaufWert);

          -- Log success
          -- (Logic to insert success message into job_log)

        END;
        ```
4.  **Configure Orchestration**:
    *   Choose a GCP orchestration service (e.g., Cloud Composer, Cloud Workflows, or BigQuery Scheduled Queries).
    *   Create a workflow/DAG that calls the `project.dataset.ausd_bp_ta_cntrct_evn` BigQuery Stored Procedure, passing the necessary parameters.
    *   Configure scheduling, error handling, and alerting within the chosen orchestration tool.
    *   Language: Python (for Cloud Composer/Airflow DAG), YAML (for Cloud Workflows), or BigQuery SQL (for Scheduled Queries).
5.  **Parameter/Configuration Table (Optional)**:
    *   Create a BigQuery table to store job-specific configurations, such as default parameter values, target dataset/table names, or logging destinations, if needed.
    *   Language: BigQuery DDL.