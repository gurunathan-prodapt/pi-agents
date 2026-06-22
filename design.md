# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bcp_iccid.ksh`, acts as an orchestration layer for preparing selected "Basisprodukte" (basic products) for BERT's demand scoring system. Its primary function is to handle input parameters (snapshot date and restart value), set up the execution environment, perform initial parameter validation, and manage logging and error handling. Crucially, it delegates the core data processing logic to a separate "kernel script" named `k_ausd_bp_ta_bcp_iccid.ksh`. The business purpose is to create a snapshot of contract cache data from the DWH for demand scoring.

## 2. Source Inventory
The primary component of this job is a single KornShell script.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`
*   **Technology**: KornShell (ksh)
*   **Summary**: Orchestration layer for data preparation, parameter handling, and logging. Invokes a kernel script for core logic.
*   **Category**: shell
*   **Tool**: KornShell
*   **Tier**: medium
*   **Migration Bucket**: semi_auto
*   **Invoked Kernel Script**: `k_ausd_bp_ta_bcp_iccid.ksh` (details of this script are not part of the current job analysis and need separate investigation).

## 3. Target Architecture
The migration target is Google Cloud Platform, specifically BigQuery. The orchestration logic of `r_ausd_bp_ta_bcp_iccid.ksh` will be translated into a BigQuery Stored Procedure. This stored procedure will handle parameter parsing, validation, and logging. The invocation of the "kernel script" will be replaced by a call to another BigQuery Stored Procedure. External dependencies like logging and error messaging will leverage BigQuery tables and potentially Cloud Logging/Monitoring, with email notifications possibly handled by Cloud Functions triggered by BigQuery events or logs.

**BigQuery Components:**
*   **BigQuery Stored Procedure (Orchestrator)**: `project.dataset.ausd_bp_ta_ibcp_ccid` (replacing `r_ausd_bp_ta_bcp_iccid.ksh`).
*   **BigQuery Stored Procedure (Kernel)**: `project.dataset.k_ausd_bp_ta_bcp_iccid` (replacing `k_ausd_bp_ta_bcp_iccid.ksh`, this needs to be developed separately).
*   **Audit Table**: `project.dataset.job_audit` (for tracking job status, entry numbers, and metadata).
*   **Log Table**: `project.dataset.job_log` (for detailed job execution logs).
*   **Error Log Table**: `project.dataset.job_error_log` (for specific error events).
*   **Parameter/Configuration Tables**: (Optional) For environment-specific settings or job defaults.

## 4. Data Flow & Lineage
**Original Flow:**
1.  An upstream UC4 job (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) invokes the `r_ausd_bp_ta_bcp_iccid.ksh` script.
2.  `r_ausd_bp_ta_bcp_iccid.ksh` performs parameter parsing, validation, and environment setup.
3.  `r_ausd_bp_ta_bcp_iccid.ksh` logs job start and status using `DWMSG_*` functions.
4.  `r_ausd_bp_ta_bcp_iccid.ksh` invokes `k_ausd_bp_ta_bcp_iccid.ksh` with parameters: job ID, cutoff date, job entry number, and restart value.
5.  `k_ausd_bp_ta_bcp_iccid.ksh` (the kernel script) performs the core data processing (extraction, transformation, loading).
6.  Upon completion, `r_ausd_bp_ta_bcp_iccid.ksh` logs success or failure and exits.

**Target Flow (BigQuery):**
1.  An orchestrator (e.g., Cloud Composer DAG, or direct BigQuery stored procedure call) invokes the main BigQuery Stored Procedure `project.dataset.ausd_bp_ta_ibcp_ccid` with `p_stichtag` and `p_wiederanlaufWert` as arguments.
2.  `project.dataset.ausd_bp_ta_ibcp_ccid` (the orchestrator SP) performs parameter validation, determines system date, and records job start in `project.dataset.job_audit` and `project.dataset.job_log`.
3.  `project.dataset.ausd_bp_ta_ibcp_ccid` calls the `project.dataset.k_ausd_bp_ta_bcp_iccid` BigQuery Stored Procedure (the kernel SP) with the processed parameters.
4.  `project.dataset.k_ausd_bp_ta_bcp_iccid` executes the core data processing logic (to be defined in its migration).
5.  Upon successful return from `project.dataset.k_ausd_bp_ta_bcp_iccid`, the orchestrator SP `project.dataset.ausd_bp_ta_ibcp_ccid` updates the job status to 'OK' in `project.dataset.job_audit` and logs success to `project.dataset.job_log`.
6.  In case of errors, the orchestrator SP catches exceptions, logs them to `project.dataset.job_error_log` and `project.dataset.job_log`, and updates `project.dataset.job_audit` with 'ERROR' status.

## 5. Transformation Logic

The `r_ausd_bp_ta_bcp_iccid.ksh` script's logic primarily involves:

*   **Parameter Parsing and Defaults**:
    *   Reads `-s` (Stichtag/cutoff date `DDMMYYYY`) and `-l` (restart value `DWH_VERTRAG_ID`).
    *   Defaults `p_wiederanlaufWert` to 0 if not provided.
    *   Defaults `p_stichtag` to current system date (`DDMMYYYY`) if not provided.
    *   *BigQuery Mapping*: This will be directly translated into `IFNULL` conditions and `DECLARE` statements within the `project.dataset.ausd_bp_ta_ibcp_ccid` stored procedure. `CURRENT_DATE()` and `FORMAT_DATE` will be used for date handling.

*   **Parameter Validation**:
    *   Checks if `p_stichtag` is set.
    *   Exits with error codes (e.g., 192, 193) on validation failure.
    *   *BigQuery Mapping*: `IF` statements with `RAISE` will replace explicit exits. Error details will be logged into `project.dataset.job_error_log`.

*   **Environment Sourcing**:
    *   `$HOME/.dw_init`: General environment setup.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helpers.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helpers.
    *   *BigQuery Mapping*: These are shell-specific and will not have direct BigQuery equivalents. Environmental variables will be passed as stored procedure parameters or configured at the job execution level (e.g., Cloud Composer environment variables). The helper script logic will be re-implemented directly in BigQuery SQL functions or within the stored procedure logic.

*   **Job Logging and Status Management**:
    *   Uses `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung` to manage job identifiers, log file names, create audit entries, and report status.
    *   *BigQuery Mapping*: These functions will be replaced by direct `INSERT` and `UPDATE` statements against the `project.dataset.job_audit`, `project.dataset.job_log`, and `project.dataset.job_error_log` tables.

*   **Error Trapping (`trap`)**:
    *   Shell-specific mechanism for handling signals (INT, STOP, CONT, ERR).
    *   *BigQuery Mapping*: This will be managed using BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for SQL errors. For external signals or orchestration failures, Cloud Composer/Workflows' native error handling and retry mechanisms would be utilized.

*   **Invocation of Kernel Script**:
    *   Executes `k_ausd_bp_ta_bcp_iccid.ksh` with parameters.
    *   *BigQuery Mapping*: This will be a `CALL` statement to the `project.dataset.k_ausd_bp_ta_bcp_iccid` stored procedure.

## 6. External Dependencies
*   **Shell Environment (`$HOME/.dw_init`)**: Contains environment variables and sourced scripts.
    *   *Replacement*: Environment variables will be explicitly passed as parameters or managed by the orchestration tool (e.g., Airflow Variables in Cloud Composer). Sourced scripts' functionality will be re-implemented in BigQuery SQL or Python.
*   **Custom Shell Libraries (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**: Provide error handling, parameter parsing, and date utility functions.
    *   *Replacement*: Their functionality will be absorbed directly into the BigQuery stored procedure. Error handling will use `BEGIN...EXCEPTION`, parameter handling will use `IF`/`CASE` statements, and date handling will use BigQuery's native date functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE`).
*   **Logging System (`DWMSG_*` functions)**: Manages log file creation, audit entries, and error reporting.
    *   *Replacement*: Dedicated BigQuery tables (`job_audit`, `job_log`, `job_error_log`) will capture all logging and auditing information.
*   **External Kernel Script (`k_ausd_bp_ta_bcp_iccid.ksh`)**: Contains the main business logic.
    *   *Replacement*: This script needs to be analyzed and migrated separately, likely to another BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bcp_iccid`) or a series of BigQuery SQL statements. Its data sources and targets need to be identified.
*   **Mail/Notification (`DWMSG_ERMITTLENR` -> SENDS_MAIL)**: The lineage indicates this function might send emails.
    *   *Replacement*: This functionality can be replaced by integrating with Google Cloud's notification services, such as triggering a Cloud Function via Pub/Sub on specific log entries in BigQuery, which then sends emails (e.g., using SendGrid or a similar service).

## 7. Unresolved / Risks
*   **Unknown Kernel Script Logic**: The most significant unresolved item is the content and functionality of `k_ausd_bp_ta_bcp_iccid.ksh`. This script holds the core data processing, and its migration design is critical and currently undefined. This presents a major risk to the overall project. A separate detailed analysis of `k_ausd_bp_ta_bcp_iccid.ksh` is mandatory.
*   **Complex Date Logic**: While `h_alis_date.ksh` is replaced by BigQuery functions, the original script comments mention `MIN(sysdate,maxladedatum)` and `FOSHoleLadedatum "DWH$TA_C_VERTRAG" v_ladedatum`. If the "maxladedatum" logic was critical, its exact implementation (from `h_alis_fos_date.ksh` or other related scripts) needs to be understood and replicated in BigQuery. The current analysis only infers `p_stichtag=$v_sysdate;`.
*   **Dynamic Script Execution**: The original script constructs the kernel script name dynamically using `${BERT_DIR_ROOT}`. This implies potential variations in the invoked script. The BigQuery target assumes a single, fixed kernel stored procedure, which might need adjustment if `BERT_DIR_ROOT` dynamically alters the script's behavior or points to different scripts.
*   **Orchestration Environment**: The shell script inherently runs in a Linux environment with access to a filesystem and other binaries. The BigQuery stored procedure environment is more restricted. Any file I/O or system commands performed by the kernel script will need complete redesign.
*   **UC4 Job Integration**: The current UC4 scheduler (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) invokes this shell script. The migration of this UC4 job to, for example, Cloud Composer, needs to be considered to ensure the correct scheduling and invocation of the new BigQuery stored procedure.

## 8. Build Plan
1.  **Define BigQuery Schema for Audit & Logs**:
    *   Create `project.dataset.job_audit` table.
    *   Create `project.dataset.job_log` table.
    *   Create `project.dataset.job_error_log` table.
    *   *Language*: BigQuery DDL
2.  **Develop BigQuery Stored Procedure for Orchestration**:
    *   Translate `r_ausd_bp_ta_bcp_iccid.ksh`'s parameter parsing, validation, date defaulting, and logging logic into `project.dataset.ausd_bp_ta_ibcp_ccid`.
    *   Include `CALL project.dataset.k_ausd_bp_ta_bcp_iccid` statement.
    *   Implement `EXCEPTION WHEN ERROR THEN` block for robust error handling.
    *   *Language*: BigQuery SQL
3.  **Analyze and Design Kernel Script Migration**:
    *   **CRITICAL PATH**: Perform a detailed analysis of `k_ausd_bp_ta_bcp_iccid.ksh` to understand its data sources, transformation logic, and targets.
    *   Design a new BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bcp_iccid`) or BigQuery SQL scripts to implement its functionality.
    *   *Language*: BigQuery SQL / Python (for PySpark on Dataproc/Spark if needed)
4.  **Implement Kernel Script BigQuery Components**:
    *   Create `project.dataset.k_ausd_bp_ta_bcp_iccid` BigQuery Stored Procedure.
    *   Implement its data processing logic, including all `READS`/`WRITES` from relevant tables.
    *   *Language*: BigQuery SQL
5.  **Develop Notification Mechanism (if required)**:
    *   If email notification (`SENDS_MAIL`) is essential, design and implement a Cloud Function or similar service triggered by BigQuery log entries to send alerts.
    *   *Language*: Python (Cloud Functions)
6.  **Integrate with Orchestration Tool**:
    *   If using Cloud Composer/Airflow, create a DAG to schedule and invoke `project.dataset.ausd_bp_ta_ibcp_ccid`.
    *   Define Airflow Variables for any external configurations.
    *   *Language*: Python (Airflow DAG)
7.  **Testing**:
    *   Unit test each BigQuery stored procedure.
    *   Integration test the full workflow from orchestrator invocation through kernel script execution and logging.
    *   Performance test the BigQuery components.