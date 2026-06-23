# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_bcp.ksh`. This job is responsible for the initial provisioning of selected base products (e.g., FAX, Data24) for BERT. It acts as an orchestration wrapper, handling parameter parsing, environment setup, logging, and error handling, before invoking a core kernel script to perform the actual data processing. The job is designed to produce a snapshot of the contract cache in DWH on a specified cutoff date (`Stichtag`) and make it available for credit scoring.

The scope of this migration is limited to the `r_ausd_bp_ta_bpr_bcp.ksh` script and its direct functionalities. The core business logic residing in the invoked kernel script (`k_ausd_bp_ta_bpr_bcp.ksh`) is considered a separate migration unit, though its interaction with this wrapper is a key design consideration.

## 2. Source Inventory
The primary source component for this job is a single KornShell script.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh`
    *   **Technology:** KornShell
    *   **Detected Tool:** KornShell
    *   **Complexity Signals:** (None detected)
    *   **File Purpose:** (Not explicitly defined in metadata, but serves as an ETL orchestrator/wrapper)
    *   **Complexity Tier:** (Not available from metadata)
    *   **Migration Flags:** (Not available from metadata)
    *   **Migration Bucket:** `semi_auto`

## 3. Target Architecture
The target architecture in BigQuery will involve the following components:

*   **BigQuery Stored Procedure:** The KornShell wrapper script's logic will be converted into a BigQuery Stored Procedure. This procedure will handle parameter parsing, validation, date determination, and the orchestration of the downstream BigQuery components that will encapsulate the logic of `k_ausd_bp_ta_bpr_bcp.ksh`.
*   **BigQuery Job Log Table:** A dedicated BigQuery table (`project.dataset.job_log`) will replace the file-based logging mechanism. This table will store job metadata, status, errors, and informational messages.
*   **BigQuery Stored Procedure (for `k_ausd_bp_ta_bpr_bcp.ksh`):** It is assumed that the core kernel script (`k_ausd_bp_ta_bpr_bcp.ksh`) will also be migrated to a separate BigQuery Stored Procedure, which will be called by this wrapper procedure.
*   **External Orchestration (Optional):** If the `k_ausd_bp_ta_bpr_bcp.ksh` script cannot be fully migrated into BigQuery SQL, or if there are other external dependencies, Cloud Workflows, Cloud Composer, or Cloud Run may be used to orchestrate the BigQuery Stored Procedures and any remaining external processes.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_bcp.ksh` script itself does not directly process data. Its primary function is to orchestrate a data processing task.

*   **Invocation:** The script is likely invoked by a UC4 job (`DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BCP.xml` INVOKES `SCRIPT:R_AUSD_BP_TA_BPR_BCP.KSH`).
*   **Input:** The script receives input parameters for a cutoff date (`-s Stichtag DDMMYYYY`) and an optional restart value (`-l Wiederanlaufwert`).
*   **Processing (Orchestration):**
    1.  Loads environment variables and helper scripts.
    2.  Parses input parameters.
    3.  Determines a processing `Stichtag` (cutoff date), defaulting to the system date if not provided.
    4.  Initializes logging and error handling mechanisms.
    5.  Invokes the `k_ausd_bp_ta_bpr_bcp.ksh` script, passing the parsed parameters and logging context.
*   **Output:**
    1.  Logs job execution details, status, and any errors to the console and a log file.
    2.  The actual data output is produced by the invoked `k_ausd_bp_ta_bpr_bcp.ksh` script (the specific data sources/targets are not directly evident from this wrapper script, but are implicitly handled by the kernel script).
    3.  Updates job status on completion.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_bcp.ksh` script's transformation logic primarily concerns parameter normalization and orchestration:

*   **Parameter Initialization:**
    *   `p_wiederanlaufWert`: Defaults to `0` if not explicitly provided via the `-l` parameter.
    *   `p_stichtag`: If not provided via the `-s` parameter, it defaults to the current system date in `DDMMYYYY` format.
*   **Error Handling:** The script uses a custom error handling framework (`f_alis_msgerr.ksh`, `DWMSG_*` functions) and shell traps (`INT`, `STOP`, `CONT`, `ERR`) to manage runtime errors and report status.
*   **Invocation of Kernel Script:** The script constructs a command to execute `k_ausd_bp_ta_bpr_bcp.ksh` with specific arguments derived from the parsed parameters and internal job identifiers. This is the main "action" of the script.

The script does not contain any direct data transformation or aggregation logic. All such logic is delegated to the `k_ausd_bp_ta_bpr_bcp.ksh` script.

## 6. External Dependencies
The `r_ausd_bp_ta_bpr_bcp.ksh` script has the following external dependencies:

*   **Environment Initialization File:** `$HOME/.dw_init`
    *   **Replacement in BigQuery:** Environment variables will be replaced by BigQuery Stored Procedure parameters, session variables, or a BigQuery configuration table.
*   **Error/Logging Framework Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Replacement in BigQuery:** These functionalities will be re-implemented using BigQuery Stored Procedures, User-Defined Functions (UDFs), or inline SQL logic, specifically leveraging a BigQuery job log table.
*   **Core Business Logic Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_bcp.ksh`
    *   **Replacement in BigQuery:** This script needs to be migrated to its own BigQuery Stored Procedure, which will then be called by the migrated `r_ausd_bp_ta_bpr_bcp` procedure.
*   **Shell Utilities:** `getopts`, `print`, `trap`, `tee`, `set -e`.
    *   **Replacement in BigQuery:** BigQuery scripting capabilities (e.g., `DECLARE`, `SET`, `IF`, `EXCEPTION WHEN ERROR`) will replace these shell constructs. Logging redirection (`tee`) will be handled by inserting records into the BigQuery job log table.

## 7. Unresolved / Risks
*   **Core Kernel Script (`k_ausd_bp_ta_bpr_bcp.ksh`):** The full business logic resides in this invoked script. Its migration design and implementation are critical for the complete functionality of this job. Without it, the wrapper serves no data processing purpose. This is the biggest dependency and potential risk.
*   **`trap`-based OS Signal Handling:** Shell traps for error and interrupt handling (`INT`, `STOP`, `CONT`, `ERR`) do not have a direct equivalent in BigQuery SQL. These will be replaced by `BEGIN...EXCEPTION...END` blocks and status logging in BigQuery.
*   **Framework Functions:** The `DWMSG_*`, `DWDate_*`, and `pruefeParameterGesetzt` functions need to be carefully analyzed and re-implemented as BigQuery procedures/UDFs or integrated directly into the BigQuery stored procedure.
*   **Environment Context:** The sourcing of `$HOME/.dw_init` implies a reliance on specific environment variables that need to be replicated as parameters or configuration in the BigQuery environment.
*   **Date Format Assumptions:** The script assumes `DDMMYYYY` for dates. This should be explicitly handled in BigQuery to avoid unexpected behavior due to different date parsing rules.

## 8. Build Plan

The migration of this job to BigQuery will follow these steps:

1.  **Define BigQuery Schema for Logging:**
    *   Create the `project.dataset.job_log` table to capture job execution details.
        ```sql
        CREATE TABLE `project.dataset.job_log` (
            job_run_id INT64,
            job_name STRING,
            job_kennung STRING,
            log_timestamp TIMESTAMP,
            status STRING,
            error_nr INT64,
            error_arg STRING,
            stichtag STRING,
            wiederanlaufwert INT64,
            message STRING
        );
        ```

2.  **Migrate Helper Functions to BigQuery:**
    *   Analyze and convert the logic within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` (especially `DWDate_Gib_Zeitraum` and `pruefeParameterGesetzt`) into BigQuery UDFs or, if more complex, BigQuery Stored Procedures. If simple, these can be inlined into the main procedure.

3.  **Migrate Core Kernel Script (`k_ausd_bp_ta_bpr_bcp.ksh`):**
    *   This is a separate, major task. Analyze `k_ausd_bp_ta_bpr_bcp.ksh` to determine its data sources, transformations, and targets.
    *   Design and implement `k_ausd_bp_ta_bpr_bcp` as a BigQuery Stored Procedure, ensuring all its logic, including data manipulation and I/O, is replicated in BigQuery.

4.  **Create BigQuery Stored Procedure for `r_ausd_bp_ta_bpr_bcp.ksh`:**
    *   Develop a BigQuery Stored Procedure (e.g., `p_ausd_bp_ta_bpr_bcp`) that encapsulates the orchestration logic of the original KornShell script.
    *   **Language:** BigQuery SQL (Scripting).
    *   **Inputs:** `in_stichtag STRING`, `in_wiederanlaufWert STRING`, `in_job_source STRING`, `in_run_mode STRING`.
    *   **Logic:**
        *   Implement parameter defaulting and validation as outlined in the "Transformation Logic" section.
        *   Replace shell-based logging with `INSERT` statements into the `project.dataset.job_log` table.
        *   Replace shell traps with BigQuery's `BEGIN...EXCEPTION...END` blocks for error handling.
        *   Include a `CALL` statement to the migrated BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_bcp.ksh`, passing the necessary parameters.
    *   **Output:** The procedure will primarily manage and log the execution flow; its data output will be indirect via the invoked kernel procedure.

5.  **Orchestration (if required):**
    *   If there are external dependencies or if the overall workflow requires external scheduling, design an Airflow DAG (using Cloud Composer) or Cloud Workflows to invoke the BigQuery Stored Procedure, passing the necessary parameters.

6.  **Testing:**
    *   Thoroughly test the BigQuery Stored Procedure with various parameter combinations, including cases for missing parameters and edge dates, to ensure functional equivalence with the original script.
    *   Verify correct logging of job status and errors in the `job_log` table.
    *   Integrate testing with the migrated `k_ausd_bp_ta_bpr_bcp` procedure.