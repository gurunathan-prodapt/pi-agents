# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

## 1. Purpose & Scope
This shell script, `r_ausd_bp_ta_bcp_iccid.ksh`, serves as an orchestration wrapper for an ETL job named "Bereitstellung Basisprodukte BERT" (Provisioning Base Products BERT). Its primary function is to prepare the environment, parse input parameters, manage logging, and then invoke a core "kernel" script, `k_ausd_bp_ta_bcp_iccid.ksh`, which is responsible for the actual data processing. The job aims to generate a cutoff-date-based extract of contract cache data from the Data Warehouse (DWH) and make it available for credit scoring (Forderungsscoring - FOS). It handles parameters for the cutoff date (`Stichtag`) and an optional restart value (`Wiederanlaufwert`) for incremental processing.

## 2. Source Inventory
The job is primarily composed of one KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`
    *   **Technology:** KornShell (shell)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** ETL Orchestrator / Wrapper Script

## 3. Target Architecture
The migration target is Google BigQuery. The current shell script will be refactored into a BigQuery Stored Procedure, acting as the orchestration layer. The core data transformation logic, currently residing in the invoked kernel script (`k_ausd_bp_ta_bcp_iccid.ksh`), will also be migrated into a separate BigQuery Stored Procedure.

Key BigQuery components will include:
*   **Main Stored Procedure:** `project.dataset.ausd_bp_ta_ibcp_ccid` to encapsulate the wrapper script's logic (parameter handling, logging, and invocation of the kernel procedure).
*   **Kernel Stored Procedure:** `project.dataset.k_ausd_bp_ta_bcp_iccid` (placeholder) to contain the actual data selection, transformation, and load logic, interacting with BigQuery tables equivalent to `vertrag_cache` and `fos_tabelle`.
*   **Logging/Audit Table:** `project.dataset.dwmsg_log` to capture job execution logs, status, and metadata, replacing the file-based logging of the original script.
*   **Job Sequence Table:** `project.dataset.dwmsg_job_sequence` to manage entry numbers for logging.
*   **Dataset Configuration:** BigQuery datasets to house the stored procedures and tables.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bcp_iccid.ksh` script acts as a controlling wrapper.
1.  **Initialization:** It sources several helper KornShell scripts for environment setup, error handling, parameter parsing, and date functions. These include `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
2.  **Parameter Processing:** It parses command-line arguments `-s` (Stichtag) and `-l` (Wiederanlaufwert).
3.  **Date Determination:** It determines the effective `Stichtag`, defaulting to the system date if not provided.
4.  **Logging Setup:** Initializes job-specific logging mechanisms.
5.  **Core Logic Invocation:** The wrapper then invokes the `k_ausd_bp_ta_bcp_iccid.ksh` script, passing the parsed parameters. This kernel script is expected to perform the actual data extraction, transformation, and loading (ETL) operations on data, likely involving tables like `DWH$TA_C_VERTRAG` (based on comments in the original script regarding `maxladedatum` for `DWH\$TA_C_VERTRAG`).
6.  **Status Reporting:** Upon completion of the kernel script, the wrapper updates the job status in its log.

**Lineage Gaps:** The `lineage_edges` query for this specific `run_id` and `node_id` did not return any direct dependencies (INVOKES, READS, WRITES) for `r_ausd_bp_ta_bcp_iccid.ksh`. This suggests that the automated lineage extraction might not have captured the dynamic invocation of `k_ausd_bp_ta_bcp_iccid.ksh` or the sourcing of helper scripts. The data flow to and from specific tables is therefore presumed to occur within the `k_ausd_bp_ta_bcp_iccid.ksh` script.

## 5. Transformation Logic
The transformation logic within `r_ausd_bp_ta_bcp_iccid.ksh` itself is primarily orchestrational and involves:
*   **Parameter Defaulting:** `p_wiederanlaufWert` is set to `0` if not provided. `p_stichtag` defaults to the system date (`v_sysdate`) if not supplied.
*   **Date Formatting:** Uses helper functions (`DWDate_Gib_Zeitraum`) to determine and format dates.
*   **Error Handling:** Utilizes a custom error concept (`f_alis_msgerr.ksh`) with specific error numbers and arguments, and sets up `trap` statements for robust exit handling.
*   **Logging:** Generates log file names and writes execution details to a log.
*   **Delegation:** The core data transformation (contract selection based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` thresholds) is fully delegated to the `k_ausd_bp_ta_bcp_iccid.ksh` script.

**BigQuery Transformation:**
The wrapper's logic will be translated into a BigQuery Stored Procedure, where:
*   Shell parameter parsing (`getopts`) will become procedure parameters.
*   Conditional logic (`if`) for defaults and validation will use `IF` statements and `RAISE` for errors.
*   File-based logging will be replaced by `INSERT` and `UPDATE` statements to a BigQuery audit table.
*   The invocation of `k_ausd_bp_ta_bcp_iccid.ksh` will become a `CALL` to the corresponding BigQuery Stored Procedure.

A placeholder for `k_ausd_bp_ta_bcp_iccid` in BigQuery SQL demonstrates:
*   Parsing `p_stichtag` to `DATE`.
*   Handling `p_wiederanlaufWert`.
*   A `DELETE` statement to remove records `>= v_restart_value` from `fos_tabelle`.
*   An `INSERT INTO` statement to select records from a `vertrag_cache` (DWH equivalent) based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `dwh_vertrag_id` criteria.

## 6. External Dependencies
Based on the `lineage_assembled_jobs` query, there are no explicitly identified external systems for this job (e.g., Oracle, SFTP, S3).
The script primarily interacts with:
*   **Local File System/Environment:** Sourcing of configuration and utility scripts from `$HOME` and `${BERT_DIR_ROOT}`. These will be replaced by BigQuery-native configurations (e.g., procedure parameters, configuration tables, environment variables within a workflow orchestration tool).
*   **Invoked Kernel Script:** `k_ausd_bp_ta_bcp_iccid.ksh` is the primary internal dependency that holds the core data logic. Its internal dependencies (e.g., source tables like `DWH$TA_C_VERTRAG` and target tables like `FOS-Tabelle`) are critical for the end-to-end data flow.

## 7. Unresolved / Risks
*   **Kernel Script Details:** The detailed logic, data sources, and targets of the invoked kernel script (`k_ausd_bp_ta_bcp_iccid.ksh`) are not fully analyzed within this document. A separate analysis and design for this kernel script are required to complete the end-to-end migration. The BigQuery pseudocode for the kernel script is a placeholder based on comments in the wrapper script.
*   **Utility Script Functionality:** The full functionality of the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs to be understood. While `h_alis_parameter.ksh` is replaced by procedure parameters and `h_alis_date.ksh` by BigQuery date functions, the error handling framework (`f_alis_msgerr.ksh`) will require careful mapping to BigQuery's exception handling and a dedicated logging mechanism.
*   **Shell Traps:** The shell's `trap` mechanism for signal handling is not directly portable to BigQuery SQL. BigQuery stored procedures offer `EXCEPTION WHEN ERROR THEN ... END` blocks for error handling, which provide similar functionality but require careful translation of the original error logic.
*   **Environment Sourcing (`. $HOME/.dw_init`):** This is a gap that typically requires mapping environment variables to BigQuery procedure parameters, configuration tables, or secure credential management.
*   **"AL??" Comments:** The script contains commented-out lines like `#AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum`. This indicates potential alternative or legacy logic that was once considered or partially implemented. It should be confirmed whether this logic is still relevant or can be safely ignored.
*   **No Lineage Edges:** The absence of `lineage_edges` for the wrapper script itself indicates a gap in automated dependency discovery for this specific file, requiring manual inference and verification of its interactions.

## 8. Build Plan
The migration to BigQuery will involve generating the following artifacts:

1.  **`r_ausd_bp_ta_bcp_iccid_procedure.sql` (BigQuery SQL Stored Procedure):**
    *   Implements the parameter handling, date determination, and logging orchestration logic of the original `r_ausd_bp_ta_bcp_iccid.ksh`.
    *   Includes calls to the `k_ausd_bp_ta_bcp_iccid` BigQuery stored procedure.
    *   Language: BigQuery SQL.

2.  **`k_ausd_bp_ta_bcp_iccid_procedure.sql` (BigQuery SQL Stored Procedure):**
    *   (Placeholder) Will contain the core ETL logic for selecting and inserting/deleting data from the equivalent BigQuery tables (e.g., `vertrag_cache`, `fos_tabelle`).
    *   Language: BigQuery SQL.

3.  **`dwmsg_log_table.sql` (BigQuery SQL DDL):**
    *   Creates the BigQuery table `project.dataset.dwmsg_log` for capturing job execution logs and status.
    *   Language: BigQuery SQL.

4.  **`dwmsg_job_sequence_table.sql` (BigQuery SQL DDL):**
    *   Creates the BigQuery table `project.dataset.dwmsg_job_sequence` for managing entry numbers.
    *   Language: BigQuery SQL.

5.  **`procedure_parameters.json` (Configuration File):**
    *   Defines procedure inputs and potential default values for external orchestration.

6.  **`orchestration_workflow.yaml` (Optional - Workflow Definition):**
    *   If external scheduling (e.g., Cloud Composer/Airflow) is used, a workflow definition to trigger the BigQuery stored procedure.
    *   Language: YAML/Python (for Airflow).

7.  **`validation_rules.sql` (Optional - BigQuery SQL):**
    *   Contains explicit SQL validation and assertion helpers, replacing parts of the original `pruefeParameterGesetzt` functionality.