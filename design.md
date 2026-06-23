# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope

This job, identified as `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh` (version V2.0.0, "Bereitstellung Basisprodukte BERT"), is responsible for the initial provision of selected base products for the BERT system. Its primary function is to create a cutoff-date extraction of the contract cache data from the Data Warehouse (DWH) and make it available for Forderungsscoring (FOS) consumption. It accepts a specific cutoff date (`Stichtag`) as a parameter; if not provided, it defaults to the current system date. The job also supports a restart mechanism (`Wiederanlaufwert`) to process only contracts with IDs greater than a specified value, with corresponding deletion and reprocessing of records at or above the threshold. This script acts as a wrapper, orchestrating parameter handling, logging, and delegating the core data processing to an internal "kernel script."

## 2. Source Inventory

The assembled job consists of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh`
*   **Technology**: KornShell (`ksh`)
*   **Category**: shell
*   **Complexity Tier**: Undetermined (no file_complexity metadata found)
*   **Migration Bucket**: semi_auto

## 3. Target Architecture

The migration target platform is Google BigQuery. The existing KornShell script will be refactored into a BigQuery Stored Procedure, complemented by BigQuery tables for logging and job control. Orchestration will be handled by a cloud-native service such as Cloud Composer, Workflows, or Dataform.

*   **Main Script**: `r_ausd_bp_ta_rn_da_vda_tk.ksh` will be converted into a BigQuery Stored Procedure (e.g., `project.dataset.bereitstellung_basisprodukte_bert`). This procedure will manage parameter input, validation, default value assignment, and error handling.
*   **Core Processing Logic**: The invoked "kernel script" `k_ausd_bp_ta_rn_da_vda_tk.ksh` (which performs the actual data extraction and transformation) will also be migrated to a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_rn_da_vda_tk`).
*   **Logging and Status**: The existing `DWMSG` framework and file-based logging will be replaced by dedicated BigQuery tables (e.g., `project.dataset.job_log`, `project.dataset.job_status`, `project.dataset.job_control`) for auditing, error reporting, and job lifecycle management.
*   **Orchestration**: A Cloud Composer DAG or Google Cloud Workflow will be used to schedule and execute the main BigQuery Stored Procedure, passing required parameters.

## 4. Data Flow & Lineage

The current script `r_ausd_bp_ta_rn_da_vda_tk.ksh` serves as an entry point.
1.  **Environment Setup**: It sources `$HOME/.dw_init` and various utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment variables, error handling, parameter parsing, and date functions.
2.  **Parameter Processing**: It parses command-line arguments for `Stichtag` (`-s`) and `Wiederanlaufwert` (`-l`). It sets default values if parameters are missing (e.g., `Stichtag` defaults to `sysdate`, `Wiederanlaufwert` defaults to `0`).
3.  **Logging Initialization**: It initializes a job entry and log file using the `DWMSG` framework.
4.  **Core Logic Invocation**: The script's main function is to invoke `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`, passing it parameters including `JobKennung`, `Stichtag`, `DW_EintragsNr`, and `Wiederanlaufwert`. This `k_ausd_bp_ta_rn_da_vda_tk.ksh` script is expected to contain the primary data extraction, transformation, and loading logic for the contract cache data.
5.  **Error Handling & Status**: It uses `trap` commands for error handling and updates job status via the `DWMSG` framework.

The migration will follow this logical flow:
*   The Cloud Composer DAG will trigger the main BigQuery Stored Procedure.
*   The BigQuery Stored Procedure (`bereitstellung_basisprodukte_bert`) will handle parameter validation, date calculations, and logging to BigQuery tables.
*   It will then call the second BigQuery Stored Procedure (`k_ausd_bp_ta_rn_da_vda_tk`) with the processed parameters.
*   This core procedure will contain the SQL logic to read from source tables (e.g., DWH contract cache tables), apply transformations, and write to target tables (FOS-Tabelle).
*   Logging of errors and success messages will occur directly to BigQuery log tables.

## 5. Transformation Logic

The KornShell script primarily orchestrates execution and handles control flow, parameter management, and logging. The transformation logic for the data itself is delegated to the `k_ausd_bp_ta_rn_da_vda_tk.ksh` script.

*   **Parameter Handling**:
    *   `getopts` will be replaced by BigQuery Stored Procedure input parameters.
    *   Defaulting `p_wiederanlaufWert` to `0` and `p_stichtag` to `v_sysdate` will be implemented using `IFNULL` or `COALESCE` within the BigQuery Stored Procedure.
*   **Date Functions**:
    *   `DWDate_Gib_Zeitraum` will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions to obtain and format the system date (`DDMMYYYY`).
*   **Conditional Logic**:
    *   Shell `if [[ -z ... ]]` and `if [ ! $ErrNr -eq 0 ]` constructs will be translated to BigQuery SQL's `IF ... THEN ... ELSE ... END IF;` statements.
*   **Error Handling**:
    *   `trap` mechanisms will be replaced by BigQuery Scripting's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, logging details to BigQuery audit tables and using `SIGNAL SQLSTATE` for error propagation.
    *   The `pruefeParameterGesetzt` function will be replaced by explicit `IF ... THEN SIGNAL` checks.
*   **Logging**:
    *   The custom `DWMSG_*` functions (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) and file redirection (`>> $LogDatei`) will be replaced by `INSERT` statements into BigQuery logging tables (`project.dataset.job_log`, `project.dataset.job_status`).
*   **Invoking Core Logic**:
    *   The shell execution `${Name_Kernskript}` will be replaced by a `CALL` statement to the BigQuery Stored Procedure corresponding to `k_ausd_bp_ta_rn_da_vda_tk.ksh`.
*   **Restart Logic**:
    *   The `Wiederanlaufwert` logic, which implies deleting and then re-inserting records based on `DWH_VERTRAG_ID`, will be implemented using BigQuery's `DELETE` and `INSERT` (or `MERGE`) statements within the `k_ausd_bp_ta_rn_da_vda_tk` stored procedure.

## 6. External Dependencies

The `lineage_assembled_jobs` record indicates no direct external system dependencies (`external_systems: []`) for this specific job. However, based on the script's content and the CM MCP analysis, the following dependencies are present:

*   **Environment Initialization Files**: `$HOME/.dw_init` and other utility shell scripts under `${BERT_DIR_ROOT}/allgemein/is/util/bin/`.
    *   **Replacement**: Environment variables and configurations previously set by these scripts will be managed through BigQuery Stored Procedure parameters, BigQuery `OPTIONS` (for session variables), or dedicated configuration tables within BigQuery. Utility functions will be refactored into BigQuery UDFs or part of the stored procedures.
*   **Custom Logging Framework**: The `DWMSG` framework.
    *   **Replacement**: This will be entirely replaced by insertions into BigQuery logging and audit tables as detailed in Section 5.
*   **Core Processing Script**: `k_ausd_bp_ta_rn_da_vda_tk.ksh`.
    *   **Replacement**: This script will be converted into a BigQuery Stored Procedure, which will encapsulate the core data manipulation logic. Its dependencies on source and target tables (e.g., "DWH contract cache" and "FOS-Tabelle") will be explicitly defined within this new BigQuery procedure.

## 7. Unresolved / Risks

*   **Core Logic Complexity**: The actual data extraction, transformation, and loading logic resides within the `k_ausd_bp_ta_rn_da_vda_tk.ksh` script, which was not analyzed in this initial step. The complexity of this core script is currently unknown and represents the primary remaining analytical task for a complete migration design. Without its analysis, the full scope of data transformations and potential BigQuery-specific challenges (e.g., large data volumes, complex SQL constructs, window functions) cannot be fully assessed.
*   **Missing Complexity Metadata**: The `file_complexity` table did not return any rows for the seed script. This means no detailed complexity tier (simple, medium, complex, very_complex) or specific migration flags were identified by automated analysis. This lack of information could hide potential migration challenges or lead to an underestimation of effort.
*   **Implicit Database Access**: While the current script doesn't directly access a database, the invoked kernel script (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) is clearly intended to interact with "DWH" tables and "FOS-Tabelle." The exact schemas, table names, and database type (e.g., Oracle, Teradata) of these implicit data sources are not explicitly defined by this script and will need to be determined during the analysis of `k_ausd_bp_ta_rn_da_vda_tk.ksh`.
*   **Date Logic Nuances**: The script comments mention `MIN(sysdate,maxladedatum)` but the implementation defaults to `sysdate`. This subtle difference might need explicit confirmation of the intended behavior for the BigQuery implementation.
*   **Unicode/Character Encoding**: The original script comments contain special characters (e.g., `ausgewhlter`). This suggests potential character encoding issues that should be addressed during migration to BigQuery, ensuring data integrity for non-ASCII characters.

## 8. Build Plan

The following artifacts will be generated for the BigQuery migration:

1.  **BigQuery Stored Procedure: `bereitstellung_basisprodukte_bert`** (BQSQL)
    *   **Purpose**: Main wrapper procedure, handles parameter parsing (`p_stichtag`, `p_wiederanlaufWert`), validation, defaults, logging, and orchestration of the core logic.
    *   **Input Parameters**: `p_stichtag` (STRING), `p_wiederanlaufWert` (INT64)
    *   **Contents**:
        *   Declaration of variables for job metadata, system date, etc.
        *   Logic to initialize `v_restart_value` and `v_stichtag` based on input parameters and defaults.
        *   Validation logic for `v_stichtag`.
        *   `INSERT` statements into `project.dataset.job_log` for job start, info messages, and completion.
        *   `INSERT` into `project.dataset.job_control` for audit purposes.
        *   `BEGIN...EXCEPTION WHEN ERROR THEN...END` block to capture and log errors, updating `project.dataset.job_status`.
        *   A `CALL` statement to `project.dataset.k_ausd_bp_ta_rn_da_vda_tk`.
        *   `UPDATE` statement for `project.dataset.job_status` on success.
    *   **Language**: BigQuery SQL

2.  **BigQuery Stored Procedure: `k_ausd_bp_ta_rn_da_vda_tk`** (BQSQL)
    *   **Purpose**: Encapsulate the core data extraction and manipulation logic previously in `k_ausd_bp_ta_rn_da_vda_tk.ksh`.
    *   **Input Parameters**: `v_job_kennung` (STRING), `v_stichtag` (STRING), `v_job_eintragsnr` (INT64), `v_restart_value` (INT64)
    *   **Contents**: (To be determined after analysis of the original `k_ausd_bp_ta_rn_da_vda_tk.ksh` script) This will likely involve `SELECT` queries from source DWH tables, `DELETE` statements (based on `v_restart_value`), `INSERT` or `MERGE` statements to populate the target FOS table, and any necessary data transformations.
    *   **Language**: BigQuery SQL

3.  **BigQuery Table: `project.dataset.job_log`** (DDL)
    *   **Purpose**: Centralized logging for job execution, errors, and informational messages.
    *   **Schema**: `job_name` (STRING), `job_version` (STRING), `job_kennung` (STRING), `log_level` (STRING), `log_message` (STRING), `created_at` (TIMESTAMP)
    *   **Language**: BigQuery DDL

4.  **BigQuery Table: `project.dataset.job_status`** (DDL)
    *   **Purpose**: Track the current status of each job run.
    *   **Schema**: `job_kennung` (STRING), `job_entry_nr` (INT64), `status` (STRING), `updated_at` (TIMESTAMP)
    *   **Language**: BigQuery DDL

5.  **BigQuery Table: `project.dataset.job_control`** (DDL)
    *   **Purpose**: Store control parameters and audit information for job runs, including `Stichtag` and `Wiederanlaufwert`.
    *   **Schema**: `job_kennung` (STRING), `stichtag` (STRING), `sysdate_ddmmyyyy` (STRING), `restart_value` (INT64), `created_at` (TIMESTAMP)
    *   **Language**: BigQuery DDL

6.  **Orchestration Artifact**: Cloud Composer DAG (Python) or Google Cloud Workflow (YAML)
    *   **Purpose**: Schedule the execution of `project.dataset.bereitstellung_basisprodukte_bert` and manage retries, dependencies, and monitoring.
    *   **Contents**: Task to execute the BigQuery Stored Procedure, potentially passing parameters from Airflow variables or sensor outputs.
    *   **Language**: Python (for Cloud Composer) or YAML (for Workflows)