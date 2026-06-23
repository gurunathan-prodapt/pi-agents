# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

## 1. Purpose & Scope
The shell script `r_ausd_adressen.ksh` serves as an initial wrapper for a temporary address extraction process from the CRS (presumably a source system). Its primary function is to orchestrate the execution of a core extraction script (`k_ausd_adressen.ksh`), handling parameter parsing, environment setup, logging, and error handling. This job generates a snapshot (Stichtags-Abzug) of address data for business partners and invoice recipients, which forms the basis for further data preparation. The scope of this migration design focuses specifically on transforming the `r_ausd_adressen.ksh` wrapper logic into a BigQuery equivalent.

## 2. Source Inventory
The assembled job consists of a single KornShell script:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh`
*   **Technology**: KornShell (ksh)
*   **Complexity Tier**: Medium
*   **Automation Bucket**: Semi-Auto
*   **Purpose**: ETL wrapper script.

This script sources several utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) and invokes a core script (`k_ausd_adressen.ksh`), which are considered dependencies but not direct "component files" of this specific assembled job.

## 3. Target Architecture
The legacy `r_ausd_adressen.ksh` wrapper script will be migrated to a BigQuery Stored Procedure, `project.dataset.sp_temp_adressabzug_crs`. This stored procedure will handle parameter input, date calculations, basic validation, and orchestrate the call to a yet-to-be-migrated core BigQuery procedure (representing `k_ausd_adressen.ksh`).

Supporting BigQuery components will include:
*   **`project.dataset.job_control`**: An audit table to track job execution status, start/end times, and parameter values.
*   **`project.dataset.job_log`**: An audit table to store detailed log messages (INFO, ERROR) that currently go to a flat file.
*   **`project.dataset.job_error_log`**: An audit table to specifically capture error events.

## 4. Data Flow & Lineage
The original script performs the following actions:
1.  **Environment Setup**: Sources `$HOME/.dw_init` and several utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Parsing**: Reads command-line arguments `-s` (Stichtag/cutoff date) and `-l` (Wiederanlaufwert/restart value).
3.  **Defaulting**: Sets `p_wiederanlaufWert` to 0 if not provided and `p_stichtag` to the current system date if missing.
4.  **Validation**: Checks if `p_stichtag` is set.
5.  **Logging Initialization**: Uses `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo` to initialize logging and set up error traps.
6.  **Core Script Invocation**: Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_adressen.ksh` with the parsed and defaulted parameters, redirecting its output to the log file.
7.  **Status Update**: On successful completion, `DWMSG_SetzeStatusOK` is called.

In BigQuery, this flow will be:
1.  **Invocation**: The `sp_temp_adressabzug_crs` stored procedure will be invoked, likely by an external orchestrator like Airflow, with `p_stichtag` and `p_wiederanlaufWert` as parameters.
2.  **Internal Variable Setup**: Environment variables and sourced utility scripts will be replaced by internal procedure variables or references to other BigQuery UDFs/procedures.
3.  **Parameter Handling**: The BigQuery procedure will directly receive parameters. Defaulting logic for `p_wiederanlaufWert` and `p_stichtag` will be implemented using `IFNULL` and `FORMAT_DATE(CURRENT_DATE())`.
4.  **Validation**: Parameter validation will use `IF ... THEN SIGNAL` statements.
5.  **Logging & Error Handling**: `DWMSG` functions will be replaced by `INSERT` statements into the `job_log` and `job_error_log` audit tables. Error trapping (`trap`) will be handled by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block.
6.  **Core Procedure Invocation**: A placeholder `CALL `project.dataset.sp_ausd_adressen`(...)` will represent the execution of the migrated `k_ausd_adressen.ksh` logic.
7.  **Status Update**: The `job_control` table will be `UPDATE`d to reflect `RUNNING`, `OK`, or `FAILED` status.

The original script is invoked by a UC4 job, as indicated by `lineage_edges` showing `UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_ADRESSEN.xml` invoking `SCRIPT:R_AUSD_ADRESSEN.KSH`. This UC4 scheduler will need to be replaced, likely by an Airflow DAG.

## 5. Transformation Logic
The `r_ausd_adressen.ksh` script primarily contains orchestration logic and minimal data transformation:
*   **Parameter Defaulting**:
    *   `p_wiederanlaufWert` is defaulted to `0` if not provided. In BigQuery, this maps to `IFNULL(p_wiederanlaufWert, 0)`.
    *   `p_stichtag` is defaulted to the system date (`DDMMYYYY`) if not provided. In BigQuery, this maps to `IFNULL(NULLIF(TRIM(p_stichtag), ''), FORMAT_DATE('%d%m%Y', CURRENT_DATE()))`.
*   **Date Determination**: `DWDate_Gib_Zeitraum` to get the system date will be replaced by `CURRENT_DATE()` and `FORMAT_DATE` in BigQuery.
*   **Parameter Validation**: `pruefeParameterGesetzt` will be replaced by `IF ... THEN SIGNAL` constructs.
*   **Logging**: All `print` statements and `DWMSG_` function calls will be replaced by `INSERT` statements into BigQuery logging tables (`job_log`, `job_error_log`).

The core data extraction and transformation logic resides within the `k_ausd_adressen.ksh` script. This script was not part of the initial analysis and represents the main transformation unit that will need separate migration to BigQuery SQL/procedures.

## 6. External Dependencies
The original script has the following dependencies:
*   **Internal Helper Scripts**:
    *   `. $HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling.
    These will be absorbed into the BigQuery stored procedure (e.g., `DWDate_Gib_Zeitraum` to BigQuery native date functions, `pruefeParameterGesetzt` to `ASSERT` or `IF SIGNAL`, logging functions to `INSERT` statements into audit tables). Environment variables like `$BERT_DIR_ROOT` will be replaced by BigQuery `DECLARE`d variables, procedure parameters, or values from configuration tables.
*   **Core Extraction Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_adressen.ksh`: This is the crucial dependency. Its logic must be fully migrated to a BigQuery Stored Procedure (e.g., `project.dataset.sp_ausd_adressen`) or a series of SQL scripts. This migration is outside the scope of this *wrapper* script's design but is essential for the overall job to function.
*   **Scheduler**:
    *   The `lineage_edges` indicate this job is invoked by a UC4 scheduler (`DW.BERT_P_ADRESSEN.xml`). This will be replaced by an Airflow DAG that calls the BigQuery stored procedure.

No direct external systems (e.g., Oracle, SFTP, S3) were identified as directly consumed or produced by *this wrapper script*, although the data extracted by `k_ausd_adressen.ksh` may originate from or be destined for such systems. The input source "CRS" implies a database or file system, which will need to be accessible from BigQuery (e.g., via federated queries or Cloud Storage ingestion).

## 7. Unresolved / Risks
*   **Core Logic (`k_ausd_adressen.ksh`)**: The most significant unresolved item is the content and complexity of `k_ausd_adressen.ksh`. This script contains the actual address extraction logic and its migration (B2/B3/B4) will dictate the overall migration effort. Without its analysis, the complete end-to-end BigQuery solution cannot be finalized.
*   **Filesystem Operations / OS Commands**: The wrapper script itself sources files and executes another script. If `k_ausd_adressen.ksh` performs extensive filesystem operations, invokes external OS commands, or interacts with legacy systems not directly supported by BigQuery, these parts may require redesign (B4) and implementation in Python (e.g., running on Cloud Functions, Cloud Run, or within Airflow tasks) rather than pure BigQuery SQL.
*   **`trap` mechanism**: The shell `trap` command provides specific error handling behavior. While `BEGIN...EXCEPTION` blocks in BigQuery handle errors, subtle differences in how signals are caught and processed might require careful testing and potentially more granular error handling within BigQuery procedures or the orchestrating Airflow DAG.
*   **`usage` function**: The `usage()` function for displaying help is a shell-specific feature. This will be replaced by external documentation or help text within the BigQuery procedure comments.

## 8. Build Plan
1.  **Design `project.dataset.sp_temp_adressabzug_crs` (BigQuery Stored Procedure)**:
    *   Implement parameter handling, defaulting, and validation logic as described in the "Transformation Logic" section.
    *   Integrate logging into `project.dataset.job_log`, `project.dataset.job_error_log`, and `project.dataset.job_control` tables.
    *   Implement error handling using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END`.
    *   Add a placeholder `CALL `project.dataset.sp_ausd_adressen`(...)` for the core extraction logic.
    *   **Language**: BigQuery SQL (DDL for procedure and audit tables).
2.  **Design `project.dataset.sp_ausd_adressen` (BigQuery Stored Procedure for core logic)**:
    *   **Requires analysis of `k_ausd_adressen.ksh`**. This will be a separate design and build effort.
    *   This procedure will contain the main data extraction, transformation, and loading logic.
    *   **Language**: BigQuery SQL.
3.  **Create Audit Tables**:
    *   Define DDL for `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log`.
    *   **Language**: BigQuery SQL (DDL).
4.  **Develop Airflow DAG**:
    *   Create an Airflow DAG to schedule and invoke `project.dataset.sp_temp_adressabzug_crs`, passing the necessary `p_stichtag` and `p_wiederanlaufWert` parameters.
    *   The DAG will replace the legacy UC4 scheduler.
    *   **Language**: Python.