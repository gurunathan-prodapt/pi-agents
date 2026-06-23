# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

## 1. Purpose & Scope
This job, named "Bereitstellung Basisprodukte BERT" (Provisioning Base Products BERT) version V2.0.0, is a KornShell wrapper script. Its primary purpose is to orchestrate the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. Specifically, it prepares and provides a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) for scoring/consumption. The script handles input parameters for the cutoff date and a restart value, with defaults applied if not provided. It also manages logging and error handling, delegating the core data preparation logic to a downstream kernel script.

## 2. Source Inventory
The job is composed of a single KornShell script.

| File Path                                                       | Technology | Category | Tool      | Purpose | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------- | :--------- | :------- | :-------- | :------ | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh` | Shell      | shell    | KornShell |         | *Not found*     | semi_auto         |

The complexity tier information for this file was not found in the `file_complexity` table. The automation bucket is `semi_auto`.

## 3. Target Architecture
The migration target is Google BigQuery. The existing KornShell script's orchestration and parameter handling logic will be translated into a BigQuery Stored Procedure.

**BigQuery Components:**
*   **Main Stored Procedure (`project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`):** This will encapsulate the parameter parsing, validation, and job control logic.
*   **Downstream Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_opt_text`):** This will contain the actual business logic for data extraction and transformation, corresponding to the original `k_ausd_bp_ta_bpr_opt_text.ksh` script.
*   **Audit/Logging Tables:**
    *   `project.dataset.job_control`: To store job metadata, status, parameters, and timestamps.
    *   `project.dataset.job_error_log`: To record detailed error information.
    *   `project.dataset.job_message_log`: To store general job messages and logs.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_opt_text.ksh` acts as a wrapper.

**Execution Flow (Legacy):**
1.  `r_ausd_bp_ta_bpr_opt_text.ksh` is executed.
2.  It sources several helper scripts for environment setup, error handling, parameter parsing, and date functions:
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
3.  Parses command-line arguments and applies default values for cutoff date and restart value.
4.  Initializes a job logging context.
5.  Invokes the core business logic script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`, passing validated parameters.
6.  Logs success or error status based on the execution of the kernel script.

**Execution Flow (Target BigQuery):**
1.  The main BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper` is called with parameters.
2.  It initializes variables, determines the system date, and applies default values for cutoff date and restart value (similar to the shell script).
3.  Parameters are validated. If invalid, an entry is made in `job_error_log`, and an error is signaled.
4.  A new entry is created in `job_control` to track the job's execution, including a generated job number.
5.  The downstream Stored Procedure `project.dataset.k_ausd_bp_ta_bpr_opt_text` is invoked, passing the processed parameters and job metadata. This is where the core data operations will occur (reading source tables, filtering, applying restart logic, writing to target tables).
6.  Upon successful completion, the `job_control` table is updated with an 'OK' status and an entry is made in `job_message_log`.
7.  If an error occurs during the downstream procedure's execution, the `job_control` table is updated with an 'ERROR' status, an entry is made in `job_message_log`, and the error is re-raised.

There were no explicit lineage edges found in the `lineage_edges` table for this job or file, indicating that its dependencies are primarily managed via shell script sourcing and invocation, rather than explicit data lineage relationships defined in the metadata.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_opt_text.ksh` script itself performs orchestration and parameter management. The core data transformations are expected to be within the `k_ausd_bp_ta_bpr_opt_text.ksh` script (which will be a separate BigQuery stored procedure).

**Wrapper Script Transformation Logic:**
*   **Parameter Handling:**
    *   `-h`: Displays usage (will be replaced by documentation or help within the procedure).
    *   `-s <DDMMYYYY>` (cutoff date `p_stichtag`): Passed directly. If not provided, defaults to `v_sysdate` (system date).
    *   `-l <restart_value>` (`p_wiederanlaufWert`): Passed directly. If not provided, defaults to `0`.
*   **Date Determination:** The script uses `DWDate_Gib_Zeitraum` to get the system date (`v_sysdate`) in `DDMMYYYY` format. In BigQuery, this will use `CURRENT_DATE()` and `FORMAT_DATE()`.
*   **Parameter Validation:** Checks if `p_stichtag` is set. In BigQuery, this will be handled by `IFNULL` and explicit `IF` checks with `SIGNAL` for errors.
*   **Job Control & Logging:** Manages unique job numbers (`DW_EintragsNr`), log file names (`LogDatei`), and logs various messages and statuses. In BigQuery, this will involve `INSERT` and `UPDATE` statements against `job_control`, `job_error_log`, and `job_message_log` tables.
*   **Error Trapping:** Uses `trap` commands to handle signals (`INT`, `STOP`, `CONT`, `ERR`). In BigQuery, this will be translated to `EXCEPTION WHEN ERROR THEN ... END` blocks within the stored procedure.

The actual data selection, filtering by `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM`, and the application of `DWH_VERTRAG_ID > Wiederanlaufwert` logic is assumed to be part of the downstream `k_ausd_bp_ta_bpr_opt_text.ksh` script and will be translated into SQL within the corresponding BigQuery stored procedure.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated no explicit `external_systems`. However, the script implicitly depends on:

*   **Filesystem-based helper scripts:**
    *   `$HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
*   **Invoked Kernel Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`

**Replacement in BigQuery:**
*   **Environment Initialization (`.dw_init`):** Environment variables are typically managed through deployment configurations (e.g., Cloud Composer environment variables, Cloud Build variables) or passed as parameters to BigQuery stored procedures.
*   **Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The functionalities provided by these scripts (error handling, parameter parsing, date utilities) will be directly integrated into the BigQuery Stored Procedure using native BigQuery SQL functions, control flow (`IF`, `CASE`), and the custom logging tables (`job_error_log`, `job_message_log`).
*   **Invoked Kernel Script (`k_ausd_bp_ta_bpr_opt_text.ksh`):** This will be translated into a separate BigQuery Stored Procedure (as described in Target Architecture) and called directly by the main wrapper procedure.

## 7. Unresolved / Risks
*   **Missing Complexity Data:** The `file_complexity` table did not return any rows for the source file. This means there's no automated tiering or migration flag information available from that source. The `semi_auto` bucket suggests some manual intervention or review will be required.
*   **Downstream Kernel Script Logic:** This design assumes the `k_ausd_bp_ta_bpr_opt_text.ksh` script contains primarily SQL-translatable data manipulation logic. If it contains complex file system operations, external system calls, or non-SQL logic, those parts will require further analysis and potentially alternative migration strategies (e.g., Cloud Functions, Cloud Run, Dataflow, or Python on Cloud Composer).
*   **Shell-specific constructs:** Shell-specific elements like `trap` for error handling and dynamic log file creation are not directly translatable to SQL. These will be replaced with BigQuery's procedural SQL error handling (`EXCEPTION WHEN ERROR`) and persistent audit tables.
*   **Custom functions:** The script uses custom shell functions like `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_ErmittleNr`, etc. While their general purpose is understood from context, their precise internal logic would need to be reviewed during translation to ensure correct BigQuery SQL equivalents are implemented.
*   **Character Encoding:** The comments in the source code show some German special characters (`ü`, `ä`, `ö`) that might indicate the script or its data sources use a specific character encoding (e.g., ISO-8859-1). Ensure character encoding is correctly handled during data ingestion and processing in BigQuery to avoid corruption.

## 8. Build Plan
The migration will involve creating the necessary BigQuery assets in the following order:

1.  **BigQuery Dataset:** Create the target dataset (e.g., `project.dataset`) in BigQuery.
2.  **Audit/Logging Tables (DDL):**
    *   `job_control` table creation script.
    *   `job_error_log` table creation script.
    *   `job_message_log` table creation script.
    *   (Language: BigQuery SQL)
3.  **Downstream Business Logic Stored Procedure (`k_ausd_bp_ta_bpr_opt_text`):**
    *   Translate the logic within the original `k_ausd_bp_ta_bpr_opt_text.ksh` (or its equivalent source) into a BigQuery Stored Procedure. This will involve SQL for data extraction, filtering, transformation, and loading into target tables.
    *   (Language: BigQuery SQL)
4.  **Wrapper Stored Procedure (`ausd_bp_ta_bpr_opt_text_wrapper`):**
    *   Create the main wrapper stored procedure in BigQuery.
    *   Implement parameter handling, default values, date logic, validation checks, and job control table updates.
    *   Include error handling (`EXCEPTION WHEN ERROR`) and calls to the `k_ausd_bp_ta_bpr_opt_text` stored procedure.
    *   (Language: BigQuery SQL)
5.  **Orchestration (Optional, if external dependencies cannot be fully SQL-ized):**
    *   If any part of `k_ausd_bp_ta_bpr_opt_text.ksh` or its dependencies involves non-SQL-translatable logic (e.g., complex file manipulations, external API calls not handled by BigQuery's external functions), implement those parts in a suitable GCP service like Cloud Functions, Cloud Run, or a Python script orchestrated by Cloud Composer or Cloud Workflows.
    *   (Language: Python, YAML/JSON for orchestration)
6.  **Deployment Script:** A script to deploy the BigQuery tables and stored procedures.
    *   (Language: Shell script or Python using BigQuery API/CLI)
7.  **Testing:** Develop unit and integration tests for each BigQuery stored procedure and the overall job flow.