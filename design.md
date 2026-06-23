# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

## 1. Purpose & Scope

This document outlines the migration plan for the KornShell script `r_ausd_v_ta_p_discount_rr.ksh` to Google Cloud Platform, primarily leveraging BigQuery for data processing and Cloud Composer (Airflow) for orchestration.

The source script acts as a wrapper and orchestration component for a core data reconciliation process related to the `ta_p_discount_rr` table. Its main responsibilities include:
*   Initializing the runtime environment.
*   Parsing and validating command-line parameters.
*   Initializing and interacting with a custom logging and error handling framework (`DWMSG_` functions).
*   Invoking a core processing script, `k_ausd_v_ta_p_discount_rr.ksh`, with appropriate parameters.
*   Logging job status and handling execution errors.

The scope of this migration focuses on replatforming this shell-based orchestration logic to a BigQuery Stored Procedure, managed by an Airflow DAG. The core processing logic within `k_ausd_v_ta_p_discount_rr.ksh` is assumed to be migrated separately to a BigQuery Stored Procedure or equivalent.

## 2. Source Inventory

| File Path                                                                   | Technology | Category | Purpose             | Tier      | Automation Bucket |
| :-------------------------------------------------------------------------- | :--------- | :------- | :------------------ | :-------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` | `KornShell`| `shell`  | `pipeline_orchestrator` | `unknown` | `semi_auto`       |

**Summary of `r_ausd_v_ta_p_discount_rr.ksh`:**
This script is a KornShell wrapper that sets up the environment by sourcing utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), parses input parameters using `getopts`, handles errors via `trap` statements and custom `DWMSG_` functions, and ultimately executes the core script `k_ausd_v_ta_p_discount_rr.ksh`, redirecting its output to a log file. It logs job start and end statuses using the `DWMSG_` framework.

## 3. Target Architecture

The migrated job will leverage the following Google Cloud Platform components:

*   **Orchestration:** Google Cloud Composer (Apache Airflow) will manage the scheduling, execution, and monitoring of the migrated workflow. An Airflow DAG will be created to invoke the BigQuery Stored Procedure.
*   **Transformation Logic:** The logic of `r_ausd_v_ta_p_discount_rr.ksh` will be converted into a BigQuery Stored Procedure. This procedure will handle parameter validation, logging, and the invocation of the migrated core logic.
*   **Data Processing:** The core data reconciliation logic (originally in `k_ausd_v_ta_p_discount_rr.ksh`) is expected to be migrated into a separate BigQuery Stored Procedure or a series of BigQuery SQL statements.
*   **Logging & Auditing:** A dedicated BigQuery table will replace the existing file-based logging mechanism, capturing job execution details, parameters, errors, and status updates.
*   **Environment Configuration:** Environment variables and sourced configuration files will be managed through Airflow Variables, BigQuery procedure parameters, or dedicated configuration tables/files in Cloud Storage.

## 4. Data Flow & Lineage

The current job `r_ausd_v_ta_p_discount_rr.ksh` functions as an orchestrator.
1.  **Initialization:** The script first sources environment setup (`.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Handling:** It processes command-line arguments.
3.  **Job Metadata & Logging:** Initializes job identifiers and log file paths using `DWMSG_` functions. Log messages are directed to a dynamically named log file.
4.  **Core Script Invocation:** It invokes `k_ausd_v_ta_p_discount_rr.ksh`, passing relevant job and parameter information, with its output redirected to the log file.
5.  **Status Update:** After the core script completes, it updates the job status via `DWMSG_SetzeStatusOK`.

**Migrated Data Flow:**
1.  **Airflow DAG:** An Airflow DAG will be the entry point, scheduled to run the workflow.
2.  **BigQuery Stored Procedure Call:** The DAG will execute a BigQuery Stored Procedure, e.g., `project.dataset.Vertragsdatenabgleich`, passing any necessary parameters.
3.  **BigQuery Stored Procedure (`Vertragsdatenabgleich`):**
    *   Handles parameter validation.
    *   Inserts log entries into `project.dataset.job_log` for job start, status, and any errors.
    *   Calls the migrated BigQuery Stored Procedure for `k_ausd_v_ta_p_discount_rr.ksh` (e.g., `project.dataset.k_ausd_v_ta_p_discount_rr`).
    *   Updates the `project.dataset.job_log` with final status.
4.  **Core Logic (e.g., `k_ausd_v_ta_p_discount_rr` BSP):** This procedure will contain the actual SQL logic to process data and output to `ta_p_discount_rr`.

Output Target: The reconciled data will ultimately reside in `ta_p_discount_rr`, which will be migrated to a BigQuery table (e.g., `project.dataset.ta_p_discount_rr`).

## 5. Transformation Logic

**Current `r_ausd_v_ta_p_discount_rr.ksh` (Wrapper Script) to BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`):**

*   **Environment Sourcing (`.dw_init`, utility scripts):**
    *   The functionality of sourcing `.dw_init` will be managed by Airflow (e.g., environment variables for the Airflow task, or dynamically retrieved parameters from a configuration file in Cloud Storage).
    *   Utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` will be replaced by equivalent BigQuery Stored Procedures or UDFs, or their core logic will be inlined into the main procedure. The `DWMSG_` functions, which are critical for logging, will be dedicated BigQuery Stored Procedures.
*   **Parameter Parsing (`getopts`):**
    *   Translated directly to BigQuery Stored Procedure input parameters (e.g., `p_h STRING`, `p_s STRING`, `p_l STRING`).
    *   The `while getopts` loop and `case` statement will be converted into BigQuery `IF` and `CASE` control flow statements for validation.
*   **Error Handling (`trap`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`):**
    *   The `trap` constructs will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN ... END` blocks to catch and handle errors within the stored procedure.
    *   `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung` calls will be mapped to calls to dedicated BigQuery Stored Procedures (e.g., `project.dataset.DWMSG_MeldeFehler`, `project.dataset.DWMSG_Fehlerbehandlung`) which will insert error details into the BigQuery logging table.
*   **Logging (`print`, redirection to `$LogDatei`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`):**
    *   All `print` statements and output redirection will be replaced by `INSERT` statements into a BigQuery logging table (e.g., `project.dataset.job_log`).
    *   `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` will be converted into BigQuery Stored Procedures to manage entries in the `job_log` table.
*   **Core Script Invocation (`${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}`):**
    *   This will be a direct call to the migrated core BigQuery Stored Procedure (e.g., `CALL project.dataset.k_ausd_v_ta_p_discount_rr(JobKennung, DW_EintragsNr)`).
*   **BigQuery SQL Pseudocode (as generated by MCP):**
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.Vertragsdatenabgleich`(
      IN p_h STRING,
      IN p_s STRING,
      IN p_l STRING
    )
    BEGIN
      DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
      DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
      DECLARE ErrNr INT64 DEFAULT 0;
      DECLARE ErrArg STRING DEFAULT '';
      DECLARE ErrVal INT64 DEFAULT 0;
      DECLARE DW_EintragsNr INT64 DEFAULT 0;
      DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_P_DISCOUNT_RR';
      DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
      DECLARE LogDatei STRING DEFAULT '';
      DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_v_ta_p_discount_rr';
      -- ... (usage_text declaration) ...

      IF p_h IS NOT NULL THEN
        SELECT usage_text;
        LEAVE;
      END IF;

      -- ... (parameter validation for p_s, p_l) ...

      IF ErrNr != 0 THEN
        INSERT INTO `project.dataset.job_log`
        (job_id, job_name, severity, error_code, error_arg, message, created_at)
        VALUES
        (DW_EintragsNr, JobKennung, 'E', ErrNr, ErrArg, 'Parameterfehler', CURRENT_TIMESTAMP());

        SELECT usage_text;
        RAISE USING MESSAGE = CONCAT('Error ', CAST(ErrNr AS STRING), ': ', ErrArg);
      END IF;

      CALL `project.dataset.DWMSG_ErmittleNr`(DW_EintragsNr);
      CALL `project.dataset.DWMSG_Logdateiname`(LogDatei, JobKennung, DW_EintragsNr);
      CALL `project.dataset.DWMSG_ErzeugeEintrag`(DW_EintragsNr, JobKennung, 'project.dataset.Vertragsdatenabgleich', LogDatei);
      CALL `project.dataset.DWMSG_SetzeStichtagInfo`(DW_EintragsNr, v_sysdate, 'DDMMYYYY');

      BEGIN
        -- ... (logging job header) ...

        CALL `project.dataset.k_ausd_v_ta_p_discount_rr`(
          JobKennung,
          DW_EintragsNr
        );

        SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS msg;
        CALL `project.dataset.DWMSG_SetzeStatusOK`(DW_EintragsNr);

      EXCEPTION WHEN ERROR THEN
        CALL `project.dataset.DWMSG_Fehlerbehandlung`(DW_EintragsNr);
        SELECT 'AppError: Abbruch' AS msg;
        RAISE;
      END;
    END;
    ```

## 6. External Dependencies

*   **Custom DWMSG_ Framework (functions `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc.):** This framework handles logging and error reporting within the legacy environment.
    *   **Replacement Strategy:** These functions will be reimplemented as BigQuery Stored Procedures that perform `INSERT` operations into a centralized BigQuery logging table. This centralizes logging and leverages BigQuery's scalability and querying capabilities.
*   **Utility Shell Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These scripts provide environment variables, error handling, parameter parsing, and date utility functions.
    *   **Replacement Strategy:**
        *   Environment variables (like `$HOME`, `BERT_DIR_ROOT`) will be managed as Airflow Variables or BigQuery Stored Procedure parameters.
        *   Specific logic from `f_alis_msgerr.ksh` related to generic error handling will be integrated into the `DWMSG_` BigQuery Stored Procedures.
        *   Parameter parsing (from `h_alis_parameter.ksh`) will be handled directly by the main BigQuery Stored Procedure's input parameters and conditional logic.
        *   Date utilities (from `h_alis_date.ksh`) will be replaced by BigQuery's native date and time functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Core Script (`k_ausd_v_ta_p_discount_rr.ksh`):** This script contains the primary business logic.
    *   **Replacement Strategy:** This script is a critical external dependency. It must be migrated to a dedicated BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_p_discount_rr`). The wrapper's migrated stored procedure will then `CALL` this new procedure.
*   **File System (for log files):** The current script writes logs to a file.
    *   **Replacement Strategy:** All logging will be directed to a BigQuery logging table, eliminating file system dependencies.

## 7. Unresolved / Risks

*   **Complexity of `k_ausd_v_ta_p_discount_rr.ksh`:** The internal logic of the core script is unknown. If it involves complex shell scripting, file system interactions, or calls to other external systems not yet identified, its migration could be `complex` or `very_complex`. This is the primary unresolved item influencing the overall project.
*   **Detailed `DWMSG_` Framework Implementation:** While the functions are identified, their complete internal logic and dependencies are not fully detailed here. A deeper dive into these utility scripts is required for a complete and accurate BigQuery implementation.
*   **Missing `file_complexity` data:** The absence of complexity tier and migration flags for the wrapper script means we rely solely on automated analysis and manual review. While the `semi_auto` bucket is assigned, there might be hidden complexities not flagged.
*   **BERT_DIR_ROOT resolution:** The exact configuration and usage of `$BERT_DIR_ROOT` across the legacy system needs to be fully understood to ensure correct parameterization in the target environment.

## 8. Build Plan

1.  **Define BigQuery Logging Schema:**
    *   Create the `project.dataset.job_log` BigQuery table with columns like `job_id`, `job_name`, `severity`, `error_code`, `error_arg`, `message`, `created_at`, etc., to capture all relevant logging information.
2.  **Migrate `DWMSG_` Utility Functions (BigQuery Stored Procedures):**
    *   Develop BigQuery Stored Procedures for each `DWMSG_` function used by the wrapper script (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`). These procedures will insert data into the `job_log` table.
3.  **Analyze and Migrate Core Script (`k_ausd_v_ta_p_discount_rr.ksh`):**
    *   Perform a detailed analysis of `k_ausd_v_ta_p_discount_rr.ksh` to identify its data sources, transformations, and target.
    *   Develop a new BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_p_discount_rr`) that encapsulates its functionality using BigQuery SQL. This is a critical prerequisite for the wrapper's migration.
4.  **Develop Main BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`):**
    *   Implement the BigQuery Stored Procedure based on the provided pseudocode in Section 5.
    *   Ensure proper parameter handling, `BEGIN...EXCEPTION` blocks for error management, and calls to the `DWMSG_` utility procedures and the `k_ausd_v_ta_p_discount_rr` core procedure.
5.  **Develop Airflow DAG:**
    *   Create a Python-based Airflow DAG.
    *   The DAG will define a `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` task to call `project.dataset.Vertragsdatenabgleich`.
    *   Configure Airflow Variables or other mechanisms to pass runtime parameters (`-s`, `-l`) to the BigQuery Stored Procedure.
6.  **Refactor General Utilities:**
    *   Any remaining generic logic from `.dw_init`, `h_alis_parameter.ksh`, `h_alis_date.ksh` that is not covered by BQ procedures or Airflow context should be converted into BigQuery UDFs or Python helper functions as part of the Airflow DAG.
7.  **Testing:**
    *   Develop comprehensive unit tests for all BigQuery Stored Procedures.
    *   Implement integration tests for the full workflow orchestrated by the Airflow DAG, including error handling scenarios.
8.  **Deployment:**
    *   Deploy the BigQuery Stored Procedures.
    *   Deploy the Airflow DAG to Cloud Composer.

**Configuration Files (for Airflow and BigQuery):**
*   `bq_job_config.json`: Configuration for BigQuery job execution.
*   `procedure_parameters.yaml`: YAML file defining parameters for the BigQuery Stored Procedures.
*   `logging_config.yaml`: Configuration for the BigQuery logging table and related procedures.
*   `workflow_orchestration_config.yaml`: Airflow DAG configuration details.
*   `dataset_mapping.json`: Mapping of legacy schema/table names to BigQuery project/dataset/table names.
*   `service_account_permissions.json`: JSON key file for service accounts with necessary BigQuery and Cloud Composer permissions.