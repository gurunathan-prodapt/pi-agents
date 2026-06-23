# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_instance.ksh`, serves as an orchestration wrapper for the initial provisioning of selected basic products for the BERT system. Its primary purpose is to generate a snapshot of contract caches from the Data Warehouse (DWH) and make this data available for "Forderungsscoring" (demand scoring). The script handles parameter parsing, environment setup, and error handling, ultimately delegating the core processing logic to a kernel script named `k_ausd_bp_ta_bpr_instance.ksh`. The job is scoped to process contracts based on a specified reference date (Stichtag) and an optional restart value.

## 2. Source Inventory
The job consists of a single source file:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh`
    *   **Technology:** KornShell Script
    *   **Tool:** KornShell
    *   **Summary:** Orchestrates the initial provisioning of selected basic products for BERT, generating a snapshot of contract caches for 'Forderungsscoring'. Handles parameter parsing, environment setup, and calls a core processing script.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** Orchestration Script

## 3. Target Architecture
The migration target is Google Cloud Platform, specifically BigQuery.
The `r_ausd_bp_ta_bpr_instance.ksh` script, being an orchestration wrapper, will be primarily translated into a BigQuery Stored Procedure. This stored procedure will handle:
*   Parameter handling, validation, and defaulting logic.
*   Logging and status tracking through dedicated audit/log tables.
*   Orchestration of downstream SQL logic, which will involve calling other BigQuery Stored Procedures corresponding to the kernel script (`k_ausd_bp_ta_bpr_instance.ksh`) and any subsequent data processing steps.

For cases where the downstream kernel script (`k_ausd_bp_ta_bpr_instance.ksh`) or other invoked components contain non-SQL logic or external dependencies, supplementary orchestration mechanisms like Cloud Composer (for Airflow DAGs), Cloud Workflows, or Cloud Run will be considered to manage these external interactions.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_instance.ksh` script itself does not directly perform data reads or writes from/to tables. Its role is purely orchestrational.
The lineage indicates that this script:
1.  **Invokes:** `k_ausd_bp_ta_bpr_instance.ksh` (located at `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`).
    *   The `r_ausd_bp_ta_bpr_instance.ksh` script passes parameters such as job identifier, reference date (`Stichtag`), entry number, and the restart threshold (`Wiederanlaufwert`) to this kernel script.
2.  **Reads:** Environment variables and configuration files (`$HOME/.dw_init`).
3.  **Writes:** Log files for job execution status and messages.

The actual data flow (reads from source tables in DWH, transformations, and writes to target tables for Forderungsscoring) is expected to occur within the `k_ausd_bp_ta_bpr_instance.ksh` script and any subsequent processes it initiates. The current lineage analysis does not provide direct evidence of these deeper data interactions involving `r_ausd_bp_ta_bpr_instance.ksh`.

**Execution Order (Migrated):**
1.  BigQuery Orchestration Stored Procedure (`project.dataset.ausd_bp_ta_bpr_instance`) is called.
2.  Parameters are validated and defaulted within the stored procedure.
3.  Job logging and status tracking tables (`project.dataset.job_log`, `project.dataset.job_metadata`) are updated.
4.  The BigQuery Stored Procedure corresponding to the kernel script (`project.dataset.k_ausd_bp_ta_bpr_instance`) is invoked with translated parameters.
5.  Upon completion of the kernel procedure, final job status (`project.dataset.job_status`) and logs are updated.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_instance.ksh` script's logic primarily focuses on control flow and parameter management:
*   **Parameter Parsing:** Uses `getopts` to parse `-s` (Stichtag) and `-l` (Wiederanlaufwert).
    *   **Migration:** These will become `IN` parameters to the BigQuery Stored Procedure.
*   **Defaulting Logic:**
    *   `p_wiederanlaufWert` defaults to `0` if not provided.
    *   `p_stichtag` defaults to the current system date (`v_sysdate`) if not provided.
    *   **Migration:** `IFNULL` and `CURRENT_DATE()` functions in BigQuery SQL will handle this logic.
*   **Date Determination:** Uses `DWDate_Gib_Zeitraum` to get the system date.
    *   **Migration:** `FORMAT_DATE(('%d%m%Y', CURRENT_DATE())` in BigQuery.
*   **Parameter Validation:** Calls `pruefeParameterGesetzt` and checks `ErrNr`.
    *   **Migration:** `IF` statements and `RAISE USING MESSAGE` or `ASSERT` in BigQuery SQL will manage validation.
*   **Error Handling and Logging:**
    *   Utilizes a custom `DWMSG` framework (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc.) for messages and logging to a `LogDatei`.
    *   Uses `trap` for signal handling (INT, STOP, CONT, ERR).
    *   **Migration:** A dedicated BigQuery `job_log` table will store messages. Error handling will use BigQuery's `EXCEPTION WHEN ERROR` block. `trap`-like behavior will be replaced with structured exception handling within the stored procedure.
*   **Orchestration:** Calls the kernel script `${Name_Kernskript}` with parsed and validated arguments.
    *   **Migration:** This will be a `CALL` statement to the corresponding BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_instance`).

## 6. External Dependencies
The `lineage_assembled_jobs` record indicates no explicit external systems or unresolved targets directly identified for this job at the lineage level. However, the script's content reveals several implicit dependencies:
*   **Environment Initialization:** `. $HOME/.dw_init`
    *   **Replacement:** Configuration parameters for the BigQuery Stored Procedure, BigQuery environment variables, or a dedicated configuration table in BigQuery.
*   **Helper Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error concept)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling)
    *   **Replacement:** These functionalities will be integrated directly into the BigQuery Stored Procedure logic using BigQuery SQL functions, parameters, and conditional logic.
*   **Kernel Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`
    *   **Replacement:** A corresponding BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_instance`) that encapsulates the core business logic. If this kernel script has non-SQL external dependencies, then a hybrid approach with Cloud Functions/Run or Airflow/Workflows might be needed to execute those parts.
*   **Operating System/Shell Features:** `getopts`, `trap`, `print`, `tee`.
    *   **Replacement:** These will be replaced by BigQuery SQL procedural statements, logging tables, and error handling constructs.

## 7. Unresolved / Risks
*   **Downstream Logic in `k_ausd_bp_ta_bpr_instance.ksh`:** The migration of the main `r_ausd_bp_ta_bpr_instance.ksh` script is straightforward as it's an orchestrator. The critical unknown is the content and dependencies of `k_ausd_bp_ta_bpr_instance.ksh`. This kernel script likely contains the actual data manipulation (SQL, potentially other shell commands) and needs its own detailed analysis and migration plan. Without this, the overall job migration is incomplete.
*   **`BERT_DIR_ROOT` variable:** This environment variable's resolution is critical for locating dependent scripts. Its value needs to be configured in the target GCP environment (e.g., as a BigQuery constant, a parameter in Cloud Composer, or an environment variable for Cloud Run/Functions).
*   **Date Logic Discrepancy:** The script's comment `AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` suggests an original intent to derive `Stichtag` from `MIN(sysdate, maxladedatum)` from a table. The current implementation defaults `Stichtag` to `v_sysdate`. This potential discrepancy should be clarified with business users to ensure the correct date logic is implemented in BigQuery.
*   **`Wiederanlaufwert` Semantics:** The "restart threshold" implies a specific interaction with target data (delete records >= threshold). The exact BigQuery implementation will depend on the business requirements for idempotency and restartability.
*   **`semi_auto` Automation Bucket:** This indicates that while automated translation is possible, some manual intervention or review will be required, likely due to the shell scripting patterns and the need to define the BigQuery environment setup.

## 8. Build Plan
The build plan focuses on generating the BigQuery Stored Procedure and supporting BigQuery components.

1.  **Define BigQuery Dataset:**
    *   Create a BigQuery dataset (e.g., `project.dataset`) to house the migrated stored procedures and logging/metadata tables.

2.  **Create BigQuery Log and Metadata Tables:**
    *   `project.dataset.job_log`: To store job execution logs, including start/end times, messages, errors, and parameters.
        ```sql
        CREATE TABLE `project.dataset.job_log` (
          job_name STRING,
          job_nr INT64,
          log_level STRING,
          message STRING,
          stichtag STRING,
          restart_value INT64,
          created_at TIMESTAMP
        );
        ```
    *   `project.dataset.job_metadata`: To store job-specific metadata for auditing and tracking.
        ```sql
        CREATE TABLE `project.dataset.job_metadata` (
          job_name STRING,
          job_nr INT64,
          log_file_name STRING,
          sysdate_ddmmyyyy STRING,
          stichtag_ddmmyyyy STRING,
          restart_value INT64,
          created_at TIMESTAMP
        );
        ```
    *   `project.dataset.job_status`: To store the latest status of a job.
        ```sql
        CREATE TABLE `project.dataset.job_status` (
          job_name STRING,
          job_nr INT64,
          status STRING,
          updated_at TIMESTAMP
        );
        ```

3.  **Generate BigQuery Stored Procedure for `r_ausd_bp_ta_bpr_instance.ksh`:**
    *   **Language:** BigQuery SQL
    *   **Filename:** `r_ausd_bp_ta_bpr_instance.sql` (or similar)
    *   **Content:** The pseudocode provided by the `shellscript_to_bqsql_design` tool will be used as the foundation, ensuring proper parameter handling, date logic, validation, and logging via the newly created tables.
    *   **Example (from tool output):**
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_instance`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          -- ... (detailed logic as per the MCP output)
          CALL `project.dataset.k_ausd_bp_ta_bpr_instance`(
            JobKennung,
            v_stichtag,
            DW_EintragsNr,
            v_wiederanlaufWert
          );
          -- ...
        END;
        ```

4.  **Develop BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_instance.ksh`:**
    *   This is a critical follow-on task. The content of `k_ausd_bp_ta_bpr_instance.ksh` needs to be analyzed and migrated separately, likely resulting in one or more BigQuery Stored Procedures and potentially new BigQuery tables.

5.  **Implement Orchestration (if needed):**
    *   If `k_ausd_bp_ta_bpr_instance.ksh` has non-SQL external dependencies, design and build a Cloud Composer DAG or Cloud Workflow to orchestrate the BigQuery Stored Procedures and any necessary Cloud Functions/Run services.

6.  **IAM & Access Control:**
    *   Configure appropriate IAM roles and permissions for service accounts that will execute these BigQuery procedures and interact with logging tables.

7.  **Testing:**
    *   Develop unit and integration tests for the BigQuery Stored Procedures to ensure functional equivalence with the legacy script's behavior.