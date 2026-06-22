# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_vvl_dwh.ksh`, is a KornShell script serving as an orchestration and wrapper framework for a data reconciliation process. Its primary business purpose is to manage the contract data reconciliation for the `ta_vvl_dwh` table. The script handles environment setup, command-line parameter parsing, robust logging, and error trapping, ultimately invoking a core processing script, `k_ausd_v_ta_vvl_dwh.ksh`, which is responsible for the actual data reconciliation logic. The scope of this migration is to translate this shell-based workflow into a Google BigQuery-centric solution, utilizing BigQuery stored procedures and scripting for the core logic, and potentially Cloud Composer for external orchestration if complex scheduling or dependency management is required.

## 2. Source Inventory
The job consists of a single primary source file and several invoked utility scripts.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh`
    *   **Technology:** KornShell Script
    *   **Summary:** This KornShell script acts as a framework for reconciling contract data for the 'ta_vvl_dwh' table. It handles parameter parsing, environment setup, logging, error trapping, and orchestrates the execution of a core processing script.
    *   **Complexity Tier:** Not available in metadata; inferred as "Medium" due to orchestration logic, environment sourcing, and error handling.
    *   **Automation Bucket:** Not available in metadata; inferred as "B2 (Semi-Automated)" due to the need for manual translation of shell-specific constructs and the integration of a core script whose complexity is unknown.

*   **Invoked Core Script:** `k_ausd_v_ta_vvl_dwh.ksh` (path: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh`)
    *   **Technology:** KornShell Script (inferred)
    *   **Purpose:** Contains the core logic for contract data reconciliation for the `ta_vvl_dwh` table.
    *   **Note:** The exact content, complexity, and automation bucket for this core script are not directly available and will require further analysis.

*   **Utility Scripts / Configuration:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
    *   **Note:** These are external shell scripts that provide common functions (logging, error handling, date manipulation) used by the main script. Their migration will involve creating equivalent BigQuery functions, stored procedures, or audit/logging tables.

## 3. Target Architecture
The target architecture will leverage Google BigQuery for data storage and processing, along with BigQuery Scripting/Stored Procedures for the migration of the shell script's logic.

*   **BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`):** This will be the primary migration target for `r_ausd_v_ta_vvl_dwh.ksh`. It will encapsulate the orchestration, parameter validation, logging, and error handling logic.
*   **BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_vvl_dwh`):** This will be the migration target for the core processing script `k_ausd_v_ta_vvl_dwh.ksh`. This procedure will contain the actual data reconciliation logic for the `ta_vvl_dwh` table.
*   **Logging and Audit Tables:**
    *   `project.dataset.dw_job_registry`: To store job execution metadata (start/end times, status, script name, etc.), replacing the function of the `DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` calls and implicitly managed log files.
    *   `project.dataset.dw_job_log`: To store detailed log messages, replacing direct `print` and `tee -a $LogDatei` operations.
    *   `project.dataset.dw_error_log`: To store error details, replacing the `DWMSG_MeldeFehler` calls and error trapping mechanisms.
*   **Configuration Table (Optional):** A BigQuery table or external configuration (e.g., in a deployment pipeline) could replace the `$HOME/.dw_init` environment file for storing environment-specific variables like `BERT_DIR_ROOT`.
*   **Orchestration (Optional):** If `r_ausd_v_ta_vvl_dwh.ksh` is part of a larger workflow or requires scheduled execution, Cloud Composer (Apache Airflow on GCP) could be used to schedule and trigger the BigQuery stored procedure.

## 4. Data Flow & Lineage
The original shell script orchestrates the execution of a core processing script which performs data operations. The migrated flow will maintain this separation of concerns.

*   **External Trigger:** The BigQuery stored procedure `project.dataset.Vertragsdatenabgleich` will be invoked by a scheduler (e.g., Cloud Composer, BigQuery Scheduled Queries, or a custom mechanism).
*   **`project.dataset.Vertragsdatenabgleich` (Wrapper SP):**
    1.  **Initialization:** Declares variables for job name, version, system date, and other metadata.
    2.  **Parameter Validation:** Checks for required parameters (e.g., `-s`, `-l` if they become active) and handles the `-h` (help) option. Invalid parameters will log an error and raise an exception.
    3.  **Job Registry Update:** Records the job's start in `project.dataset.dw_job_registry`, assigning a unique entry number.
    4.  **Core Logic Invocation:** Calls the `project.dataset.k_ausd_v_ta_vvl_dwh` stored procedure, passing necessary parameters (e.g., `JobKennung`, `DW_EintragsNr`).
    5.  **Logging:** Writes messages to `project.dataset.dw_job_log` throughout execution.
    6.  **Error Handling:** Implements `EXCEPTION WHEN ERROR` blocks to catch and log errors in `project.dataset.dw_error_log`, then updates `dw_job_registry` with a 'FAILED' status.
    7.  **Success Handling:** If successful, logs a completion message and updates `dw_job_registry` with an 'OK' status.
*   **`project.dataset.k_ausd_v_ta_vvl_dwh` (Core Logic SP):**
    1.  Receives job context (e.g., `JobKennung`, `DW_EintragsNr`) from the wrapper SP.
    2.  Performs the actual data reconciliation operations for the `ta_vvl_dwh` table (READS from and WRITES to `ta_vvl_dwh` or related staging tables, potentially from other source systems).
    3.  Returns control to the wrapper SP upon completion or error.
*   **Data Target:** The `ta_vvl_dwh` table in BigQuery. The core stored procedure is expected to read from and write to this table, or a set of related tables involved in the reconciliation.

## 5. Transformation Logic
The transformation will involve converting shell scripting constructs into BigQuery SQL scripting and stored procedures.

*   **Environment Sourcing (`. $HOME/.dw_init`):** Replaced by BigQuery stored procedure parameters, BigQuery session variables, or a dedicated BigQuery configuration table. Essential environment variables (e.g., `BERT_DIR_ROOT`) would be passed as parameters or retrieved from configuration.
*   **Parameter Parsing (`getopts`):** Replaced by input parameters to the BigQuery stored procedure `project.dataset.Vertragsdatenabgleich`.
*   **Error Handling (`set -eu`, `trap`, `DWMSG_MeldeFehler`):** Migrated to BigQuery SQL scripting's `BEGIN...EXCEPTION WHEN ERROR...END` blocks for error trapping. Custom error logging will involve `INSERT` statements into the `dw_error_log` table. `SIGNAL SQLSTATE` will be used to raise errors.
*   **Logging (`print`, `tee`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`):** Replaced by `INSERT` statements into the `dw_job_registry` and `dw_job_log` tables. Log file names will be generated dynamically as in the original script but stored as a field in the registry.
*   **Script Invocation (`${Name_Kernskript}`):** The invocation of `k_ausd_v_ta_vvl_dwh.ksh` will be translated into a `CALL` statement to a corresponding BigQuery stored procedure: `CALL project.dataset.k_ausd_v_ta_vvl_dwh(JobKennung, DW_EintragsNr)`.
*   **Date Formatting (`date +%d%m%Y`):** Replaced by BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Core Logic:** The business logic within `k_ausd_v_ta_vvl_dwh.ksh` will need to be translated from KornShell/SQL embedded within it into native BigQuery SQL, potentially using temporary tables, DML operations, and BigQuery functions.

## 6. External Dependencies
The original script references several external shell scripts for common utilities and an environment initialization file.

*   **`$HOME/.dw_init`:** This environment setup file contains variables like `BERT_DIR_ROOT`. In BigQuery, this will be replaced by:
    *   **Migration Strategy:** Directly hardcoding necessary paths as constants within the BigQuery stored procedure (if they are static).
    *   **Migration Strategy:** Passing values as parameters to the stored procedure.
    *   **Migration Strategy:** Storing configuration in a BigQuery table (e.g., `project.dataset.config_params`) and querying it at the start of the stored procedure.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These provide logging, error handling, and parameter/date functionalities.
    *   **Migration Strategy:** Their functionalities will be absorbed into the main BigQuery stored procedure using BigQuery scripting features (e.g., `BEGIN...EXCEPTION`, `INSERT` into logging tables, `FORMAT_DATE`).
    *   **Migration Strategy:** For more complex, reusable logic, these could be migrated into separate, smaller BigQuery stored procedures or user-defined functions (UDFs).
*   **External Systems:** The `lineage_assembled_jobs` record indicated no external systems (e.g., Oracle, SFTP, S3) are directly involved in this specific job at the orchestration level. The core script `k_ausd_v_ta_vvl_dwh.ksh` is assumed to handle any potential database interactions, which would need further analysis for its specific data sources.

## 7. Unresolved / Risks
*   **Core Script `k_ausd_v_ta_vvl_dwh.ksh`:** The content and complexity of the core processing script are unknown. This is the biggest unresolved item. Its migration will be a separate, critical sub-task. The design assumes it can be fully translated into a BigQuery stored procedure.
*   **Specifics of `ta_vvl_dwh` reconciliation:** The details of *how* the contract data is reconciled are contained within `k_ausd_v_ta_vvl_dwh.ksh`. Without its analysis, the precise SQL for data transformations cannot be defined.
*   **Error Code Mapping:** The original script uses specific error codes (192, 193). These would need to be mapped to BigQuery error states or a custom error code system within the `dw_error_log` table.
*   **Idempotency and Restartability:** The BigQuery stored procedures should be designed with idempotency and restartability in mind, especially for data reconciliation tasks. The existing `DW_EintragsNr` and status tracking provide a good basis for this.
*   **Performance Tuning:** Once the core logic is migrated to BigQuery SQL, performance tuning will be essential, especially for large volumes of contract data.

## 8. Build Plan
The migration will follow a staged approach:

1.  **Define BigQuery Schema for Logging and Audit:**
    *   Create `project.dataset.dw_job_registry` table.
    *   Create `project.dataset.dw_job_log` table.
    *   Create `project.dataset.dw_error_log` table.
    *   *Language:* BigQuery DDL.
2.  **Analyze and Design Core Script `k_ausd_v_ta_vvl_dwh.ksh`:**
    *   Read `k_ausd_v_ta_vvl_dwh.ksh` source code.
    *   Perform static analysis to identify data sources, transformations, and target tables.
    *   Design `project.dataset.k_ausd_v_ta_vvl_dwh` BigQuery stored procedure.
    *   *Language:* Manual analysis, BigQuery SQL.
3.  **Develop `project.dataset.k_ausd_v_ta_vvl_dwh` Stored Procedure:**
    *   Translate the core reconciliation logic into BigQuery SQL.
    *   Integrate logging into `dw_job_log` and error handling into `dw_error_log`.
    *   *Language:* BigQuery SQL (Stored Procedure).
4.  **Develop `project.dataset.Vertragsdatenabgleich` Stored Procedure:**
    *   Translate the wrapper script `r_ausd_v_ta_vvl_dwh.ksh` into BigQuery SQL stored procedure.
    *   Implement parameter handling, environment variable equivalents, and the job execution framework.
    *   Integrate `CALL project.dataset.k_ausd_v_ta_vvl_dwh` for the core logic.
    *   Integrate logging (`dw_job_registry`, `dw_job_log`) and error handling (`dw_error_log`).
    *   *Language:* BigQuery SQL (Stored Procedure).
5.  **Testing:**
    *   Unit test `project.dataset.k_ausd_v_ta_vvl_dwh`.
    *   Unit test `project.dataset.Vertragsdatenabgleich` with various parameters (valid, invalid, help).
    *   End-to-end testing of the entire job.
    *   *Language:* BigQuery SQL (Test scripts).
6.  **Deployment and Orchestration:**
    *   Deploy the BigQuery stored procedures.
    *   Configure a scheduler (e.g., Cloud Composer DAG) to invoke `project.dataset.Vertragsdatenabgleich`.
    *   *Language:* BigQuery DDL/DML, Python (for Airflow DAG).