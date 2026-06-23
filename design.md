# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

## 1. Purpose & Scope

This document outlines the migration design for the ETL job identified by `job_id: 6d73ee79`, whose seed name is `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`.

The original KornShell script, `r_ausd_v_ta_vertrag_tmp.ksh`, serves as a wrapper for the "Vertragsdatenabgleich" (contract data reconciliation) process, specifically targeting the `ta_vertrag_tmp` table. Its primary functions are:
*   **Orchestration:** Setting up the environment, handling parameters, and invoking a core processing script (`k_ausd_v_ta_vertrag_tmp.ksh`).
*   **Logging and Error Handling:** Implementing a robust error trapping mechanism and maintaining a job log for status and progress.

The scope of this migration design specifically addresses the wrapper script (`r_ausd_v_ta_vertrag_tmp.ksh`) and its direct interactions, preparing it for execution on the BigQuery platform. The detailed logic of the core script (`k_ausd_v_ta_vertrag_tmp.ksh`) will be addressed in a subsequent, more detailed design document, although its integration points are considered here.

## 2. Source Inventory

The job consists of a single primary component file:

| File Path                                                                   | Technology | Tier          | Automation Bucket | Summary                                                                                                                                                                                                                                                                                             |
| :-------------------------------------------------------------------------- | :--------- | :------------ | :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh` | KornShell  | Medium (Assumed) | Semi-Auto         | This KornShell script serves as a wrapper for the 'Vertragsdatenabgleich' (contract data reconciliation) process, specifically for the 'ta_vertrag_tmp' table. It handles parameter parsing, environment setup, error trapping, and then orchestrates the execution of a core processing script. |

*Note: The complexity tier for this file was not explicitly found in `file_complexity` table. Based on the `lineage_assembled_jobs.purpose_note` stating "stage dist: medium=1", it is assumed to be `Medium` complexity.*

## 3. Target Architecture

The target architecture for this job on BigQuery will leverage BigQuery Stored Procedures for orchestration and logic execution, and dedicated BigQuery tables for logging and status management.

*   **Wrapper Script Migration:** The `r_ausd_v_ta_vertrag_tmp.ksh` wrapper script will be migrated to a BigQuery Stored Procedure, e.g., `project.dataset.sp_vertragsdatenabgleich`. This procedure will handle parameter parsing, environment setup, error handling, and the invocation of the core logic.
*   **Core Script Integration:** The `k_ausd_v_ta_vertrag_tmp.ksh` core script will be migrated to its own BigQuery Stored Procedure, e.g., `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`. The wrapper SP will call this core SP.
*   **Logging and Monitoring:** Dedicated BigQuery tables will replace the file-based logging system:
    *   `project.dataset.job_registry`: To register job metadata.
    *   `project.dataset.job_audit_log`: For detailed audit trail messages.
    *   `project.dataset.job_status`: To track the overall status (OK/ERR) of job runs.
*   **Data Storage:** The `ta_vertrag_tmp` table will be migrated to a native BigQuery table, e.g., `project.dataset.ta_vertrag_tmp`.

This approach ensures a cloud-native solution that aligns with BigQuery best practices for ETL orchestration and data processing.

## 4. Data Flow & Lineage

The current job `6d73ee79` acts primarily as an orchestrator. The data flow can be summarized as follows:

1.  **Initialization:** The `sp_vertragsdatenabgleich` procedure starts, initializes program metadata, and parses any input parameters.
2.  **Environment Setup:** Configuration values and utility functions (previously sourced `.ksh` files like `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are replaced by BigQuery stored procedure variables, configuration tables, or equivalent BigQuery functions.
3.  **Logging & Status:** An entry is created in `project.dataset.job_registry`, and audit messages are inserted into `project.dataset.job_audit_log` at various stages (job start, parameter parsing, core script invocation).
4.  **Core Logic Invocation:** The `sp_vertragsdatenabgleich` procedure calls `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`. This core procedure is expected to perform the actual reconciliation logic, likely involving reading from and writing to `project.dataset.ta_vertrag_tmp`.
5.  **Error Handling:** `EXCEPTION` blocks in the BigQuery stored procedure capture runtime errors, updating `project.dataset.job_status` with error details and logging messages to `project.dataset.job_audit_log`.
6.  **Completion:** Upon successful completion of the core script, a success message is logged, and the job status in `project.dataset.job_status` is updated to 'OK'.

The main data interaction (read/write on `ta_vertrag_tmp`) is delegated to the `sp_k_ausd_v_ta_vertrag_tmp`, while `sp_vertragsdatenabgleich` provides the robust execution wrapper.

## 5. Transformation Logic

The migration of `r_ausd_v_ta_vertrag_tmp.ksh` to `project.dataset.sp_vertragsdatenabgleich` involves mapping shell script constructs to BigQuery SQL capabilities:

*   **Parameter Parsing:** The `getopts` command in the shell script will be replaced by `IN` parameters of the BigQuery stored procedure (e.g., `p_job_kennung STRING`, `p_run_date DATE`, `p_show_help BOOL`).
*   **Environment Variables:** Shell environment variables like `ProgName`, `ProgVersion`, `BERT_DIR_ROOT`, `JobKennung`, `v_sysdate` will be translated into `DECLARE` variables within the BigQuery stored procedure or fetched from configuration tables.
*   **File Sourcing:** The sourcing of utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be handled by either encapsulating their logic directly within the main stored procedure, or by creating separate utility functions/procedures that are called.
*   **Logging:** `print` statements and redirection to `$LogDatei` will be replaced by `INSERT` statements into BigQuery logging tables (`project.dataset.job_audit_log`).
*   **Error Handling (Traps):** The `trap` commands in the shell script will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to manage runtime errors. Custom error messages (`DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`) will map to specific inserts into `project.dataset.job_audit_log` and `project.dataset.job_status`.
*   **Date Operations:** `date +%d%m%Y` will be converted to BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Script Invocation:** The execution of `${Name_Kernskript}` will be replaced by a `CALL` statement to the `project.dataset.sp_k_ausd_v_ta_vertrag_tmp` stored procedure, passing necessary parameters (`v_job_kennung`, `v_eintragsnr`).
*   **Output Messages:** The final "Die Abarbeitung wurde ohne erkennbare Fehler beendet" message will be logged to `project.dataset.job_audit_log` and the job status table.

The core reconciliation logic for `ta_vertrag_tmp` is not directly transformed here, as it resides in the `k_ausd_v_ta_vertrag_tmp.ksh` script, which requires its own migration design.

## 6. External Dependencies

The original KornShell script has several external dependencies:

*   **Shell Utilities:**
    *   `$HOME/.dw_init`: Initializes environment variables.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility.
    *   **Migration:** These shell utilities will be replaced by native BigQuery SQL constructs: `DECLARE` variables, BigQuery functions (e.g., for date formatting), or their logic embedded directly into the BigQuery stored procedure. Configuration values from `.dw_init` will be managed as procedure parameters or looked up from BigQuery configuration tables.
*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`: The primary script containing the business logic for `ta_vertrag_tmp`.
    *   **Migration:** This will be migrated to a separate BigQuery stored procedure, `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`, which will then be called by `sp_vertragsdatenabgleich`.
*   **Messaging/Error Functions:**
    *   `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`: These are custom messaging and error handling functions.
    *   **Migration:** These functions will be replaced by `INSERT` statements into the `project.dataset.job_audit_log` and `project.dataset.job_status` tables, ensuring that all log entries and status updates are persisted within BigQuery.
*   **Data Table:**
    *   `ta_vertrag_tmp`: The target table for the contract data reconciliation process.
    *   **Migration:** This table will be migrated to `project.dataset.ta_vertrag_tmp` in BigQuery.

There are no explicit external systems like Oracle, SFTP, or S3 identified as direct dependencies of this wrapper script.

## 7. Unresolved / Risks

*   **Core Script Logic:** The most significant unresolved item is the detailed transformation logic within `k_ausd_v_ta_vertrag_tmp.ksh`. This script is critical for the actual data reconciliation of `ta_vertrag_tmp`. Its content and complexity must be analyzed to determine the appropriate BigQuery migration strategy (e.g., BigQuery SQL, Dataflow, PySpark).
*   **"DWMSG" Functionality:** While generally understood as logging/error handling, if any of the `DWMSG_...` functions have complex side effects or interactions beyond simple logging (e.g., triggering external alerts via non-standard channels), these details would need further investigation and specific migration plans.
*   **Input Parameters (`-s`, `-l`):** The wrapper script's `getopts` handles `-s` and `-l` but the provided code snippet does not explicitly process them. It's assumed they are passed to the core script. This behavior needs to be confirmed during the core script's analysis to ensure proper parameter propagation in the BigQuery migration.
*   **Complexity Tier:** The complexity tier for `r_ausd_v_ta_vertrag_tmp.ksh` was assumed to be "Medium" due to missing data in `file_complexity`. If a subsequent manual review or more detailed analysis indicates higher complexity, the effort estimate and migration strategy might need adjustment.

## 8. Build Plan

The following steps outline the build plan for migrating this job to BigQuery:

1.  **Create BigQuery Dataset:**
    *   **Action:** Create a dedicated BigQuery dataset (e.g., `project.dataset`) to house the migrated tables and stored procedures.
    *   **Language:** DDL (BigQuery SQL)

2.  **Migrate `ta_vertrag_tmp` Table:**
    *   **Action:** Migrate the `ta_vertrag_tmp` table schema and data to `project.dataset.ta_vertrag_tmp`. This might involve schema translation and initial data loading.
    *   **Language:** DDL, DML (BigQuery SQL)

3.  **Implement Logging and Status Tables:**
    *   **Action:** Create the `project.dataset.job_registry`, `project.dataset.job_audit_log`, and `project.dataset.job_status` tables as outlined in the target architecture.
    *   **Language:** DDL (BigQuery SQL)

4.  **Develop `sp_vertragsdatenabgleich` (Wrapper SP):**
    *   **Action:** Translate the `r_ausd_v_ta_vertrag_tmp.ksh` script into a BigQuery stored procedure `project.dataset.sp_vertragsdatenabgleich`. This includes parameter handling, internal variable declarations, BigQuery logging calls, and error handling logic.
    *   **Language:** BigQuery SQL

5.  **Design and Develop `sp_k_ausd_v_ta_vertrag_tmp` (Core SP):**
    *   **Action:** Conduct a separate detailed design and then develop the `k_ausd_v_ta_vertrag_tmp.ksh` script's logic into `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`. This will include all data transformation and reconciliation logic for `ta_vertrag_tmp`.
    *   **Language:** Primarily BigQuery SQL; potentially Python for Dataflow/Dataproc if the logic is non-SQL compatible.

6.  **Integrate and Test:**
    *   **Action:** Ensure `sp_vertragsdatenabgleich` correctly calls `sp_k_ausd_v_ta_vertrag_tmp` and that logging and error handling function as expected. Perform unit and integration testing.
    *   **Language:** BigQuery SQL, Testing Framework (e.g., Python with `pytest` for BigQuery interactions)

7.  **Update Orchestration:**
    *   **Action:** Modify the existing scheduler (e.g., move from a legacy scheduler to Cloud Composer or Cloud Workflows) to invoke the new `project.dataset.sp_vertragsdatenabgleich` BigQuery stored procedure.
    *   **Language:** Orchestration-specific configuration (e.g., Python for Airflow DAGs, YAML for Cloud Workflows).