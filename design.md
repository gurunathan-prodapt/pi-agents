# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_inv_acc.ksh` to Google BigQuery. The script serves as an orchestration wrapper for a contract data reconciliation job specifically targeting the `ta_inv_acc` table. Its primary functions include:
- Initializing the runtime environment by sourcing configuration files.
- Parsing command-line arguments.
- Implementing a custom logging and error handling framework (`DWMSG_*`).
- Invoking a core kernel script, `k_ausd_v_ta_inv_acc.ksh`, responsible for the actual reconciliation logic.
- Managing job status and logging its execution details.
The scope of this migration focuses on transforming the wrapper's orchestration, parameter handling, and logging functionalities into BigQuery-native constructs. The core reconciliation logic within `k_ausd_v_ta_inv_acc.ksh` is assumed to be migrated separately.

## 2. Source Inventory
| File Path                                                             | Technology | Tier   | Automation Bucket |
| :-------------------------------------------------------------------- | :--------- | :----- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh` | KornShell  | medium | semi_auto         |

The script is a KornShell script of medium complexity, categorized for semi-automated migration. No specific complexity signals or migration flags were identified.

## 3. Target Architecture
The KornShell wrapper script will be migrated to a BigQuery Stored Procedure. This procedure will encapsulate the orchestration, parameter validation, and logging functionalities.

- **BigQuery Stored Procedure:** `project.dataset.sp_vertragsdatenabgleich` will replace the `r_ausd_v_ta_inv_acc.ksh` script. It will accept parameters equivalent to the original script's command-line arguments and environment variables.
- **Logging Tables:** Custom logging and error handling (`DWMSG_*` functions) will be replaced by dedicated BigQuery tables, e.g., `project.dataset.dw_job_entries`, `project.dataset.dw_error_log`, and `project.dataset.dw_job_status`.
- **Configuration:** Environment variables and sourced configuration (`.dw_init`, utility scripts) will be replaced by BigQuery stored procedure parameters, BigQuery scripting variables, or dedicated configuration tables.
- **Core Logic Invocation:** The invocation of `k_ausd_v_ta_inv_acc.ksh` will be replaced by a `CALL` statement to a separate BigQuery stored procedure (e.g., `project.dataset.sp_k_ausd_v_ta_inv_acc`), which will host the migrated core reconciliation logic.

## 4. Data Flow & Lineage
The original script is primarily a control flow and orchestration mechanism and does not directly process business data.
- **Source:** The script itself (`r_ausd_v_ta_inv_acc.ksh`)
- **Transforms:** No direct data transformations occur within this wrapper. Its transformations are limited to:
    - Uppercasing a job identifier (`JobKennung`).
    - Formatting the current date.
- **Targets:**
    - Log files (will be migrated to BigQuery logging tables).
    - Job status updates (will be migrated to BigQuery status tables).
    - Invocation of the core script (`k_ausd_v_ta_inv_acc.ksh`), which is expected to perform the actual data processing and produce output data.

**Execution Order:**
1.  BigQuery Stored Procedure `project.dataset.sp_vertragsdatenabgleich` is invoked.
2.  Parameters are validated.
3.  Job entry is created in `project.dataset.dw_job_entries`.
4.  The core reconciliation logic is executed via `CALL project.dataset.sp_k_ausd_v_ta_inv_acc(...)`.
5.  On successful completion, job status is updated in `project.dataset.dw_job_status` and `project.dataset.dw_job_entries`.
6.  On error, an entry is made in `project.dataset.dw_error_log`, and job status is updated.

## 5. Transformation Logic
The transformation logic for this wrapper script is primarily concerned with migrating shell script constructs to BigQuery SQL:

-   **Parameter Handling:** The `getopts` mechanism for command-line arguments will be replaced by named parameters in the BigQuery stored procedure. Validation logic will use `IF` statements.
-   **Environment Variables:** Shell environment variables like `BERT_DIR_ROOT`, `HOME`, `LogDatei`, `JobKennung` will be converted to stored procedure parameters, scripting variables, or values retrieved from a configuration table.
-   **Functions:**
    -   The `usage()` function will be replaced by documentation comments within the stored procedure and potentially a `SELECT` statement to return help text if a specific help flag is passed.
    -   The custom `DWMSG_*` logging and error functions will be reimplemented as separate BigQuery stored procedures or directly as `INSERT`/`UPDATE` statements on the logging tables.
-   **Date Formatting:** Shell `date +%d%m%Y` will map to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
-   **Conditional Logic:** `if [ ! $ErrNr -eq 0 ]` and `case $param in ...)` constructs will be directly translated to BigQuery SQL `IF...THEN...ELSEIF...END IF` statements.
-   **Error Trapping:** The `trap INT/ERR` mechanism will be replaced by BigQuery's `EXCEPTION WHEN ERROR THEN ... END` blocks for robust error handling.
-   **File Operations:** Log file writes (e.g., `>> $LogDatei 2>&1`, `tee -a $LogDatei`) will be replaced by `INSERT` statements into BigQuery logging tables.

## 6. External Dependencies
The `lineage_assembled_jobs` analysis indicated no explicit external systems (like Oracle, SFTP, S3) referenced by this job. However, the script has implicit dependencies:

-   **Sourced Environment/Utility Scripts:**
    -   `$HOME/.dw_init`: This environment initialization script will need to be analyzed to extract relevant variables and configurations. These will be passed as parameters to the BigQuery stored procedure or stored in a BigQuery configuration table.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: These utility scripts and the `DWMSG_*` functions they provide will be reimplemented as BigQuery stored procedures or SQL functions that interact with the BigQuery logging and status tables.
-   **Invoked Core Script:**
    -   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh`: This is the most significant dependency. This script needs to be migrated independently, likely into its own BigQuery stored procedure (`project.dataset.sp_k_ausd_v_ta_inv_acc`) or a series of SQL queries orchestrated by Dataform or Airflow. The current wrapper will then `CALL` this new BigQuery component.

## 7. Unresolved / Risks
-   **Semi-Automated Migration:** The `semi_auto` classification suggests that some manual effort will be required. This likely relates to the custom `DWMSG_*` framework and the dependency on the core `k_ausd_v_ta_inv_acc.ksh` script.
-   **Core Script Migration:** The migration of `k_ausd_v_ta_inv_acc.ksh` is a prerequisite and a potential risk if its internal logic is complex or relies on non-BigQuery compatible functionalities (e.g., file system manipulation, direct OS calls). This design assumes `k_ausd_v_ta_inv_acc.ksh` can be migrated to BigQuery SQL or an equivalent Python-based data processing job if required.
-   **Environmental Context:** The precise contents and implications of `$HOME/.dw_init` and other sourced utility scripts are not fully known without deeper analysis. These may introduce subtle dependencies that need careful handling during migration to BigQuery.
-   **Error Handling Fidelity:** While BigQuery `EXCEPTION` blocks provide robust error handling, the exact behavior of `trap INT/ERR` in KornShell, especially concerning process interruption signals, might have nuances not perfectly replicated.

## 8. Build Plan
The migration will proceed in the following ordered steps:

1.  **Define Logging and Status Tables:**
    *   Create BigQuery tables: `project.dataset.dw_job_entries` (to store job execution details), `project.dataset.dw_error_log` (for detailed error messages), and `project.dataset.dw_job_status` (for overall job status).
    *   **Language:** BigQuery DDL (SQL)

2.  **Migrate DWMSG Functions:**
    *   Create BigQuery stored procedures or functions (e.g., `sp_dwmsg_ermittle_nr`, `sp_dwmsg_logdateiname`, `sp_dwmsg_erzeuge_eintrag`, `sp_dwmsg_setze_stichtag_info`, `sp_dwmsg_meldefehler`, `sp_dwmsg_fehlerbehandlung`, `sp_dwmsg_setze_status_ok`) that interact with the newly defined logging and status tables.
    *   **Language:** BigQuery SQL

3.  **Migrate Configuration/Environment:**
    *   Identify critical variables from `$HOME/.dw_init` and related scripts. Define these as parameters for the main stored procedure or create a BigQuery configuration table to store them.
    *   **Language:** BigQuery DDL (SQL) for tables, BigQuery SQL for procedure parameters.

4.  **Create Core Script Stored Procedure (Placeholder):**
    *   Create a placeholder BigQuery stored procedure, `project.dataset.sp_k_ausd_v_ta_inv_acc`, with input parameters that mirror the arguments passed to the original `k_ausd_v_ta_inv_acc.ksh` script. This procedure will eventually contain the migrated core business logic.
    *   **Language:** BigQuery SQL

5.  **Develop Main Orchestration Stored Procedure:**
    *   Create the `project.dataset.sp_vertragsdatenabgleich` stored procedure based on the provided BigQuery Pseudocode.
    *   Implement parameter validation.
    *   Integrate calls to the new `DWMSG` BigQuery procedures for logging and status updates.
    *   Include the `CALL` statement to `project.dataset.sp_k_ausd_v_ta_inv_acc`.
    *   Implement error handling using `BEGIN...EXCEPTION WHEN ERROR THEN...END`.
    *   **Language:** BigQuery SQL

6.  **Migrate `k_ausd_v_ta_inv_acc.ksh`:**
    *   This step involves a separate, detailed migration effort for the core reconciliation script, likely transforming it into a complex BigQuery stored procedure or a Dataform workflow.
    *   **Language:** BigQuery SQL (or Python/Dataform if required by the core script's complexity).

7.  **Testing:**
    *   Thoroughly test the `sp_vertragsdatenabgleich` procedure with various parameter combinations, including error scenarios.
    *   Verify correct logging and status updates in the BigQuery tables.
    *   Test the integration with the (initially placeholder) `sp_k_ausd_v_ta_inv_acc`.
    *   **Language:** BigQuery SQL (for test scripts)

This plan ensures that the orchestration layer is migrated first, establishing the necessary infrastructure (logging, error handling, parameter management) before tackling the potentially more complex core business logic.