# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh

## 1. Purpose & Scope

This document outlines the migration design for the KornShell script `k_ausd_v_ta_vvl_upgrade.ksh` to Google Cloud Platform, targeting BigQuery for data processing and orchestration.

The original script functions as a control and orchestration wrapper for a SQL script, `d_ausd_v_ta_vvl_upgrade.sql`. Its primary responsibilities include:
*   Parsing and validating command-line parameters (`JobKennung`, `EintragsNr`).
*   Sourcing shared utility scripts for error handling, date functions, parameter parsing, and SQL*Plus interaction.
*   Setting a target table name (`ta_vvl_upgrade`).
*   Executing the core SQL script via a helper function (`starteSQLSkript`).
*   Capturing the number of processed records from a temporary file.
*   Implementing custom error handling and logging.

The scope of this migration is to re-implement the orchestration and control logic of this KornShell script in BigQuery, integrating seamlessly with a separately migrated BigQuery SQL version of `d_ausd_v_ta_vvl_upgrade.sql`.

## 2. Source Inventory

The job consists of a single KornShell script acting as an orchestrator.

| File Path                                                                   | Technology | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                           |
| :-------------------------------------------------------------------------- | :--------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh` | KornShell  | medium | semi_auto         | KornShell control script for `r_ausd_vertrag.ksh`, handling job execution, parameter parsing, error handling, and calling a SQL script (`d_ausd_v_ta_vvl_upgrade.sql`) to update the `ta_vvl_upgrade` table. It loads environment variables, sources helper scripts for error handling, date operations, parameter processing, and SQL*Plus invocation. It validates inputs and executes the main SQL script. |

## 3. Target Architecture

The migrated solution will primarily leverage BigQuery stored procedures and tables.

*   **BigQuery Stored Procedure:** The core orchestration logic of `k_ausd_v_ta_vvl_upgrade.ksh` will be re-implemented as a BigQuery stored procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This procedure will handle parameter input, validation, calling the migrated SQL logic, and result logging.
*   **BigQuery Data Tables:**
    *   `project.dataset.ta_vvl_upgrade`: The target table that the `d_ausd_v_ta_vvl_upgrade.sql` script (once migrated) will update.
    *   `project.dataset.job_error_log`: A dedicated logging table to capture error messages and job execution details, replacing the custom shell error framework.
    *   `project.dataset.job_table`: A table for job registration and status tracking.
    *   `project.dataset.job_result_log`: A table to store the number of processed records and job completion details, replacing the temporary file mechanism.
*   **External Orchestration (Optional):** If the original job was part of a larger workflow (e.g., scheduled by UC4), an external orchestration tool like Cloud Composer (Airflow) or Cloud Workflows might be used to invoke the BigQuery stored procedure.

## 4. Data Flow & Lineage

The current data flow is:

1.  **Input Parameters:** `p_JobKennung` and `p_EintragsNr` are passed to `k_ausd_v_ta_vvl_upgrade.ksh`.
2.  **Shell Script Execution:** The script initializes environment variables, sources utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Validation:** Parameters are validated using `pruefeParameterGesetzt`.
4.  **SQL Script Invocation:** The script calls `starteSQLSkript` which in turn executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_vvl_upgrade.sql`.
5.  **Database Interaction:** The `d_ausd_v_ta_vvl_upgrade.sql` script performs operations (presumably updates) on the `ta_vvl_upgrade` table.
6.  **Record Count Capture:** The number of processed records is written to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`), which is then read back into the `v_records` variable.

**Migrated Data Flow:**

1.  **Input Parameters:** `p_JobKennung` and `p_EintragsNr` will be passed as arguments to the BigQuery stored procedure `r_ausd_vertrag_control`.
2.  **Stored Procedure Execution:** The stored procedure will perform parameter validation.
3.  **Job Registration/Logging:** The procedure will interact with `project.dataset.job_table` and `project.dataset.job_error_log` for tracking and error reporting.
4.  **Core SQL Logic:** The migrated SQL logic from `d_ausd_v_ta_vvl_upgrade.sql` will be integrated directly into the stored procedure or called as a separate stored procedure/query, targeting `project.dataset.ta_vvl_upgrade`.
5.  **Record Count:** The record count will be captured directly within the BigQuery SQL using `SELECT COUNT(*)` or returned as an `OUT` parameter, then logged to `project.dataset.job_result_log`.

## 5. Transformation Logic

The `k_ausd_v_ta_vvl_upgrade.ksh` script primarily acts as an orchestrator; it contains no direct data transformation logic. Its logic focuses on:

*   **Environment Setup:** Sourcing `.dw_init` and utility scripts. In BigQuery, this will be handled by explicit declarations or configuration.
*   **Parameter Handling:** Using `getopts` for command-line arguments. In BigQuery, this translates to stored procedure input parameters.
*   **Validation:** Checking for mandatory parameters. In BigQuery, this becomes `IF` statements within the stored procedure.
*   **Error Management:** A custom shell error framework (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`). In BigQuery, this will be replaced by standard SQL error handling (`SIGNAL SQLSTATE`) and logging to a dedicated error table.
*   **External Script Execution:** Calling `d_ausd_v_ta_vvl_upgrade.sql` via `starteSQLSkript`. The logic of `d_ausd_v_ta_vvl_upgrade.sql` itself is the main transformation, which needs to be migrated to BigQuery SQL as a separate component or integrated into the main procedure.
*   **Result Capture:** Reading `v_records` from a temporary file. In BigQuery, this will be achieved by direct SQL queries (e.g., `SELECT COUNT(*)`) and storing the result in a logging table.

## 6. External Dependencies

The original script has the following external dependencies:

*   **Environment Files:** `$HOME/.dw_init`
    *   **Replacement:** Configuration values (project ID, dataset name, root directories) will be explicitly defined during deployment or passed as parameters to the BigQuery stored procedure.
*   **Utility KornShell Scripts:**
    *   `f_alis_msgerr.ksh`: Error messaging.
    *   `h_alis_date.ksh`: Date handling.
    *   `h_alis_parameter.ksh`: Parameter parsing.
    *   `h_alis_sqlplus.ksh`: SQL*Plus interaction.
    *   **Replacement:** These functions will be re-implemented directly using BigQuery SQL scripting constructs (e.g., `IF`, `DECLARE`, `CURRENT_TIMESTAMP()`, `FORMAT()`) and integrated into the BigQuery stored procedure. The custom error handling will be replaced by logging to `job_error_log`.
*   **SQL Script:** `d_ausd_v_ta_vvl_upgrade.sql`
    *   **Replacement:** This critical component will be fully migrated to BigQuery SQL. Its logic will either be inlined into the main orchestration stored procedure or created as a separate BigQuery stored procedure, called by the orchestrator.
*   **Database Table:** `ta_vvl_upgrade`
    *   **Replacement:** This table will be migrated to BigQuery as `project.dataset.ta_vvl_upgrade`.
*   **Temporary Files:** `$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`
    *   **Replacement:** The ephemeral record count storage will be replaced by directly capturing results within BigQuery SQL and persisting them to a dedicated logging table (`job_result_log`).

## 7. Unresolved / Risks

*   **Implicit Dependencies:** While `references_out` provided some clarity, the `lineage_edges` query for this job was empty. This suggests that some dependencies (especially sourcing of utility `.ksh` files) might not be fully captured by automated lineage, requiring careful manual verification of included scripts.
*   **`d_ausd_v_ta_vvl_upgrade.sql` Migration:** The core business logic resides in this SQL script. Its complexity and specific SQL dialect (likely Oracle PL/SQL, given the context) will determine the effort required for its BigQuery migration, which is a prerequisite for a complete migration of this job.
*   **Custom Error Framework:** The custom shell error handling needs a complete redesign into BigQuery's error handling and logging mechanisms. This includes understanding all error codes and messages for accurate translation.
*   **Parameter Mapping:** The exact mapping and data types of `p_JobKennung` and `p_EintragsNr` in BigQuery need to be confirmed based on their usage in `d_ausd_v_ta_vvl_upgrade.sql`.
*   **"Deactivate Old Active Jobs" Logic:** The comment indicates a job management functionality ("alte aktive Jobs werden einfach dekativiert"). This logic needs to be explicitly identified in the `d_ausd_v_ta_vvl_upgrade.sql` or related components and integrated into the BigQuery solution.

## 8. Build Plan

The migration will follow these steps:

1.  **DDL Generation for BigQuery Tables:**
    *   `project.dataset.ta_vvl_upgrade`: Create the target data table based on the schema of the original `ta_vvl_upgrade`.
    *   `project.dataset.job_table`: Define schema for job registration (e.g., `job_kennung`, `eintrags_nr`, `tab_name`, `status`, `created_ts`).
    *   `project.dataset.job_error_log`: Define schema for error logging (e.g., `error_ts`, `procedure_name`, `error_code`, `error_arg`, `job_kennung`, `eintrags_nr`).
    *   `project.dataset.job_result_log`: Define schema for result logging (e.g., `job_kennung`, `eintrags_nr`, `tab_name`, `records_processed`, `finished_ts`).
2.  **Migrate Core SQL Script:**
    *   Analyze and convert `d_ausd_v_ta_vvl_upgrade.sql` to BigQuery SQL syntax. This will likely result in a new BigQuery stored procedure (e.g., `project.dataset.d_ausd_v_ta_vvl_upgrade_proc`).
3.  **Develop BigQuery Orchestration Stored Procedure:**
    *   Create `project.dataset.r_ausd_vertrag_control` (or similar).
    *   Implement parameter input for `p_JobKennung` and `p_EintragsNr`.
    *   Incorporate parameter validation logic.
    *   Integrate error handling with `job_error_log`.
    *   Add job registration logic to `job_table`.
    *   Call the migrated `d_ausd_v_ta_vvl_upgrade_proc`.
    *   Capture and log `v_records` to `job_result_log`.
    *   Include success/completion messages.
4.  **Unit and Integration Testing:**
    *   Test the individual BigQuery stored procedures.
    *   Test the end-to-end flow of the orchestration procedure with various inputs (valid, invalid).
    *   Verify data correctness in `ta_vvl_upgrade` and accuracy of logging tables.
5.  **Deployment and Scheduling:**
    *   Deploy BigQuery DDL and stored procedures.
    *   Configure a scheduler (e.g., Cloud Composer, Cloud Scheduler) to invoke `project.dataset.r_ausd_vertrag_control` with appropriate parameters.