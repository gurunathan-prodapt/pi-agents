```markdown
# MIGRATION_NOTES: r_ausd_v_ta_discount_rr.ksh

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_v_ta_discount_rr.ksh` from its legacy environment to Google Cloud's BigQuery platform. The original script acted as a wrapper for a core data reconciliation process, handling environment setup, parameter parsing, error logging, and status reporting.

The migration transforms this shell-based orchestration into a BigQuery-native solution, primarily utilizing BigQuery stored procedures for logic execution and BigQuery tables for persistent logging and configuration management. The core data reconciliation logic, originally residing in `k_ausd_v_ta_discount_rr.ksh`, is represented by a placeholder BigQuery stored procedure, awaiting its own detailed migration.

## 2. Generated Artifacts

The migration produced the following BigQuery DDL and SQL stored procedure files:

*   **`ddl/your_project.your_dataset.job_logging_table.sql`**
    *   **Role**: Defines the BigQuery table used for centralizing all job execution logs, including messages, timestamps, and job statuses. This replaces the original file-based logging.
*   **`ddl/your_project.your_dataset.configuration_table.sql`**
    *   **Role**: Defines the BigQuery table to store configuration parameters and environment variables (e.g., `BERT_DIR_ROOT`) that were previously sourced from shell scripts or hardcoded.
*   **`sprocs/your_project.your_dataset.DWMSG_ErmittleNr.sql`**
    *   **Role**: A BigQuery stored procedure that generates a unique sequential job entry number for logging purposes, mimicking a similar function in the original environment.
*   **`sprocs/your_project.your_dataset.DWMSG_Logdateiname.sql`**
    *   **Role**: A BigQuery stored procedure that conceptually determines a log file name. In the BigQuery context, this is primarily for record-keeping within the logging table, as actual logging is table-based.
*   **`sprocs/your_project.your_dataset.DWMSG_ErzeugeEintrag.sql`**
    *   **Role**: A BigQuery stored procedure responsible for inserting new log entries into the `job_logging_table`, encapsulating the common logging logic.
*   **`sprocs/your_project.your_dataset.DWMSG_SetzeStichtagInfo.sql`**
    *   **Role**: A BigQuery stored procedure to log information about the reference date (`Stichtag`) for the job.
*   **`sprocs/your_project.your_dataset.DWMSG_MeldeFehler.sql`**
    *   **Role**: A BigQuery stored procedure to log specific error messages, marking the job as `FAILED` in the log.
*   **`sprocs/your_project.your_dataset.DWMSG_Fehlerbehandlung.sql`**
    *   **Role**: A BigQuery stored procedure that centralizes error handling logic, logging detailed error information and updating the job status to `FAILED`. This replaces the `trap` and `DWMSG_Fehlerbehandlung` functions from the KornShell script.
*   **`sprocs/your_project.your_dataset.DWMSG_SetzeStatusOK.sql`**
    *   **Role**: A BigQuery stored procedure to log a success message and update the job's overall status to `SUCCESS`.
*   **`sprocs/your_project.your_dataset.k_ausd_v_ta_discount_rr.sql`**
    *   **Role**: A placeholder BigQuery stored procedure representing the migrated core data reconciliation logic from `k_ausd_v_ta_discount_rr.ksh`. This procedure requires separate, detailed migration.
*   **`sprocs/your_project.your_dataset.Vertragsdatenabgleich.sql`**
    *   **Role**: The main BigQuery orchestration stored procedure. This replaces the `r_ausd_v_ta_discount_rr.ksh` wrapper script, handling parameter parsing, calling helper logging/error procedures, and invoking the core logic.

## 3. Key Design Decisions

The migration strategy involved several key design decisions to translate shell script functionality into a BigQuery-native paradigm:

*   **Orchestration Shift**: The KornShell wrapper's role of orchestrating execution, environment setup, and error handling has been fully transitioned to a main BigQuery stored procedure (`Vertragsdatenabgleich`). This centralizes control within the BigQuery environment.
*   **Centralized Logging**: File-based logging (`>> $LogDatei 2>&1`) was replaced by a dedicated BigQuery `job_logging_table`. This provides structured, queryable, and scalable logging, leveraging BigQuery's strengths for data storage and analysis. Helper stored procedures (`DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`, etc.) encapsulate the logging logic.
*   **Structured Error Handling**: The shell's `trap` mechanism and custom `DWMSG_*` error functions are replaced by BigQuery's `BEGIN...EXCEPTION...END` blocks for robust error catching within SQL, combined with dedicated error logging procedures (`DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`). This provides more granular error reporting and recovery within the SQL context.
*   **Configuration Management**: Environment variables and sourced configuration files (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`) are replaced by a BigQuery `configuration_table` and `DECLARE` variables within stored procedures. This ensures configurations are managed within the data platform and are accessible to all BigQuery components.
*   **Parameter Handling**: The `getopts` utility for command-line argument parsing is replaced by direct input parameters to the BigQuery stored procedures, aligning with BigQuery's procedural interface.
*   **Utility Function Migration**: Common utility functions from sourced KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are reimplemented as BigQuery helper stored procedures or integrated directly into the main orchestration procedure, reducing external dependencies.
*   **Core Logic Invocation**: The execution of the core script (`k_ausd_v_ta_discount_rr.ksh`) as a child process is replaced by a direct `CALL` to its migrated BigQuery stored procedure equivalent (`k_ausd_v_ta_discount_rr`).

**Notable Trade-offs:**

*   **Loss of Direct OS Interaction**: Migrating to BigQuery stored procedures inherently removes direct access to the underlying operating system and its utilities (e.g., `tee`, `getopts`, `trap` signals). This is a deliberate trade-off for increased platform-native integration, scalability, and managed service benefits.
*   **Core Script Dependency**: The migration of the wrapper is dependent on the eventual migration of the core `k_ausd_v_ta_discount_rr.ksh` script. The current solution includes a placeholder, highlighting this dependency.
*   **External Orchestration for Advanced Features**: While BigQuery stored procedures offer robust internal error handling, complex scheduling, retries, and broader workflow management (e.g., cross-system dependencies) might still benefit from external orchestration tools like Cloud Composer (Airflow) or Cloud Workflows.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`your_project.your_dataset`) exists. If not, create it:
    ```sql
    CREATE SCHEMA `your_project.your_dataset`;
    ```
2.  **Table Creation**: Execute the DDL scripts to create the logging and configuration tables:
    ```sql
    -- Create job_logging_table
    bq query --use_legacy_sql=false < ddl/your_project.your_dataset.job_logging_table.sql
    -- Create configuration_table
    bq query --use_legacy_sql=false < ddl/your_project.your_dataset.configuration_table.sql
    ```
3.  **Stored Procedure Deployment**: Deploy all generated BigQuery stored procedures:
    ```bash
    for file in sprocs/*.sql; do
        bq query --use_legacy_sql=false < "$file"
    done
    ```
4.  **Configuration Data Insertion**: Populate the `configuration_table` with necessary values, such as `BERT_DIR_ROOT`. This is crucial for the main orchestration procedure to function correctly.
    ```sql
    INSERT INTO `your_project.your_dataset.configuration_table` (config_key, config_value)
    VALUES ('BERT_DIR_ROOT', '/path/to/your/bert_root_in_bq_context'); -- Adjust value as needed
    ```
5.  **IAM/Permissions**: Ensure the service account or user executing the BigQuery stored procedures has the necessary IAM roles:
    *   `BigQuery Data Editor` on `your_project.your_dataset` to create/update tables and execute procedures.
    *   `BigQuery Job User` to run BigQuery jobs.
    *   If using Cloud Composer, the Composer service account will need these permissions.
6.  **Scheduling**: Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer, or a custom application) to invoke the main `your_project.your_dataset.Vertragsdatenabgleich` stored procedure at the desired frequency and with the correct parameters.
7.  **Core Script Migration**: **Crucially, the `k_ausd_v_ta_discount_rr.sql` placeholder procedure must be replaced with the actual migrated logic of the core script before go-live.** This involves a separate design and implementation effort.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further follow-up:

*   **Core Script (`k_ausd_v_ta_discount_rr.ksh`) Migration**: This is the most significant unresolved component. The current `k_ausd_v_ta_discount_rr` BigQuery stored procedure is a placeholder. Its actual content, complexity, and chosen migration technology (BQ SQL, PySpark, Dataflow, etc.) will dictate the final end-to-end solution.
*   **Shell Traps (`INT`, `ERR`) Equivalence**: While `BEGIN...EXCEPTION...END` blocks provide robust error handling within BigQuery SQL, the exact behavior of shell signal traps (e.g., for external interrupts) is not directly replicable. For scenarios requiring sophisticated interrupt handling or external process control, external orchestration (e.g., Cloud Composer) might be necessary.
*   **Parameter `-s` and `-l` Usage**: The original script declares these parameters but does not explicitly handle them within the provided `r_ausd_v_ta_discount_rr.ksh` content. Their intended use in the core script (`k_ausd_v_ta_discount_rr.ksh`) needs to be clarified during its migration to ensure correct parameter passing and logic.
*   **`BERT_DIR_ROOT` Value**: The placeholder value for `BERT_DIR_ROOT` in the `configuration_table` needs to be replaced with the actual, relevant path or identifier within the BigQuery/GCP context. This might represent a dataset ID, a GCS bucket path, or another configuration relevant to the migrated core logic.

## 6. Validation

To validate the migrated `Vertragsdatenabgleich` job:

1.  **Execute the Main Stored Procedure**:
    *   **Without help flag**:
        ```sql
        CALL `your_project.your_dataset.Vertragsdatenabgleich`(
            p_param_s => 'test_s_value',
            p_param_l => 'test_l_value',
            p_param_h => FALSE
        );
        ```
    *   **With help flag**:
        ```sql
        CALL `your_project.your_dataset.Vertragsdatenabgleich`(
            p_param_s => NULL, -- Parameters are ignored when help is true
            p_param_l => NULL,
            p_param_h => TRUE
        );
        ```
2.  **Check `job_logging_table`**:
    *   Query the `your_project.your_dataset.job_logging_table` to inspect the log entries generated by the execution.
    ```sql
    SELECT *
    FROM `your_project.your_dataset.job_logging_table`
    ORDER BY created_at DESC;
    ```
3.  **"Passing" Criteria**:
    *   **Successful Execution**: The `job_logging_table` should contain a sequence of `INFO` messages, culminating in a `SUCCESS` entry for the `job_key` corresponding to the execution. The final `status` for the job should be `SUCCESS`.
    *   **Error Handling Test**: Intentionally introduce an error (e.g., by modifying the placeholder `k_ausd_v_ta_discount_rr` to `RAISE BQ.ABORTED ERROR 'Simulated error';`). Re-run the main procedure. The `job_logging_table` should show `ERROR` messages and a final `FAILED` status for the job.
    *   **Parameter Logging**: Verify that the `p_param_s` and `p_param_l` values passed to the main procedure are correctly logged in the `job_logging_table`.
    *   **Help Flag Behavior**: When `p_param_h` is `TRUE`, the logs should indicate "Help requested" and the procedure should return without executing the core logic.
    *   **Core Logic (Post-Migration)**: Once `k_ausd_v_ta_discount_rr` is fully migrated, "passing" will also include verifying its specific data reconciliation outcomes (e.g., correct updates, inserts, or merges in target tables).

## 7. Rollback Procedure

In case of issues or a need to revert, follow these steps:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the `your_project.your_dataset.Vertragsdatenabgleich` BigQuery stored procedure.
2.  **Delete BigQuery Objects**: Remove the deployed BigQuery stored procedures and tables.
    ```sql
    -- Drop the main orchestration procedure
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.Vertragsdatenabgleich`;
    -- Drop the core logic placeholder procedure
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.k_ausd_v_ta_discount_rr`;
    -- Drop all helper DWMSG procedures
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_ErmittleNr`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_Logdateiname`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_ErzeugeEintrag`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_SetzeStichtagInfo`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_MeldeFehler`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_Fehlerbehandlung`;
    DROP PROCEDURE IF EXISTS `your_project.your_dataset.DWMSG_SetzeStatusOK`;
    -- Drop configuration and logging tables (optional, consider retaining logs for forensics)
    DROP TABLE IF EXISTS `your_project.your_dataset.configuration_table`;
    DROP TABLE IF EXISTS `your_project.your_dataset.job_logging_table`;
    ```
    *Note: Retaining `job_logging_table` might be beneficial for post-mortem analysis, but ensure it doesn't interfere with future deployments.*
3.  **Revert to Original Script**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh` KornShell script in its legacy environment. Ensure its scheduling and dependencies are restored to their pre-migration state.
4.  **Verify Original Functionality**: Confirm that the original KornShell script is executing correctly and producing expected results in the legacy environment.