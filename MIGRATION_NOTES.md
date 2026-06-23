# MIGRATION_NOTES.md

## 1. Summary

The legacy KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh`, along with its invoked core reconciliation logic (`k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL`), has been migrated. The target platform is Google Cloud Platform, utilizing BigQuery for data processing and orchestration. The original script's responsibilities, including environment setup, parameter parsing, logging, error handling, and core logic invocation, have been translated into BigQuery Stored Procedures and associated logging tables.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_logging_tables.sql`**
    *   **Role**: This DDL script defines the schema for three essential BigQuery tables:
        *   `project.dataset.job_audit`: Tracks the lifecycle and status of each job run (start/end times, status, parameters, messages).
        *   `project.dataset.job_error_log`: Stores detailed error messages and stack traces when a job fails.
        *   `project.dataset.job_log`: Records general informational messages, warnings, and debug logs during job execution.
    *   **Purpose**: Replaces the custom `DWMSG_*` logging framework and provides a structured, queryable logging solution within BigQuery.

*   **`sql/stored_procedures/k_ausd_v_ta_inv_acc.sql`**
    *   **Role**: This BigQuery Stored Procedure, named `project.dataset.k_ausd_v_ta_inv_acc`, encapsulates the core data reconciliation logic previously found in `k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL`.
    *   **Purpose**: It is responsible for performing the actual data manipulation (reads from source tables, reconciliation, writes to `ta_inv_acc`). It includes logging to `job_log` and error handling to `job_error_log`. Note: The actual reconciliation SQL logic is marked as `TODO` and needs to be implemented.

*   **`sql/stored_procedures/Vertragsdatenabgleich.sql`**
    *   **Role**: This BigQuery Stored Procedure, named `project.dataset.Vertragsdatenabgleich`, serves as the main wrapper for the entire reconciliation process. It directly replaces the original `r_ausd_v_ta_inv_acc.ksh` script.
    *   **Purpose**: It handles parameter parsing, initializes job auditing, orchestrates the call to the core logic (`k_ausd_v_ta_inv_acc`), and manages overall job status and error reporting to the `job_audit` and `job_error_log` tables. It also includes a help message functionality.

## 3. Key design decisions

*   **Orchestration and Wrapper Logic Migration**:
    *   **Decision**: The KornShell wrapper (`r_ausd_v_ta_inv_acc.ksh`) was migrated to a BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`).
    *   **Rationale**: This centralizes the orchestration directly within BigQuery, leveraging its native scripting capabilities for parameter handling, flow control, and error management. It avoids the overhead and complexity of external shell environments.
    *   **Trade-offs**: Loses direct access to shell utilities and file system operations, requiring all logic to be expressed in BigQuery SQL or integrated via BigQuery's external functions if necessary. For more complex scheduling and dependency management, Cloud Composer (Apache Airflow) can be used to orchestrate this BigQuery Stored Procedure.

*   **Core Reconciliation Logic Migration**:
    *   **Decision**: The core reconciliation logic (`k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL`) was encapsulated into another BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_inv_acc`).
    *   **Rationale**: This keeps the data processing close to the data itself (BigQuery), maximizing performance and scalability. It allows for modularity, making the core logic reusable and testable independently.
    *   **Trade-offs**: Requires a complete translation of the SQL from the legacy system to BigQuery Standard SQL, including potential data type and function mapping.

*   **Logging and Error Handling**:
    *   **Decision**: The custom `DWMSG_*` framework and shell `trap` commands were replaced by dedicated BigQuery logging tables (`job_audit`, `job_error_log`, `job_log`) and BigQuery's native `BEGIN...EXCEPTION WHEN ERROR...END` blocks.
    *   **Rationale**: Provides a structured, queryable, and centralized logging solution within BigQuery. This allows for easier monitoring, debugging, and auditing of job executions compared to parsing flat log files. BigQuery's error handling is robust and integrates well with the procedural logic.
    *   **Trade-offs**: Requires querying BigQuery tables to view logs, rather than tailing a file. The specific business logic embedded in some `DWMSG_*` calls might need explicit re-implementation.

*   **Parameter Handling**:
    *   **Decision**: Command-line parameters (`-h`, `-s`, `-l`) were translated into `IN` parameters for the BigQuery Stored Procedures.
    *   **Rationale**: This is the standard and most efficient way to pass inputs to BigQuery Stored Procedures, maintaining the original script's configurability.

*   **Environment Setup**:
    *   **Decision**: Sourcing of `.dw_init` and other shell environment configurations was replaced by explicit variable declarations within the BigQuery Stored Procedures or by fetching configuration from BigQuery lookup tables.
    *   **Rationale**: BigQuery Stored Procedures operate in a self-contained environment. This approach ensures all necessary configurations are explicit and managed within the BigQuery ecosystem.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as used in the generated code) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`;
        ```

2.  **Logging and Audit Tables Deployment**:
    *   Execute the `sql/ddl/job_logging_tables.sql` script to create the `job_audit`, `job_error_log`, and `job_log` tables in your target BigQuery dataset.
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/job_logging_tables.sql
        ```

3.  **Target `ta_inv_acc` Table Creation**:
    *   The `ta_inv_acc` table, which is the target of the reconciliation, must exist in BigQuery with the correct schema. This DDL was not part of the migration scope and needs to be created manually based on the legacy system's schema.

4.  **Source Table Schemas and Data**:
    *   Ensure all source tables referenced by the original `D_AUSD_V_TA_INV_ACC.SQL` (and thus by the new `k_ausd_v_ta_inv_acc` procedure) are migrated to BigQuery and accessible with the correct schemas and data.

5.  **Implement Core Reconciliation Logic**:
    *   The `sql/stored_procedures/k_ausd_v_ta_inv_acc.sql` file contains a `TODO` section. The actual BigQuery Standard SQL logic from `D_AUSD_V_TA_INV_ACC.SQL` must be translated and implemented within this stored procedure. This is a critical step.

6.  **Deploy Stored Procedures**:
    *   Once the core logic is implemented, deploy both `sql/stored_procedures/k_ausd_v_ta_inv_acc.sql` and `sql/stored_procedures/Vertragsdatenabgleich.sql` to your BigQuery dataset.
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/k_ausd_v_ta_inv_acc.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/Vertragsdatenabgleich.sql
        ```

7.  **IAM Permissions**:
    *   The service account or user executing these BigQuery procedures must have the following IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (to create/update tables and insert/update log entries).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `BigQuery Data Viewer` on any source datasets/tables used by `k_ausd_v_ta_inv_acc`.

8.  **Scheduling**:
    *   If using Cloud Composer, deploy the corresponding Airflow DAG (e.g., `r_ausd_v_ta_inv_acc_dag.py` if created) to schedule the `project.dataset.Vertragsdatenabgleich` procedure.
    *   Alternatively, schedule the `CALL project.dataset.Vertragsdatenabgleich(FALSE, NULL, NULL);` statement using Cloud Scheduler, a custom Cloud Function, or other orchestration tools.

## 5. Known gaps & unresolved references

*   **Core Reconciliation Logic (`D_AUSD_V_TA_INV_ACC.SQL`) Implementation**: The most significant gap is the actual translation of the SQL logic from `D_AUSD_V_TA_INV_ACC.SQL` into the `project.dataset.k_ausd_v_ta_inv_acc` stored procedure. This was outside the scope of the automated generation and requires manual effort.
*   **Unused Parameters (`-s`, `-l`)**: The original script declared parameters `-s` and `-l` but did not use them. They are included as `p_s` and `p_l` in the `Vertragsdatenabgleich` procedure but remain unused. Their original business purpose (if any) needs to be clarified. If they are truly vestigial, they can be removed.
*   **`BERT_DIR_ROOT` and Custom Utilities**: The original script relied on a `BERT_DIR_ROOT` environment variable and custom shell utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These have been replaced by BigQuery-native constructs (logging tables, stored procedure parameters, `CURRENT_DATE()`, `FORMAT_DATE()`). Any specific business logic embedded within these utilities that goes beyond generic logging/date handling needs to be identified and re-implemented.
*   **`ta_inv_acc` Table Schema**: The exact schema of the target `ta_inv_acc` table is critical for the core reconciliation logic and was not provided. It must be defined and created in BigQuery.
*   **`DWMSG_*` Framework Specifics**: While the logging framework has been replaced, the exact behavior of certain `DWMSG_*` functions (e.g., how `DW_EintragsNr` was determined, specific content of `DWMSG_SetzeStichtagInfo`) might contain subtle business logic that needs to be replicated in the BigQuery logging or core logic. The `v_stichtag_info` variable in the generated code is currently a placeholder.
*   **Source Table Schemas**: The core reconciliation logic will read from various source tables. Their schemas and locations in BigQuery must be known and accessible.

## 6. Validation

To ensure the migrated job functions correctly, the following validation steps should be performed:

1.  **Unit Testing of Core Logic**:
    *   **How to run**: After implementing the `TODO` section in `project.dataset.k_ausd_v_ta_inv_acc`, execute it directly with representative test data.
        ```sql
        CALL `project.dataset.k_ausd_v_ta_inv_acc`('test_job_id_core', 'YYYYMMDD', '20231026');
        ```
    *   **Passing criteria**:
        *   The procedure completes without error.
        *   The `job_log` table contains expected informational messages.
        *   The `ta_inv_acc` table (or any other target tables) is updated/inserted with the correct data based on the reconciliation logic.

2.  **Integration Testing of Wrapper and Core Logic**:
    *   **How to run**: Execute the main wrapper procedure `project.dataset.Vertragsdatenabgleich`.
        ```sql
        CALL `project.dataset.Vertragsdatenabgleich`(FALSE, NULL, NULL);
        ```
    *   **Passing criteria**:
        *   The procedure completes without error.
        *   The `job_audit` table shows a `SUCCESS` status for the generated `job_id`.
        *   The `job_log` table contains a complete sequence of log messages from both the wrapper and the core logic.
        *   The `ta_inv_acc` table contains the expected reconciled data.

3.  **Help Message Validation**:
    *   **How to run**: Execute the wrapper with the help parameter.
        ```sql
        CALL `project.dataset.Vertragsdatenabgleich`(TRUE, NULL, NULL);
        ```
    *   **Passing criteria**:
        *   A help message is returned as a result set.
        *   The `job_audit` table shows a `SUCCESS` status with a message indicating help was displayed.

4.  **Error Handling Validation**:
    *   **How to run**: Introduce a deliberate error in the `k_ausd_v_ta_inv_acc` procedure (e.g., reference a non-existent table) and then execute `project.dataset.Vertragsdatenabgleich`.
    *   **Passing criteria**:
        *   The `project.dataset.Vertragsdatenabgleich` procedure terminates with an error.
        *   The `job_audit` table shows a `FAILED` status for the `job_id`.
        *   The `job_error_log` table contains a detailed error message and stack trace.

5.  **Data Reconciliation Validation**:
    *   **How to run**: Run the migrated BigQuery job with a known dataset. Run the original legacy script with the *exact same* dataset.
    *   **Passing criteria**: The output (e.g., the contents of the `ta_inv_acc` table) from the BigQuery job must be identical to the output from the legacy system. This is the ultimate validation of functional correctness.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deployment, the following rollback procedure can be followed:

1.  **Stop New Executions**: Immediately halt any scheduled executions of the `project.dataset.Vertragsdatenabgleich` BigQuery Stored Procedure (e.g., disable the Cloud Composer DAG or Cloud Scheduler job).

2.  **Revert to Legacy System**: Resume execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh` script on the legacy platform. Ensure the legacy environment is fully operational and configured to process data.

3.  **Data Restoration (if necessary)**:
    *   If the migrated BigQuery job made any modifications to the `ta_inv_acc` table or other critical data, and these modifications are deemed incorrect or corrupted, restore the affected BigQuery tables from a known good backup taken *before* the migration go-live.
    *   Carefully assess the impact of any partial runs and determine if a full data rollback is required.

4.  **Decommission BigQuery Artifacts (Optional)**:
    *   Once the legacy system is confirmed to be stable and processing data correctly, the BigQuery stored procedures and logging tables can be optionally dropped to avoid confusion or accidental execution.
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.Vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_v_ta_inv_acc`;
        DROP TABLE IF EXISTS `project.dataset.job_audit`;
        DROP TABLE IF EXISTS `project.dataset.job_error_log`;
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        ```
    *   The target `ta_inv_acc` table should only be dropped if it was exclusively created for the migration and contains no valuable data.

5.  **Root Cause Analysis**: Investigate the reason for the rollback, address the identified issues, and plan for a re-migration if appropriate.