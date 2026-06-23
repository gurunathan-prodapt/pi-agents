# MIGRATION_NOTES.md

## 1. Summary

This migration job involved the KornShell script `vobs/dw_source/istools/seu/template/.dw_global`, which is responsible for initializing global environment variables and system paths within a Data Warehouse (DW) environment.

The functionality has been migrated to a **BigQuery Stored Procedure** (`project.dataset.dw_global_init`) for the core logic (validation, path construction, NLS settings) and will be orchestrated by an **external orchestration layer** (e.g., Cloud Composer, Cloud Workflows, or a Python wrapper) to handle OS-level environment mutation and conditional external script sourcing.

## 2. Generated artifacts

The following artifact was generated as part of this migration:

*   **`bigquery/stored_procedures/dw_global_init.sql`**: This file contains the BigQuery SQL code for the `dw_global_init` stored procedure.
    *   **Role**: Encapsulates the logic for validating required environment variables, constructing `LD_LIBRARY_PATH` and `PATH` based on Oracle Home, and setting Oracle NLS parameters. It takes current environment values as input and returns the resolved configuration as output. It also includes a placeholder for Cognos setup detection.
*   **External Orchestration Script (Conceptual)**: While not directly generated, a Python script or similar orchestration definition is required.
    *   **Role**: This script will be responsible for:
        *   Collecting initial environment variables from the execution context.
        *   Performing the file existence check for `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`.
        *   Calling the `dw_global_init` BigQuery Stored Procedure with appropriate inputs.
        *   Capturing the output parameters from the stored procedure.
        *   Applying the resolved environment settings (e.g., `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`) to the runtime environment for subsequent steps.
        *   Handling any Cognos-specific environment setup if `cognos_setup_exists` is true.

## 3. Key design decisions

*   **Hybrid Approach (BigQuery SP + External Orchestration)**: The `.dw_global` script primarily deals with environment variable manipulation and conditional logic, which are not directly supported in BigQuery SQL.
    *   **BigQuery Stored Procedure**: Chosen to encapsulate the validation logic and the derivation of path/NLS settings. This leverages BigQuery's capabilities for structured logic and allows for centralized management of these configuration rules.
    *   **External Orchestration**: Essential for handling OS-level operations such as reading current environment variables, performing file system checks (for Cognos script), and crucially, *mutating the execution environment* for subsequent steps in a pipeline. This addresses the inherent limitation of BigQuery SQL not being able to directly modify the operating system environment.
*   **Input/Output Parameterization**: All environment variables that were previously implicitly available to the KornShell script are now explicit input parameters to the BigQuery Stored Procedure. The derived values are returned as explicit output parameters. This makes the procedure deterministic and testable.
*   **Handling of External Dependencies (Oracle/Cognos)**:
    *   **Oracle**: `ORACLE_HOME` and related paths are treated as input parameters. The BigQuery SP calculates the new `LD_LIBRARY_PATH` and `PATH` based on these inputs. Downstream processes requiring Oracle connectivity will need to be re-architected to use BigQuery's federation or data ingestion, as direct Oracle client interaction is not possible from BigQuery.
    *   **Cognos**: The conditional sourcing of `setpya.sh` is handled by a boolean input parameter (`cognos_setup_exists`). The actual application of Cognos-derived settings must occur within the external orchestration layer, as BigQuery cannot source shell scripts.
*   **Error Reporting**: The original script printed error messages to `stderr`. The BigQuery SP uses `SELECT` statements to output messages, which the orchestrator can capture and log. A future enhancement (see Known Gaps) will be to use `RAISE ERROR` for critical failures.

## 4. Manual steps before go-live

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` in the example) exists. If not, create it:
        ```bash
        bq mk --dataset --default_location=US project:dataset
        ```
2.  **IAM/Permissions**:
    *   **Deployment**: The user or service account deploying the `dw_global_init` stored procedure must have `bigquery.routines.create` and `bigquery.routines.update` permissions (e.g., `BigQuery Data Editor` or `BigQuery Admin` role) on the target dataset.
    *   **Execution**: The service account used by the external orchestration layer to call the stored procedure must have `bigquery.routines.call` permission (e.g., `BigQuery User` role) on the stored procedure.
    *   **Orchestration Service Account**: Ensure the service account for Cloud Composer/Workflows has appropriate permissions to interact with BigQuery and any other necessary GCP services.
3.  **External Orchestration Setup**:
    *   **Develop Orchestration Script**: Create the Python script (for Cloud Composer/Workflows) or equivalent that wraps the BigQuery stored procedure call. This script must:
        *   Collect initial environment variables.
        *   Implement the file existence check for `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`.
        *   Call the `dw_global_init` BigQuery stored procedure.
        *   Parse the output from the stored procedure.
        *   Apply the resulting environment variables (e.g., `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`) to the runtime context for subsequent tasks.
        *   Handle Cognos-specific environment setup if `cognos_setup_exists` is true.
    *   **Deploy Orchestration**: Deploy the orchestration script (e.g., as a Cloud Composer DAG, Cloud Workflow definition, or a Cloud Function).
4.  **Configuration Management**:
    *   **Connection Strings/Secrets**: If any downstream processes still require direct Oracle connectivity (e.g., via Cloud SQL for Oracle), ensure connection strings and credentials are securely managed (e.g., in Secret Manager) and accessible by the orchestration layer.
    *   **Environment Variables**: Ensure the initial values for `DW_DIR_ROOT`, `DW_DIR_PROT`, etc., are correctly configured and passed to the orchestration script.
5.  **Scheduling**:
    *   Configure the schedule for the orchestration job (e.g., via Cloud Composer's scheduling features) to run at the appropriate time, typically as an initial step in a broader ETL workflow.

## 5. Known gaps & unresolved references

*   **Environment Mutation (B4)**: BigQuery SQL cannot directly mutate the OS environment. This critical functionality is entirely dependent on the external orchestration layer correctly interpreting the stored procedure's output and applying those settings to the subsequent tasks in the pipeline. This is the primary reason for the `semi_auto` automation bucket.
*   **External Script Sourcing (B4)**: The conditional sourcing of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` is a shell-specific operation. The BigQuery SP only indicates if the script exists. The external orchestration layer must explicitly handle the logic to apply Cognos-specific environment settings if required. The exact mechanism for this (e.g., parsing the shell script, replicating its effects, or calling a separate utility) needs to be defined and implemented within the orchestrator.
*   **Fatal Error Handling (B4)**: The current BigQuery Stored Procedure prints error messages but does not explicitly `RAISE ERROR` if critical input variables are missing. This means the orchestrator would need to parse the output messages to detect a failure. **Recommendation**: Modify the BigQuery SP to use `RAISE ERROR` when `fehler` is not empty, allowing for more robust error handling by the calling orchestration layer.
*   **Scope of `dwh_init` (B4)**: This migration focuses solely on `.dw_global`. The broader `dwh_init` script, which sources `.dw_global`, and any other scripts that rely on the environment setup provided by `.dw_global`, will need to be analyzed and migrated or adapted to consume the environment settings provided by the new orchestration layer.
*   **Commented Code (B4)**: The commented-out sections for `PATH` and `SQLPATH` expansion in the original KornShell script were not migrated. If these sections become active or relevant in the future, they will require a separate re-evaluation and potential migration.
*   **Oracle Client Environment Re-architecture**: The original script set up an Oracle client environment. While the BigQuery SP handles the path construction, any downstream processes that previously relied on a local Oracle client installation will need to be re-architected. This might involve using BigQuery's federation capabilities, migrating data to BigQuery, or using Cloud SQL for Oracle with appropriate connectivity from the orchestration layer.

## 6. Validation

To validate the migration, both the BigQuery Stored Procedure and the external orchestration layer must be tested.

**A. BigQuery Stored Procedure Validation:**

1.  **Deployment Check**:
    *   Verify the stored procedure `project.dataset.dw_global_init` exists in BigQuery:
        ```bash
        bq show --routine project:dataset.dw_global_init
        ```
2.  **Test Cases (using `bq query` or BigQuery console):**
    *   **Case 1: All required inputs provided, `cognos_setup_exists = FALSE`**
        ```sql
        CALL `project.dataset.dw_global_init`(
          'gs://dw_root', 'gs://dw_prot', 'gs://dw_cubes', 'gs://dw_imp_d1',
          'gs://dw_imp_xtra', 'gs://dw_imp_ctel', '/opt/oracle/product/19c',
          '/usr/lib', '/usr/bin', FALSE
        );
        ```
        *   **Passing Criteria**: The procedure executes successfully without errors. The output result set contains the expected `LD_LIBRARY_PATH_OUT`, `PATH_OUT`, `NLS_LANG_OUT`, etc., with values correctly derived (e.g., `LD_LIBRARY_PATH_OUT` should start with `/opt/oracle/product/19c/lib:`). No "Fehler" messages should be present.
    *   **Case 2: Missing critical input (e.g., `DW_DIR_ROOT`), `cognos_setup_exists = FALSE`**
        ```sql
        CALL `project.dataset.dw_global_init`(
          NULL, 'gs://dw_prot', 'gs://dw_cubes', 'gs://dw_imp_d1',
          'gs://dw_imp_xtra', 'gs://dw_imp_ctel', '/opt/oracle/product/19c',
          '/usr/lib', '/usr/bin', FALSE
        );
        ```
        *   **Passing Criteria**: The procedure executes and outputs messages indicating the missing variable (e.g., "Fehler in .dw_global:", "Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !"). (Note: Currently, it does not `RAISE ERROR`, so successful execution with error messages is the expected behavior).
    *   **Case 3: All required inputs provided, `cognos_setup_exists = TRUE`**
        ```sql
        CALL `project.dataset.dw_global_init`(
          'gs://dw_root', 'gs://dw_prot', 'gs://dw_cubes', 'gs://dw_imp_d1',
          'gs://dw_imp_xtra', 'gs://dw_imp_ctel', '/opt/oracle/product/19c',
          '/usr/lib', '/usr/bin', TRUE
        );
        ```
        *   **Passing Criteria**: The procedure executes successfully and includes the message "Cognos setup script detected; external setup must be applied outside BigQuery." in its output.

**B. External Orchestration Layer Validation:**

1.  **Local/Dev Environment Test**:
    *   Run the orchestration script (e.g., Python wrapper) in a controlled environment.
    *   **Passing Criteria**:
        *   The script successfully connects to BigQuery and calls the `dw_global_init` stored procedure.
        *   It correctly passes the initial environment variables as inputs.
        *   It correctly performs the `setpya.sh` file existence check.
        *   It captures the output parameters from the BigQuery SP.
        *   It logs or applies the derived environment variables (e.g., `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`) to its own execution context or to subsequent tasks.
        *   Error conditions (e.g., missing inputs) are correctly detected and handled by the orchestrator (e.g., by failing the job).
2.  **End-to-End Pipeline Test**:
    *   Integrate the new orchestration component into a test version of the `dwh_init` workflow or a representative downstream process.
    *   **Passing Criteria**:
        *   The entire pipeline executes successfully.
        *   Any subsequent steps that rely on the environment variables set by `.dw_global` (e.g., Oracle client tools, Cognos-dependent applications) function correctly, indicating that the environment was properly configured by the orchestration layer.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following steps outline the rollback procedure:

1.  **Disable New Orchestration**:
    *   Immediately disable or pause the new external orchestration job (e.g., Cloud Composer DAG, Cloud Workflow) that calls the BigQuery Stored Procedure. This prevents further execution of the migrated logic.
2.  **Re-enable Original Script**:
    *   Revert any changes made to the `dwh_init` script or other calling scripts to source/execute the original `vobs/dw_source/istools/seu/template/.dw_global` KornShell script.
    *   Ensure the original environment and dependencies for the KornShell script are fully restored and functional.
3.  **Verify Original Functionality**:
    *   Run the original `dwh_init` workflow or a representative job that relies on `.dw_global` to confirm that the legacy system is fully operational.
4.  **Delete BigQuery Stored Procedure (Optional, for clean-up)**:
    *   Once the original system is stable, the migrated BigQuery Stored Procedure can be deleted to clean up resources:
        ```bash
        bq rm -r -f --routine project:dataset.dw_global_init
        ```
    *   Remove any associated orchestration scripts or definitions from the cloud environment.

This rollback procedure ensures a quick return to the previous stable state while allowing for investigation and remediation of the migration issues.