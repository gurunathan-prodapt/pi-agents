# MIGRATION_NOTES.md

## 1. Summary

This migration re-platforms the functionality of the KornShell script `.dw_global` from `vobs/dw_source/istools/seu/template/` to a Google Cloud Platform (GCP) BigQuery and Apache Airflow (Cloud Composer) environment.

The original script served as a foundational environment setup script, initializing global runtime environment variables, performing critical validation checks for required variables (`DW_DIR_ROOT`, `ORACLE_HOME`, etc.), setting Oracle-related paths (`LD_LIBRARY_PATH`, `PATH`), and defining NLS settings. It also conditionally sourced an external Cognos PowerPlay setup script.

The target platform leverages:
*   **BigQuery Stored Procedure:** For core validation logic and derivation of configuration values.
*   **Apache Airflow (Cloud Composer):** For orchestration, parameter management, and handling external system interactions (like the conditional Cognos setup).

This migration transforms imperative shell script logic into a declarative, cloud-native configuration and validation process.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`bigquery-sql/dw_global_init.sql`**
    *   **Role:** This SQL file defines a BigQuery Stored Procedure named `dw_global_init`. Its primary role is to encapsulate the validation logic for critical environment configuration parameters (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`) and to derive the new `LD_LIBRARY_PATH`, `PATH`, and NLS settings. It accepts configuration values as input parameters and raises an error if any required parameters are missing or empty, mimicking the original script's termination behavior. Upon successful validation, it returns the derived configuration values via a `SELECT` statement.
    *   **Note:** This file contains placeholders `your-gcp-project-id` and `your_dataset_name` which must be replaced with the actual GCP project ID and BigQuery dataset where the procedure will be deployed.

*   **`src/dags/dw_global_init_dag.py`**
    *   **Role:** This Python file defines an Apache Airflow DAG (`dw_global_init_dag`). Its role is to orchestrate the execution of the migrated logic. It performs the following steps:
        1.  **`get_configuration_values`**: A Python task that simulates fetching configuration values (corresponding to the original script's environment variables) from various sources. In a production environment, this would typically involve Airflow Variables, Google Secret Manager, or a BigQuery configuration table.
        2.  **`call_dw_global_init_procedure`**: An Airflow `BigQueryExecuteQueryOperator` task that calls the `dw_global_init` BigQuery Stored Procedure, passing the fetched configuration values as parameters.
        3.  **`handle_cognos_setup`**: A Python task that addresses the conditional sourcing of the Cognos PowerPlay setup script. This task provides a cloud-native approach to check for Cognos setup requirements and can push relevant environment configurations for downstream tasks via XComs.
    *   **Note:** This DAG contains placeholder comments for `your-gcp-project-id` and `your_dataset_name` and uses `os.getenv` for configuration, which should be replaced with more robust Airflow-native methods (e.g., Airflow Variables, Secret Manager) in a production setup.

## 3. Key Design Decisions

The migration strategy for `.dw_global` involved several key design decisions to adapt a shell script's environment manipulation to a cloud-native, BigQuery-centric architecture:

*   **Separation of Concerns (BigQuery SP + Airflow):**
    *   **Decision:** The core validation logic and the derivation of configuration values (like `LD_LIBRARY_PATH` components) were moved to a BigQuery Stored Procedure. The orchestration, parameter management, and handling of external dependencies (like Cognos) were assigned to an Airflow DAG.
    *   **Rationale:** BigQuery SQL is not designed for direct operating system environment variable manipulation. By separating the concerns, BigQuery can efficiently handle data-related logic (validation, derivation of structured configuration), while Airflow provides the necessary control plane for workflow management, external system interaction, and dynamic parameter passing. This aligns with cloud best practices for modularity and scalability.

*   **Configuration Management over Environment Variables:**
    *   **Decision:** Legacy environment variables (`$VAR`) are re-interpreted as parameters to the BigQuery Stored Procedure, entries in a BigQuery configuration table, or Airflow Variables/Secret Manager entries. Derived values are returned by the stored procedure or passed via Airflow XComs.
    *   **Rationale:** Direct shell environment mutation is not possible or desirable in a BigQuery SQL context. This approach transforms implicit runtime state into explicit, manageable configuration data, improving auditability and control.

*   **BigQuery `RAISE` for Validation:**
    *   **Decision:** The shell script's `if [ -z ... ]` checks and `echo ERROR; exit 1` logic were translated to BigQuery SQL `IF ... THEN RAISE USING MESSAGE = 'Error message'; END IF;`.
    *   **Rationale:** `RAISE` provides a native BigQuery mechanism for controlled error handling and termination, ensuring that invalid configurations halt execution at the earliest possible point, similar to the original script's behavior.

*   **Orchestration Layer for External Dependencies:**
    *   **Decision:** File existence checks (`if [ -f ... ]`) and the conditional sourcing of the Cognos setup script were offloaded to the Airflow DAG.
    *   **Rationale:** BigQuery SQL cannot interact with the file system or execute external shell scripts. Airflow, being a Python-based orchestrator, can easily implement these checks using Python's `os.path.exists` (for local files, though cloud-native alternatives like GCS checks are preferred) or by calling external services/APIs. This allows for flexible handling of legacy external systems.

*   **Trade-offs:**
    *   **Increased Complexity:** The solution involves multiple components (BigQuery, Airflow, potentially Secret Manager), which is more complex than a single shell script. This complexity is managed by leveraging cloud-native services.
    *   **Loss of Direct Shell Environment Mutation:** Downstream systems that *strictly* rely on the shell process's environment being mutated by this specific script will need refactoring to consume configuration from the new Airflow-managed flow or BigQuery. This is a fundamental shift in paradigm.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `your_dataset_name`) exists in your GCP project (e.g., `your-gcp-project-id`). If not, create it.
    *   `bq mk --dataset your-gcp-project-id:your_dataset_name`

2.  **BigQuery Stored Procedure Deployment:**
    *   Replace `your-gcp-project-id` and `your_dataset_name` placeholders in `bigquery-sql/dw_global_init.sql` with your actual project and dataset IDs.
    *   Deploy the `dw_global_init` stored procedure to your BigQuery dataset. This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
    *   Example `bq` command: `bq query --project_id=your-gcp-project-id --dataset_id=your_dataset_name --file=bigquery-sql/dw_global_init.sql`

3.  **IAM Permissions for BigQuery:**
    *   The service account used by your Airflow environment (Cloud Composer) must have `BigQuery Data Editor` or equivalent permissions on the target dataset to execute the stored procedure.
    *   Specifically, `bigquery.routines.update` (for `CREATE OR REPLACE PROCEDURE`) and `bigquery.routines.call` (for `CALL`).

4.  **Airflow (Cloud Composer) Environment Setup:**
    *   **GCP Connection:** Ensure your Airflow environment has a properly configured `google_cloud_default` connection (or another named connection) that allows it to interact with BigQuery.
    *   **Configuration Management:**
        *   **Airflow Variables / Secret Manager:** Decide on a robust method for managing the configuration values (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`, etc.) that are currently hardcoded or read from `os.getenv` in `src/dags/dw_global_init_dag.py`.
            *   **Option A (Airflow Variables):** Create Airflow Variables for each configuration item (e.g., `dw_dir_root`, `oracle_home`). Update the `get_config_values` task to read from `Variable.get()`.
            *   **Option B (Secret Manager):** Store sensitive configurations in Google Secret Manager and configure Airflow to access them. Update the `get_config_values` task to retrieve secrets.
            *   **Option C (BigQuery Config Table):** Create a BigQuery table to store configurations and update `get_config_values` to query this table.
        *   **Project/Dataset IDs:** Update the `gcp_project_id` and `bq_dataset_id` variables in `src/dags/dw_global_init_dag.py` with your actual values, or configure them via Airflow Variables.
    *   **Cognos Setup Flag:** If the `handle_cognos_setup` task is to be active, configure the `ENABLE_COGNOS_CLOUD_SETUP` environment variable (or an Airflow Variable) to `true` in your Airflow environment.
    *   **DAG Deployment:** Upload `src/dags/dw_global_init_dag.py` to your Airflow DAGs folder in Cloud Storage.

5.  **Downstream Job Refactoring:**
    *   Identify all legacy jobs that previously sourced `.dw_global`. These jobs must be refactored to consume the configuration values provided by the new Airflow orchestration (e.g., via XComs, or by querying a BigQuery configuration table populated by the DAG). This is a critical step to ensure continuity.

## 5. Known Gaps & Unresolved References

*   **Cognos Integration Clarity:** The exact nature and future of the Cognos PowerPlay dependency remain a significant unresolved item.
    *   **Action Required:** A clear strategy is needed for Cognos: Is it being retired? Migrated? If migrated, how will its environment setup be handled in a cloud-native context (e.g., containerization, dedicated service)? The current `handle_cognos_setup` task in the Airflow DAG is a placeholder and needs to be fully implemented based on this strategy.
*   **Runtime Environment Mutation (Fundamental Shift):** The core challenge of this migration is that BigQuery SQL and Airflow do not directly mutate the operating system's environment variables in the same way a shell script does.
    *   **Impact:** Any downstream system that *strictly* relies on the shell process's environment being mutated by this specific script will require significant refactoring. The current design passes configuration explicitly (via parameters, XComs, or config tables). This gap highlights a fundamental paradigm shift that needs to be understood and addressed by all consumers of this "environment setup."
*   **Error Handling Granularity:** While BigQuery `RAISE` provides termination, the original script's `echo` statements offered detailed, multi-line error messages.
    *   **Impact:** The `RAISE` message in BigQuery is a single string. Replicating the exact verbose output might require more elaborate logging within the BigQuery stored procedure (e.g., logging to a separate table) or more sophisticated error reporting mechanisms in the Airflow orchestration layer.
*   **Downstream Job Refactoring (B4 Item):** The `MIGRATION DESIGN DOCUMENT` explicitly flags "Downstream Job Refactoring" as a necessary step.
    *   **Impact:** This is a significant follow-up item. All jobs that previously relied on sourcing `.dw_global` must be identified and updated to consume the configuration values from the new BigQuery/Airflow pipeline. This is not a trivial task and requires careful planning and execution.

## 6. Validation

Validation involves ensuring that the migrated components correctly replicate the behavior of the original `.dw_global` script.

**How to Run Tests:**

1.  **Deploy BigQuery Stored Procedure:** Ensure `dw_global_init.sql` is deployed to your BigQuery dataset.
2.  **Deploy Airflow DAG:** Upload `dw_global_init_dag.py` to your Cloud Composer environment's DAGs folder.
3.  **Configure Airflow Variables/Secrets:** Set up the necessary configuration values (e.g., `dw_dir_root`, `oracle_home`) in Airflow Variables or Google Secret Manager as per your chosen configuration strategy.
4.  **Trigger the Airflow DAG:** Manually trigger the `dw_global_init_dag` from the Airflow UI.
5.  **Monitor Execution:** Observe the task execution in the Airflow UI. Check the logs for each task.
6.  **Test BigQuery Stored Procedure Directly:** For isolated testing of the validation logic, you can call the `dw_global_init` procedure directly from the BigQuery console with various valid and invalid input parameters.
    *   **Valid Call Example:**
        ```sql
        CALL `your-gcp-project-id.your_dataset_name.dw_global_init`(
            p_dw_dir_root => '/app/dw',
            p_dw_dir_prot => '/app/dw/prot',
            p_dw_dir_cubes => '/app/dw/cubes',
            p_dw_dir_imp_d1 => '/app/dw/imp/d1',
            p_dw_dir_imp_xtra => '/app/dw/imp/xtra',
            p_dw_dir_imp_ctel => '/app/dw/imp/ctel',
            p_oracle_home => '/usr/local/oracle/product/12.2.0/dbhome_1',
            p_initial_ld_library_path => '/usr/lib',
            p_initial_path => '/usr/local/bin:/usr/bin:/bin'
        );
        ```
    *   **Invalid Call Example (missing `p_dw_dir_root`):**
        ```sql
        CALL `your-gcp-project-id.your_dataset_name.dw_global_init`(
            p_dw_dir_root => '', -- Intentionally empty
            p_dw_dir_prot => '/app/dw/prot',
            p_dw_dir_cubes => '/app/dw/cubes',
            p_dw_dir_imp_d1 => '/app/dw/imp/d1',
            p_dw_dir_imp_xtra => '/app/dw/imp/xtra',
            p_dw_dir_imp_ctel => '/app/dw/imp/ctel',
            p_oracle_home => '/usr/local/oracle/product/12.2.0/dbhome_1',
            p_initial_ld_library_path => '/usr/lib',
            p_initial_path => '/usr/local/bin:/usr/bin:/bin'
        );
        ```

**What "Passing" Means:**

*   **BigQuery Stored Procedure:**
    *   For valid inputs: The procedure executes successfully and returns a `SELECT` statement with the correctly derived `ld_library_path`, `path`, `nls_lang`, `nls_date_format`, and `nls_date_language` values.
    *   For invalid inputs (missing required parameters): The procedure terminates with a `RAISE` error message clearly indicating which parameters were missing, matching the original script's error behavior.
*   **Airflow DAG:**
    *   The `dw_global_init_dag` completes successfully without any task failures.
    *   The `get_configuration_values` task correctly fetches all required configuration parameters.
    *   The `call_dw_global_init_procedure` task executes the BigQuery stored procedure successfully.
    *   The `handle_cognos_setup` task logs its actions correctly, either indicating that Cognos setup was skipped or that cloud-native Cognos environment configuration was performed as expected.
    *   (Optional) If downstream tasks are integrated, they should successfully consume the configuration values passed via XComs or other mechanisms.
*   **Derived Values:** The `ld_library_path` and `path` values derived by the BigQuery stored procedure should accurately reflect the concatenation logic of the original shell script (e.g., `ORACLE_HOME/lib` prepended to `LD_LIBRARY_PATH`). The NLS settings should match the hardcoded values.

## 7. Rollback Procedure

In case of issues or if the migration needs to be reverted, follow these steps:

1.  **Halt New Executions:**
    *   In the Airflow UI, pause the `dw_global_init_dag` to prevent any new runs.

2.  **Revert Downstream Jobs:**
    *   Revert any downstream jobs that were refactored to use the new BigQuery/Airflow configuration back to their original state, where they sourced the legacy `.dw_global` script. Ensure the legacy `.dw_global` script is accessible and functional in the original environment.

3.  **Remove Airflow DAG:**
    *   Delete `dw_global_init_dag.py` from your Cloud Composer environment's DAGs folder. This will un-deploy the DAG from Airflow.

4.  **Revert BigQuery Stored Procedure (Optional but Recommended):**
    *   If the `dw_global_init` stored procedure was created with `CREATE OR REPLACE PROCEDURE`, you can simply drop it if it's no longer needed.
    *   `DROP PROCEDURE IF EXISTS \`your-gcp-project-id.your_dataset_name.dw_global_init\`;`
    *   Alternatively, if a previous version of the procedure existed, you could revert to that version if it was backed up.

5.  **Verify Legacy System Functionality:**
    *   Thoroughly test the original environment and any dependent jobs to ensure they are functioning correctly with the legacy `.dw_global` script.

This rollback procedure ensures a clean reversion to the previous state, allowing the legacy system to continue operating while issues with the migration are addressed.