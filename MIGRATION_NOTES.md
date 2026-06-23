# MIGRATION_NOTES.md

## 1. Summary

This migration addresses the KornShell environment initialization script `vobs/dw_source/istools/seu/template/.dw_init`. The original script's primary function was to define and export various directory paths and host-related environment variables, including dynamic resolution of `ORACLE_HOME`, and sourcing other configuration files (`.dw_global`, `.dw_lokal`).

The script has been migrated to a Google Cloud Platform (GCP) target platform, specifically leveraging **Google BigQuery** for configuration storage and environment variable emulation, orchestrated by **Cloud Composer (Apache Airflow)**. The core logic of setting environment variables is translated into a BigQuery stored procedure, while the dynamic aspects (like `ORACLE_HOME` detection and sourcing external files) are managed by the orchestration layer and BigQuery configuration tables.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bigquery/ddl/dw_global_config.sql`**
    *   **Role:** This DDL script defines the BigQuery table `project.dataset.dw_global_config`. This table is designed to store key-value pair configurations that were originally defined in the `$HOME/.dw_global` file. It provides a structured and queryable replacement for the shell script's sourcing mechanism.
*   **`bigquery/ddl/dw_lokal_config.sql`**
    *   **Role:** Similar to `dw_global_config.sql`, this DDL script creates the BigQuery table `project.dataset.dw_lokal_config`. It will hold configurations originally found in the `$HOME/.dw_lokal` file, offering the same benefits of structured storage and accessibility.
*   **`bigquery/procedures/init_dw_environment.sql`**
    *   **Role:** This BigQuery stored procedure, `project.dataset.init_dw_environment`, encapsulates the core logic of the original `.dw_init` KornShell script. It declares and sets BigQuery variables corresponding to the original environment variables (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`). It accepts parameters for dynamic values like `home_path`, `login_placeholder`, and boolean flags for `ORACLE_HOME` detection, mimicking the original script's conditional logic. The procedure concludes by selecting all resolved variables, making them available for inspection or further processing.
*   **`python/orchestration/dw_init_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG (for Cloud Composer) named `dw_environment_init`. Its purpose is to orchestrate the execution of the BigQuery components. It includes tasks to ensure the `dw_global_config` and `dw_lokal_config` tables exist and then calls the `init_dw_environment` BigQuery stored procedure, passing necessary parameters (retrieved from Airflow Variables). This DAG serves as the entry point for initializing the environment in the new GCP ecosystem.

## 3. Key design decisions

*   **Orchestration-centric approach:** Given that the original script is an environment setup and orchestration script rather than a direct data transformation, a pure BigQuery SQL translation was insufficient. The chosen approach integrates BigQuery for configuration and variable emulation with an orchestration tool (Airflow/Cloud Composer) to manage dynamic aspects and execution flow. This allows for robust scheduling, parameter management, and error handling.
*   **BigQuery for Configuration Storage:** Instead of sourcing shell scripts (`.dw_global`, `.dw_lokal`), their contents are migrated to dedicated BigQuery configuration tables (`dw_global_config`, `dw_lokal_config`). This centralizes configuration, makes it queryable, and integrates it natively into the BigQuery ecosystem.
*   **BigQuery Stored Procedure for Variable Emulation:** The core environment variable assignments are translated into a BigQuery stored procedure using `DECLARE` and `SET` statements. This allows for a direct mapping of the original script's variable definitions and conditional logic within a BigQuery-native context. The `SELECT` statement at the end of the procedure provides a clear output of the resolved environment.
*   **Parameterization for Dynamic Values:** Variables like `$HOME` and the `<login>` placeholder are replaced by parameters (`home_path`, `login_placeholder`) passed to the BigQuery procedure by the orchestrator. This enhances flexibility and allows for environment-specific configurations without modifying the core BigQuery logic.
*   **External Resolution of `ORACLE_HOME`:** Direct filesystem checks for `ORACLE_HOME` are not possible in BigQuery. The design delegates this responsibility to the orchestration layer. The orchestrator is expected to determine the correct `ORACLE_HOME` (if still relevant) or provide boolean flags (`oracle_exists_816`, etc.) as parameters to the BigQuery procedure, which then applies the original conditional logic. This maintains the dynamic nature while adapting to the cloud environment.
*   **`umask` Irrelevance:** The `umask 022` setting is not applicable in a BigQuery-native context, as BigQuery manages storage permissions internally. This setting was intentionally omitted from the target architecture.
*   **Trade-offs:**
    *   **Increased Complexity:** The migration introduces an orchestration layer (Airflow) and BigQuery components, which is more complex than a single shell script. This complexity is justified by the need for cloud-native integration, scalability, and maintainability.
    *   **Loss of Direct Filesystem Access:** The inability to perform direct filesystem checks (e.g., for `ORACLE_HOME`) requires external pre-processing or configuration, shifting responsibility to the orchestration layer.
    *   **Parameter Management Overhead:** Managing parameters (like `home_path`, `login_placeholder`, `oracle_exists_*` flags) via Airflow Variables or other configuration services adds an extra layer of setup and maintenance compared to simple shell environment variables.

## 4. Manual steps before go-live

Before deploying and running the migrated components, the following manual steps are required:

1.  **BigQuery Project and Dataset Creation:**
    *   Ensure the GCP project (`your-gcp-project-id`) and the BigQuery dataset (`dataset`) referenced in the DDL and procedure scripts exist. If not, create them.
    *   `gcloud projects create your-gcp-project-id` (if project doesn't exist)
    *   `bq mk --dataset your-gcp-project-id:dataset`
2.  **IAM/Permissions:**
    *   The service account used by Cloud Composer (or any other orchestration tool) must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the `project.dataset` to create tables and procedures, and execute procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   Ensure the user deploying the DDL and procedure has similar permissions.
3.  **Populate Configuration Tables:**
    *   Extract the key-value pairs from the original `$HOME/.dw_global` and `$HOME/.dw_lokal` files.
    *   Insert these configurations into the newly created BigQuery tables `project.dataset.dw_global_config` and `project.dataset.dw_lokal_config`.
    *   Example:
        ```sql
        INSERT INTO `project.dataset.dw_global_config` (config_key, config_value, description)
        VALUES ('MY_GLOBAL_VAR', 'value_from_global', 'Description of global var');
        ```
4.  **Resolve Placeholders and Set Airflow Variables:**
    *   **`home_path`**: Determine the root path for your data warehouse environment in GCP. This replaces the `$HOME` variable. Set this as an Airflow Variable: `dw_home_path`.
        *   Example: `airflow variables set dw_home_path "/gcp/data_warehouse_root"`
    *   **`login_placeholder`**: Resolve the actual customer login value that the original `<login>` placeholder represented. Set this as an Airflow Variable: `dw_login_placeholder`.
        *   Example: `airflow variables set dw_login_placeholder "customer_a_login"`
    *   **`ORACLE_HOME` Resolution:**
        *   Determine if an `initial_oracle_home` should be pre-set. If so, set `dw_initial_oracle_home` Airflow Variable.
        *   Crucially, determine the existence of the various Oracle versions (8.1.6, 7.3.4, etc.) in the target environment (if Oracle connections are still relevant). Set the corresponding boolean Airflow Variables (`dw_oracle_exists_816`, `dw_oracle_exists_734`, etc.) to `True` or `False`.
        *   Example: `airflow variables set dw_oracle_exists_816 "True"`
5.  **Deploy BigQuery DDL and Procedure:**
    *   Execute `bigquery/ddl/dw_global_config.sql` and `bigquery/ddl/dw_lokal_config.sql` to create the configuration tables.
    *   Execute `bigquery/procedures/init_dw_environment.sql` to create the stored procedure.
    *   This can be done via `bq query` command, BigQuery UI, or as part of a CI/CD pipeline.
6.  **Deploy Airflow DAG:**
    *   Upload `python/orchestration/dw_init_dag.py` to your Cloud Composer environment's DAGs folder.

## 5. Known gaps & unresolved references

*   **Dynamic `ORACLE_HOME` Resolution Logic:** The BigQuery procedure relies on boolean flags (`oracle_exists_816`, etc.) passed by the orchestrator. The mechanism by which the orchestrator *determines* these flags (e.g., by querying an external system, checking a configuration service, or having them hardcoded based on environment setup) needs to be explicitly defined and implemented *outside* of this migration. This is a critical dependency if Oracle connections are still required downstream.
*   **`$HOME` Replacement (`home_path`):** The original script's reliance on `$HOME` is replaced by the `home_path` parameter. The exact value for `home_path` must be determined and consistently set across all environments (dev, test, prod) via Airflow Variables or a similar configuration management system.
*   **`<login>` Placeholder Resolution (`login_placeholder`):** The `DW_DIR_CUSTOMER=<login>` line in the original script was a placeholder. The actual value for `login_placeholder` must be provided and managed, likely through Airflow Variables, to correctly set `DW_DIR_CUSTOMER`.
*   **Downstream Consumption of Variables:** This migration provides the *mechanism* to set and retrieve environment variables. However, the original script's purpose was to make these variables available to *subsequent processes*. The migration does not define *how* these variables will be consumed by downstream BigQuery jobs or other GCP services. This will likely involve:
    *   Passing the output of the `init_dw_environment` procedure (the `SELECT` statement) as XComs in Airflow to subsequent tasks.
    *   Having subsequent BigQuery procedures call `init_dw_environment` themselves or query the configuration tables.
    *   Using the resolved values in other GCP services (e.g., Dataflow templates, Cloud Functions) that are triggered by the orchestrator.
*   **`DW_DIR_IMP_MP_ZM` Correction:** The original KornShell script had a typo where `DW_DIR_IMP_MP_TS` was exported twice. The generated BigQuery procedure correctly assigns `DW_DIR_IMP_MP_ZM` to `CONCAT(home_path, '/daten/mp/zm')`. This is a correction, not a gap, but worth noting.
*   **`RAISE` Statement Behavior:** The `RAISE` statement in the BigQuery procedure will stop execution if `ORACLE_HOME` cannot be resolved. The orchestration layer should be configured to catch this error and handle it appropriately (e.g., retry, alert).

## 6. Validation

To validate the successful migration and functionality:

1.  **Deploy and Trigger the Airflow DAG:**
    *   Ensure the `dw_environment_init` DAG is deployed to your Cloud Composer environment.
    *   Manually trigger the DAG from the Airflow UI.
2.  **Monitor DAG Execution:**
    *   Verify that all tasks within the `dw_environment_init` DAG (e.g., `create_dw_global_config_table`, `create_dw_lokal_config_table`, `call_init_dw_environment_procedure`) complete successfully without errors.
    *   Check Airflow task logs for any warnings or errors.
3.  **Verify BigQuery Objects:**
    *   Confirm that the `project.dataset.dw_global_config` and `project.dataset.dw_lokal_config` tables exist in BigQuery.
    *   Confirm that the `project.dataset.init_dw_environment` stored procedure exists.
4.  **Validate Configuration Table Contents:**
    *   Query the `dw_global_config` and `dw_lokal_config` tables to ensure they contain the expected key-value pairs extracted from the original `.dw_global` and `.dw_lokal` files.
    *   `SELECT * FROM `project.dataset.dw_global_config`;`
    *   `SELECT * FROM `project.dataset.dw_lokal_config`;`
5.  **Validate Procedure Output:**
    *   The `init_dw_environment` procedure includes a `SELECT` statement that outputs all resolved environment variables.
    *   If the `call_init_dw_environment_procedure` Airflow task is configured to write its output to a destination table (as commented out in the DAG), query that table to inspect the resolved values.
    *   Alternatively, you can manually call the procedure in BigQuery with test parameters and inspect its output:
        ```sql
        CALL `project.dataset.init_dw_environment`(
            home_path => '/test/root',
            login_placeholder => 'test_user',
            initial_oracle_home => '', -- Or a specific path
            oracle_exists_816 => TRUE,
            oracle_exists_734 => FALSE,
            oracle_exists_733 => FALSE,
            oracle_exists_732 => FALSE,
            oracle_exists_723 => FALSE
        );
        ```
    *   **"Passing" means:**
        *   The Airflow DAG completes successfully.
        *   The BigQuery procedure executes without raising an error (especially for `ORACLE_HOME` resolution).
        *   The `SELECT` statement within the procedure (or its output if captured) returns the expected directory paths and `ORACLE_HOME` value based on the input parameters and configuration table data.
        *   The values for `DW_DIR_ROOT`, `DW_DIR_PROT`, `ORACLE_HOME`, etc., match the expected behavior of the original KornShell script for given inputs.

## 7. Rollback procedure

In case of issues or if the migration needs to be reverted:

1.  **Undeploy Airflow DAG:**
    *   Remove `python/orchestration/dw_init_dag.py` from the Cloud Composer DAGs folder. This will stop any scheduled or triggered runs of the DAG.
2.  **Delete BigQuery Stored Procedure:**
    *   Execute the following DDL to drop the procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.init_dw_environment`;
        ```
3.  **Delete BigQuery Configuration Tables (Optional, but recommended for clean rollback):**
    *   Execute the following DDL to drop the tables. Be cautious as this will delete all data within them.
        ```sql
        DROP TABLE IF EXISTS `project.dataset.dw_global_config`;
        DROP TABLE IF EXISTS `project.dataset.dw_lokal_config`;
        ```
4.  **Revert Airflow Variables:**
    *   If any Airflow Variables were modified or created specifically for this migration (e.g., `dw_home_path`, `dw_login_placeholder`, `dw_oracle_exists_*`), delete or revert them to their previous state.
5.  **Re-enable Original KornShell Script:**
    *   Ensure that the original `vobs/dw_source/istools/seu/template/.dw_init` KornShell script is active and being used by downstream processes as it was before the migration attempt. This might involve re-configuring job schedulers or application entry points to use the legacy script.