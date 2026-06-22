# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/istools/seu/template/.dw_global`. The original script was responsible for setting global environment variables, directory paths, Oracle home, library paths, system paths, and NLS settings for a legacy Data Warehouse (DW) environment, and conditionally sourcing a Cognos PowerPlay setup script.

The functionality has been re-platformed to a BigQuery-compatible solution, primarily a BigQuery Stored Procedure, orchestrated by Apache Airflow on Google Cloud Platform. The BigQuery Stored Procedure (`your_project_id.your_dataset_name.dw_global_init`) now encapsulates the validation and computation logic, with Airflow managing its invocation, parameter passing, and output consumption.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`bigquery/ddl/create_dw_global_dataset.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) to create the BigQuery dataset (`your_project_id.your_dataset_name`) where the stored procedure and any related configuration tables will reside. This is a foundational step for deploying other BigQuery assets.

*   **`bigquery/stored_procedures/dw_global_init.sql`**
    *   **Role:** This is the core migrated logic. It's a BigQuery Stored Procedure that replicates the original KornShell script's functionality. It accepts input parameters representing environment variables, performs validation checks, computes derived paths (e.g., `LD_LIBRARY_PATH`, `PATH`), sets static NLS values, and returns all these computed and validated values as a single-row table. It also includes a `cognos_note` to signal the need for external Cognos setup if applicable.

*   **`bigquery/ddl/create_config_table.sql`**
    *   **Role:** Provides the DDL for an optional BigQuery configuration table (`your_project_id.your_dataset_name.dw_global_config`). This table can be used to store key-value pairs for configuration parameters, offering a centralized way to manage inputs for the `dw_global_init` stored procedure, especially if not all parameters are directly managed by Airflow Variables.

*   **`bigquery/data/initial_config_data.sql`**
    *   **Role:** Contains Data Manipulation Language (DML) to populate the `dw_global_config` table with initial configuration values. These values mirror the parameters required by the `dw_global_init` stored procedure, providing default or initial settings.

*   **`airflow/dags/dw_global_orchestration_dag.py`**
    *   **Role:** An Apache Airflow Directed Acyclic Graph (DAG) responsible for orchestrating the execution of the `dw_global_init` BigQuery Stored Procedure. It performs the following key functions:
        *   Fetches configuration parameters from Airflow Variables.
        *   Invokes the `dw_global_init` stored procedure using the fetched parameters.
        *   Captures the `RETURNS TABLE` output from the stored procedure into a temporary BigQuery table.
        *   Processes the results from the temporary table, pushing the computed environment variables and any Cognos-related notes to Airflow XComs for downstream tasks.

## 3. Key design decisions

*   **Re-platforming to BigQuery Stored Procedure:** The core logic was moved from a KornShell script to a BigQuery Stored Procedure (`dw_global_init`). This decision aligns with the target cloud-native BigQuery environment, allowing the environment setup logic to reside closer to the data processing engine.
*   **Airflow for Orchestration:** Apache Airflow was chosen as the orchestration layer. This enables:
    *   **Cloud-native execution:** Leveraging GCP services for job scheduling and execution.
    *   **Parameter management:** Airflow Variables provide a robust way to manage input parameters (equivalent to original environment variables) for the BigQuery Stored Procedure.
    *   **Output consumption:** Airflow can easily invoke the BigQuery Stored Procedure, capture its `RETURNS TABLE` output, and make the computed configuration values available to subsequent tasks via XComs.
    *   **Handling external dependencies:** Airflow provides the flexibility to manage the "sourcing" of the Cognos setup script's effects, either by applying equivalent cloud-native configuration or by invoking external tools if Cognos is also migrated.
*   **Parameterization over Hardcoding:** All original "environment variables" are now explicit parameters to the BigQuery Stored Procedure. This enhances flexibility, testability, and allows for dynamic configuration via Airflow Variables or a BigQuery configuration table.
*   **`RETURNS TABLE` for Output:** The BigQuery Stored Procedure returns its computed values as a single-row table. This is a standard BigQuery pattern for returning structured results from procedures, which Airflow can then query and parse.
*   **Abstracting Cognos Setup:** Direct shell sourcing of `setpya.sh` is not possible in BigQuery. The design introduces a boolean parameter (`p_cognos_setup_exists`) and a `cognos_note` in the procedure's output. This signals to the orchestrator that external action is required, decoupling the Cognos setup from the core environment variable logic.
*   **Stateless Execution:** BigQuery Stored Procedures are stateless and cannot directly mutate the operating system's environment variables. The design addresses this by explicitly returning computed values, which the Airflow DAG then consumes and can pass to downstream tasks.
*   **BigQuery `RAISE` for Critical Errors:** Unlike the original KornShell script which might `echo` errors and continue, the BigQuery Stored Procedure uses `RAISE USING MESSAGE` for critical validation failures. This immediately terminates execution, ensuring that downstream processes do not proceed with an invalid configuration. This is a trade-off for stricter error handling.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Execute `bigquery/ddl/create_dw_global_dataset.sql` to create the BigQuery dataset (`your_project_id.your_dataset_name`). Ensure `your_project_id`, `your_dataset_name`, and `your_gcp_region` are replaced with actual values.

2.  **BigQuery Stored Procedure Deployment:**
    *   Execute `bigquery/stored_procedures/dw_global_init.sql` to create the `dw_global_init` stored procedure within the newly created dataset. Ensure `your_project_id.your_dataset_name` is correctly specified.

3.  **BigQuery Configuration Table (Optional but Recommended):**
    *   If using the BigQuery configuration table strategy, execute `bigquery/ddl/create_config_table.sql` to create the `dw_global_config` table.
    *   Populate the configuration table by executing `bigquery/data/initial_config_data.sql`. Adjust the `config_value` entries to reflect your actual environment settings.

4.  **IAM/Permissions:**
    *   **BigQuery:** The service account used by Airflow (or the user deploying/executing) needs `BigQuery Data Editor` or `BigQuery Admin` roles for creating the dataset, stored procedure, and config table. For execution, `BigQuery User` and `BigQuery Data Viewer` roles are sufficient.
    *   **Airflow:** The Airflow service account (e.g., associated with your Cloud Composer environment) requires permissions to:
        *   Execute BigQuery jobs (`bigquery.jobs.create`).
        *   Read/write to BigQuery datasets and tables (`bigquery.datasets.get`, `bigquery.tables.getData`, `bigquery.tables.update`, `bigquery.tables.create`).
        *   Access Airflow Variables (if using them for configuration).

5.  **Airflow Connection Strings:**
    *   Ensure a Google Cloud connection named `google_cloud_default` is configured in your Airflow environment. This connection is used by the `BigQueryHook` and `BigQueryInsertJobOperator`.

6.  **Airflow Variables / Secrets:**
    *   Create the following Airflow Variables (or use a secrets manager integrated with Airflow) to provide parameters to the DAG:
        *   `gcp_project_id`: Your Google Cloud Project ID.
        *   `bq_dataset_name`: The name of your BigQuery dataset (e.g., `dw_global_config`).
        *   `dw_dir_root`, `dw_dir_prot`, `dw_dir_cubes`, `dw_dir_imp_d1`, `dw_dir_imp_xtra`, `dw_dir_imp_ctel`: Corresponding directory paths.
        *   `oracle_home`: The Oracle Home path.
        *   `existing_ld_library_path`: The existing `LD_LIBRARY_PATH` to be prepended.
        *   `existing_path`: The existing `PATH` to be appended.
        *   `cognos_setup_exists`: Set to `true` or `false` based on whether the Cognos setup script's effects need to be considered.

7.  **Airflow DAG Deployment & Scheduling:**
    *   Upload `airflow/dags/dw_global_orchestration_dag.py` to your Airflow DAGs folder.
    *   Define the `schedule_interval` in the DAG to match the desired execution frequency (e.g., `@daily`, `None` for manual triggers).
    *   Ensure the Airflow environment has the `apache-airflow-providers-google` provider installed.

## 5. Known gaps & unresolved references

The migration introduces several changes and leaves some aspects for follow-up:

*   **Environment Variable Export:** The BigQuery Stored Procedure operates in a stateless environment. It cannot directly "export" environment variables to the operating system like the original KornShell script. The computed values are returned as a table and captured by Airflow. Downstream processes must be re-engineered to consume these values from Airflow XComs or a persistent configuration store rather than relying on OS environment variables.
*   **Shell Sourcing (`. /path/to/script.sh`):** The direct sourcing of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` has no direct BigQuery equivalent. The `p_cognos_setup_exists` flag and `cognos_note` in the procedure's output serve as a signal. The actual effects of `setpya.sh` (e.g., setting specific Cognos environment variables or invoking tools) must be analyzed and either:
    *   Replicated as BigQuery configuration/logic if possible.
    *   Handled by a separate task in the Airflow orchestration layer (e.g., a Cloud Function, a containerized task, or a separate Airflow operator) if Cognos remains an external dependency.
*   **Filesystem Interaction:** The original script's `[ -f ... ]` check for file existence is not directly supported in BigQuery. This has been abstracted into the `p_cognos_setup_exists` boolean parameter, which must be determined and passed by the orchestrator.
*   **Error Handling (No Hard Exit in Original):** The original KornShell script printed error messages but might have continued execution in some cases. The BigQuery Stored Procedure, using `RAISE USING MESSAGE`, will immediately terminate execution upon encountering a critical validation error. This is a change in fault tolerance that should be noted; if the original behavior intended for partial execution, this needs review.
*   **NLS Settings Impact:** While `NLS_LANG`, `NLS_DATE_FORMAT`, and `NLS_DATE_LANGUAGE` are declared as variables in the BigQuery Stored Procedure, they do not have the same global impact as Oracle's NLS settings. Any BigQuery operations requiring specific date/time formatting or locale-specific behavior must use explicit SQL functions (e.g., `FORMAT_TIMESTAMP`, `PARSE_TIMESTAMP`) with these values as parameters.
*   **Legacy Oracle Dependency:** The reliance on `ORACLE_HOME` in the original script indicates a broader dependency on an Oracle environment. While the path derivation is migrated, the full impact of this on other migrated components that might have used the Oracle client needs to be thoroughly assessed and potentially re-engineered (e.g., migrating Oracle databases to GCP, using federated queries, or data transfer services).

## 6. Validation

Validation ensures the migrated solution functions as expected.

### How to run the tests:

1.  **BigQuery Stored Procedure Unit Test:**
    *   Open the BigQuery console.
    *   Execute the `dw_global_init` stored procedure directly with various sets of parameters:
        *   **Valid parameters:** Provide all required `p_dw_dir_*` and `p_oracle_home` values.
        *   **Missing critical parameters:** Call the procedure with `NULL` or empty strings for one or more required parameters (e.g., `p_dw_dir_root`).
        *   **`p_cognos_setup_exists` variations:** Test with `TRUE` and `FALSE`.
    *   Example execution:
        ```sql
        CALL `your_project_id.your_dataset_name.dw_global_init`(
          p_dw_dir_root => '/app/dw/root',
          p_dw_dir_prot => '/app/dw/prot',
          p_dw_dir_cubes => '/app/dw/cubes',
          p_dw_dir_imp_d1 => '/app/dw/imp_d1',
          p_dw_dir_imp_xtra => '/app/dw/imp_xtra',
          p_dw_dir_imp_ctel => '/app/dw/imp_ctel',
          p_oracle_home => '/opt/oracle/product/19c',
          p_existing_ld_library_path => '/usr/local/lib',
          p_existing_path => '/usr/local/bin:/usr/bin:/bin',
          p_cognos_setup_exists => TRUE
        );
        ```

2.  **Airflow DAG Integration Test:**
    *   Ensure the `airflow/dags/dw_global_orchestration_dag.py` is deployed to your Airflow environment.
    *   Set up all required Airflow Variables (as listed in Section 4).
    *   Trigger the `dw_global_orchestration` DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI, checking task logs for `fetch_config_parameters`, `call_dw_global_init`, and `process_dw_global_output`.

### What "passing" means:

1.  **BigQuery Stored Procedure:**
    *   **Valid Inputs:** The procedure executes successfully and returns a single-row table containing:
        *   All input `p_dw_dir_*` and `p_oracle_home` values correctly reflected.
        *   `computed_ld_library_path` correctly prepended with `${ORACLE_HOME}/lib`.
        *   `computed_path` correctly appended with `${ORACLE_HOME}/bin:`.
        *   `nls_lang`, `nls_date_format`, `nls_date_language` set to their static default values.
        *   `cognos_note` is `NULL` if `p_cognos_setup_exists` is `FALSE`, or contains the expected message if `TRUE`.
    *   **Invalid Inputs (Missing Critical Variables):** The procedure `RAISE`s an error message indicating which environment variable(s) are not set, and execution terminates.

2.  **Airflow DAG:**
    *   The DAG run completes successfully without any failed tasks.
    *   The `fetch_config_parameters` task successfully retrieves all Airflow Variables and pushes them to XCom.
    *   The `call_dw_global_init` task executes the BigQuery Stored Procedure without errors.
    *   The `process_dw_global_output` task successfully reads the results from the temporary BigQuery table.
    *   The `dw_global_vars` XCom pushed by `process_dw_global_output` contains a dictionary with all expected computed environment variables and NLS settings.
    *   If `cognos_setup_exists` was `TRUE`, the logs for `process_dw_global_output` should show the `Cognos setup note`.
    *   No unexpected errors or warnings are present in the task logs.

## 7. Rollback procedure

In case of issues or unexpected behavior after migration, follow these steps to roll back to the original KornShell script execution:

1.  **Deactivate Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_global_orchestration` DAG to prevent further executions.

2.  **Remove Migrated BigQuery Assets:**
    *   Drop the `dw_global_init` stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_name.dw_global_init`;
        ```
    *   (Optional) If the `dw_global_config` table was created, drop it:
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_name.dw_global_config`;
        ```
    *   (Optional) If no other assets depend on it, drop the BigQuery dataset:
        ```sql
        DROP SCHEMA IF EXISTS `your_project_id.your_dataset_name`;
        ```

3.  **Revert Downstream Systems:**
    *   Ensure all downstream jobs or processes that were modified to consume configuration from the Airflow DAG's output are reverted to their original state, where they consume environment variables from the legacy KornShell script's execution context.

4.  **Re-enable Original KornShell Script:**
    *   Verify that the original KornShell script `vobs/dw_source/istools/seu/template/.dw_global` is accessible and executable by `dwh_init` and any other calling processes.
    *   Confirm that the environment where the original script runs has all necessary dependencies (e.g., Oracle client, Cognos setup) correctly configured.

5.  **Monitor:**
    *   Closely monitor the legacy system to ensure it functions correctly after the rollback.