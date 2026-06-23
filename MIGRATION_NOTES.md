# MIGRATION_NOTES.md

## 1. Summary

This migration involved transitioning a set of legacy KornShell environment initialization routines, specifically `vobs/dw_source/istools/seu/template/.dw_init` and `vobs/dw_source/istools/seu/template/.dw_global`, to the Google Cloud Platform. The original scripts were responsible for setting up critical runtime environment variables, including directory paths, Oracle client configurations, and NLS settings, for subsequent data warehouse operations.

The target platform for this migration is Google Cloud Platform, leveraging **BigQuery** for centralized configuration management and validation, with **Cloud Composer (Apache Airflow)** identified as the potential orchestration layer for external interactions. The core logic of environment variable declaration, conditional assignment, and validation has been re-implemented as a BigQuery stored procedure.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL artifacts:

*   **`sql/ddl/dw_runtime_config.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `dw_runtime_config` BigQuery table. This table serves as the central repository for all derived and validated runtime configuration variables (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`, NLS settings) that were previously set as environment variables by the KornShell scripts. Downstream BigQuery processes will query this table to retrieve necessary configuration.

*   **`sql/ddl/oracle_home_config.sql`**
    *   **Role:** Defines the DDL for the `oracle_home_config` BigQuery table. This lookup table stores a list of potential Oracle client installation paths and their active status. It replaces the original KornShell script's filesystem probing logic for dynamically determining `ORACLE_HOME`.

*   **`sql/data/oracle_home_config_initial_data.sql`**
    *   **Role:** Provides initial `INSERT` statements to populate the `oracle_home_config` table with example Oracle HOME paths. These entries are illustrative and must be reviewed and adjusted to reflect the actual active Oracle client installations relevant to the target environment.

*   **`sql/stored_procedures/dw_init_validate_config.sql`**
    *   **Role:** This is the core BigQuery SQL stored procedure that encapsulates the translated logic from both `.dw_init` and `.dw_global`. It performs the following key functions:
        *   Declares and sets BigQuery script variables corresponding to the original environment variables.
        *   Translates filesystem-based path derivations (e.g., `$HOME/aktuell`) into BigQuery string concatenations, using input parameters for dynamic components like `$HOME`.
        *   Determines `ORACLE_HOME` by querying the `oracle_home_config` table.
        *   Implements validation checks using `ASSERT` statements, mirroring the error checking from `.dw_global`.
        *   Persists the final, validated configuration into the `dw_runtime_config` table, making it available for other BigQuery jobs.

## 3. Key Design Decisions

*   **BigQuery as the Configuration Source of Truth:**
    *   **Why:** Centralizing configuration in BigQuery (`dw_runtime_config`) provides a robust, SQL-queryable, and easily auditable source of truth for all runtime parameters. This aligns with a cloud-native data warehouse architecture, where configuration is managed alongside data. It eliminates reliance on ephemeral shell environment variables and local filesystem paths.
    *   **Trade-offs:** Requires all downstream processes to adapt to querying BigQuery for configuration instead of inheriting shell environment variables. OS-level environment variables (`LD_LIBRARY_PATH`, `PATH`, `umask`) cannot be directly managed within BigQuery and require external handling.

*   **`oracle_home_config` Table for `ORACLE_HOME` Resolution:**
    *   **Why:** The original scripts dynamically probed the local filesystem for `ORACLE_HOME`. In a cloud environment, direct filesystem access for such purposes is not feasible or desirable. The `oracle_home_config` table provides a managed, declarative way to define and activate specific Oracle client paths, replacing the dynamic filesystem checks with a BigQuery table lookup.
    *   **Trade-offs:** Loses the dynamic detection capability. Requires manual maintenance of the `oracle_home_config` table to reflect active Oracle client versions. If the environment frequently changes Oracle client installations, this table needs to be updated accordingly.

*   **BigQuery Stored Procedure for Logic Encapsulation:**
    *   **Why:** BigQuery stored procedures allow for encapsulating complex SQL logic, including variable declarations, conditional statements, and `ASSERT` for validation. This enables a direct translation of the KornShell script's procedural logic into a BigQuery-native, executable unit.
    *   **Trade-offs:** BigQuery SQL scripting has limitations compared to a full-fledged programming language. Certain OS-level operations (e.g., executing external scripts, setting `umask`) cannot be performed directly within a BigQuery stored procedure.

*   **Orchestration Layer (Cloud Composer/Python) for External Interactions:**
    *   **Why:** To handle aspects of the original scripts that are inherently OS-level or involve external systems (e.g., Cognos `setpya.sh`, `umask`, `LD_LIBRARY_PATH` for external tools), an orchestration layer like Cloud Composer (Airflow) is necessary. This layer can trigger the BigQuery stored procedure, retrieve configuration, and then apply it to external compute environments or execute external scripts as needed.
    *   **Trade-offs:** Introduces an additional component (orchestrator) and complexity for managing these external dependencies.

## 4. Manual Steps Before Go-Live

Before the migrated solution can be fully operational, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_project_id.your_dataset_id` in the generated code) exists. If not, create it.

2.  **IAM/Permissions Configuration:**
    *   The service account or user executing the DDLs and stored procedure needs `BigQuery Data Editor` or `BigQuery Admin` roles on the target dataset.
    *   Downstream BigQuery jobs or users reading from `dw_runtime_config` will need `BigQuery Data Viewer` permissions.
    *   If Cloud Composer is used, its service account will need appropriate BigQuery permissions to invoke the stored procedure and potentially other GCP resources.

3.  **Populate `oracle_home_config` with Production Data:**
    *   The `sql/data/oracle_home_config_initial_data.sql` file provides example data. **Crucially, this must be updated** with the actual, active Oracle client installation paths relevant to your environment. Ensure the `is_active` flag and `priority` are correctly set for the desired `ORACLE_HOME`.

4.  **Determine and Provide Procedure Parameters:**
    *   The `dw_init_validate_config` stored procedure requires `p_home_directory`, `p_dw_dir_customer`, and `p_dw_host_customer` as input parameters. These values must be determined for the production environment and passed during the procedure invocation (e.g., by an orchestrator).
        *   `p_home_directory`: The equivalent of `$HOME` in the cloud environment, likely a Cloud Storage bucket path or a base directory in a compute instance.
        *   `p_dw_dir_customer`: The customer-specific directory path.
        *   `p_dw_host_customer`: The customer-specific hostname (e.g., `dxcst3.bn.detemobil.de`).

5.  **Address Missing `.dw_lokal` Functionality:**
    *   The original `.dw_lokal` script was not found. Its content and purpose are unknown. **This is a critical gap.** Before go-live, it must be determined if `.dw_lokal` contained essential configurations or logic. If so, that functionality needs to be explicitly added to the `dw_init_validate_config` stored procedure or managed externally.

6.  **External System Setup (Cognos, `umask`, `LD_LIBRARY_PATH`):**
    *   **Cognos PowerPlay:** If Cognos is still in use and requires specific environment setup (e.g., `setpya.sh`), this must be handled by the orchestration layer (e.g., a Cloud Composer DAG) or by configuring the environment of the Cognos process directly.
    *   **`umask 022`:** This OS-level file permission setting needs to be configured at the operating system level of any compute environment (e.g., Cloud Run, GKE, Composer worker) where files are generated and require this specific permission mask.
    *   **`LD_LIBRARY_PATH` and `PATH`:** If external tools (e.g., custom binaries, client libraries) require these OS-level environment variables, the orchestrator must construct and set them in the execution environment of those tools, using the `ORACLE_HOME` value retrieved from `dw_runtime_config`.

7.  **Scheduling:**
    *   If using Cloud Composer, create an Airflow DAG to schedule the execution of the `dw_init_validate_config` BigQuery stored procedure. This DAG should also handle passing the necessary parameters (`p_home_directory`, etc.).

## 5. Known Gaps & Unresolved References

*   **Missing `.dw_lokal` Script:** The content and purpose of the `.dw_lokal` script, which was sourced by `.dw_init`, remain unknown. This is a significant unresolved dependency. Its absence could lead to missing critical configurations or unexpected behavior. **Action Required:** Investigate and recover `.dw_lokal` or confirm its irrelevance. If critical, its logic must be integrated into the BigQuery stored procedure or managed externally.
*   **Cognos PowerPlay Setup (`setpya.sh`):** The conditional sourcing of `setpya.sh` for Cognos cannot be replicated in BigQuery. If Cognos remains a dependency, its environment setup must be externalized and managed by the orchestration layer (e.g., Cloud Composer) or directly within the Cognos deployment.
*   **`umask 022` Setting:** This is an OS-level file permission setting with no direct BigQuery equivalent. It must be configured at the operating system level of any compute environment that generates files and requires this specific permission mask.
*   **`LD_LIBRARY_PATH` and `PATH` Manipulation:** These are OS-level environment variables. While `ORACLE_HOME` is stored in BigQuery, the construction and application of `LD_LIBRARY_PATH` and `PATH` for external tools must be handled by the orchestration layer or the execution environment of those tools.
*   **Dynamic `ORACLE_HOME` Detection:** The original filesystem-based `ORACLE_HOME` detection is replaced by a static lookup in `oracle_home_config`. If the environment requires highly dynamic or frequently changing Oracle client installations, the `oracle_home_config` table will require regular updates or a more automated management process.
*   **`DW_DIR_IMP_MP_ZM` Typo:** The original `.dw_init` script had a potential typo where `DW_DIR_IMP_MP_TS` was used twice. The generated code corrected the second instance to `DW_DIR_IMP_MP_ZM` based on the pattern. This should be verified against the intended original logic.

## 6. Validation

To validate the successful migration and functionality of the environment initialization:

1.  **Deploy DDLs:**
    *   Execute `sql/ddl/dw_runtime_config.sql` to create the `dw_runtime_config` table.
    *   Execute `sql/ddl/oracle_home_config.sql` to create the `oracle_home_config` table.
    *   **Passing:** Tables are created successfully in the specified BigQuery dataset.

2.  **Populate `oracle_home_config`:**
    *   Execute `sql/data/oracle_home_config_initial_data.sql` (after customizing with actual production Oracle HOME paths).
    *   **Passing:** The `oracle_home_config` table contains the expected Oracle HOME entries, with the correct `is_active` and `priority` flags.

3.  **Deploy Stored Procedure:**
    *   Execute `sql/stored_procedures/dw_init_validate_config.sql` to create or replace the stored procedure.
    *   **Passing:** The stored procedure is created successfully.

4.  **Execute Stored Procedure:**
    *   Call the `dw_init_validate_config` stored procedure with representative test parameters for `p_home_directory`, `p_dw_dir_customer`, and `p_dw_host_customer`.
    *   Example:
        ```sql
        CALL `your_project_id.your_dataset_id.dw_init_validate_config`(
            '/gcs/your-bucket/dw_home',
            '/customer/data',
            'dxcst3.bn.detemobil.de'
        );
        ```
    *   **Passing:** The procedure executes without errors (i.e., no `ASSERT` statements fail).

5.  **Verify `dw_runtime_config` Content:**
    *   Query the `dw_runtime_config` table to inspect the populated configuration values.
    *   Example:
        ```sql
        SELECT * FROM `your_project_id.your_dataset_id.dw_runtime_config` ORDER BY config_name;
        ```
    *   **Passing:** The `dw_runtime_config` table contains all expected configuration names and their corresponding values, matching the logic derived from the original KornShell scripts and the provided input parameters. `ORACLE_HOME` should reflect the highest priority active entry from `oracle_home_config`.

6.  **Downstream Integration Test:**
    *   Run a sample downstream BigQuery job or query that relies on these configuration values.
    *   **Passing:** The downstream job successfully retrieves and utilizes the configuration from `dw_runtime_config` without issues.

## 7. Rollback Procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Stop New Executions:**
    *   Immediately halt any new scheduled or manual executions of the `dw_init_validate_config` stored procedure or any orchestrator DAGs that trigger it.

2.  **Revert Downstream Processes:**
    *   Revert any changes made to downstream BigQuery jobs or external applications that were modified to retrieve configuration from `dw_runtime_config`. Restore them to their original state of relying on the legacy environment setup.

3.  **Delete BigQuery Objects:**
    *   Execute the following commands to drop the generated BigQuery tables and stored procedure:
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.dw_init_validate_config`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.dw_runtime_config`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.oracle_home_config`;
        ```

4.  **Re-enable Legacy Scripts:**
    *   Ensure the original KornShell scripts (`.dw_init`, `.dw_global`) are fully functional and accessible in the legacy environment.
    *   Re-enable any legacy scheduling mechanisms that invoked these scripts.

5.  **Revert Orchestrator Changes (if applicable):**
    *   If a Cloud Composer DAG or other orchestration was deployed, disable or delete it.

6.  **Verify Legacy Functionality:**
    *   Confirm that the legacy environment initialization process is fully restored and that all dependent jobs and applications are functioning correctly using the original setup.