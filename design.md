# Migration Design — vobs/dw_source/istools/seu/template/.dw_global

## 1. Purpose & Scope
This migration job focuses on a KornShell script, `.dw_global`, which serves as a foundational environment setup script for a Data Warehouse (DW) environment. Its primary purpose is to initialize global runtime environment variables, perform critical validation checks for required variables (like `DW_DIR_ROOT`, `ORACLE_HOME`, etc.), set Oracle-related paths (`LD_LIBRARY_PATH`, `PATH`), and define NLS (National Language Support) settings. Optionally, it sources an external Cognos PowerPlay setup script if available. The script is intended to be sourced exclusively by `dwh_init`. The overall job is assembled from one component and is considered of medium stage distribution.

The scope of this migration is to re-platform the functionality of this shell script to a BigQuery-centric environment, acknowledging that direct environment variable manipulation is not a BigQuery SQL capability.

## 2. Source Inventory
*   **File:** `vobs/dw_source/istools/seu/template/.dw_global`
*   **Technology:** Shell script (KornShell)
*   **Complexity Tier:** Simple (assumed, as no specific complexity tier was found in analysis)
*   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The target architecture for this component will involve:
*   **BigQuery Stored Procedure:** The core validation logic and the derived environment variable values will be encapsulated within a BigQuery stored procedure (e.g., `project.dataset.dw_global_init`). This procedure will accept necessary configuration as input parameters.
*   **Configuration Management:** Environment variable equivalents will be managed as:
    *   Parameters to the BigQuery stored procedure.
    *   Session variables if applicable to the execution context.
    *   Entries in a dedicated BigQuery configuration table.
    *   Managed environment variables within an orchestration tool (e.g., Cloud Composer).
*   **External Orchestration:** File existence checks and shell script sourcing logic will be handled by an external orchestration layer (e.g., Google Cloud Composer/Apache Airflow, Cloud Workflows, or Cloud Run). This layer will manage the flow, call the BigQuery stored procedure with appropriate parameters, and handle any remaining external system interactions.
*   **Metadata/Configuration:** Oracle-specific runtime path setups, which are not directly applicable in BigQuery SQL, will be modeled as metadata or configuration parameters, which can then be consumed by services interacting with Oracle (e.g., BigQuery federated queries to Oracle, or data transfer services).

## 4. Data Flow & Lineage
The original script's "data flow" is primarily the modification of the runtime environment.
*   **Inputs:** The script expects several environment variables to be pre-set: `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME`. It also implicitly relies on existing `LD_LIBRARY_PATH` and `PATH` values.
*   **Processing:**
    1.  **Validation:** Checks if critical `DW_DIR_*` and `ORACLE_HOME` variables are set. If not, it prints error messages and terminates.
    2.  **Environment Setting:** It constructs and exports `LD_LIBRARY_PATH` and `PATH` by incorporating `ORACLE_HOME` paths. It sets `NLS_LANG`, `NLS_DATE_FORMAT`, and `NLS_DATE_LANGUAGE` to specific values.
    3.  **External Sourcing:** It conditionally sources `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`.
*   **Outputs:** The effective "outputs" are the modified process environment variables. No direct data is read from or written to files or databases by this script itself.

**Target BigQuery Data Flow:**
*   **Inputs:** The BigQuery stored procedure `dw_global_init` will receive `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME` as STRING parameters.
*   **Processing:** The stored procedure will perform validation checks using BigQuery SQL's `IF` statements and `RAISE` an error if any required parameters are missing or empty. It will derive the target values for library paths and NLS settings.
*   **Outputs:** The stored procedure itself will not directly "set" environment variables but will either return the derived configuration values (e.g., via `SELECT` statements) or implicitly manage them through parameter passing in an orchestrated workflow. Error messages will be emitted via `SELECT` statements and `RAISE` will manage termination.
*   **External Orchestration:** An orchestrator (e.g., Airflow) will be responsible for calling `dw_global_init`, capturing its outputs or managing subsequent steps based on its success/failure, and handling the Cognos setup if still required.

## 5. Transformation Logic
The transformation will convert the shell script's imperative environment manipulation into a configuration and validation process within BigQuery and external orchestration.

*   **Environment Variables (`$VAR` in Bash):**
    *   **Source:** Bash environment variables like `$DW_DIR_ROOT`, `$ORACLE_HOME`, `$LD_LIBRARY_PATH`.
    *   **Target:** In BigQuery, these will be handled as parameters to a stored procedure or entries in a configuration table.
*   **Conditionals (`if [ -z ... ]`, `if [ -f ... ]`):**
    *   **Source:** Shell `if` statements for checking if variables are set or if files exist.
    *   **Target:**
        *   Variable checks will be translated to `IF parameter IS NULL OR parameter = '' THEN ... END IF;` within BigQuery SQL stored procedures.
        *   File existence checks (`-f`) will be offloaded to the external orchestration layer (e.g., Python `os.path.exists` or Cloud Storage checks).
*   **Loops (`for varname in $fehler`):**
    *   **Source:** Shell `for` loop for iterating over missing variables.
    *   **Target:** BigQuery SQL `FOR varname IN (SELECT value FROM UNNEST(missing_vars) AS value WHERE value IS NOT NULL AND value != '') DO ... END FOR;` to iterate over an array of missing parameters.
*   **Output (`echo`):**
    *   **Source:** `echo` statements for printing status and error messages.
    *   **Target:** BigQuery SQL `SELECT 'message' AS message;` for logging/output, and `RAISE USING MESSAGE = 'Error message';` for controlled termination.
*   **Environment Export (`export VAR=value`):**
    *   **Source:** `export` commands to set and modify environment variables like `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`.
    *   **Target:** Not directly replicable in BigQuery SQL. The derived values (e.g., `CONCAT(ORACLE_HOME, '/lib')`) will be either returned by the stored procedure, stored in a configuration table, or directly consumed as parameters by subsequent tasks in the orchestration layer. The `NLS_*` settings would become explicit parameters for any client connecting to BigQuery or for specific functions/queries where locale matters.
*   **Shell Sourcing (`. script.sh`):**
    *   **Source:** Sourcing of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`.
    *   **Target:** This will require custom logic in the orchestration layer. If the Cognos environment is still relevant, the necessary environment setup from `setpya.sh` must be replicated in the orchestration runtime (e.g., in a Python environment) or provided via equivalent configuration to a container running Cognos-dependent tasks.

## 6. External Dependencies
*   **Oracle Environment (`ORACLE_HOME`, `LD_LIBRARY_PATH`, `PATH`):**
    *   **Legacy:** Direct dependency on an Oracle client installation for library and binary paths.
    *   **Target:** The need for `ORACLE_HOME` in a BigQuery context implies interaction with an external Oracle database. This could be satisfied via:
        *   BigQuery federated queries to Oracle.
        *   BigQuery Data Transfer Service for ingesting data from Oracle.
        *   Separate compute instances (e.g., Cloud SQL for PostgreSQL or a managed Oracle instance) that are configured to connect to the source Oracle system.
        *   The values of `ORACLE_HOME`, `LD_LIBRARY_PATH`, `PATH` would become configuration parameters for these services or for Python/Java client code running on GCP.
*   **Cognos PowerPlay (`/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`):**
    *   **Legacy:** Conditional sourcing of a Cognos-specific shell script.
    *   **Target:** This dependency requires clarification. If Cognos PowerPlay is being migrated, its environment setup will be part of that migration. If Cognos remains external or is retired, this part of the script's logic will need to be:
        *   Removed if no longer relevant.
        *   Replicated in a cloud-native way (e.g., within a Docker container for Cognos, with environment variables set via Cloud Run or Kubernetes secrets).
        *   The orchestration layer would perform any necessary pre-processing or environment preparation based on this historical dependency.
*   **Unresolved External Systems:** The `lineage_assembled_jobs` record indicates no explicitly identified external systems directly involved with this job beyond what's inferred from the script content.

## 7. Unresolved / Risks
*   **Runtime Environment Mutation:** The fundamental challenge is that BigQuery SQL cannot directly modify the operating system's runtime environment variables. The proposed solution relies on interpreting these as configuration parameters. If any downstream system directly depends on the *shell process's* environment being mutated by this specific script, this migration strategy might require further adaptation or a hybrid approach.
*   **Cognos Integration:** The exact nature and future of the Cognos dependency are not fully resolved. If Cognos remains a critical part of the ecosystem, its integration with the new BigQuery data platform and the replication of its environment setup need a clear strategy.
*   **Error Handling Granularity:** While BigQuery `RAISE` provides termination, the original script's `echo` statements provide detailed, multi-line error messages. Replicating this exact verbose output might require more elaborate logging or separate error-reporting mechanisms in the BigQuery stored procedure or orchestration.
*   **Absence of Complexity Data:** The lack of `file_complexity` data means the assessment of "Simple" is an assumption. Hidden complexities or edge cases in the script's logic, though unlikely for an environment setup script, could be missed.

## 8. Build Plan
1.  **BigQuery Stored Procedure Development:**
    *   **Action:** Create a BigQuery stored procedure `project.dataset.dw_global_init` that accepts the required `DW_DIR_*` and `ORACLE_HOME` values as parameters. Implement the validation logic (checking for `NULL` or empty strings).
    *   **Language:** BigQuery SQL
2.  **Configuration Parameterization:**
    *   **Action:** Determine the storage mechanism for the configuration values (e.g., a dedicated BigQuery lookup table, environment variables in Cloud Composer, or secrets in Secret Manager). Populate these configurations with the appropriate values.
    *   **Language:** SQL (for config table), Python/YAML (for orchestration/secrets).
3.  **Orchestration Layer Integration:**
    *   **Action:** Develop an Airflow DAG (or Cloud Workflow) that orchestrates the execution. This DAG will:
        *   Fetch configuration values (from Secret Manager, Config Table, etc.).
        *   Call the `project.dataset.dw_global_init` BigQuery stored procedure, passing the fetched configurations as parameters.
        *   Implement a Python operator to handle the conditional sourcing logic of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`. This operator would check for the availability of Cognos setup (e.g., via Cloud Storage files or an API call to a Cognos instance) and prepare the environment for subsequent Cognos-dependent tasks if needed.
    *   **Language:** Python (for Airflow DAG).
4.  **Downstream Job Refactoring:**
    *   **Action:** Identify all legacy jobs that `source` `.dw_global`. Refactor these jobs to directly consume the configuration values passed by the new orchestration layer or query the BigQuery configuration table.
    *   **Language:** Varies depending on the downstream job (Python, SQL, Java, etc.).
5.  **Testing:**
    *   **Action:** Thoroughly test the BigQuery stored procedure for validation logic. Test the end-to-end orchestrated workflow, ensuring correct parameter passing and error handling.
    *   **Language:** Python (for orchestration tests), BigQuery SQL (for unit tests of stored procedure).