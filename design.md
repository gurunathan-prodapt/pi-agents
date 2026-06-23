# Migration Design — vobs/dw_source/istools/seu/template/.dw_init

## 1. Purpose & Scope
This KornShell script, `.dw_init`, serves as an environment initialization script for an "Information Services" system. Its primary purpose is to define and export various directory paths and host-related environment variables, which are likely consumed by subsequent scripts or applications within the system. It also dynamically determines and sets the `ORACLE_HOME` variable based on available Oracle installations on the filesystem and sources two other configuration scripts (`~/.dw_global` and `~/.dw_lokal`). The script's scope is to set up a consistent operating environment before other components of the Information Services system execute.

## 2. Source Inventory
The job consists of a single source file:
*   **File:** `vobs/dw_source/istools/seu/template/.dw_init`
    *   **Technology:** KornShell
    *   **Summary:** Initializes environment variables for an 'Information Services' system, primarily defining directory paths and setting the ORACLE_HOME variable.
    *   **Complexity Tier:** `medium`
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The target platform is Google BigQuery. Given that the source script is primarily an environment setup and orchestration script rather than a direct data transformation script, the migration will involve translating its logic into BigQuery-compatible patterns and an orchestration layer.

*   **Environment Variables:** The exported environment variables (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_*`, `GEN_HOME`, `DW_DIR_CUSTOMER`, `DW_HOST_CUSTOMER`, `ORACLE_HOME`) will be managed as:
    *   BigQuery Scripting `DECLARE` variables or `SET` statements within a BigQuery procedure/script.
    *   Configuration parameters passed by an orchestration tool (e.g., Cloud Composer/Airflow, Cloud Functions, or a custom Python application).
    *   Potentially stored in a dedicated BigQuery configuration table or as JSON configuration files in Google Cloud Storage for broader accessibility.
*   **Oracle Home Resolution:** The dynamic resolution of `ORACLE_HOME` based on filesystem checks will be handled by:
    *   The orchestration layer, which would determine the correct `ORACLE_HOME` (if still relevant in the target architecture, e.g., for external database connections) and pass it as a parameter.
    *   A pre-existing configuration mapping or a simplified conditional logic within a BigQuery script if the values are static or derived from known parameters.
*   **Sourced Configuration Files (`.dw_global`, `.dw_lokal`):** The contents of these files will be migrated into:
    *   Dedicated BigQuery configuration tables (e.g., `project.dataset.dw_global_config`, `project.dataset.dw_lokal_config`).
    *   JSON configuration files stored in Google Cloud Storage, which can be read by data pipelines or orchestration tasks.
*   **Umask:** The `umask 022` setting is not applicable in a BigQuery-native context as BigQuery manages storage permissions internally.

## 4. Data Flow & Lineage
The original script's "data flow" is primarily the flow of environment configuration. It has no direct data inputs or outputs in the sense of tables or files being read/written for transformation.

**Original System:**
1.  Execution of `.dw_init`.
2.  `ORACLE_HOME` is determined based on filesystem checks.
3.  Environment variables are set (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`).
4.  `~/.dw_global` and `~/.dw_lokal` are sourced, potentially setting more variables.
5.  These exported variables become available to subsequent processes that invoke this script or run in the same shell session.

**Target BigQuery Ecosystem:**
The environmental initialization role will be absorbed by the orchestration layer.

1.  **Orchestration Trigger:** An orchestration tool (e.g., Cloud Composer/Airflow) initiates a job.
2.  **Configuration Loading:** The orchestrator either:
    *   Reads configuration values (equivalent to `DW_DIR_*` variables) from BigQuery configuration tables or Cloud Storage JSON files.
    *   Determines `ORACLE_HOME` (if an Oracle connection is still required by downstream processes) through external configuration or a separate pre-step.
3.  **BigQuery Script/Procedure Execution:** A BigQuery script or stored procedure is executed. This script will use `DECLARE` statements to define parameters and `SET` statements to assign values, mimicking the shell script's variable exports.
4.  **Error Handling:** The `RAISE` statement in BigQuery can replace the `echo` and `exit` for `ORACLE_HOME` resolution failure.

There are no direct data transformations or dependencies between BigQuery tables introduced by this specific component. The `lineage_edges` for this component were empty, confirming its role as an environmental setup.

## 5. Transformation Logic
The transformation logic is conceptual, translating shell environment setup into BigQuery-compatible constructs.

**Original Logic:**
*   **Variable Assignment:** `DW_DIR_ROOT=$HOME/aktuell; export DW_DIR_ROOT`
*   **Conditional `ORACLE_HOME`:** `if [ -z "$ORACLE_HOME" ] then ... elif ... else ... fi`
*   **Sourcing scripts:** `. $HOME/.dw_global`, `. $HOME/.dw_lokal`
*   **Umask:** `umask 022`

**Target Logic (BigQuery Pseudocode Equivalent):**

```sql
-- This represents the environment setup, which could be part of a larger BigQuery script
-- or a standalone procedure that outputs configuration for other BQ jobs.

-- Declare variables, setting default values (can be overridden by orchestration parameters)
DECLARE DW_DIR_ROOT STRING DEFAULT CONCAT(@home_path, '/aktuell');
DECLARE DW_DIR_PROT STRING DEFAULT CONCAT(@home_path, '/daten/logfiles');
DECLARE DW_DIR_CUBES STRING DEFAULT CONCAT(@home_path, '/daten/cubes');
-- ... (other DW_DIR_IMP_* variables similar to above) ...
DECLARE GEN_HOME STRING DEFAULT CONCAT(DW_DIR_ROOT, '/generator');
DECLARE DW_DIR_CUSTOMER STRING DEFAULT @login_placeholder; -- Passed as parameter
DECLARE DW_HOST_CUSTOMER STRING DEFAULT 'dxcst3.bn.detemobil.de';

DECLARE ORACLE_HOME_VAR STRING DEFAULT @initial_oracle_home; -- Optional: if ORACLE_HOME is already set externally
DECLARE resolved_oracle_home_path STRING DEFAULT NULL;

-- Emulate ORACLE_HOME detection. @oracle_exists_* are boolean flags passed by orchestrator.
IF ORACLE_HOME_VAR IS NULL OR ORACLE_HOME_VAR = '' THEN
  IF @oracle_exists_816 THEN
    SET resolved_oracle_home_path = '/appl/local/oracle/8.1.6';
  ELSEIF @oracle_exists_734 THEN
    SET resolved_oracle_home_path = '/appl/local/oracle/7.3.4';
  ELSEIF @oracle_exists_733 THEN
    SET resolved_oracle_home_path = '/appl/local/oracle/oracle.7.3.3';
  ELSEIF @oracle_exists_732 THEN
    SET resolved_oracle_home_path = '/appl/local/oracle/7.3.2';
  ELSEIF @oracle_exists_723 THEN
    SET resolved_oracle_home_path = '/appl/local/oracle/7.2.3';
  ELSE
    RAISE USING MESSAGE = 'Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen ! Aborting.';
  END IF;
  SET ORACLE_HOME_VAR = resolved_oracle_home_path;
END IF;

-- Sourcing of .dw_global and .dw_lokal:
-- This would be replaced by reading from BQ config tables or parameters.
-- Example: Load values from a table for specific configuration groups
-- SELECT config_key, config_value FROM project.dataset.dw_global_config;

-- Output or make variables available for subsequent steps (e.g., through a SELECT statement,
-- or by using them in further BigQuery script logic).
SELECT
  DW_DIR_ROOT,
  DW_DIR_PROT,
  DW_DIR_CUBES,
  -- ... (other DW_DIR_IMP_* variables) ...
  GEN_HOME,
  DW_DIR_CUSTOMER,
  DW_HOST_CUSTOMER,
  ORACLE_HOME_VAR AS ORACLE_HOME;
```

## 6. External Dependencies
The original script has several external dependencies:

*   **Filesystem (for Oracle Home detection):**
    *   `/appl/local/oracle/oracle.8.1.6`
    *   `/appl/local/oracle/7.3.4`
    *   `/appl/local/oracle/oracle.7.3.3`
    *   `/appl/local/oracle/7.3.2`
    *   `/appl/local/oracle/7.2.3`
    *   **Replacement:** In the BigQuery environment, direct filesystem checks are not possible. The information about the active `ORACLE_HOME` (if Oracle is still an external system) must be determined *before* the BigQuery job runs, likely by an external orchestration system. This system would then pass a pre-resolved `ORACLE_HOME` path as a parameter to the BigQuery job, or boolean flags indicating the existence of specific Oracle versions.
*   **Sourced Scripts:**
    *   `$HOME/.dw_global`
    *   `$HOME/.dw_lokal`
    *   **Replacement:** The configuration defined in these scripts will be migrated into BigQuery configuration tables (e.g., `project.dataset.dw_global_config`, `project.dataset.dw_lokal_config`) or maintained as external JSON/YAML configuration files in Google Cloud Storage. The orchestration layer or BigQuery scripts would then query these tables/files to retrieve the necessary configuration.
*   **Oracle Database:** Although not directly interacted with by this script, the setting of `ORACLE_HOME` implies downstream processes connect to an Oracle database.
    *   **Replacement:** If downstream processes still require Oracle, connections would be established using appropriate GCP services (e.g., Dataflow for ETL from Oracle, or Cloud SQL for Proxy connections) with connection details securely managed (e.g., in Secret Manager).

## 7. Unresolved / Risks
*   **Orchestration Integration:** The `semi_auto` migration bucket and the nature of this script highlight the need for a robust orchestration strategy. This script's logic cannot be purely converted to BigQuery SQL; it requires an external component to manage environment variable equivalents and source other configuration.
*   **Home Directory (`$HOME`):** The script heavily relies on the `$HOME` environment variable. In a BigQuery context, this will likely be replaced by a configurable root path passed as a parameter by the orchestrator.
*   **Placeholder `<login>`:** The `DW_DIR_CUSTOMER=<login>` line is a placeholder in the source. This must be resolved during migration by providing the actual login value, likely through an orchestration parameter or a configuration table lookup.
*   **Dynamic `ORACLE_HOME` Resolution:** The dynamic detection of `ORACLE_HOME` based on filesystem paths is a functionality gap in BigQuery. The exact logic to replace this outside of BigQuery needs to be defined. It implies that the target environment might still have access to Oracle installations, or that the `ORACLE_HOME` value becomes a static configuration parameter.
*   **No Lineage/Dependencies:** The lack of incoming or outgoing lineage edges for this script in the provided `lineage_edges` data suggests it might be a foundational script executed at the start of a session or process, not directly interacting with other files in the analyzed job graph. This makes its migration dependent on understanding *what* consumes these environment variables.

## 8. Build Plan

The migration of `.dw_init` will involve generating a BigQuery script/procedure and establishing an orchestration layer.

1.  **Design and Create BigQuery Configuration Tables (if needed):**
    *   Define schemas for `dw_global_config` and `dw_lokal_config` tables in BigQuery (e.g., `config_key STRING, config_value STRING`).
    *   Populate these tables with the relevant key-value pairs extracted from the original `$HOME/.dw_global` and `$HOME/.dw_lokal` files.
    *   **Language:** DDL (BigQuery SQL)
2.  **Create BigQuery Initialization Script/Procedure:**
    *   Generate a BigQuery SQL script or stored procedure that:
        *   Declares BigQuery variables for all `DW_DIR_*`, `GEN_HOME`, `DW_DIR_CUSTOMER`, `DW_HOST_CUSTOMER` variables.
        *   Sets these variables using hardcoded values, parameters passed at runtime, or values retrieved from the BigQuery configuration tables.
        *   Includes the conditional logic for `ORACLE_HOME` resolution, accepting boolean flags (e.g., `@oracle_exists_816`) as parameters and raising an error if resolution fails.
        *   Optionally, selects all these variables to make them available to a calling orchestration component or a subsequent BigQuery job.
    *   **Language:** BigQuery SQL
3.  **Develop Orchestration Wrapper (e.g., Airflow DAG, Cloud Function):**
    *   Create an orchestration component (e.g., a Python script for a Cloud Composer DAG or a Cloud Function) that:
        *   Defines the necessary parameters (e.g., `home_path`, `login_placeholder`, `oracle_exists_816`, etc.).
        *   Executes the BigQuery initialization script/procedure, passing the resolved parameters.
        *   Handles any pre-processing for `ORACLE_HOME` detection if this cannot be purely configuration-driven.
        *   Manages the lifecycle of the BigQuery job.
    *   **Language:** Python (for Airflow/Cloud Functions)