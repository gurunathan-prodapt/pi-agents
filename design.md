# Migration Design — vobs/dw_source/istools/seu/template/.dw_init

## 1. Purpose & Scope
This migration design document details the transition of a legacy KornShell environment initialization routine, primarily composed of `.dw_init` and `.dw_global` scripts, to the Google Cloud Platform, targeting BigQuery for configuration management and potentially Cloud Composer/Airflow for orchestration. The original scripts are responsible for setting up a comprehensive runtime environment, including directory paths, Oracle client configurations, and other system-level environment variables, which are critical for subsequent data warehouse operations and tool executions. This "assembled job" focuses solely on the environment setup aspect, with no direct data transformation or processing within these specific scripts. The objective is to translate this environment setup logic into a BigQuery-compatible and cloud-native equivalent, ensuring that downstream processes receive the necessary configuration.

## 2. Source Inventory
The assembled job consists of two core KornShell scripts, `.dw_init` and `.dw_global`. A third script, `.dw_lokal`, is referenced but was not found in the source system.

*   **File:** `vobs/dw_source/istools/seu/template/.dw_init`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Undetermined (no entry in `file_complexity`)
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** The primary entry point for environment initialization. It sets base directory variables, dynamically determines the `ORACLE_HOME` path based on filesystem checks, and sources `.dw_global` and `.dw_lokal`.

*   **File:** `vobs/dw_source/istools/seu/template/.dw_global`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Undetermined (no entry in `file_complexity`)
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** Sets global environment variables dependent on those defined in `.dw_init`. This includes Oracle client-related paths (`LD_LIBRARY_PATH`, `PATH`) and NLS settings (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`). It also conditionally sources a Cognos PowerPlay setup script. The script performs validation checks for critical environment variables and exits if they are not set.

*   **File:** `vobs/dw_source/istools/seu/template/.dw_lokal`
    *   **Technology:** KornShell (inferred)
    *   **Complexity Tier:** Undetermined (file not found)
    *   **Automation Bucket:** Undetermined (file not found)
    *   **Purpose:** Intended for local parameter settings, sourced by `.dw_init`. This file was not found in the source system.

## 3. Target Architecture
The environment setup logic will be migrated to a combination of BigQuery for persistent configuration and validation, and potentially Cloud Composer (Apache Airflow) for orchestration and handling of external system interactions that cannot be directly translated to BigQuery.

*   **BigQuery Configuration Tables:**
    *   `project.dataset.dw_runtime_config`: A configuration table to store and manage the derived environment variables, such as `DW_DIR_ROOT`, `DW_DIR_PROT`, `ORACLE_HOME`, etc. This table will serve as the source of truth for runtime parameters for downstream BigQuery jobs.
    *   `project.dataset.oracle_home_config`: A lookup table to manage valid and active Oracle client installation paths. This will replace the filesystem probing logic of the original scripts.

*   **BigQuery Stored Procedures:**
    *   `project.dataset.dw_init_validate_config()`: A BigQuery SQL stored procedure that encapsulates the logic from `.dw_init` and `.dw_global`. It will:
        *   Declare and set BigQuery script variables mirroring the `DW_DIR_*` and other environment variables.
        *   Validate the presence and correctness of critical configuration values using `ASSERT` statements, replicating the error checking in `.dw_global`.
        *   Resolve `ORACLE_HOME` by querying the `oracle_home_config` table, replacing the filesystem-based detection.
        *   Persist the final validated configuration into the `dw_runtime_config` table for use by other BigQuery processes.

*   **Orchestration Layer (Cloud Composer / Python):**
    *   A Python-based orchestrator (e.g., an Airflow DAG in Cloud Composer) will be responsible for:
        *   Triggering the `dw_init_validate_config` BigQuery stored procedure.
        *   Handling any remaining external interactions that cannot be directly managed by BigQuery (e.g., if a similar Cognos `setpya.sh` equivalent is needed, or if external scripts require specific `LD_LIBRARY_PATH` adjustments that are not applicable within BigQuery).
        *   Managing the `umask` equivalent if necessary in an external compute environment.
        *   Providing values for `DW_DIR_CUSTOMER` and `DW_HOST_CUSTOMER` as BigQuery procedure parameters or configuration table entries.

## 4. Data Flow & Lineage
The original scripts primarily modify the execution environment rather than processing data directly. The migration maintains this conceptual flow.

**Legacy Flow:**
1.  **Execution Trigger:** A user or scheduler invokes `.dw_init`.
2.  **Environment Setup (`.dw_init`):**
    *   Sets core directory variables (e.g., `DW_DIR_ROOT`).
    *   Probes the filesystem to determine `ORACLE_HOME`.
    *   Sources `.dw_global`.
    *   Attempts to source `.dw_lokal`.
3.  **Global Variable Setup (`.dw_global`):**
    *   Validates essential environment variables.
    *   Sets Oracle client environment variables (`LD_LIBRARY_PATH`, `PATH`, NLS settings).
    *   Conditionally sources Cognos setup script.
4.  **Downstream Processes:** Other legacy scripts and tools then inherit and utilize these environment variables.

**Target Flow:**
1.  **Orchestration Trigger:** An Airflow DAG (or similar) is triggered.
2.  **BigQuery Configuration Procedure (`dw_init_validate_config`):**
    *   The orchestrator invokes the `project.dataset.dw_init_validate_config()` stored procedure.
    *   The procedure reads parameters (e.g., `$HOME` equivalent, customer login) or configuration from BigQuery tables.
    *   It validates these inputs and queries `project.dataset.oracle_home_config` to determine the correct `ORACLE_HOME` equivalent.
    *   It then inserts the finalized configuration into `project.dataset.dw_runtime_config`.
3.  **Downstream BigQuery Jobs:** Subsequent BigQuery stored procedures or queries can read configuration from `project.dataset.dw_runtime_config` to obtain necessary paths, hostnames, or NLS settings.
4.  **External System Interaction (Orchestrator):** If any legacy process still requires an actual Oracle client or Cognos setup outside BigQuery, the orchestrator manages this by passing the configuration derived from BigQuery tables to the relevant external components.

## 5. Transformation Logic
The transformation logic involves re-implementing shell script environment variable assignments and conditional logic using BigQuery SQL scripting capabilities.

*   **Variable Assignments:**
    *   Shell variables like `DW_DIR_ROOT=$HOME/aktuell; export DW_DIR_ROOT` will be mapped to `DECLARE DW_DIR_ROOT STRING DEFAULT CONCAT(COALESCE(CAST(SYSTEM_USER() AS STRING), ''), '/aktuell');` within the BigQuery stored procedure, potentially using BigQuery parameters for dynamic components like `$HOME`.
    *   The various `DW_DIR_IMP_XX` variables will be similarly translated.
*   **Conditional Logic (ORACLE_HOME detection):**
    *   The `if [ -z "$ORACLE_HOME" ]` and `if [ -d /appl/local/oracle/... ]` constructs will be replaced by a lookup against the `project.dataset.oracle_home_config` BigQuery table. This table will pre-define available Oracle homes and their active status, removing the need for filesystem checks.
*   **Environment Variable Checks (`.dw_global`):**
    *   The `if [ -z "$VAR" ]` checks will become `ASSERT VAR IS NOT NULL AND VAR != '' AS 'Error message';` statements in BigQuery, ensuring critical configurations are present.
*   **Path and Library Path:**
    *   `LD_LIBRARY_PATH` and `PATH` manipulation, which are OS-level concepts, will not have a direct BigQuery equivalent. If these are truly necessary for external tools, their values will be determined by the BigQuery configuration procedure and then passed by the orchestrator to those external tools. For BigQuery-native operations, these are not relevant.
*   **NLS Settings:**
    *   `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE` will be persisted as configuration values in `dw_runtime_config` and can be used to set session variables in BigQuery (e.g., `SET NLS_DATE_FORMAT = 'DD-MON-YY';`) or passed to external SQL clients as needed.
*   **Cognos Sourcing:**
    *   `if [ -f /appl/local/cognos/cognos5.2/pya52b17/setpya.sh ] then . /appl/local/cognos/cognos5.2/pya52b17/setpya.sh fi` will be handled by the orchestrator. If Cognos is still in use and requires specific environment setup, the orchestrator will execute an equivalent script or configure the environment of the Cognos process directly.
*   **`umask 022`:**
    *   This is an OS-level file permission setting and has no direct BigQuery equivalent. It will need to be configured at the operating system level of any compute environment (e.g., Cloud Run, GKE, Composer worker) where files are generated.

## 6. External Dependencies
The original environment setup has several key external dependencies:

*   **Oracle Database / Client:**
    *   **Legacy:** Scripts probe local filesystem for `ORACLE_HOME` and set Oracle client-specific environment variables (`LD_LIBRARY_PATH`, `PATH`, NLS settings).
    *   **Target:** `ORACLE_HOME` detection will be replaced by a lookup in the BigQuery `oracle_home_config` table. NLS settings will be stored in `dw_runtime_config`. If an actual Oracle client connection is required for external processes, the necessary parameters will be supplied by the orchestrator from the BigQuery configuration. Data ingestion from Oracle databases would utilize standard BigQuery data transfer services or custom Python/Java clients.
*   **Cognos PowerPlay:**
    *   **Legacy:** Conditionally sources `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`.
    *   **Target:** If Cognos is still required, its setup will be externalized to the orchestration layer (e.g., Cloud Composer). The orchestrator would be responsible for ensuring the correct environment for any Cognos-related tasks, possibly using a Docker image with Cognos pre-installed.
*   **Remote Host (`dxcst3.bn.detemobil.de`):**
    *   **Legacy:** `DW_HOST_CUSTOMER=dxcst3.bn.detemobil.de` is exported.
    *   **Target:** The hostname will be stored as a configuration value in `dw_runtime_config`. Any process needing to interact with this host will retrieve the value from BigQuery and establish connections via appropriate GCP networking services (e.g., VPC, VPN, Cloud Interconnect) if it's an on-premise system, or directly if it's a cloud resource.
*   **Local Filesystem (`$HOME`, `/appl/local/oracle`):**
    *   **Legacy:** Scripts rely heavily on local filesystem paths for directories and Oracle installations.
    *   **Target:** Filesystem paths will be replaced by object storage (Cloud Storage buckets) for data (e.g., for log files, cubes, import directories). Oracle `HOME` paths will be managed via BigQuery configuration tables. Local filesystem checks are not possible in BigQuery and will be abstracted away.

## 7. Unresolved / Risks
*   **Missing `.dw_lokal`:** The script `.dw_lokal` is explicitly sourced by `.dw_init` but was not found in the source repository or analysis. This is a critical unresolved dependency. Without its content, it's impossible to determine its purpose or the environment variables it might set. This poses a significant risk as its absence could lead to runtime errors or incorrect environment configurations if the functionality it provided was essential.
    *   **Mitigation:** Further investigation is required to locate `.dw_lokal` or to confirm its irrelevance in the current environment. If it cannot be recovered, its functionality needs to be reverse-engineered or confirmed to be non-critical for the target environment.
*   **Dynamic `ORACLE_HOME` Detection:** The original script's logic to dynamically determine `ORACLE_HOME` by checking filesystem paths cannot be directly replicated in BigQuery. The proposed solution involves a pre-configured BigQuery table (`oracle_home_config`). If the dynamic nature of `ORACLE_HOME` was due to highly variable environments, this static configuration might require more frequent updates or a more sophisticated external mechanism to manage.
*   **Cognos Setup:** The conditional sourcing of `setpya.sh` for Cognos. The actual need for Cognos and its migration strategy (if still used) is beyond the scope of this environment setup. Assuming Cognos is migrated, its environment setup will be handled externally by the orchestration layer.
*   **`umask` Setting:** The `umask 022` setting is an OS-level file permission control. This has no direct BigQuery equivalent. Any file creation operations in the target environment (e.g., in Cloud Storage or Cloud Run instances) will need their permissions managed by the respective GCP service configurations or by explicitly setting permissions in an external script.

## 8. Build Plan
The migration will involve creating BigQuery database objects and potentially Python orchestration code.

1.  **Define BigQuery Schema for Configuration Tables:**
    *   Create `project.dataset.dw_runtime_config` table (e.g., `config_name STRING, config_value STRING, created_at TIMESTAMP`).
    *   Create `project.dataset.oracle_home_config` table (e.g., `candidate STRING, is_active BOOL`).
2.  **Populate `oracle_home_config`:**
    *   Insert known and validated Oracle installation paths into `oracle_home_config` with their active status. This replaces the filesystem probing logic.
3.  **Develop BigQuery Stored Procedure:**
    *   Create `project.dataset.dw_init_validate_config()` (language: BigQuery SQL). This procedure will contain the translated logic from `.dw_init` and `.dw_global`, including variable declarations, conditional logic for `ORACLE_HOME`, validation asserts, and insertion of final configuration into `dw_runtime_config`.
4.  **Develop Orchestration Layer (Optional, as needed):**
    *   If external systems or fine-grained environment control are still necessary, develop an Airflow DAG (language: Python) in Cloud Composer to:
        *   Invoke the `dw_init_validate_config` BigQuery stored procedure.
        *   Pass retrieved configuration parameters to external tools or environments.
        *   Handle any specific Cognos setup or OS-level configurations (e.g., `umask`) for external compute resources.
5.  **Refactor Downstream Processes:**
    *   Update all downstream processes that previously relied on the shell environment variables to instead retrieve their configuration from the `project.dataset.dw_runtime_config` BigQuery table or from parameters passed by the orchestrator.