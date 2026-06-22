# Migration Design — vobs/dw_source/istools/seu/template/.dw_global

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `vobs/dw_source/istools/seu/template/.dw_global`. The original purpose of this script is to set global environment variables for a legacy Data Warehouse (DW) environment, including directory paths, Oracle home, library paths, system paths, and NLS (National Language Support) settings. It also performs validation checks for critical environment variables and conditionally sources a Cognos PowerPlay setup script. The script is intended to be invoked by `dwh_init` as an environment bootstrapper.

The scope of this migration is to re-platform this environment setup functionality from a KornShell script executing in a Unix-like environment to a BigQuery-compatible solution, likely a BigQuery Stored Procedure, integrated with a cloud-native orchestration layer.

## 2. Source Inventory
The job consists of a single source file:

*   **File:** `vobs/dw_source/istools/seu/template/.dw_global`
    *   **Technology:** KornShell (shell script)
    *   **Summary:** Sets global environment variables and paths for a Data Warehouse (DW) environment, performing checks for critical variables and sourcing a Cognos setup script.
    *   **Complexity Tier:** `medium`
    *   **Migration Bucket:** `semi_auto`

## 3. Target Architecture
The migrated functionality will leverage Google Cloud Platform services, primarily BigQuery and an orchestration service.

*   **BigQuery Stored Procedure:** The core logic for validating and computing environment variable equivalents will be encapsulated within a BigQuery Stored Procedure named `project.dataset.dw_global_init`. This procedure will accept necessary parameters and return computed configuration values.
*   **Configuration Management:** Input "environment variables" will be treated as parameters to the BigQuery Stored Procedure. For persistence or centralized management, these configurations (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`) could reside in a BigQuery configuration table or be managed by a secrets manager/configuration service (e.g., Secret Manager, Parameter Store).
*   **Orchestration Layer:** A cloud-native orchestration service (e.g., Cloud Composer/Airflow, Cloud Workflows) will be responsible for:
    *   Invoking the BigQuery Stored Procedure, passing required configuration parameters.
    *   Capturing and utilizing the output (computed configuration values) from the stored procedure for downstream tasks.
    *   Handling the "sourcing" of the Cognos setup script's effects, potentially by applying equivalent cloud-native configuration or by invoking external tools if Cognos is also migrated.

## 4. Data Flow & Lineage
The original script does not process data in the traditional ETL sense (reads from source tables, transforms, writes to target tables). Instead, it establishes an operational environment by setting shell variables.

*   **Inputs (Logical):**
    *   Existing environment variables: `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME`, `LD_LIBRARY_PATH` (existing), `PATH` (existing).
    *   Boolean flag for Cognos setup script existence (`cognos_setup_exists`).
*   **Transformations:**
    *   **Validation:** Checks if critical input variables (DW_DIR_ROOT, ORACLE_HOME, etc.) are set. If any are missing, an error message is generated (and in BigQuery, an exception will be raised).
    *   **Path Construction:**
        *   `LD_LIBRARY_PATH` is constructed by prepending `${ORACLE_HOME}/lib` to the existing `LD_LIBRARY_PATH`.
        *   `PATH` is constructed by appending `${ORACLE_HOME}/bin` to the existing `PATH`.
    *   **NLS Settings:** `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE` are set to static values.
*   **Outputs (Logical):**
    *   The computed values for `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`, along with the validated directory paths (`DW_DIR_ROOT`, etc., and `ORACLE_HOME`), will be treated as the output of the BigQuery Stored Procedure. These can be returned as a result set or stored in a configuration table.
    *   **Implicit Dependency:** The conditional sourcing of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` represents an implicit invocation of external setup. In BigQuery, this will be represented as a flag requiring external orchestration.

## 5. Transformation Logic
The KornShell script's logic will be translated into a BigQuery SQL Stored Procedure.

**Original KornShell Snippets and Corresponding BigQuery SQL Translation:**

1.  **Environment Variable Validation:**
    ```ksh
    if [ -z "$DW_DIR_ROOT" ]
    then
        fehler="$fehler DW_DIR_ROOT "
    fi
    # ... similar checks for other variables ...

    if [ ! -z "$fehler" ]
    then
        echo "Fehler in .dw_global:"
        for varname in $fehler
        do
            echo "   Umgebungsvariable $varname ist nicht gesetzt !"
        done
        echo "Breche ab .."
    fi
    ```
    **BigQuery SQL Translation:**
    This will be implemented using `DECLARE`, `IF`, and `RAISE USING MESSAGE` within the stored procedure. Each required environment variable will be a parameter to the procedure.
    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.dw_global_init`(
      DW_DIR_ROOT STRING,
      -- ... other parameters for required variables ...
      ORACLE_HOME STRING
    )
    BEGIN
      DECLARE fehler STRING DEFAULT '';
      IF DW_DIR_ROOT IS NULL OR DW_DIR_ROOT = '' THEN
        SET fehler = CONCAT(fehler, ' DW_DIR_ROOT ');
      END IF;
      -- ... similar IF blocks for other variables ...

      IF fehler IS NOT NULL AND fehler != '' THEN
        RAISE USING MESSAGE = CONCAT(
          'Fehler in .dw_global: ',
          'Umgebungsvariable(n) nicht gesetzt: ',
          fehler,
          ' Breche ab ..'
        );
      END IF;
      -- ... rest of the procedure ...
    END;
    ```

2.  **Path Derivation:**
    ```ksh
    LD_LIBRARY_PATH=${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}; export LD_LIBRARY_PATH
    PATH="$PATH:$ORACLE_HOME/bin:"; export PATH
    ```
    **BigQuery SQL Translation:**
    These will be computed and declared as local variables within the stored procedure.
    ```sql
    DECLARE computed_LD_LIBRARY_PATH STRING;
    DECLARE computed_PATH STRING;

    SET computed_LD_LIBRARY_PATH = CONCAT(ORACLE_HOME, '/lib:', IFNULL(existing_LD_LIBRARY_PATH, ''));
    SET computed_PATH = CONCAT(IFNULL(existing_PATH, ''), ':', ORACLE_HOME, '/bin:');
    ```

3.  **NLS Settings:**
    ```ksh
    NLS_LANG=GERMAN_GERMANY.WE8ISO8859P1; export NLS_LANG
    NLS_DATE_FORMAT=DD-MON-YY; export NLS_DATE_FORMAT
    NLS_DATE_LANGUAGE=AMERICAN; export NLS_DATE_LANGUAGE
    ```
    **BigQuery SQL Translation:**
    These will be set as declared variables. BigQuery does not have direct equivalents for Oracle NLS settings; these values would be used as parameters in SQL functions (e.g., `FORMAT_TIMESTAMP`, `PARSE_TIMESTAMP`) or configuration for data loading.
    ```sql
    DECLARE NLS_LANG STRING DEFAULT 'GERMAN_GERMANY.WE8ISO8859P1';
    DECLARE NLS_DATE_FORMAT STRING DEFAULT 'DD-MON-YY';
    DECLARE NLS_DATE_LANGUAGE STRING DEFAULT 'AMERICAN';
    ```

4.  **Cognos PowerPlay Sourcing:**
    ```ksh
    if [ -f  /appl/local/cognos/cognos5.2/pya52b17/setpya.sh ]
    then
    	. /appl/local/cognos/cognos5.2/pya52b17/setpya.sh
    fi
    ```
    **BigQuery SQL Translation:**
    Direct shell sourcing is not possible. A boolean parameter `cognos_setup_exists` will signal the need for external action.
    ```sql
    IF cognos_setup_exists THEN
      -- Replace shell sourcing with orchestration-managed configuration
      -- No direct BigQuery equivalent for ". /path/to/setpya.sh"
      SELECT 'Cognos setup script exists; external orchestration must apply its effects.' AS cognos_setup_note;
    END IF;
    ```

## 6. External Dependencies
The original script has the following external dependencies:

*   **Oracle Environment:** The use of `ORACLE_HOME` and the setting of `LD_LIBRARY_PATH` and `PATH` for Oracle binaries indicates a dependency on an Oracle client installation. In the BigQuery target environment, direct Oracle client interaction is not supported. Any downstream processes that required this Oracle environment setup will need to be re-engineered. This may involve:
    *   Migrating source Oracle databases to Google Cloud (e.g., Cloud SQL, Bare Metal Solution for Oracle).
    *   Using BigQuery's federated query capabilities if Oracle data needs to be accessed directly from BigQuery.
    *   Utilizing data transfer services (e.g., Cloud Data Fusion, Storage Transfer Service) to move data from Oracle to BigQuery.
*   **Cognos PowerPlay:** The conditional sourcing of `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` points to a dependency on Cognos. If Cognos PowerPlay is still in use and needs specific environment setup, this will require an external solution within the orchestration layer (e.g., a separate task to configure the Cognos environment if it's also migrated to GCP, or a separate job if it remains on-premise). BigQuery cannot directly execute or apply the effects of this shell script.
*   **Filesystem:** The `[ -f ... ]` check for the Cognos setup script indicates a dependency on the local filesystem. In GCP, this would be replaced by checking for the existence of a configuration flag, or if the Cognos script content is also migrated, checking for an object in Cloud Storage via an orchestration tool.

## 7. Unresolved / Risks
Several aspects of the original script's behavior cannot be directly replicated in BigQuery and pose migration risks or require alternative solutions:

*   **Environment Variable Export:** BigQuery stored procedures operate within a stateless, isolated execution context and cannot directly mutate the operating system's environment variables. The "exported" variables will need to be explicitly returned as a result set from the stored procedure or persisted to a configuration table. Downstream processes will then need to consume these values.
*   **Shell Sourcing:** The `. /path/to/script.sh` command in KornShell sources another script into the current shell process, allowing it to modify the current environment. This behavior has no direct BigQuery equivalent. The effects of the `setpya.sh` script must be analyzed and either:
    *   Replicated as BigQuery configuration/logic.
    *   Handled by a separate task in the orchestration layer if Cognos remains an external dependency.
*   **Filesystem Interaction:** The `[ -f ... ]` check for file existence is not directly supported in BigQuery. This check needs to be abstracted into a configuration parameter (`cognos_setup_exists` boolean) or handled by the orchestration layer.
*   **Error Handling (No Hard Exit):** The original script prints error messages but doesn't explicitly `exit` on missing variables. The BigQuery stored procedure, using `RAISE`, will stop execution immediately. If the original behavior intended for execution to continue after errors, this represents a change in fault tolerance that needs to be reviewed.
*   **NLS Settings:** While `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE` can be declared as variables in BigQuery, they do not have the same global impact as Oracle's NLS settings. Date/time formatting and locale-specific operations in BigQuery must use explicit SQL functions.
*   **Legacy Oracle Dependency:** The reliance on `ORACLE_HOME` points to a broader dependency on an Oracle environment. The full impact of this on other migrated components must be thoroughly assessed.

## 8. Build Plan
The build plan focuses on implementing the BigQuery Stored Procedure and integrating it into an orchestration framework.

1.  **BigQuery Dataset Creation:**
    *   Create a BigQuery dataset, e.g., `projects/<PROJECT_ID>/datasets/<DATASET_NAME>`, to house the stored procedure and any related configuration tables.
    *   **File:** `bigquery/ddl/create_dw_global_dataset.sql` (or similar for IaC)
    *   **Language:** BigQuery DDL

2.  **BigQuery Stored Procedure Deployment:**
    *   Implement the `dw_global_init` stored procedure based on the provided pseudocode, incorporating the validation and computation logic.
    *   **File:** `bigquery/stored_procedures/dw_global_init.sql`
    *   **Language:** BigQuery SQL

3.  **Configuration Management Strategy:**
    *   Decide on how input parameters (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`) will be provided to the stored procedure. Options include:
        *   Directly passed by an orchestrator.
        *   Fetched from a BigQuery configuration table.
        *   Retrieved from a cloud-native configuration/secrets management service.
    *   If using a configuration table, create and populate it.
    *   **File:** `bigquery/ddl/create_config_table.sql`, `bigquery/data/initial_config_data.sql` (or IaC for config)
    *   **Language:** BigQuery DDL/DML (or configuration specific language)

4.  **Orchestration Component Development:**
    *   Develop an Airflow DAG (or equivalent in another orchestrator) that:
        *   Retrieves necessary input parameters for `dw_global_init`.
        *   Executes the `project.dataset.dw_global_init` stored procedure using a BigQuery operator.
        *   Captures the result set from the stored procedure containing the computed environment settings.
        *   Makes these computed settings available to subsequent tasks in the DAG or persists them for external consumption.
        *   Includes a task to address the Cognos setup if required (e.g., executing a separate cloud function or containerized task).
    *   **File:** `airflow/dags/dw_global_orchestration_dag.py`
    *   **Language:** Python (for Airflow DAG)

5.  **Testing:**
    *   Develop unit and integration tests for the BigQuery Stored Procedure.
    *   Develop end-to-end tests for the orchestration DAG to ensure correct parameter passing and output handling.

6.  **Documentation:**
    *   Update documentation for consuming systems regarding the new configuration retrieval method (from BQ stored procedure output/config table rather than shell environment).