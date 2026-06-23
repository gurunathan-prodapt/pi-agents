# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_inv_def.ksh`. The script serves as an orchestration wrapper for the data synchronization process of contract data into the `ta_inv_def` table. Its primary functions include parameter parsing, environment setup, error trapping, and logging, before invoking a core script (identified as `k_ausd_v_ta_inv_def.ksh`) to perform the actual data operations. The purpose of this migration is to re-platform this ETL workflow from a KornShell environment to Google BigQuery, specifically leveraging BigQuery Stored Procedures for orchestration and SQL for data transformations.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh`
*   **Technology**: KornShell
*   **Complexity Tier**: medium
*   **Migration Bucket**: semi_auto
*   **Summary**: This script orchestrates a contract data reconciliation job for the `ta_inv_def` table, handling environment setup, parameter validation, error handling, logging, and invoking a core processing script.

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery. The existing KornShell wrapper script will be converted into a BigQuery Stored Procedure, and its associated logging and error handling mechanisms will be replaced by dedicated audit tables within BigQuery. The core business logic currently executed by `k_ausd_v_ta_inv_def.ksh` will also need to be migrated, either as a separate BigQuery Stored Procedure or a set of BigQuery SQL statements, potentially with external Python/Cloud Run components if non-SQL logic is involved.

**BigQuery Components:**
*   **`project.dataset.sp_vertragsdatenabgleich`**: BigQuery Stored Procedure, serving as the migrated wrapper for the ETL job.
*   **`project.dataset.job_audit`**: BigQuery Table for logging job execution status, start/end times, and general job metadata.
*   **`project.dataset.job_error_log`**: BigQuery Table for detailed error logging during job execution.
*   **`project.dataset.sp_k_ausd_v_ta_inv_def`**: Placeholder for the migrated core logic, likely another BigQuery Stored Procedure or a set of SQL statements.

## 4. Data Flow & Lineage
The original shell script (`r_ausd_v_ta_inv_def.ksh`) orchestrates the execution of a core script (`k_ausd_v_ta_inv_def.ksh`). While the `lineage_edges` table did not explicitly capture this invocation (likely due to its dynamic nature within the shell script), the script content confirms this dependency.

**Legacy Flow:**
1.  `r_ausd_v_ta_inv_def.ksh` starts.
2.  Initializes environment (sourcing `.dw_init`, error handling functions).
3.  Parses command-line parameters.
4.  Sets up logging by determining job entry number and log file name.
5.  Invokes `k_ausd_v_ta_inv_def.ksh` with parameters.
6.  Logs success or failure.

**Target BigQuery Flow:**
1.  The BigQuery Stored Procedure `project.dataset.sp_vertragsdatenabgleich` is invoked.
2.  It handles parameter validation (e.g., `-h` for help).
3.  Generates a new job entry ID and logs the job's start into `project.dataset.job_audit`.
4.  Calls the `project.dataset.sp_k_ausd_v_ta_inv_def` stored procedure (or equivalent SQL logic) to perform the core data reconciliation.
5.  Upon successful completion of the core logic, `project.dataset.sp_vertragsdatenabgleich` updates the `project.dataset.job_audit` table with a success status.
6.  In case of errors, it catches exceptions, logs details to `project.dataset.job_error_log`, and updates `project.dataset.job_audit` with an error status.

The data flow within the core script (`k_ausd_v_ta_inv_def.ksh`) is not detailed in this design, as its logic would need separate analysis and migration into `sp_k_ausd_v_ta_inv_def`.

## 5. Transformation Logic
The `r_ausd_v_ta_inv_def.ksh` script itself is an orchestration layer, with minimal direct data transformation logic. Its key functions and their BigQuery equivalents are:

*   **Parameter Handling (`getopts`)**: Will be translated into input parameters for the BigQuery Stored Procedure `sp_vertragsdatenabgleich`. For instance, the `-h` option for help will be a `p_help` parameter.
*   **Environment Setup (`. $HOME/.dw_init`)**: Global variables and configurations will be managed through BigQuery `DECLARE` statements within the stored procedure, or potentially by sourcing values from a BigQuery configuration table.
*   **Error Trapping (`trap`)**: Replaced by BigQuery's `EXCEPTION WHEN ERROR THEN` blocks within the stored procedure, providing robust error handling.
*   **Logging (`DWMSG_*` functions, `tee -a $LogDatei`)**: The custom logging framework will be replaced by direct `INSERT` statements into `project.dataset.job_audit` and `project.dataset.job_error_log` tables. This centralizes logging within BigQuery for easier monitoring and analysis.
*   **Core Script Invocation (`${Name_Kernskript} ...`)**: This will be translated into a `CALL` statement for the migrated core stored procedure, `project.dataset.sp_k_ausd_v_ta_inv_def`.

The actual data synchronization logic for `ta_inv_def` is assumed to be within `k_ausd_v_ta_inv_def.ksh` and will require separate migration to BigQuery SQL.

## 6. External Dependencies
The original script references several external components:

*   **`$HOME/.dw_init`**: An environment initialization file. In BigQuery, this would be replaced by either direct declarations within the stored procedure or by retrieving configuration values from a BigQuery configuration table.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`**: A shell script for error messaging. Its functionality will be absorbed into the BigQuery `EXCEPTION` blocks and error logging table `project.dataset.job_error_log`.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`**: A shell script for parameter handling. Its functionality will be replaced by BigQuery Stored Procedure input parameters and conditional logic.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`**: A shell script for date handling. This will be replaced by BigQuery SQL date functions like `CURRENT_DATE()` and `FORMAT_DATE()`.
*   **`k_ausd_v_ta_inv_def.ksh`**: The core script performing the actual data synchronization. This is the primary external dependency for business logic and will require its own migration into BigQuery.

There were no explicit external systems (like Oracle, SFTP, S3) identified in the initial `lineage_assembled_jobs` output for this specific job. Any such dependencies would likely surface during the analysis of `k_ausd_v_ta_inv_def.ksh`.

## 7. Unresolved / Risks
*   **Core Business Logic Migration**: The most significant unresolved item is the migration of `k_ausd_v_ta_inv_def.ksh`. This script contains the actual data synchronization logic, which needs to be analyzed, designed, and migrated separately. This will determine the ultimate data flow and transformations.
*   **Dynamic Script Sourcing**: The shell script uses `. $HOME/.dw_init` and other sourced scripts. The exact content and dependencies of these sourced scripts are not fully known and might hide additional external dependencies or complex logic that needs to be addressed during migration.
*   **Logging Framework Details**: The specific details of the `DWMSG_` functions are not fully known. The migration assumes a standard logging to audit tables, but if there are intricate details or external integrations in the `DWMSG_` functions, they might require further analysis.
*   **`tee -a $LogDatei`**: This command appends to a log file. In BigQuery, this is replaced by inserts into a logging table. If there are external consumers of these specific log files, an alternative export mechanism (e.g., to Cloud Storage) might be needed.

## 8. Build Plan
1.  **Define BigQuery Audit Tables**:
    *   Create `project.dataset.job_audit` table.
    *   Create `project.dataset.job_error_log` table.
2.  **Migrate `r_ausd_v_ta_inv_def.ksh` to BigQuery Stored Procedure**:
    *   Develop `project.dataset.sp_vertragsdatenabgleich` in BigQuery SQL, incorporating parameter handling, error handling, and logging to the newly created audit tables.
    *   **Language**: BigQuery SQL (Stored Procedure).
3.  **Analyze and Migrate `k_ausd_v_ta_inv_def.ksh` (Core Logic)**:
    *   Perform a detailed analysis of the `k_ausd_v_ta_inv_def.ksh` script to understand its data sources, transformations, and target tables.
    *   Design and implement the migration of this core logic. This will likely involve creating a new BigQuery Stored Procedure (`project.dataset.sp_k_ausd_v_ta_inv_def`) or a series of SQL scripts/views. If the core logic involves non-SQL operations (e.g., complex file manipulations, API calls), consider using Cloud Functions, Cloud Run, or Dataflow as part of the solution, orchestrated by Cloud Composer.
    *   **Language**: BigQuery SQL (for transformations), Python (for potential non-SQL logic).
4.  **Integrate Core Logic Call**:
    *   Update `project.dataset.sp_vertragsdatenabgleich` to `CALL` the migrated core logic (`project.dataset.sp_k_ausd_v_ta_inv_def`) with appropriate parameters.
5.  **Develop Orchestration (if needed)**:
    *   If there are multiple BigQuery components or external services involved in the overall workflow, consider using Cloud Composer (Airflow) to orchestrate the execution of these components.
    *   **Language**: Python (for Airflow DAGs).
6.  **Testing**:
    *   Develop unit and integration tests for `sp_vertragsdatenabgleich` and `sp_k_ausd_v_ta_inv_def`.
    *   Perform end-to-end testing of the migrated workflow.