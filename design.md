# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell (ksh) script `k_ausd_bp_ta_cntrct_dist.ksh` to Google Cloud Platform, targeting BigQuery for data processing and potentially Cloud Composer (Airflow) for orchestration.

The script serves as a control and orchestration component. Its primary purpose is to:
- Parse command-line parameters (Job ID, entry number, reference date, restart value).
- Set up the environment by sourcing helper scripts.
- Validate input parameters and date formats.
- Orchestrate the execution of a core SQL script (`d_ausd_bp_ta_cntrct_dist.sql`).
- Handle error conditions and log messages.
- Capture the record count from the executed SQL process and potentially update a job-tracking table.

The scope of this migration is to re-implement the orchestration logic and data processing within the BigQuery ecosystem, ensuring functional equivalence and adherence to cloud-native best practices.

## 2. Source Inventory
The job consists of a single primary source file:
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`
  - **Technology**: KornShell (Ksh)
  - **Summary**: This ksh script acts as a control script, parsing parameters, setting up the environment, performing date and parameter validation, and orchestrating the execution of a core SQL script for data processing.
  - **File Purpose**: Orchestration / Pipeline Orchestrator
  - **Complexity Tier**: Not explicitly provided, but inferred as 'Medium' due to parameter parsing, external script dependencies, and conditional logic.
  - **Automation Bucket**: `semi_auto` (Final Rate: 0.65)
  - **Key Dependencies (from script and analysis)**:
    - Utility scripts: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`.
    - Core SQL script: `d_ausd_bp_ta_cntrct_dist.sql` (assumed to contain the main data transformation logic).
    - Outputs: `PoolBasisprodukt` (likely a target table for `d_ausd_bp_ta_cntrct_dist.sql`), `bert_k_ausd_bp_ta_cntrct_dist.tmp` (temporary file for record count).

## 3. Target Architecture
The migrated solution will leverage BigQuery for all data storage and transformation logic. The orchestration layer will likely be managed by Cloud Composer (Airflow), providing robust scheduling, dependency management, and logging capabilities.

- **Orchestration**: Cloud Composer (Airflow) DAG
    - The Airflow DAG will manage the overall workflow, including parameter passing, invoking BigQuery procedures, and handling success/failure states.
- **Data Storage & Transformation**: Google BigQuery
    - Source tables currently accessed by `d_ausd_bp_ta_cntrct_dist.sql` will be migrated to BigQuery tables.
    - The `PoolBasisprodukt` target table will be a BigQuery table.
    - The core logic of `d_ausd_bp_ta_cntrct_dist.sql` will be converted into a BigQuery SQL script or a BigQuery Stored Procedure.
    - The temporary file `bert_k_ausd_bp_ta_cntrct_dist.tmp` will be replaced by BigQuery table operations (e.g., `COUNT(*)` results stored in a variable or an audit table).
- **Logging & Monitoring**: BigQuery Logging, Cloud Monitoring
    - Error messages and job status will be logged to dedicated BigQuery audit tables and integrated with Cloud Monitoring for alerts.

## 4. Data Flow & Lineage
The original data flow involves the ksh script invoking a SQL script which reads from source tables and writes to target tables.

**Legacy Data Flow:**
1. `k_ausd_bp_ta_cntrct_dist.ksh` (Orchestration):
    - Parses parameters (`-j`, `-f`, `-s`, `-l`).
    - Sources helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`).
    - Calls `starteSQLSkript` which executes `d_ausd_bp_ta_cntrct_dist.sql`.
    - Writes record count to `bert_k_ausd_bp_ta_cntrct_dist.tmp`.
    - Potentially updates a job-tracking table.
2. `d_ausd_bp_ta_cntrct_dist.sql` (Data Transformation):
    - Reads from unspecified source tables (implied by `PoolBasisprodukt` output).
    - Writes to `PoolBasisprodukt` table.

**Target BigQuery Data Flow:**
1. **Airflow DAG (`k_ausd_bp_ta_cntrct_dist_dag`)**:
    - Triggered by schedule or external event.
    - Extracts parameters for the BigQuery stored procedure.
    - Calls `project.dataset.r_ausd_bp_ta_cntrct_dist` (the main orchestration stored procedure).
    - Monitors the execution and handles retries/failures.
2. **BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_cntrct_dist`)**:
    - Receives parameters (e.g., `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    - Performs parameter and date validation using BigQuery SQL scripting.
    - Logs job start to `project.dataset.job_log`.
    - Calls another BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_cntrct_dist_core`) which encapsulates the logic from `d_ausd_bp_ta_cntrct_dist.sql`.
    - Retrieves record count (e.g., `COUNT(*)` from `PoolBasisprodukt`).
    - Logs job end and record count to `project.dataset.job_log`.
3. **BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_cntrct_dist_core`)**:
    - Reads from migrated source BigQuery tables.
    - Performs data transformations (from original `d_ausd_bp_ta_cntrct_dist.sql`).
    - Writes (inserts/updates) to the target BigQuery table `PoolBasisprodukt`.

## 5. Transformation Logic
The transformation involves converting shell script logic into BigQuery SQL scripting (stored procedures) and potentially Python for Airflow orchestration.

**Original Logic (ksh script)**:
- **Environment Setup**: Sourcing multiple `.ksh` files to define functions and variables (e.g., `DWMSG_MeldeFehler`, `pruefeParameterGesetzt`, `DWDate_Datum_Check`, `starteSQLSkript`).
- **Parameter Parsing**: `getopts` for command-line arguments.
- **Parameter Validation**: Checks if `p_JobKennung`, `p_Stichtag`, `p_EintragsNr` are set using `pruefeParameterGesetzt`.
- **Date Derivation**: Uses `gestern.ksh` to get yesterday's and today's dates.
- **Date Validation**: `DWDate_Datum_Check` for `DDMMYYYY` format.
- **SQL Execution**: `starteSQLSkript` function executes `d_ausd_bp_ta_cntrct_dist.sql` with numerous parameters.
- **Record Count**: Reads from a temporary file `bert_k_ausd_bp_ta_cntrct_dist.tmp` using `cat`.
- **Error Handling**: Sets `ErrNr`, `ErrArg` and uses `DWMSG_MeldeFehler` for logging and exits.
- **Commented Post-processing**: `sed`, `sort`, `join` commands suggest potential flat file manipulations which are currently inactive.

**Target Logic (BigQuery SQL Stored Procedure)**:
- **Parameter Handling**: The main BigQuery stored procedure `project.dataset.r_ausd_bp_ta_cntrct_dist` will accept input parameters directly.
- **Validation**:
    - `IF ... THEN ... END IF` statements will replace shell conditionals for parameter checks.
    - `ASSERT` statements or explicit `RAISE` will replace error exits.
    - `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` will validate the date format.
- **Date Derivation**: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` will replace `gestern.ksh`.
- **SQL Execution**: The core SQL logic from `d_ausd_bp_ta_cntrct_dist.sql` will be encapsulated in a separate BigQuery stored procedure (e.g., `project.dataset.d_ausd_bp_ta_cntrct_dist_core`). The orchestrating procedure will call this core procedure.
- **Record Count**: `SELECT COUNT(*)` on the target table will replace reading from `tmpFile`. The result will be assigned to a `DECLARE`d variable.
- **Error Logging**: Errors will be logged to a dedicated `project.dataset.error_log` table, and `RAISE` will be used for procedural error handling.
- **Job Logging**: Start/end and status will be recorded in `project.dataset.job_log`.
- **Commented Logic**: If these operations become active, they would be re-implemented using standard BigQuery SQL transformations (e.g., `REGEXP_REPLACE`, `GROUP BY`, `ORDER BY`, `JOIN` clauses on BigQuery tables).

## 6. External Dependencies
The original script does not appear to have direct connections to external systems (like Oracle, SFTP, S3) based on the `external_systems` output being empty. All identified dependencies are other shell scripts or an internal SQL script.

- **Legacy Dependencies**:
    - **Utility KornShell Scripts**: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`.
        - **Replacement**: These utility functions will be re-implemented directly within the BigQuery stored procedure as SQL scripting constructs (conditionals, date functions, logging mechanisms) or by leveraging built-in BigQuery functions. Environment variables (like `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by BigQuery dataset/project references or procedure parameters.
    - **`d_ausd_bp_ta_cntrct_dist.sql`**: This is the core SQL script.
        - **Replacement**: This will be migrated to a dedicated BigQuery SQL script or stored procedure (`project.dataset.d_ausd_bp_ta_cntrct_dist_core`), designed for optimal performance in BigQuery.
    - **Temporary File `bert_k_ausd_bp_ta_cntrct_dist.tmp`**: Used for storing record counts.
        - **Replacement**: This will be replaced by capturing the result of a `COUNT(*)` operation directly into a BigQuery scripting variable within the stored procedure, or by inserting the count into a job audit/log table.

## 7. Unresolved / Risks
- **Unresolved Targets**: The `unresolved_targets` field was empty, suggesting all references were resolved in the original analysis.
- **Complexity of `d_ausd_bp_ta_cntrct_dist.sql`**: The design assumes `d_ausd_bp_ta_cntrct_dist.sql` contains standard SQL that can be directly translated or optimized for BigQuery. If it contains complex procedural logic (e.g., PL/SQL specific features, cursors, loops not easily translatable to BigQuery scripting), further analysis and potential re-design (e.g., Python-based data transformation in Cloud Dataflow/Dataproc or more complex BigQuery UDFs/stored procedures) might be required. This is a potential risk that needs verification of the SQL script's content.
- **Commented-out Logic**: The presence of commented-out `sed`, `sort`, `join` commands indicates inactive but potentially relevant logic. This needs to be reviewed with stakeholders to determine if this functionality is still required. If so, it would involve additional BigQuery SQL transformations.
- **Error Reporting (`DWMSG_MeldeFehler`)**: While a BigQuery error logging table and `RAISE` statements are proposed, the exact integration with existing monitoring or alerting systems needs to be designed.
- **Job Tracking (`FOSJobErzeugeEintrag`)**: The original script commented out `FOSJobErzeugeEintrag`. If job tracking was performed elsewhere or is required, the `job_log` table in BigQuery needs to capture all necessary metadata.

## 8. Build Plan
The migration will involve building the following components:

1.  **BigQuery DDL for Target Tables**:
    *   Create the `PoolBasisprodukt` table in BigQuery, mirroring its original schema but optimized for BigQuery (e.g., partitioning, clustering).
    *   Create `project.dataset.error_log` table for logging errors.
    *   Create `project.dataset.job_log` table for logging job execution details.

2.  **BigQuery Stored Procedure: `project.dataset.d_ausd_bp_ta_cntrct_dist_core` (SQL)**:
    *   Translate the logic of `d_ausd_bp_ta_cntrct_dist.sql` into optimized BigQuery SQL, creating this stored procedure. This procedure will contain the core data transformation.

3.  **BigQuery Stored Procedure: `project.dataset.r_ausd_bp_ta_cntrct_dist` (SQL)**:
    *   Implement the orchestration logic from `k_ausd_bp_ta_cntrct_dist.ksh` as a BigQuery stored procedure. This procedure will include:
        *   Parameter definitions.
        *   Validation logic.
        *   Date derivation.
        *   Calls to `project.dataset.d_ausd_bp_ta_cntrct_dist_core`.
        *   Record counting.
        *   Logging to `error_log` and `job_log`.

4.  **Cloud Composer Airflow DAG (`k_ausd_bp_ta_cntrct_dist_dag.py`) (Python)**:
    *   Create an Airflow DAG to schedule and orchestrate the BigQuery stored procedure execution.
    *   Define operators to call `project.dataset.r_ausd_bp_ta_cntrct_dist` with appropriate parameters.
    *   Implement error handling and retry mechanisms.

5.  **Data Ingestion (if necessary)**:
    *   If `d_ausd_bp_ta_cntrct_dist.sql` reads from external systems or files, design and implement ingestion pipelines (e.g., Cloud Data Transfer, Cloud Storage, Cloud Pub/Sub, Cloud Dataflow) to bring source data into BigQuery.

6.  **Testing**:
    *   Develop unit and integration tests for BigQuery stored procedures and the Airflow DAG to ensure functional equivalence and data integrity.

This plan addresses the full migration of the KSH script, transforming its orchestration and data processing logic into a BigQuery-centric, Cloud Composer-orchestrated solution.