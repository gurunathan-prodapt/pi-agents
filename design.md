# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_bp_ta_bpr_instance.ksh`. This script acts as an orchestration layer for an SQL data preparation job. Its primary responsibilities include:
*   Loading the runtime environment and helper libraries.
*   Parsing and validating command-line parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   Validating the format of the `p_Stichtag` parameter.
*   Deriving "today" and "yesterday" dates.
*   Invoking an SQL script (`d_ausd_bp_ta_bpr_instance.sql`) to perform data transformations.
*   Reading the number of processed records from a temporary file (`bert_k_ausd_bp_ta_bpr_instance.tmp`).
*   Potentially creating a job-tracking entry in a job table (currently commented out).

The scope of this migration is to translate the functionality of this shell script and its invoked SQL into the Google Cloud Platform, specifically leveraging BigQuery for data processing and potentially Cloud Composer (Airflow) or other orchestration services for the control flow.

## 2. Source Inventory
The job consists of a single primary source file, `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`
    *   **Technology**: KornShell Script
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **Summary**: Orchestrates the execution of an SQL script for data preparation, handling parameter parsing, validation, and environment setup.
    *   **Complexity Tier**: medium
    *   **Migration Bucket**: semi_auto
    *   **Purpose**: Pipeline Orchestrator, Environment Bootstrapper, Data Validator, Monitor Logger.
    *   **Key Dependencies**:
        *   `.dw_init` (environment initialization)
        *   `f_alis_msgerr.ksh` (error handling)
        *   `h_alis_date.ksh` (date validation)
        *   `h_alis_parameter.ksh` (parameter parsing/validation)
        *   `h_alis_sqlplus.ksh` (SQL*Plus wrapper)
        *   `gestern.ksh` (date derivation)
        *   `d_ausd_bp_ta_bpr_instance.sql` (core SQL transformation logic)
        *   `PoolBasisprodukt` (database table, likely target of SQL operations)
        *   `bert_k_ausd_bp_ta_bpr_instance.tmp` (temporary file for record count)

## 3. Target Architecture
The migrated solution will primarily reside within Google Cloud Platform, utilizing the following components:

*   **BigQuery**: This will be the target for all data storage and SQL-based transformations. The `d_ausd_bp_ta_bpr_instance.sql` logic will be translated into BigQuery SQL and encapsulated within a BigQuery Stored Procedure. The `PoolBasisprodukt` table will be recreated in BigQuery.
*   **Cloud Composer (Airflow)** or **Cloud Workflows**: An orchestration service will be used to manage the control flow previously handled by the KornShell script. This will involve:
    *   Parsing and validating parameters.
    *   Calling the BigQuery Stored Procedure.
    *   Handling date derivation.
    *   Managing logging and error handling.
*   **Cloud Logging / Monitoring**: For centralized logging and monitoring of the migrated jobs.
*   **Service Accounts**: To manage authentication and authorization for BigQuery and orchestration services.

## 4. Data Flow & Lineage
The original data flow orchestrated by `k_ausd_bp_ta_bpr_instance.ksh` is as follows:

1.  **Environment Setup**: The script sources various helper `.ksh` scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) to set up its execution environment, error handling, date functions, and SQL*Plus interaction.
2.  **Parameter Input**: Command-line arguments (`-j`, `-f`, `-s`, `-l`) are parsed into variables (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
3.  **Parameter Validation**: Essential parameters (`p_JobKennung`, `p_Stichtag`, `p_EintragsNr`) are checked for presence. The `p_Stichtag` is validated for `DDMMYYYY` format using `DWDate_Datum_Check`.
4.  **Date Derivation**: The `gestern.ksh` script is executed to derive `p_datum_heute` and `p_datum_gestern`.
5.  **SQL Execution**: The core data transformation logic is within `d_ausd_bp_ta_bpr_instance.sql`, which is executed via the `starteSQLSkript` function (presumably a wrapper around SQL*Plus). This SQL script reads from and writes to various tables, with `PoolBasisprodukt` identified as a key output target.
6.  **Record Count**: After the SQL execution, the script reads the processed record count from `bert_k_ausd_bp_ta_bpr_instance.tmp`.
7.  **Job Tracking (Commented)**: A call to `FOSJobErzeugeEintrag` for job tracking is present but commented out.

**Target Data Flow (Conceptual):**

1.  **Orchestrator (e.g., Airflow DAG)**:
    *   Receives input parameters.
    *   Performs parameter validation (or delegates to BigQuery Stored Procedure).
    *   Derives "today" and "yesterday" dates.
    *   Invokes the BigQuery Stored Procedure for `r_ausd_bp_ta_bpr_instance`.
    *   Captures the return value for processed records (if exposed by the BQ SP).
    *   Manages logging to Cloud Logging.
2.  **BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_instance`)**:
    *   Accepts parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`, `v_datum_heute`, `v_datum_gestern`).
    *   Performs in-procedure validation (if not done by orchestrator).
    *   Calls another BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_bpr_instance_core`) which encapsulates the translated `d_ausd_bp_ta_bpr_instance.sql` logic.
    *   Handles record counting (e.g., via an `OUT` parameter or by reading from a dedicated logging/staging table).
    *   Optionally inserts into a BigQuery job tracking table.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_bp_ta_bpr_instance.sql`, which will need to be fully translated to BigQuery SQL syntax. This SQL script is responsible for operations on the `PoolBasisprodukt` table.

The orchestration logic from the KornShell script will be transformed as follows:

*   **Parameter Handling**: The `getopts` logic will be replaced by parameters passed directly to the orchestrator (e.g., Airflow DAG parameters) and then mapped to parameters of the BigQuery Stored Procedure.
*   **Environment Sourcing**: The `.dw_init` and other sourced helper scripts will be replaced by native BigQuery functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`) or Python functions within the orchestration layer.
*   **Date Derivation (`gestern.ksh`)**: This will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
*   **SQL Execution (`starteSQLSkript`)**: This will be replaced by a direct call to the BigQuery Stored Procedure via the orchestration tool.
*   **Temporary File (`bert_k_ausd_bp_ta_bpr_instance.tmp`)**: The record count mechanism will be replaced by either an `OUT` parameter from the BigQuery Stored Procedure or by the procedure writing the count to a BigQuery logging/staging table that the orchestrator can query.
*   **Error Handling**: The `DWMSG_MeldeFehler` mechanism will be replaced by standard error handling within the BigQuery Stored Procedure (e.g., `RAISE USING MESSAGE`) and the orchestration framework's logging capabilities.

## 6. External Dependencies
The original script has dependencies on several local shell scripts and an implicit database system (likely Oracle, given the SQL*Plus wrapper).

*   **Local Shell Scripts**:
    *   `.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
    *   **Replacement**: These will be replaced by native BigQuery SQL functions, BigQuery Stored Procedure logic, or Python functions within the orchestration layer (e.g., Airflow tasks).
*   **SQL Script**: `d_ausd_bp_ta_bpr_instance.sql`
    *   **Replacement**: This SQL script will be fully translated into BigQuery SQL and implemented as a BigQuery Stored Procedure or a Dataform model.
*   **Database**: Implicit Oracle database (via `h_alis_sqlplus.ksh` and interaction with tables like `PoolBasisprodukt`).
    *   **Replacement**: All data will reside in BigQuery. Existing Oracle tables will be migrated to BigQuery tables.
*   **Temporary File**: `bert_k_ausd_bp_ta_bpr_instance.tmp`
    *   **Replacement**: Record counts will be handled internally by BigQuery Stored Procedures (e.g., `OUT` parameters) or stored in a temporary BigQuery table.

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_bp_ta_bpr_instance.sql`**: The specific SQL logic within this file is not detailed. If it contains complex, proprietary, or highly optimized Oracle-specific SQL, its translation to BigQuery SQL might be challenging and require significant refactoring.
*   **`starteSQLSkript` Function**: The exact implementation of `starteSQLSkript` and what it passes to SQL*Plus is unknown. This wrapper might handle connection details, error codes, or other environment-specific configurations that need to be replicated or replaced in the BigQuery context.
*   **`FOSJobErzeugeEintrag` / `FOSJobDeaktivate`**: These commented-out functions suggest an existing job management or tracking system. If these become active later or represent a critical part of the wider system, a BigQuery equivalent (e.g., a logging table or a Cloud Function) would need to be implemented.
*   **Error Numbering**: The script uses specific error numbers (`ErrNr=193`, `ErrNr=192`). A mapping or a new error handling strategy in GCP would be needed if this is critical for downstream systems.
*   **External System Integration**: No explicit external systems (like SFTP, S3, other databases) were identified for *this specific job*. However, the overall legacy environment might have such integrations that need to be considered in a broader migration plan.
*   **Character Encoding**: The comment `Andre Lbbers` suggests potential encoding issues (``). This should be addressed during migration to ensure correct character representation in BigQuery.

## 8. Build Plan
The migration will follow these steps to build the target components:

1.  **BigQuery Schema Definition (SQL)**:
    *   Define the schema for `PoolBasisprodukt` and any other tables `d_ausd_bp_ta_bpr_instance.sql` interacts with in BigQuery.
    *   **Language**: BigQuery DDL
2.  **Translate `d_ausd_bp_ta_bpr_instance.sql` to BigQuery Stored Procedure (SQL)**:
    *   Translate the core SQL logic into a BigQuery Stored Procedure, e.g., `project.dataset.d_ausd_bp_ta_bpr_instance_core`.
    *   **Language**: BigQuery SQL
3.  **Develop BigQuery Orchestration Stored Procedure (SQL)**:
    *   Create a BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_bp_ta_bpr_instance`, to handle parameter validation, date derivation, calling `d_ausd_bp_ta_bpr_instance_core`, and record counting.
    *   **Language**: BigQuery SQL
4.  **Develop Orchestration Layer (Python)**:
    *   Create an Airflow DAG (for Cloud Composer) or a Cloud Workflow definition.
    *   This component will parse command-line arguments, pass them to the BigQuery Stored Procedure, handle high-level error reporting, and schedule the execution.
    *   **Language**: Python (for Airflow DAG) or YAML (for Cloud Workflows)
5.  **Implement Logging and Monitoring**:
    *   Integrate with Cloud Logging for all script output and errors.
    *   Set up Cloud Monitoring alerts as needed.
    *   **Language**: Configuration (YAML/JSON)
6.  **IAM and Access Control**:
    *   Configure GCP IAM roles and service accounts with appropriate permissions for BigQuery datasets and the orchestration service.
    *   **Language**: Configuration (YAML/JSON)