# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope

This document outlines the migration design for the `r_ausd_v_ta_cntrct_valid.ksh` KornShell script. This script functions as a wrapper and orchestration component for a contract data reconciliation process, specifically targeting the `ta_cntrct_valid` table. Its primary responsibilities include environment initialization, parameter parsing, logging setup, error handling, and the invocation of a core business logic script (`k_ausd_v_ta_cntrct_valid.ksh`). The migration aims to re-platform this orchestration logic to Google Cloud's BigQuery environment, with an emphasis on using BigQuery Stored Procedures or a Cloud Composer (Airflow) DAG for orchestration. The actual data transformation logic, contained within the core script, is considered a separate migration unit that would feed into this orchestration layer.

## 2. Source Inventory

This job is composed of a single primary source file:

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`
    *   **Technology**: KornShell (ksh)
    *   **Summary**: This script acts as an orchestrator. It sets up the execution environment, initializes a custom logging/error handling framework, parses command-line arguments, and then executes a separate core KSH script, `k_ausd_v_ta_cntrct_valid.ksh`, which is presumed to contain the actual data reconciliation logic. It manages job logging and status updates based on the outcome of the core script.
    *   **Complexity Tier (Inferred)**: Medium
    *   **Migration Flags (Inferred)**: Custom logging framework, Orchestration/Wrapper script, External script invocation (specifically a core KSH script), Parameter parsing, Custom error handling.
    *   **Automation Bucket (Inferred)**: B2 (Semi-Automated) - The wrapper logic itself can be semi-automatically converted to a BigQuery Stored Procedure or Airflow DAG, but the core script's migration needs separate assessment.

## 3. Target Architecture

The target architecture on Google Cloud Platform (GCP) for this job will primarily leverage BigQuery for data processing and a BigQuery-native orchestration mechanism.

*   **Orchestration**: The current `r_ausd_v_ta_cntrct_valid.ksh` wrapper will be re-implemented as:
    *   **Option 1 (Preferred)**: A BigQuery Stored Procedure, encapsulating the parameter handling, logging, and invocation of the core logic (which would also be migrated to a BigQuery Stored Procedure or BigQuery SQL). This leverages BigQuery's native capabilities for scheduling and execution.
    *   **Option 2 (Alternative for complex cases)**: A Cloud Composer (Apache Airflow) DAG, especially if the core `k_ausd_v_ta_cntrct_valid.ksh` script involves more than just SQL operations (e.g., file movements, external API calls) that cannot be directly translated to BigQuery SQL. This DAG would orchestrate the execution of BigQuery jobs.
*   **Data Processing**: The core business logic currently in `k_ausd_v_ta_cntrct_valid.ksh` will be migrated to BigQuery SQL and/or BigQuery Stored Procedures.
*   **Logging & Monitoring**:
    *   The custom `DWMSG_` logging framework will be replaced by:
        *   BigQuery logging tables: A dedicated `job_log` table will store job metadata, status, and error messages.
        *   Google Cloud Logging: Standard output/error from BigQuery jobs or Cloud Composer tasks will be captured and centralized in Cloud Logging.
*   **Parameter Management**: Command-line parameters will be translated to BigQuery Stored Procedure arguments or Airflow DAG parameters. Environment variables from `.dw_init` will be managed via GCP Secret Manager or configuration stored in BigQuery datasets/tables, accessible at runtime.

## 4. Data Flow & Lineage

The current script acts as an orchestrator, so its data flow is primarily control flow rather than data transformation.

**Current (Legacy) Data Flow:**

1.  **Wrapper Execution**: `r_ausd_v_ta_cntrct_valid.ksh` starts.
2.  **Environment Setup**: Sources `$HOME/.dw_init` for environment variables.
3.  **Framework Sourcing**: Sources `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` for custom functions (e.g., `DWMSG_`).
4.  **Parameter Parsing**: Parses command-line arguments.
5.  **Logging Initialization**: Uses `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag` to set up job logging.
6.  **Core Script Invocation**: Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh` with job parameters, redirecting output to the log file.
7.  **Status Update**: Upon completion, uses `DWMSG_SetzeStatusOK` or `DWMSG_Fehlerbehandlung` to update job status.

**Target (BigQuery) Data Flow:**

1.  **Orchestration Trigger**: A Cloud Scheduler job or a parent Airflow DAG triggers the BigQuery Stored Procedure (e.g., `project.dataset.BERT_V_TA_CNTRCT_VALID`).
2.  **Procedure Execution**: The BigQuery Stored Procedure initializes logging entries in a `project.dataset.job_log` table.
3.  **Core Logic Invocation**: The wrapper procedure calls another BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_cntrct_valid`) which contains the actual reconciliation logic. This core procedure will read from source tables (e.g., `source_system.ta_contracts`) and write/update target tables (e.g., `dw_staging.ta_cntrct_valid`).
4.  **Status & Error Handling**: The wrapper procedure handles exceptions from the core logic, updates the `project.dataset.job_log` table with success or failure status and error messages.
5.  **Cloud Logging**: All BigQuery job logs are automatically captured by Cloud Logging.

## 5. Transformation Logic

The `r_ausd_v_ta_cntrct_valid.ksh` script itself does not contain any direct data transformation logic. Its "transformation" is one of control flow and metadata management.

**Mapping of Wrapper Logic (Legacy KSH -> Target BigQuery Stored Procedure):**

*   **Environment Sourcing (`.dw_init`)**:
    *   **Legacy**: Shell sources environment file for variables.
    *   **Target**: Configuration values will be passed as Stored Procedure parameters, retrieved from GCP Secret Manager, or stored in BigQuery configuration tables.
*   **Utility Script Sourcing (`f_alis_msgerr.ksh`, etc.)**:
    *   **Legacy**: Shell sources common functions for logging, error handling, date utilities.
    *   **Target**: These functions will be re-implemented directly within the BigQuery Stored Procedure or as separate helper Stored Procedures/UDFs (e.g., for date formatting). Custom error logging will use the `job_log` table and BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.
*   **Parameter Parsing (`getopts`)**:
    *   **Legacy**: Shell `getopts` for command-line arguments.
    *   **Target**: BigQuery Stored Procedure input parameters (e.g., `IN p_job_kennung STRING`, `IN p_eintragsnr INT64`).
*   **Logging Initialization (`DWMSG_*`)**:
    *   **Legacy**: Custom shell functions manage job numbers, log file names, and write entries.
    *   **Target**: Dedicated BigQuery SQL statements to insert/update rows in the `project.dataset.job_log` table. Job IDs can be generated using BigQuery sequences (if available) or by querying `MAX(job_nr) + 1`.
*   **Core Script Invocation (`${Name_Kernskript}`)**:
    *   **Legacy**: Executes another KSH script.
    *   **Target**: Calls a separate BigQuery Stored Procedure (e.g., `CALL project.dataset.k_ausd_v_ta_cntrct_valid(...)`) that implements the core data reconciliation logic.
*   **Error Trapping (`trap`)**:
    *   **Legacy**: Shell `trap` commands for `INT` and `ERR` signals.
    *   **Target**: BigQuery Stored Procedure `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error handling.
*   **Success/Failure Status (`DWMSG_SetzeStatusOK`)**:
    *   **Legacy**: Custom function to update job status.
    *   **Target**: `UPDATE` statements on the `project.dataset.job_log` table to reflect job status (`OK`, `ERROR`).

## 6. External Dependencies

The `lineage_assembled_jobs` record indicated no explicit external systems. However, analysis of the source code reveals dependencies on a custom framework.

*   **`$HOME/.dw_init`**: This file is likely a shell script that sets up environment variables.
    *   **Legacy**: Provides environment-specific settings.
    *   **Target Replacement**: These settings will be managed within GCP. Sensitive information should be stored in **Secret Manager**. Other configuration can be passed as parameters to the BigQuery Stored Procedure or stored in dedicated BigQuery configuration tables.
*   **Custom `DWMSG_` Utility Scripts**: (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)
    *   **Legacy**: A suite of KornShell scripts providing logging, error reporting, and date utilities specific to the legacy environment.
    *   **Target Replacement**: Re-implement equivalent functionality using BigQuery SQL. This includes:
        *   **Logging**: Direct inserts/updates to a BigQuery `job_log` table.
        *   **Error Reporting**: BigQuery's `EXCEPTION` handling and populating `error_message` in the `job_log` table.
        *   **Date Utilities**: BigQuery's built-in date functions (e.g., `FORMAT_DATE`, `CURRENT_DATE()`).
*   **Core Script (`k_ausd_v_ta_cntrct_valid.ksh`)**: This is the most significant internal dependency.
    *   **Legacy**: A separate KornShell script containing the actual contract reconciliation business logic.
    *   **Target Replacement**: This script requires its own migration design. It will likely be translated into a BigQuery Stored Procedure or a series of BigQuery SQL statements. Its input/output will interact directly with BigQuery tables.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_v_ta_cntrct_valid.ksh`) Logic**: The detailed logic of the core script is currently unknown. Its complexity (e.g., shell commands, external calls, data manipulation) will heavily influence the overall migration effort. If it involves complex file I/O, external system interactions, or non-SQL logic, it may require a Python/PySpark implementation on Dataproc or Cloud Functions instead of pure BigQuery SQL. This is the **primary unresolved item and risk**.
*   **Full `DWMSG_` Framework Scope**: While the wrapper uses several `DWMSG_` functions, the full extent and complexity of the legacy logging/error framework are not fully known. There might be additional functionalities or reporting mechanisms that need to be replicated or replaced.
*   **Environment Variables from `.dw_init`**: The specific content and criticality of variables defined in `$HOME/.dw_init` are unknown. These need to be analyzed to ensure proper translation to GCP configuration management.
*   **Parameter Usage (`-s`, `-l`)**: The script parses `-s` and `-l` but does not use them directly. Their intended use in the core script `k_ausd_v_ta_cntrct_valid.ksh` needs to be understood and accounted for in the target BigQuery stored procedure parameters.

## 8. Build Plan

The build plan focuses on the wrapper/orchestration component. The core script's migration requires a separate, detailed design.

**Phase 1: Foundation & Logging (BigQuery SQL)**

1.  **Design `job_log` Table Schema**: Define a BigQuery table schema for capturing job execution metadata (job ID, job name, start/end time, status, error messages, parameters).
    *   **Language**: BigQuery DDL
    *   **Output**: `job_log_table_ddl.sql`
2.  **Implement Logging Utility Stored Procedures**: Create helper procedures for logging activities, mimicking `DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`.
    *   **Language**: BigQuery SQL
    *   **Output**: `log_utils_sp.sql` (e.g., `create_job_entry_sp`, `update_job_status_sp`)

**Phase 2: Wrapper Stored Procedure (BigQuery SQL)**

1.  **Translate Wrapper to BigQuery Stored Procedure**: Convert the `r_ausd_v_ta_cntrct_valid.ksh` orchestration logic into a BigQuery Stored Procedure. This procedure will handle parameters, call logging utilities, and invoke a placeholder for the core logic.
    *   **Language**: BigQuery SQL
    *   **Output**: `r_ausd_v_ta_cntrct_valid_wrapper_sp.sql` (e.g., `CREATE OR REPLACE PROCEDURE project.dataset.BERT_V_TA_CNTRCT_VALID(...)`)

**Phase 3: Core Logic Integration (Placeholder)**

1.  **Define Core Logic Placeholder**: Create a placeholder BigQuery Stored Procedure for `k_ausd_v_ta_cntrct_valid.ksh` to allow the wrapper to be tested. This procedure will initially contain a simple `SELECT 1;` or a basic error simulation.
    *   **Language**: BigQuery SQL
    *   **Output**: `k_ausd_v_ta_cntrct_valid_placeholder_sp.sql`

**Phase 4: Orchestration (Cloud Scheduler / Composer)**

1.  **Cloud Scheduler (Simple Trigger)**: If the job is to be simply scheduled, configure a Cloud Scheduler job to trigger the BigQuery Stored Procedure.
    *   **Language**: GCP Deployment Manager / Terraform / gcloud commands
    *   **Output**: `scheduler_config.yaml` / `scheduler_script.sh`
2.  **Cloud Composer DAG (Advanced Orchestration)**: If more complex dependency management or non-BigQuery tasks are needed, develop an Apache Airflow DAG in Cloud Composer.
    *   **Language**: Python
    *   **Output**: `r_ausd_v_ta_cntrct_valid_dag.py`

**Phase 5: Environment Configuration**

1.  **Secret Manager Integration**: Store sensitive parameters (if any) from `.dw_init` in Secret Manager.
    *   **Language**: gcloud commands / Terraform
    *   **Output**: `secret_manager_config.sh`
2.  **IAM & Service Accounts**: Configure appropriate IAM roles and service accounts for BigQuery job execution and Secret Manager access.
    *   **Language**: gcloud commands / Terraform
    *   **Output**: `iam_config.sh`

This plan assumes a separate, concurrent migration effort for the actual data reconciliation logic within `k_ausd_v_ta_cntrct_valid.ksh`.