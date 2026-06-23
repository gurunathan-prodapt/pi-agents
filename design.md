# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

## 1. Purpose & Scope
This migration job focuses on `r_ausd_v_ta_acc_ref.ksh`, a KornShell wrapper script responsible for orchestrating a contract data reconciliation process for the `ta_acc_ref` table. Its primary functions include:
*   Initializing the execution environment.
*   Parsing command-line parameters.
*   Setting up logging and error handling mechanisms.
*   Invoking a core processing script, `k_ausd_v_ta_acc_ref.ksh`.
*   Managing job status and reporting completion.

The scope of this document is limited to the migration of `r_ausd_v_ta_acc_ref.ksh` and its direct responsibilities, acknowledging the invocation of `k_ausd_v_ta_acc_ref.ksh` as a crucial dependency that requires its own separate migration effort.

## 2. Source Inventory
The job is classified as 'medium' complexity and falls into the 'semi_auto' migration bucket, indicating that while some parts can be automated, significant manual intervention and design effort are required.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh`**
    *   **Technology:** KornShell (ksh)
    *   **Category:** Shell script
    *   **Tool:** KornShell
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Semi-Auto
    *   **Purpose:** Orchestration, parameter handling, logging, error handling for contract data reconciliation.

*   **Key Dependencies (identified from code, not `component_files` for this job):**
    *   **`k_ausd_v_ta_acc_ref.ksh`**: The "kernel script" containing the primary business logic for `ta_acc_ref` data reconciliation, invoked by the wrapper. This script was not included in the `component_files` for this job's lineage analysis and will require separate analysis and migration.
    *   **Utility Scripts**:
        *   `$HOME/.dw_init`: Environment initialization.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling helper.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.

## 3. Target Architecture
The target platform is Google BigQuery. The migrated solution will leverage BigQuery's capabilities for data processing, metadata management, and logging.

*   **BigQuery Stored Procedures**: The primary `r_ausd_v_ta_acc_ref.ksh` script will be migrated to a BigQuery stored procedure (e.g., `project.dataset.vertragsdatenabgleich_wrapper`). This procedure will handle parameter intake, basic validation, and orchestration.
*   **BigQuery Audit/Log Tables**: Filesystem-based logging will be replaced by dedicated BigQuery tables (e.g., `project.dataset.job_audit_log`, `project.dataset.job_error_log`).
*   **Core Logic Migration**: The `k_ausd_v_ta_acc_ref.ksh` script's functionality will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.k_ausd_v_ta_acc_ref`) or, if non-SQL logic is dominant, to a Python-based external workflow (e.g., Cloud Functions, Cloud Run).
*   **Orchestration**: Cloud Composer (Airflow), Cloud Workflows, or BigQuery Scheduled Queries will be used to invoke the main BigQuery stored procedure, passing parameters as required.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_acc_ref.ksh` script acts as a coordinator, not directly processing data.
1.  **Start**: The script is initiated, typically by a scheduler.
2.  **Environment Setup**: System-wide initialization scripts are sourced.
3.  **Parameter Intake**: Command-line arguments (`-h`, `-s`, `-l`) are parsed.
4.  **Logging & Error Setup**: A job entry is created, and traps are set for error handling. Logging metadata such as `JobKennung` and `DW_EintragsNr` are generated.
5.  **Core Logic Invocation**: The script then executes `k_ausd_v_ta_acc_ref.ksh`, passing `JobKennung` and `DW_EintragsNr`. The output of `k_ausd_v_ta_acc_ref.ksh` is redirected to the log file.
6.  **Status Update**: Upon completion (success or failure) of `k_ausd_v_ta_acc_ref.ksh`, the wrapper script updates the job status via its internal messaging framework.
7.  **Exit**: The script exits with an appropriate status code.

**BigQuery Data Flow**:
*   The orchestrator (e.g., Cloud Composer) triggers `project.dataset.vertragsdatenabgleich_wrapper`.
*   The wrapper procedure logs job status to `job_audit_log` and `job_error_log` tables.
*   The wrapper procedure calls `project.dataset.k_ausd_v_ta_acc_ref` (the migrated core logic).
*   `k_ausd_v_ta_acc_ref` performs data processing, likely reading from source BigQuery tables (migrated from legacy `ta_acc_ref` source) and writing to target BigQuery tables.
*   Final job status is updated in `job_audit_log`.

## 5. Transformation Logic
The `r_ausd_v_ta_acc_ref.ksh` script itself contains minimal data transformation logic:
*   **Job Identifier Uppercasing**: `typeset -u JobKennung="BERT_V_TA_ACC_REF"`
    *   **Target**: In BigQuery, this can be achieved using `UPPER()` function or by simply defining the variable with the uppercase value.
*   **System Date Formatting**: `v_sysdate=$(date +%d%m%Y)`
    *   **Target**: In BigQuery, this translates to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation**: Basic checks for missing or unknown parameters.
    *   **Target**: Replaced by explicit procedure parameter definitions and `IF/ELSEIF` blocks in BigQuery SQL for validation.

The primary data transformation logic is within `k_ausd_v_ta_acc_ref.ksh`, which is outside the current scope but critical for a complete end-to-end solution. It is assumed that `k_ausd_v_ta_acc_ref.ksh` will involve SQL operations to reconcile contract data.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated no explicit external systems or unresolved targets for *this specific assembled job*. However, analysis of the source code reveals the following dependencies:

*   **Filesystem**: For sourcing `.dw_init` and other utility scripts, and for log file creation.
    *   **Replacement**: BigQuery Audit/Log tables and BigQuery-native environment management.
*   **`k_ausd_v_ta_acc_ref.ksh`**: The core processing script.
    *   **Replacement**: This script needs to be migrated to a BigQuery Stored Procedure or a Python-based external process (Cloud Function/Cloud Run/Dataflow) depending on its internal complexity (SQL vs. non-SQL operations). The `r_ausd_v_ta_acc_ref.ksh` wrapper's migrated version will call this new BigQuery SP or external process.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**: These provide common functionalities.
    *   **Replacement**: Their logic should be re-implemented either directly within the main BigQuery stored procedure or as separate, smaller BigQuery UDFs or helper stored procedures.
*   **Legacy Databases/Sources**: If `k_ausd_v_ta_acc_ref.ksh` interacts with specific legacy databases, those will need to be migrated to BigQuery tables or accessed via external connections (e.g., BigQuery federated queries) if they remain external.

## 7. Unresolved / Risks
*   **`k_ausd_v_ta_acc_ref.ksh` Logic**: The exact business logic and data sources/targets within `k_ausd_v_ta_acc_ref.ksh` are not detailed in this job's lineage. This is the largest unknown and risk. A detailed analysis and migration plan for `k_ausd_v_ta_acc_ref.ksh` is imperative for a successful end-to-end migration.
*   **Shell `trap` Equivalency**: The shell's `trap` command for signal handling (e.g., `INT`, `ERR`) does not have a direct BigQuery SQL equivalent. This will be handled using BigQuery's `BEGIN...EXCEPTION...END` blocks for robust error management and status updates in the audit tables.
*   **Sourcing Shell Scripts**: The `. filename` command for sourcing environment and utility scripts will be replaced by explicit calls to BigQuery stored procedures or integration of their logic directly.
*   **Environment Variables**: Reliance on `$HOME`, `$BERT_DIR_ROOT` will be replaced by procedure parameters, BigQuery variables, or configuration external to the procedure.
*   **Error Codes and Messaging**: The custom `DWMSG_*` error concept needs to be re-implemented using BigQuery's logging tables and error handling mechanisms.

## 8. Build Plan
The migration will proceed in an ordered fashion to ensure foundational components are in place before dependent ones.

1.  **BigQuery Dataset Setup (SQL DDL)**
    *   Create a dedicated BigQuery dataset (e.g., `project.dataset`) for the migrated job.
    *   Create `job_audit_log` and `job_error_log` tables within this dataset to replace filesystem logging. Define schemas for these tables to capture relevant job execution metadata, timestamps, and error details.

2.  **Migrate Core Logic (`k_ausd_v_ta_acc_ref.ksh`) (BQ SQL / Python)**
    *   **Action**: Analyze `k_ausd_v_ta_acc_ref.ksh` in detail.
    *   **Decision**: Determine if it can be purely SQL-based (leading to a BigQuery Stored Procedure) or requires non-SQL components (leading to a Python-based solution on Cloud Functions/Run/Dataflow).
    *   **Output**:
        *   `project.dataset.k_ausd_v_ta_acc_ref` (BigQuery Stored Procedure) OR
        *   Python script for Cloud Functions/Run/Dataflow.

3.  **Migrate Utility Logic (BQ SQL)**
    *   **Action**: Re-implement the core functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` as BigQuery UDFs or small helper stored procedures if they provide standalone reusable logic. Otherwise, integrate their logic directly into the main wrapper procedure.
    *   **Output**: (Optional) `project.dataset.f_alis_msgerr_sp`, `project.dataset.h_alis_parameter_sp`, `project.dataset.h_alis_date_sp`.

4.  **Migrate Wrapper Script (`r_ausd_v_ta_acc_ref.ksh`) (BQ SQL)**
    *   **Action**: Create the main orchestration stored procedure based on the provided BigQuery SQL pseudocode.
    *   **Details**:
        *   Define procedure parameters for `-s` and `-l`.
        *   Implement usage (`-h`) logic.
        *   Integrate parameter validation.
        *   Replace `DWMSG_*` calls with `INSERT` statements into `job_audit_log` and `job_error_log`.
        *   Implement `BEGIN...EXCEPTION...END` blocks for error handling.
        *   Call the migrated `k_ausd_v_ta_acc_ref` BigQuery stored procedure.
    *   **Output**: `project.dataset.vertragsdatenabgleich_wrapper` (BigQuery Stored Procedure).

5.  **Orchestration Layer (Cloud Composer / Workflows)**
    *   **Action**: Develop an orchestration script (e.g., Cloud Composer DAG, Cloud Workflow YAML) to schedule and invoke `project.dataset.vertragsdatenabgleich_wrapper`.
    *   **Details**: Pass necessary parameters, handle scheduling, and monitor execution.
    *   **Output**: Airflow DAG (`.py`) or Cloud Workflow (`.yaml`).

This build plan is ordered to ensure that the core components are in place before the wrapper and orchestration are built around them. The successful migration of `k_ausd_v_ta_acc_ref.ksh` is the critical path.