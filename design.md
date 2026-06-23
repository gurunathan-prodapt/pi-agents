# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

## 1. Purpose & Scope
This shell script, `r_ausd_v_ta_period.ksh`, serves as an orchestration wrapper for a core process responsible for synchronizing contract data within the `ta_period` table. Its primary functions include:
*   Initializing the execution environment.
*   Parsing and validating command-line parameters (`-h`, `-s`, `-l`).
*   Setting up a robust logging and error handling framework using a set of sourced utility scripts.
*   Invoking a core business logic script, `k_ausd_v_ta_period.ksh`, passing relevant job metadata.
*   Tracking and updating the job's status (started, success, error) in a control mechanism.
*   Outputting job-related information and status messages to both console and a dynamically named log file.

The scope of this migration focuses on translating this KornShell wrapper script and its orchestration logic to BigQuery SQL stored procedures and supporting tables, while acknowledging that the core business logic within `k_ausd_v_ta_period.ksh` would require a separate, detailed migration effort.

## 2. Source Inventory
This job consists of a single primary source file, a KornShell script, acting as an orchestrator.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Automatic (B2)
    *   **Summary:** Orchestrates the execution of a core script for synchronizing contract data in the 'ta_period' table, handling parameter parsing, error logging, and job status reporting.
    *   **Inferred Internal Dependencies (from script content):**
        *   **Sourced Environment/Utilities:**
            *   `$HOME/.dw_init` (environment initialization)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging framework)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling utilities)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
        *   **Invoked Core Script:**
            *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh` (The actual data synchronization logic is presumed to be within this script, which this wrapper calls.)

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform (GCP) services, with BigQuery as the central data warehousing and processing engine.

*   **BigQuery Stored Procedures:**
    *   `project.dataset.vertragsdatenabgleich_wrapper`: This will be the main stored procedure, replacing the `r_ausd_v_ta_period.ksh` wrapper. It will handle parameter parsing, job control, logging, and invocation of the core data synchronization logic.
    *   `project.dataset.k_ausd_v_ta_period`: This represents the migrated core business logic script. It will be another stored procedure (or a set of procedures/SQL statements) responsible for the actual data synchronization in the `ta_period` table. The details of this are outside the scope of this specific wrapper migration but are assumed to exist.
*   **BigQuery Tables for Job Control and Logging:**
    *   `project.dataset.job_control`: An audit table to track the status and metadata of each job execution (equivalent to the job entry management in the shell script). Columns will include `job_entry_nr`, `job_name`, `script_name`, `log_file_name` (as a logical identifier), `stichtag`, `stichtag_format`, `status`, `created_ts`, `finished_ts`.
    *   `project.dataset.job_log`: A detailed log table to store output messages, including job start/end messages and messages from the core script (replacing the file-based log). Columns will include `job_name`, `job_entry_nr`, `log_message`, `created_ts`.
    *   `project.dataset.job_error_log`: A table to capture detailed error information for failed job runs. Columns will include `job_name`, `job_entry_nr`, `error_nr`, `error_arg`, `error_message`, `created_ts`.
*   **Orchestration (Optional, for external scheduling/triggering):**
    *   Cloud Composer (Managed Airflow): Could be used to schedule and orchestrate the BigQuery stored procedure calls, especially if there are dependencies on external systems or other GCP services.
    *   Cloud Functions/Cloud Workflows: Could trigger the main BigQuery stored procedure based on events or schedules.

## 4. Data Flow & Lineage
The original shell script orchestrates execution, parameter handling, and logging. The migrated flow in BigQuery will mimic this structure.

*   **Execution Trigger:** The `project.dataset.vertragsdatenabgleich_wrapper` stored procedure will be the entry point, potentially invoked by a scheduler (e.g., Cloud Composer, or a BigQuery scheduled query).
*   **Parameter Handling:** Input parameters (`p_s`, `p_l`, `p_help`) will be passed directly to the stored procedure. Validation logic will be translated from `getopts` to BigQuery SQL conditional statements.
*   **Job Initialization & Logging:**
    1.  The wrapper SP will determine a new `job_entry_nr` by querying `project.dataset.job_control`.
    2.  It will insert a new record into `project.dataset.job_control` with `status = 'STARTED'`.
    3.  Job metadata and messages will be inserted into `project.dataset.job_log`.
    4.  `stichtag` information will be updated in `project.dataset.job_control`.
*   **Core Logic Invocation:** The wrapper SP will `CALL` the `project.dataset.k_ausd_v_ta_period` stored procedure, passing necessary parameters like `JobKennung` and `DW_EintragsNr`.
*   **Error Handling:**
    *   BigQuery's `EXCEPTION WHEN ERROR THEN` blocks will replace shell `trap` commands for `ERR` and `INT`.
    *   Error details will be inserted into `project.dataset.job_error_log`.
    *   The `project.dataset.job_control` table will be updated with `status = 'ERROR'`.
*   **Success Handling:**
    *   Upon successful completion of the core procedure, success messages will be inserted into `project.dataset.job_log`.
    *   The `project.dataset.job_control` table will be updated with `status = 'OK'` and a `finished_ts`.
*   **Data Interaction:** The `project.dataset.k_ausd_v_ta_period` (core) stored procedure is expected to perform the actual `READS` from and `WRITES` to the `ta_period` table (or its BigQuery equivalent).

## 5. Transformation Logic
The transformation will focus on re-implementing the shell script's control flow, parameter handling, and logging mechanisms using BigQuery SQL scripting capabilities.

*   **Environment Variables:**
    *   Shell variables like `$HOME`, `$BERT_DIR_ROOT`, `ProgName`, `ProgVersion`, `JobKennung`, `ErrNr`, `ErrArg`, `LogDatei`, `DW_EintragsNr` will be mapped to `DECLARE`d variables within the BigQuery stored procedure.
    *   External configuration (`.dw_init`) will be replaced by configuration parameters passed to the procedure, or potentially by a BigQuery configuration table.
*   **Parameter Parsing (`getopts`):**
    *   Translated into `IF` conditions at the beginning of the BigQuery stored procedure to validate input parameters (`p_s`, `p_l`, `p_help`).
    *   Error codes (`192`, `193`) will be translated to custom `SIGNAL SQLSTATE` errors and logged.
*   **Date Generation (`date +%d%m%Y`):**
    *   Replaced with BigQuery SQL functions like `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Utility Script Calls (`.` or `source`):**
    *   Utility functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) will be replaced by direct SQL `INSERT` or `UPDATE` statements into the `job_control`, `job_log`, and `job_error_log` tables, or by calls to other small utility stored procedures if common logic warrants it.
*   **Conditional Logic (`if [ ! $ErrNr -eq 0 ]`):**
    *   Directly mapped to BigQuery `IF ErrNr != 0 THEN ... END IF;` constructs.
*   **Command Execution (`${Name_Kernskript} -j ...`):**
    *   The invocation of `k_ausd_v_ta_period.ksh` will be replaced by a `CALL project.dataset.k_ausd_v_ta_period(JobKennung, DW_EintragsNr);` statement.
*   **Logging (`print`, `tee -a`, `>> $LogDatei`):**
    *   All console and file output will be converted to `INSERT` statements into the `project.dataset.job_log` table.
*   **Error Trapping (`trap INT ERR`):**
    *   Replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` block, allowing for centralized error handling, logging to `job_error_log`, and setting the job status to `ERROR`.

## 6. External Dependencies
The `lineage_assembled_jobs` query showed no external systems for this job. However, the script itself makes several references:

*   **Environment Initialization (`$HOME/.dw_init`):** This file is likely to contain environment variables or functions. In BigQuery, this will be replaced by:
    *   Passing parameters to stored procedures.
    *   Using BigQuery datasets/project IDs directly.
    *   Potentially, a separate configuration table in BigQuery.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These provide logging, parameter, and date utilities. Their functionality will be directly integrated into the main `vertragsdatenabgleich_wrapper` stored procedure using BigQuery SQL scripting capabilities or by dedicated, smaller BigQuery helper procedures.
*   **Core Business Logic Script (`k_ausd_v_ta_period.ksh`):** This is the most significant dependency. It's an internal dependency and its migration needs to be treated as a separate, but linked, project. It will be replaced by a BigQuery stored procedure (or a set of SQL scripts) that performs the actual data synchronization on the `ta_period` table. The current wrapper design assumes this core component will also be migrated to BigQuery.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_period.ksh`) Logic:** The migration design for the wrapper assumes that the core script, `k_ausd_v_ta_period.ksh`, will also be migrated to BigQuery stored procedures or SQL. The complexity and specific logic within this core script are unknown and represent a significant unresolved item. If `k_ausd_v_ta_period.ksh` involves non-SQL operations, external system interactions not yet identified, or complex procedural logic, those will require dedicated analysis and a potential redesign (B4) to Python/PySpark on Dataflow or Dataproc, orchestrated by Cloud Composer.
*   **Exact `DWMSG_` Functionality:** The precise implementation details of the `DWMSG_` functions (e.g., `DWMSG_MeldeFehler`) are not fully known from the wrapper script alone. The migration assumes they primarily interact with a logging/control database. If they perform more complex operations (e.g., sending emails, invoking other external processes), these will need to be identified and handled through GCP services like Cloud Pub/Sub, Cloud Functions, or Cloud Composer.
*   **Shell Trap Equivalence:** While `BEGIN...EXCEPTION` blocks provide robust error handling in BigQuery, they are not a direct 1:1 replacement for shell `trap` mechanisms, especially for `INT` (interrupt) signals. The BigQuery design ensures logical error handling and status updates but relies on the invocation environment (e.g., Cloud Composer) to handle job interruption signals gracefully.
*   **Configuration Management:** The `.dw_init` file is currently sourced. The migration suggests using BigQuery configuration tables or procedure parameters. A clear strategy for managing these configurations across environments (dev, test, prod) needs to be defined.

## 8. Build Plan
The build plan will focus on generating the BigQuery components.

1.  **Define BigQuery Dataset:** Create a dedicated BigQuery dataset (e.g., `project.dataset`) to house the migrated stored procedures and control tables.
2.  **Create Control and Log Tables:**
    *   Generate DDL for `project.dataset.job_control`.
    *   Generate DDL for `project.dataset.job_log`.
    *   Generate DDL for `project.dataset.job_error_log`.
    *   **Language:** BigQuery DDL
3.  **Migrate Wrapper Logic to BigQuery Stored Procedure:**
    *   Translate the `r_ausd_v_ta_period.ksh` script into the `project.dataset.vertragsdatenabgleich_wrapper` BigQuery stored procedure. This includes parameter handling, environment variable translation, `DWMSG_` function replacements with SQL inserts/updates, and the `CALL` to the core procedure.
    *   **Language:** BigQuery SQL (Scripting)
4.  **Migrate Core Business Logic (`k_ausd_v_ta_period.ksh`):**
    *   *(Dependent on separate analysis and design for `k_ausd_v_ta_period.ksh`)*
    *   Generate one or more BigQuery stored procedures or SQL scripts that encapsulate the data synchronization logic. This will be named `project.dataset.k_ausd_v_ta_period`.
    *   **Language:** BigQuery SQL (DML, DDL, Scripting)
5.  **Integrate with Orchestration (Optional):**
    *   If external scheduling or complex multi-step workflows are required, create an Airflow DAG in Cloud Composer to schedule the `project.dataset.vertragsdatenabgleich_wrapper` stored procedure.
    *   **Language:** Python (for Airflow DAG)

**Ordered List of Files to Generate:**

1.  `project.dataset.job_control.sql` (DDL for job_control table)
2.  `project.dataset.job_log.sql` (DDL for job_log table)
3.  `project.dataset.job_error_log.sql` (DDL for job_error_log table)
4.  `project.dataset.vertragsdatenabgleich_wrapper.sql` (BigQuery SQL stored procedure for the wrapper)
5.  `project.dataset.k_ausd_v_ta_period.sql` (BigQuery SQL stored procedure for the core logic - *placeholder, requires further design*)
6.  `airflow_dag_vertragsdatenabgleich.py` (Optional Airflow DAG for orchestration)