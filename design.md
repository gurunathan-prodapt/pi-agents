# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_v_ta_cntrct_templ.ksh`, serves as a wrapper and orchestration layer for the contract data reconciliation process specifically for the `ta_cntrct_templ` table. Its primary purpose is to set up the execution environment, parse command-line parameters, establish error handling and logging mechanisms, and then invoke a core processing script (`k_ausd_v_ta_cntrct_templ.ksh`) that contains the actual reconciliation logic. The script ensures proper job logging, status tracking, and error reporting for the overall workflow.

## 2. Source Inventory
The job consists of a single KornShell script identified as `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`.

*   **File Name**: `r_ausd_v_ta_cntrct_templ.ksh`
*   **Technology**: KornShell
*   **Category**: shell
*   **Complexity Tier**: medium
*   **Migration Bucket**: semi_auto
*   **Summary**: A KornShell script acting as a wrapper for contract data reconciliation of `ta_cntrct_templ`. It manages environment setup, parameter parsing, error logging, and the invocation of a core processing script.
*   **Core Invoked Script**: This script explicitly invokes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`, which is expected to contain the main business logic for `ta_cntrct_templ` data reconciliation.

## 3. Target Architecture
The target platform is BigQuery. The `r_ausd_v_ta_cntrct_templ.ksh` wrapper script will be migrated to a BigQuery Stored Procedure, `sp_vertragsdatenabgleich`, responsible for orchestration, parameter handling, and audit logging. The core processing logic, currently in `k_ausd_v_ta_cntrct_templ.ksh`, is also expected to be migrated to a separate BigQuery Stored Procedure, `sp_k_ausd_v_ta_cntrct_templ`, which will be invoked by the wrapper procedure.

To support auditing and logging, the following BigQuery tables will be created:
*   `project.dataset.job_registry`: Stores job metadata, status, and control information (e.g., `job_nr`, `job_kennung`, `script_name`, `status`).
*   `project.dataset.job_log`: Stores general informational messages for each job run (e.g., `job_nr`, `job_kennung`, `log_level`, `message`).
*   `project.dataset.job_error_log`: Stores detailed error information (e.g., `job_nr`, `job_kennung`, `err_nr`, `err_arg`, `message`).

Orchestration of this BigQuery Stored Procedure, along with its core counterpart, could be managed by Cloud Composer (Airflow) or BigQuery Scheduled Queries.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_cntrct_templ.ksh` functions as an entry point.
1.  **Initialization**: It sources several utility scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`) to set up the environment and access common functions.
2.  **Parameter Handling**: It processes command-line parameters (e.g., `-h` for help, and implicitly `s`, `l`) using `getopts`. Errors in parameter parsing lead to an exit.
3.  **Logging Setup**: It initializes a job identifier (`JobKennung`, `BERT_V_TA_CNTRCT_TEMPL`), determines a unique job number (`DW_EintragsNr`), and sets up a log file (`LogDatei`). It also defines error trapping mechanisms.
4.  **Core Logic Invocation**: The script's main action is to execute the core script `${Name_Kernskript}` (which resolves to `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`), passing `JobKennung` and `DW_EintragsNr` as parameters. The output of the core script is redirected to the `LogDatei`.
5.  **Status Reporting**: Upon successful completion of the core script, a success message is logged, and the job status is updated. Error traps handle abnormal terminations.

In BigQuery, this flow will be:
1.  The `sp_vertragsdatenabgleich` procedure accepts input parameters (`p_show_help`, `p_param_s`, `p_param_l`).
2.  Logging and auditing details (job ID, start time) are recorded in `project.dataset.job_registry` and `project.dataset.job_log`.
3.  Parameter validation logic will be implemented using `IF` statements.
4.  The procedure then calls the `sp_k_ausd_v_ta_cntrct_templ` procedure, passing necessary execution context (e.g., job ID, identifier).
5.  Error handling uses `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, updating `project.dataset.job_error_log` and `project.dataset.job_registry` with failure details.
6.  Successful completion updates `project.dataset.job_registry` with a success status.

## 5. Transformation Logic
The `r_ausd_v_ta_cntrct_templ.ksh` script itself contains no data transformation logic. Its transformation logic is purely procedural and related to job control and error handling:

*   **Parameter Parsing**: The `getopts` mechanism will be replaced by direct input parameters to the BigQuery Stored Procedure (`p_show_help BOOL`, `p_param_s STRING`, `p_param_l STRING`).
*   **Job Identification**: `ProgName`, `ProgVersion`, `JobKennung`, `DW_EintragsNr`, `v_sysdate` variables will be translated into BigQuery `DECLARE` variables, with `v_sysdate` using `FORMAT_DATE(CURRENT_DATE())`.
*   **Logging Framework (`DWMSG_...`)**: The `DWMSG_*` functions will be replaced by `INSERT` statements into the `project.dataset.job_registry`, `project.dataset.job_log`, and `project.dataset.job_error_log` tables. Logic for generating unique job numbers will be implemented via `SELECT IFNULL(MAX(job_nr), 0) + 1 FROM job_registry`.
*   **External Script Invocation**: The `.${Name_Kernskript}` call will be translated into a `CALL project.dataset.sp_k_ausd_v_ta_cntrct_templ(...)` statement within the BigQuery Stored Procedure.
*   **Error Handling (`trap`)**: The shell `trap` commands will be replaced by BigQuery SQL's `EXCEPTION WHEN ERROR THEN` block, ensuring transactional integrity and proper error logging. Parameter validation errors will use `SIGNAL SQLSTATE`.
*   **Standard Output (`print`, `tee`)**: These will be replaced by `INSERT` statements into the `project.dataset.job_log` table for audit purposes.

## 6. External Dependencies
The original script has several external dependencies:

*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init`: Likely contains environment variables. In BigQuery, these will be replaced by BigQuery project/dataset IDs, runtime configuration parameters, or environment variables in an orchestration layer (e.g., Cloud Composer).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: This is a framework for error messaging. It will be replaced by the BigQuery logging tables (`job_error_log`) and the error handling logic within the Stored Procedure.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility. Replaced by BigQuery Stored Procedure input parameters and validation logic.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility. Replaced by BigQuery SQL date functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Invoked Core Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`: This is the most significant dependency. It must be migrated to a BigQuery Stored Procedure (`sp_k_ausd_v_ta_cntrct_templ`) or a series of SQL queries, depending on its content. The current design assumes it will be another BigQuery Stored Procedure.
*   **External Shell/System Features**: `getopts`, `date`, `trap`, `tee`, `print`, `set -eu`. These are fundamental shell features and will be replaced by their BigQuery SQL equivalents (input parameters, `CURRENT_DATE()`, `EXCEPTION` blocks, `INSERT` into log tables).
*   **External Framework Functions (`DWMSG_*`)**: These are custom logging/error handling functions. They will be entirely replaced by the BigQuery logging table schema and associated `INSERT`/`UPDATE` statements within the Stored Procedure.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_cntrct_templ.ksh`)**: The content and complexity of this core script are unknown. Its migration is critical and will dictate the overall complexity and approach. If it involves complex file I/O, external system interactions (other than databases), or non-SQL logic, it may require migration to a different BigQuery service (e.g., Dataflow, Dataproc, Cloud Functions) or a Python script orchestrated by Cloud Composer. This is the biggest unknown and risk.
*   **Detailed `s` and `l` Parameter Usage**: The original script declares `-s` and `-l` as parameters but their specific usage is not detailed in the wrapper's logic. Their purpose and impact on the core script need to be fully understood to ensure correct migration.
*   **`$HOME/.dw_init` Content**: The exact environment variables and configurations set by `.dw_init` need to be identified and replicated in the BigQuery environment (e.g., as deployment variables, IAM roles, or configuration tables).
*   **Shell-specific Behavior**: Replacing shell features like `tee` (for simultaneous console/file output) and `trap` (for asynchronous error handling) with BigQuery SQL equivalents means a shift in operational paradigm. While functional equivalents exist, the exact behavior and timing might differ, requiring thorough testing.
*   **Performance of Logging**: Frequent `INSERT` statements into logging tables might incur performance overhead. Batching log entries or using BigQuery's streaming inserts might be necessary for high-volume scenarios.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Define BigQuery Schema**:
    *   Create the `project.dataset.job_registry` table.
    *   Create the `project.dataset.job_log` table.
    *   Create the `project.dataset.job_error_log` table.
2.  **Migrate Logging Framework**:
    *   Implement helper BigQuery Stored Procedures or functions for common logging operations if desired, encapsulating the `INSERT` and `UPDATE` logic for the audit tables.
3.  **Migrate Core Script (`k_ausd_v_ta_cntrct_templ.ksh`)**:
    *   **Analyze**: Perform a detailed analysis of `k_ausd_v_ta_cntrct_templ.ksh` to understand its data sources, transformation logic, and dependencies.
    *   **Design**: Design its migration to BigQuery (e.g., as a BigQuery Stored Procedure `sp_k_ausd_v_ta_cntrct_templ`, or a Dataflow/PySpark job if non-SQL logic predominates). This will be a separate design document.
    *   **Build**: Implement the chosen BigQuery solution for `sp_k_ausd_v_ta_cntrct_templ`.
4.  **Migrate Wrapper Script (`r_ausd_v_ta_cntrct_templ.ksh`)**:
    *   **Build BigQuery Stored Procedure**: Create the `project.dataset.sp_vertragsdatenabgleich` procedure based on the provided pseudocode (BigQuery SQL). This will include:
        *   Parameter definitions (`p_show_help`, `p_param_s`, `p_param_l`).
        *   Declaration of internal variables (`v_prog_name`, `v_job_kennung`, etc.).
        *   Logic for parameter validation and error signaling.
        *   Logic for recording job start and status in `job_registry`.
        *   `CALL` statement to invoke `project.dataset.sp_k_ausd_v_ta_cntrct_templ`.
        *   Exception handling for errors during execution.
        *   Final status updates in `job_registry` and `job_log`.
5.  **Orchestration**:
    *   Develop a Cloud Composer DAG or BigQuery Scheduled Query to invoke `project.dataset.sp_vertragsdatenabgleich` with the required parameters.
6.  **Testing**:
    *   Thoroughly test the BigQuery Stored Procedures and the orchestration layer, comparing results and logs against the legacy system.
    *   Verify error handling and parameter passing.