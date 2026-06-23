# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_inv_acc.ksh`, serves as a wrapper script for the data reconciliation process of the `ta_inv_acc` table. Its primary business purpose is to orchestrate the execution of a core reconciliation script (`k_ausd_v_ta_inv_acc.ksh`), handling environment setup, parameter parsing, logging, and error handling for the overall process. The scope of this migration is to convert this KornShell wrapper and its invoked components to run natively on Google Cloud Platform, specifically utilizing BigQuery for data processing and a suitable orchestration mechanism.

## 2. Source Inventory
The job is composed of a single main file: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh`.

*   **File Name**: `r_ausd_v_ta_inv_acc.ksh`
*   **Technology**: KornShell Script
*   **Category**: Shell
*   **Tier**: Medium (estimated, as no explicit `file_complexity` row was found, but `lineage_assembled_jobs` indicated `stage dist: medium=1`).
*   **Automation Bucket**: Semi-Automatic (B2)
*   **Summary**: This ksh script acts as a wrapper for the `ta_inv_acc` table reconciliation, managing environment, parameters, logging, and error handling before executing a core script.

**Internal Dependencies (discovered via lineage and code analysis):**
*   `$HOME/.dw_init` (environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling framework)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh` (core processing script, invoked)
*   `SQL_SCRIPT:D_AUSD_V_TA_INV_ACC.SQL` (SQL script executed by `k_ausd_v_ta_inv_acc.ksh`)

## 3. Target Architecture
The target platform is BigQuery.

*   **Orchestration**:
    *   The wrapper functionality (environment setup, parameter validation, job metadata, logging orchestration, and invocation of the core logic) will be migrated to a BigQuery Stored Procedure.
    *   Alternatively, for more complex scheduling and dependency management, Cloud Composer (Apache Airflow) could be used to orchestrate the BigQuery Stored Procedures.
*   **Core Logic**:
    *   The core reconciliation logic contained within `k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL` will be converted into one or more BigQuery SQL scripts, potentially encapsulated within another BigQuery Stored Procedure.
*   **Logging & Error Handling**:
    *   The custom `DWMSG_*` logging and error handling framework will be replaced by a BigQuery audit log table and an error log table.
    *   BigQuery's native error handling (`EXCEPTION WHEN ERROR`) will replace shell `trap` commands.
    *   Cloud Logging can be integrated for system-level logs from BigQuery.
*   **Parameters**:
    *   Command-line parameters (`-h`, `-s`, `-l`) will be passed as arguments to the BigQuery Stored Procedure.
*   **Data**:
    *   All relational data involved in the reconciliation process will reside in BigQuery tables.

## 4. Data Flow & Lineage
The original data flow is:
1.  **`r_ausd_v_ta_inv_acc.ksh` (Wrapper)**:
    *   Initializes environment variables (via sourcing `.dw_init`, etc.).
    *   Parses input parameters.
    *   Sets up custom logging (`DWMSG_*` functions) and error traps.
    *   Invokes the core script `k_ausd_v_ta_inv_acc.ksh`.
    *   Logs success or failure status.

2.  **`k_ausd_v_ta_inv_acc.ksh` (Core Script)**:
    *   This script is invoked by the wrapper.
    *   It then `EXECUTES_SQL` `SQL_SCRIPT:D_AUSD_V_TA_INV_ACC.SQL`. This implies it's either passing parameters to or directly executing the SQL.

3.  **`SQL_SCRIPT:D_AUSD_V_TA_INV_ACC.SQL` (SQL Logic)**:
    *   This SQL script performs the actual reconciliation logic for the `ta_inv_acc` table. This likely involves `READS` from source tables and `WRITES` to `ta_inv_acc` or related reconciliation/audit tables.

**Target Data Flow (BigQuery):**
1.  **Orchestration (Cloud Composer / BigQuery Stored Procedure)**:
    *   A Cloud Composer DAG (if used) or a master BigQuery Stored Procedure (`project.dataset.Vertragsdatenabgleich`) will initiate the job.
    *   This procedure will handle parameter validation, job ID generation, and log initialization (inserting records into a `job_audit` table).
    *   It will then `CALL` a BigQuery Stored Procedure representing the core reconciliation logic.
    *   Error handling will be managed via `EXCEPTION WHEN ERROR` blocks and updates to the `job_error_log` table.
    *   Final status (OK/FAILED) will be recorded in the `job_audit` table.

2.  **Core Reconciliation Logic (BigQuery Stored Procedure)**:
    *   A BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_inv_acc`) will encapsulate the logic from the original `k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL`.
    *   This procedure will perform `SELECT` operations on source tables (e.g., `source_project.source_dataset.source_table`) and `INSERT`/`UPDATE`/`MERGE` operations on the target `ta_inv_acc` table in BigQuery.
    *   Intermediate logging specific to the core logic will be inserted into the `job_log` table.

## 5. Transformation Logic
*   **`r_ausd_v_ta_inv_acc.ksh` (Wrapper) -> `project.dataset.Vertragsdatenabgleich` (BQ Stored Procedure)**:
    *   **Shell Scripting Constructs**: `if/then/else`, `while getopts`, `case`, `print`, `exit`, `trap` will be translated to BigQuery Scripting statements: `IF`, `DECLARE`, `SET`, `SELECT`, `LEAVE`, `SIGNAL SQLSTATE`, `BEGIN...EXCEPTION...END`.
    *   **Parameter Handling**: `getopts` for `-h`, `-s`, `-l` will become `IN` parameters to the stored procedure. Parameter validation logic will be converted to `IF` conditions.
    *   **Environment Sourcing**: `. $HOME/.dw_init`, etc., will be replaced by direct declarations of variables within the BQ Stored Procedure or by fetching configuration from BigQuery lookup tables.
    *   **Logging (`DWMSG_*` calls)**: Calls like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung` will be replaced by `INSERT` or `UPDATE` statements into dedicated BigQuery logging and auditing tables (`project.dataset.job_audit`, `project.dataset.job_error_log`, `project.dataset.job_log`).
    *   **Core Script Invocation**: The `${Name_Kernskript}` invocation will be replaced by a `CALL` statement to the `project.dataset.k_ausd_v_ta_inv_acc` BigQuery Stored Procedure.
    *   **Date Formatting**: `date +%d%m%YYYY` will be converted to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.

*   **`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL` -> `project.dataset.k_ausd_v_ta_inv_acc` (BQ Stored Procedure / SQL Script)**:
    *   The SQL logic within `D_AUSD_V_TA_INV_ACC.SQL` will be directly translated to BigQuery Standard SQL, ensuring compatibility with data types and functions.
    *   Any shell commands within `k_ausd_v_ta_inv_acc.ksh` that prepare or modify the SQL execution will either be absorbed into the BigQuery Stored Procedure's logic (e.g., variable assignments) or be deemed unnecessary in the new environment.
    *   Data reconciliation logic, typically involving joins, aggregations, and conditional updates/inserts, will be re-implemented using BigQuery's analytical capabilities.

## 6. External Dependencies
The original job reported no `external_systems` or `unresolved_targets`. However, the script itself indicates dependencies on various shell utilities and a custom logging framework.

*   **Custom Shell Utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**: These will be entirely replaced. Environment variables will either be hardcoded (if static), passed as parameters, or fetched from BigQuery configuration tables. Logging and error handling will be managed by BigQuery tables and standard BigQuery scripting.
*   **Shell Runtime Utilities (`getopts`, `date`, `print`, `tee`, `trap`, `typeset -u`)**: These are shell-specific features.
    *   `getopts` will be replaced by BigQuery Stored Procedure parameters and `IF` logic for validation.
    *   `date` will be replaced by `CURRENT_DATE()` and `FORMAT_DATE()`.
    *   `print` and `tee` will be replaced by inserts into logging tables or standard BigQuery output/Cloud Logging.
    *   `trap` will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` blocks.
    *   `typeset -u` for uppercase will be replaced by `UPPER()` function in BigQuery.

## 7. Unresolved / Risks
*   **Unused Parameters (`-s`, `-l`)**: The original script declares these parameters but does not use them. This needs clarification to determine if they are vestigial or if there's a hidden path where they are used. For migration, they will be included as optional parameters in the BQ Stored Procedure but not processed unless their purpose is identified.
*   **`BERT_DIR_ROOT`**: The exact value and contents of this environment variable are not known. It's assumed to point to a directory containing the utility and core scripts. The migration will require mapping these paths to their corresponding BigQuery stored procedures or SQL scripts.
*   **`ta_inv_acc` Table Schema**: The schema of the `ta_inv_acc` table and any other tables involved in `D_AUSD_V_TA_INV_ACC.SQL` are crucial for accurate migration and were not explicitly provided. This will be a necessary input for the SQL conversion.
*   **`DWMSG_*` Framework Details**: While the MCP tool provided a good mapping, the exact logic within the `DWMSG_*` scripts (e.g., how `DW_EintragsNr` is determined, what `DWMSG_SetzeStichtagInfo` does precisely) is not fully detailed. This may require further investigation of those scripts or making informed assumptions based on their names during implementation.
*   **Core Script (`k_ausd_v_ta_inv_acc.ksh`) Functionality**: The content of this script and `D_AUSD_V_TA_INV_ACC.SQL` is the critical piece for understanding the actual data reconciliation logic. This migration document focuses on the wrapper, but the core logic itself will need detailed analysis and conversion to BigQuery SQL.

## 8. Build Plan
The migration will primarily involve translating the KornShell wrapper into a BigQuery Stored Procedure, and the core reconciliation logic into BigQuery SQL.

1.  **Define BigQuery Logging & Audit Tables (BigQuery DDL)**:
    *   `project.dataset.job_audit` (for job start/end, status, metadata)
    *   `project.dataset.job_error_log` (for detailed error messages)
    *   `project.dataset.job_log` (for general informational logging)

2.  **Migrate Core Reconciliation Logic (BigQuery SQL / Stored Procedure)**:
    *   Analyze `k_ausd_v_ta_inv_acc.ksh` and `D_AUSD_V_TA_INV_ACC.SQL`.
    *   Create `project.dataset.k_ausd_v_ta_inv_acc` BigQuery Stored Procedure or a set of BigQuery SQL scripts. This will handle the actual data processing (READS, WRITES to `ta_inv_acc`).

3.  **Migrate Wrapper Script to BigQuery Stored Procedure (BigQuery SQL)**:
    *   Create `project.dataset.Vertragsdatenabgleich` BigQuery Stored Procedure based on the pseudocode provided by the MCP tool.
    *   Implement parameter handling (`p_h`, `p_s`, `p_l`).
    *   Integrate `job_audit`, `job_error_log`, `job_log` for logging and status.
    *   Replace shell environment sourcing with BigQuery-native configuration or variables.
    *   Include error handling (`EXCEPTION WHEN ERROR`) to mirror shell `trap` functionality.
    *   Add the `CALL project.dataset.k_ausd_v_ta_inv_acc()` statement.

4.  **Develop Orchestration (Optional, if using Cloud Composer)**:
    *   Create a Cloud Composer DAG in Python (`r_ausd_v_ta_inv_acc_dag.py`) to schedule and trigger the `project.dataset.Vertragsdatenabgleich` BigQuery Stored Procedure.

5.  **Testing**:
    *   Unit tests for individual BigQuery SQL components.
    *   Integration tests for the entire BigQuery Stored Procedure workflow.
    *   Data validation to ensure reconciliation results match the legacy system.