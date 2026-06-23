# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_cntrct_valid.ksh`. This script serves as an orchestration wrapper for a contract data reconciliation process, specifically targeting the `ta_cntrct_valid` table. Its primary functions include environment setup, command-line parameter parsing, logging initialization, and error handling, culminating in the invocation of a core processing script. The job is a single-component workflow with a medium complexity distribution.

## 2. Source Inventory
The job is comprised of a single KornShell script:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`
- **Technology:** KornShell (shell script)
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-automatic (B2)
- **Migration Flags:** None

## 3. Target Architecture
The target platform for this migration is Google BigQuery. The orchestration logic of the KornShell script will be translated into a BigQuery stored procedure, or a series of BigQuery scripting blocks and stored procedures. This will involve:
- **Main Orchestration:** A BigQuery stored procedure (e.g., `sp_r_ausd_v_ta_cntrct_valid`) will replicate the control flow, parameter handling, and invocation logic.
- **Logging and Error Handling:** The existing custom logging and error handling framework (functions starting with `DWMSG_`) will be replaced by dedicated BigQuery stored procedures and audit tables. Error handling will leverage BigQuery's `BEGIN ... EXCEPTION ... END` blocks.
- **Configuration Management:** Environment variables and configuration parameters will be managed through BigQuery procedure parameters, session variables, or dedicated configuration tables.
- **Core Logic:** The actual data reconciliation logic, residing in the invoked core script (`k_ausd_v_ta_cntrct_valid.ksh`), will be migrated separately, likely into BigQuery SQL queries or additional BigQuery stored procedures.
- **Scheduling:** The job's execution will be orchestrated using Google Cloud Composer (Apache Airflow DAG) or Dataform, allowing for scheduled execution and dependency management.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_cntrct_valid.ksh` script acts as an orchestrator and does not directly read from or write to data sources. Its primary action is to invoke a "core script" responsible for the actual data processing:
- **Invocation:** `r_ausd_v_ta_cntrct_valid.ksh` INVOKES `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`.
The data flow and lineage of the actual data reconciliation will depend on the implementation details of `k_ausd_v_ta_cntrct_valid.ksh`, which is outside the scope of this wrapper script's direct data interactions.

## 5. Transformation Logic
The `r_ausd_v_ta_cntrct_valid.ksh` script itself does not contain any direct data transformation or aggregation logic. Its functionality is purely supervisory and involves:
- **Environment Initialization:** Sourcing `.dw_init`.
- **Utility Sourcing:** Including error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), and date utilities (`h_alis_date.ksh`).
- **Parameter Validation:** Using `getopts` to parse command-line arguments and exit with an error if invalid parameters are provided.
- **Logging Setup:** Determining a job entry number, constructing a log file name, and registering job start/status using `DWMSG_` functions.
- **Error Trapping:** Setting up `INT` and `ERR` traps for robust error handling.
- **Core Script Execution:** Executing `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh` with job-specific parameters.
- **Status Reporting:** Logging success or failure messages and updating the job status.

## 6. External Dependencies
No external systems were explicitly identified by the lineage analysis for this specific job run. However, the script relies on several environmental and framework components:
- **Shell Utilities:** `getopts`, `date`, `tee`, `print`. These will be replaced by equivalent BigQuery SQL scripting constructs (`DECLARE`, `SET`, `FORMAT_DATE`, `SELECT`, `INSERT`).
- **External KornShell Scripts/Framework Functions:**
    - `$HOME/.dw_init`: Environment initialization.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error message handling.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
    - `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`: Custom logging and status management functions.
    These will be migrated to BigQuery stored procedures or user-defined functions (UDFs) to replicate their functionality.
- **Core Processing Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh`. This script is the main dependency and its migration is critical.

## 7. Unresolved / Risks
No unresolved targets were explicitly identified by the lineage analysis for this specific job run. However, the migration presents the following considerations and risks:
- **Core Script (`k_ausd_v_ta_cntrct_valid.ksh`) Complexity:** The content and complexity of the invoked core script are currently unknown. Its analysis and migration will be a separate, crucial phase that will determine the overall effort for the data reconciliation logic. This is the primary reason for the "semi_auto" migration bucket.
- **Shell Trap Equivalence:** Direct replication of shell `trap` commands for `INT` and `ERR` signals in BigQuery is not possible. Equivalent error handling will require careful design using BigQuery's procedural `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks and custom logging mechanisms.
- **Environment Variable Management:** The sourcing of environment scripts like `.dw_init` implies a reliance on specific shell environments. These will need to be translated into BigQuery-compatible configuration management (e.g., table lookups, stored procedure parameters).
- **Custom Framework Porting:** The `DWMSG_*` functions are custom and will require bespoke BigQuery stored procedures or UDFs to ensure continuity of logging and status reporting.

## 8. Build Plan
The migration build plan will proceed in the following order:

1.  **Design and Implement BigQuery Logging and Status Framework (BQSQL Stored Procedures):**
    *   Create BigQuery tables for job logging and audit trails.
    *   Develop stored procedures for `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` that interact with the new audit tables.

2.  **Migrate `r_ausd_v_ta_cntrct_valid.ksh` to BigQuery Stored Procedure (BQSQL):**
    *   Create a BigQuery stored procedure, e.g., `CREATE OR REPLACE PROCEDURE `project.dataset.sp_r_ausd_v_ta_cntrct_valid`(p_job_kennung STRING, p_eintrags_nr INT64)`.
    *   Translate the parameter parsing logic (from `getopts`) into input parameters for the stored procedure.
    *   Implement environment variable usage via configuration tables or stored procedure parameters.
    *   Integrate calls to the newly developed `DWMSG_*` BigQuery stored procedures for logging and status updates.
    *   Implement the error handling logic using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks to mimic shell traps.

3.  **Analyze and Design Core Script (`k_ausd_v_ta_cntrct_valid.ksh`) Migration:**
    *   Perform a separate, detailed analysis of the core script to understand its data sources, transformations, and targets. This will likely involve further use of migration tools.
    *   Develop BigQuery SQL queries, stored procedures, or Python/PySpark jobs (if complex logic necessitates) to replicate its functionality.

4.  **Integrate Core Logic into Wrapper Stored Procedure (BQSQL):**
    *   Once the core script is migrated (e.g., to `sp_k_ausd_v_ta_cntrct_valid`), update `sp_r_ausd_v_ta_cntrct_valid` to call this new BigQuery component.

5.  **Develop Cloud Composer DAG (Python):**
    *   Create an Airflow DAG in Python that orchestrates the execution of the `sp_r_ausd_v_ta_cntrct_valid` BigQuery stored procedure.
    *   Define scheduling, dependency management, and error handling within the DAG.

This phased approach ensures that the foundational orchestration and error handling are established before tackling the potentially more complex data transformation logic within the core script.