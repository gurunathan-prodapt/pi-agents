# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_vertrag_tmp.ksh`. The primary purpose of this script is to act as a wrapper and orchestration layer for a contract data reconciliation job involving the `ta_vertrag_tmp` table. It handles environment setup, command-line parameter parsing, error handling, logging, and then invokes a core processing script (`k_ausd_v_ta_vertrag_tmp.ksh`). The scope of this migration is to re-implement this orchestration logic on the Google Cloud Platform, specifically using BigQuery for the primary processing and data storage, and potentially Cloud Composer or Workflows for orchestration if external calls are required.

## 2. Source Inventory
The job consists of a single source file:
*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`
*   **Technology**: KornShell
*   **Complexity Tier**: medium
*   **Automation Bucket**: semi_auto
*   **Summary**: This script initializes the environment, manages parameters, sets up logging, and orchestrates the execution of a core script. It does not contain direct data transformation logic.

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedure**: A BigQuery Stored Procedure, `sp_vertragsdatenabgleich`, will replace the KornShell wrapper script. This procedure will encapsulate the parameter handling, logging, and invocation of the migrated core logic.
*   **Audit Log Table**: A BigQuery table, e.g., `project.dataset.job_audit_log`, will replace the filesystem-based logging. This table will store job start/end events, error messages, and status updates.
*   **Core Logic Stored Procedure**: The core processing logic currently within `k_ausd_v_ta_vertrag_tmp.ksh` will be migrated into a separate BigQuery Stored Procedure, e.g., `project.dataset.sp_k_ausd_v_ta_vertrag_tmp`. The wrapper procedure will `CALL` this core procedure.
*   **Orchestration (Optional)**: If the core script (`k_ausd_v_ta_vertrag_tmp.ksh`) contains logic that cannot be fully translated into BigQuery SQL (e.g., external system interactions), Cloud Composer (Airflow) or Cloud Workflows might be used to orchestrate the BigQuery stored procedures and any necessary external components (e.g., Cloud Functions/Run for specific Python logic).

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_vertrag_tmp.ksh` acts as an orchestrator.
*   **Legacy Flow**:
    1.  `r_ausd_v_ta_vertrag_tmp.ksh` starts.
    2.  It sources various utility scripts for environment setup, parameter handling, and error logging (e.g., `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`).
    3.  It parses command-line parameters.
    4.  It initializes a custom error/logging framework (`DWMSG_*` functions).
    5.  It invokes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh` with specific parameters (`-j`, `-f`).
    6.  It captures the output and status of the invoked script and updates its internal logging.
    7.  It exits with a success or error code.

*   **Target Flow (BigQuery)**:
    1.  The `project.dataset.sp_vertragsdatenabgleich` BigQuery Stored Procedure is executed.
    2.  It initializes variables, handles input parameters (replacing shell `getopts`).
    3.  It records job start and metadata in `project.dataset.job_audit_log`.
    4.  It `CALL`s the `project.dataset.sp_k_ausd_v_ta_vertrag_tmp` procedure, passing necessary parameters.
    5.  It records the success or failure status and messages into `project.dataset.job_audit_log`.
    6.  Error handling is managed via BigQuery's `BEGIN...EXCEPTION...END` blocks and `SIGNAL SQLSTATE`.

## 5. Transformation Logic
The transformation logic for `r_ausd_v_ta_vertrag_tmp.ksh` focuses on re-implementing its orchestration capabilities in BigQuery SQL:
*   **Shell Script to BigQuery Stored Procedure**: The entire shell script will be converted into a BigQuery Stored Procedure `sp_vertragsdatenabgleich`.
*   **Parameter Handling**: `getopts` logic will be replaced with procedure input parameters (`IN p_h STRING`, etc.). Validation will use `IF` conditions.
*   **Environment Variables/Sourcing**: Sourcing external `.ksh` files like `.dw_init` and utility scripts will be replaced by direct BigQuery SQL constructs or pre-defined settings. The `BERT_DIR_ROOT` variable will be mapped to appropriate BigQuery dataset/project paths or passed as parameters.
*   **Logging and Error Handling**:
    *   `DWMSG_*` functions will be replaced by `INSERT` statements into the `project.dataset.job_audit_log` table.
    *   `trap` mechanisms will be replaced by BigQuery's `BEGIN...EXCEPTION...END` blocks.
    *   `exit` codes will be replaced by successful procedure completion or `SIGNAL SQLSTATE` for errors.
*   **Core Script Invocation**: The `k_ausd_v_ta_vertrag_tmp.ksh` call will be replaced by a `CALL` statement to its migrated BigQuery Stored Procedure equivalent, `sp_k_ausd_v_ta_vertrag_tmp`.
*   **Date Operations**: `date +%d%m%Y` will be replaced by BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.

## 6. External Dependencies
The original script has several implicit external dependencies:
*   **Environment Initialization (`.dw_init`)**: This will be addressed by ensuring BigQuery environment variables or procedure parameters provide the necessary context.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)**: The functionality of these scripts (error messaging, parameter processing, date functions) will be absorbed directly into the BigQuery Stored Procedure using native SQL functions or custom logic.
*   **Core Processing Script (`k_ausd_v_ta_vertrag_tmp.ksh`)**: This is the most significant dependency. Its migration is essential and will likely result in a separate BigQuery Stored Procedure, `sp_k_ausd_v_ta_vertrag_tmp`, which will contain the actual data reconciliation logic.
*   **Filesystem Logging**: The shell script's output redirection `>> $LogDatei 2>&1` and `tee -a $LogDatei` will be replaced by inserting records into the `project.dataset.job_audit_log` table.

No explicit external systems like Oracle, SFTP, or S3 were directly identified as being used by this wrapper script. Any such dependencies would reside within the core script.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_vertrag_tmp.ksh`)**: The detailed migration design for this core processing script is not part of this document and remains an unresolved item. Its complexity and any specific data sources/targets will need separate analysis and migration.
*   **Dynamic Invocation**: The original lineage analysis did not explicitly capture the `INVOKES` edge to `k_ausd_v_ta_vertrag_tmp.ksh`, likely due to the dynamic resolution of `Name_Kernskript`. This highlights a limitation in static analysis that needs to be compensated for by code review.
*   **Shell-Specific Features**: Direct equivalents for shell-specific features like `trap` for signal handling or the `tee` command (for simultaneously printing to console and file) are not directly available in BigQuery SQL. These have been addressed by mapping to BigQuery's error handling and audit logging mechanisms.
*   **Parameter `s:` and `l:`**: The script declares `-s` and `-l` as valid `getopts` parameters but does not use them. This is a minor point but notes a potential for unused or incomplete functionality in the original script.

## 8. Build Plan
The migration will involve the following steps:
1.  **Define BigQuery Dataset**: Create the target BigQuery dataset (e.g., `project.dataset`).
2.  **Create Audit Log Table**: Develop and deploy the DDL for the `job_audit_log` table in BigQuery.
    *   **Language**: BigQuery DDL
3.  **Develop Core Stored Procedure**: Migrate the logic of `k_ausd_v_ta_vertrag_tmp.ksh` into a BigQuery Stored Procedure (`sp_k_ausd_v_ta_vertrag_tmp`). This will be the next migration task.
    *   **Language**: BigQuery SQL
4.  **Develop Wrapper Stored Procedure**: Create the BigQuery Stored Procedure `sp_vertragsdatenabgleich` based on the provided pseudocode, integrating parameter handling, audit logging, and the call to `sp_k_ausd_v_ta_vertrag_tmp`.
    *   **Language**: BigQuery SQL
5.  **Deployment**: Deploy the BigQuery Stored Procedures.
6.  **Orchestration Integration**: If external orchestration is chosen (e.g., Cloud Composer), configure the DAG to schedule and invoke `sp_vertragsdatenabgleich`.
    *   **Language**: Python (for Airflow DAGs)
7.  **Testing**: Thoroughly test the BigQuery Stored Procedures and the end-to-end workflow to ensure functional equivalence and performance.