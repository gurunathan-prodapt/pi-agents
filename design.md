# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_c_bfc.ksh`, is a KornShell control script designed to orchestrate the execution of a SQL script (`d_ausd_v_ta_c_bfc.sql`) for data processing related to the `ta_c_bfc` table. Its primary purpose is to handle parameter validation, manage job statuses (ignoring active jobs, deactivating old ones, and recording new executions), and execute the core SQL logic. The script also includes error reporting and environment setup. The scope of this migration is to re-implement this orchestration and data processing logic within the Google Cloud BigQuery ecosystem.

## 2. Source Inventory
The job consists of a single KornShell script:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`
    *   **Technology**: KornShell
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **Complexity Tier**: medium
    *   **Migration Bucket**: semi_auto
    *   **File Purpose**: This is a control script that orchestrates the execution of a SQL script (d_ausd_v_ta_c_bfc.sql) for data processing. It handles parameter validation, job status checks, and error reporting, ensuring active jobs are managed and old ones deactivated.

## 3. Target Architecture
The target architecture will leverage Google Cloud BigQuery for data processing and storage. The orchestration logic currently implemented in the KornShell script will be migrated to BigQuery Stored Procedures, utilizing BigQuery's native scripting capabilities.

*   **BigQuery Stored Procedures**: The main orchestration logic, including parameter parsing, validation, job status management (activating/deactivating jobs), and calling the core data processing logic, will be encapsulated within a BigQuery Stored Procedure.
*   **BigQuery Tables**: All source and target data will reside in BigQuery tables. This includes the `ta_c_bfc` table and any tables used for job logging/metrics.
*   **External Orchestration (Optional)**: While the core orchestration will be in BigQuery Stored Procedures, a lightweight external orchestrator (e.g., Cloud Composer/Airflow DAG or Cloud Scheduler) might be used to trigger the top-level BigQuery Stored Procedure, replacing the initial shell script invocation.

## 4. Data Flow & Lineage
The original process flow is as follows:
1.  `k_ausd_v_ta_c_bfc.ksh` (KornShell script) is invoked, likely by an upstream scheduler (`r_ausd_v_ta_c_bfc.ksh`).
2.  The script sources several helper KornShell scripts for environment setup, error handling, date utilities, parameter parsing, and SQL*Plus interaction.
3.  It parses command-line parameters (`p_JobKennung`, `p_EintragsNr`).
4.  It performs validation of these parameters.
5.  The script then executes a SQL script, `d_ausd_v_ta_c_bfc.sql`, via a wrapper function `starteSQLSkript`. This SQL script is responsible for the core data processing and is expected to interact with the `ta_c_bfc` table.
6.  A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_c_bfc_$$.tmp`) is used to capture the record count from the SQL execution.
7.  The record count is read back into a shell variable `v_records`.

In the BigQuery target architecture:
1.  An external orchestrator (e.g., Airflow DAG) will invoke the main BigQuery Stored Procedure (`r_ausd_ta_c_bfc`).
2.  The `r_ausd_ta_c_bfc` Stored Procedure will handle parameter validation.
3.  It will then call another BigQuery Stored Procedure (`d_ausd_v_ta_c_bfc`) that contains the migrated SQL logic. This procedure will interact with BigQuery tables, including `ta_c_bfc` (or its BigQuery equivalent).
4.  Job status updates and record counts will be handled directly within BigQuery Stored Procedures, potentially writing to dedicated BigQuery logging/metrics tables.

## 5. Transformation Logic

The KornShell script orchestrates the execution of a SQL script and handles various control flow aspects. The transformation to BigQuery will involve converting these shell-specific constructs into BigQuery SQL and stored procedure logic.

**Key Transformation Areas:**

*   **Parameter Parsing and Validation**:
    *   **Legacy**: Uses `getopts` for command-line parameter parsing (`-j` for `p_JobKennung`, `-f` for `p_EintragsNr`). Validation functions like `pruefeParameterGesetzt` are used.
    *   **Target**: Parameters will be passed directly as arguments to a BigQuery Stored Procedure. Validation logic will be implemented using `IF` statements and `SIGNAL SQLSTATE` for error handling within the Stored Procedure.

*   **Environment Setup**:
    *   **Legacy**: Sourcing of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.
    *   **Target**: BigQuery Stored Procedures are self-contained. Environment variables and helper functions will be replaced by BigQuery functions, direct SQL operations, or embedded logic. Error handling (`DWMSG_MeldeFehler`) will be replaced by BigQuery's error handling mechanisms.

*   **SQL Script Execution**:
    *   **Legacy**: The `starteSQLSkript` function executes `d_ausd_v_ta_c_bfc.sql` via SQL*Plus.
    *   **Target**: The content of `d_ausd_v_ta_c_bfc.sql` will be migrated to a separate BigQuery Stored Procedure (e.g., `d_ausd_v_ta_c_bfc`). The main orchestration procedure will `CALL` this data processing procedure.

*   **Temporary File for Record Count**:
    *   **Legacy**: A temporary file (`tmpFile`) is used to store and retrieve the number of processed records.
    *   **Target**: Record counts can be obtained directly from the `INSERT ... SELECT` or `UPDATE` statements using `ROW_COUNT()`, or by performing a `COUNT(*)` query on the target table. This can be assigned to a `DECLARE`d variable within the Stored Procedure and optionally persisted to a logging table.

*   **Job Status Management**:
    *   **Legacy**: Comments indicate logic to ignore active jobs and deactivate old active jobs. This implies interaction with a job tracking system, possibly within the `starteSQLSkript` or `d_ausd_v_ta_c_bfc.sql`.
    *   **Target**: This logic will be implemented using BigQuery DML statements (e.g., `UPDATE`, `INSERT`) against a dedicated job tracking table in BigQuery.

## 6. External Dependencies
The original script has the following external dependencies:

*   **Database**: The script interacts with an Oracle database via `sqlplus` wrapper scripts, executing the `d_ausd_v_ta_c_bfc.sql` script which operates on the `ta_c_bfc` table.
*   **Environment Files**: Relies on `$HOME/.dw_init` and various `.ksh` helper scripts under `${BERT_DIR_ROOT}/allgemein/is/util/bin/`.
*   **Temporary Files**: Uses a temporary file in `$DW_DIR_UTL`.

**Migration Strategy for External Dependencies**:

*   **Database**: The Oracle database will be replaced by Google BigQuery. The `ta_c_bfc` table and any other tables referenced by `d_ausd_v_ta_c_bfc.sql` will be migrated to BigQuery. The SQL within `d_ausd_v_ta_c_bfc.sql` will need to be translated from Oracle SQL to BigQuery SQL.
*   **Environment Files & Helper Scripts**: These shell-specific dependencies will be eliminated. Their functionalities will be integrated into the BigQuery Stored Procedures:
    *   Environment variables will be replaced by BigQuery procedure parameters or BigQuery specific configurations.
    *   Error handling will use BigQuery's native error handling.
    *   Date utilities will use BigQuery's date and time functions.
    *   Parameter parsing and validation will be handled as described in Section 5.
*   **Temporary Files**: The use of temporary files for record counts will be replaced by BigQuery's ability to count rows directly from table operations or explicit `SELECT COUNT(*)` queries.

There are no external systems like SFTP, S3, or other databases explicitly referenced in the `external_systems` analysis for this job.

## 7. Unresolved / Risks
*   **SQL Translation Complexity**: The core data processing logic resides in `d_ausd_v_ta_c_bfc.sql`. The complexity of translating this SQL from Oracle to BigQuery SQL is currently unknown, as the content of this SQL file was not analyzed in detail. Oracle-specific functions, data types, or query patterns may require significant refactoring.
*   **Job Management Logic**: The exact implementation of "ignoring active jobs" and "deactivating old active jobs" is not fully detailed in the provided KSH script comments. It's assumed this logic is within the `starteSQLSkript` function or `d_ausd_v_ta_c_bfc.sql`. This logic needs to be fully understood and accurately re-implemented in BigQuery Stored Procedures, potentially requiring a dedicated job metadata table.
*   **Upstream/Downstream Dependencies**: The script is invoked by `r_ausd_v_ta_c_bfc.ksh`. The migration of this upstream dependency needs to be considered to ensure proper triggering of the new BigQuery job.

## 8. Build Plan
The migration will follow these steps:

1.  **Translate `d_ausd_v_ta_c_bfc.sql` to BigQuery SQL**:
    *   Analyze the content of `d_ausd_v_ta_c_bfc.sql` to understand its data processing logic, table dependencies, and Oracle-specific syntax.
    *   Convert the Oracle SQL to BigQuery-compliant SQL, creating a BigQuery Stored Procedure (e.g., `dataset.d_ausd_v_ta_c_bfc`). This will involve mapping data types, functions, and query structures.
    *   **Language**: BigQuery SQL.

2.  **Create BigQuery Stored Procedure for Orchestration (`r_ausd_ta_c_bfc`)**:
    *   Develop a BigQuery Stored Procedure (e.g., `dataset.r_ausd_ta_c_bfc`) that replicates the orchestration logic of `k_ausd_v_ta_c_bfc.ksh`.
    *   Implement parameter handling, validation, and error reporting using BigQuery scripting.
    *   Integrate job status management (checking for active jobs, updating job status) using DML statements against a BigQuery job tracking table.
    *   The procedure will call the `dataset.d_ausd_v_ta_c_bfc` Stored Procedure.
    *   Record counting will be managed internally within the BigQuery Stored Procedures.
    *   **Language**: BigQuery SQL.

3.  **Define BigQuery Tables**:
    *   Create the target `ta_c_bfc` table in BigQuery with appropriate schema and data types.
    *   Create any necessary job logging/metrics tables in BigQuery.
    *   **Language**: BigQuery DDL.

4.  **Implement External Orchestration (if necessary)**:
    *   If direct BigQuery job scheduling is insufficient, create a Cloud Composer (Airflow) DAG or Cloud Scheduler job to trigger the `dataset.r_ausd_ta_c_bfc` BigQuery Stored Procedure.
    *   **Language**: Python (for Airflow DAGs) or Cloud Scheduler configuration.

5.  **Testing**:
    *   Unit test individual BigQuery Stored Procedures.
    *   Integration test the entire data flow, from external trigger to data processing and job status updates.
    *   Validate data accuracy and completeness against the legacy system.