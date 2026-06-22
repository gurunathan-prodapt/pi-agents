# Migration Design — k_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_bp_ref.ksh`, serves as a control script primarily designed to orchestrate the execution of a core SQL script, `d_ausd_v_ta_bp_ref.sql`. Its main responsibilities include:
- Initializing the execution environment.
- Parsing input parameters (`p_JobKennung` and `p_EintragsNr`).
- Performing error checking and handling.
- Executing the SQL script, `d_ausd_v_ta_bp_ref.sql`, which processes data.
- Managing job entries, including ignoring active jobs, registering the current job, and deactivating older active jobs.
- Capturing the number of processed records from the SQL script's output.

The scope of this migration is to re-implement this KornShell orchestration logic and its invoked SQL processing within the Google BigQuery ecosystem.

## 2. Source Inventory
The primary source file for this job is a KornShell script, which acts as an orchestrator.

| File Path                                                               | Category | Tool      | Tier   | Automation Bucket |
| :---------------------------------------------------------------------- | :------- | :-------- | :----- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh` | shell    | KornShell | medium | semi_auto         |

The script explicitly invokes another critical source file, `d_ausd_v_ta_bp_ref.sql`, which contains the core data manipulation logic.

## 3. Target Architecture
The target architecture will leverage Google BigQuery's capabilities for both data processing and procedural logic.
-   **BigQuery Stored Procedure:** The orchestration logic from `k_ausd_v_ta_bp_ref.ksh`, including parameter handling, validation, error logging, and job status management, will be migrated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`).
-   **Core SQL Logic:** The data processing logic from `d_ausd_v_ta_bp_ref.sql` will be converted into BigQuery SQL. This could either be embedded directly within the main stored procedure or encapsulated in a separate BigQuery stored procedure or view, depending on its complexity and reusability.
-   **Control Tables:** Dedicated BigQuery tables will be established for:
    -   `job_control`: To manage job registrations, active flags, and status updates.
    -   `error_log`: To record any errors encountered during execution.
    -   `process_result`: To store metrics like processed record counts.
-   **Orchestration Layer (Optional):** While the core logic will reside in BigQuery, a lightweight external orchestration layer (e.g., Cloud Composer/Airflow DAG, Cloud Functions, or Cloud Workflows) might be considered if complex external dependencies, advanced scheduling, or external parameter injection are required. This layer would primarily be responsible for invoking the BigQuery Stored Procedure and managing its execution context.

## 4. Data Flow & Lineage
The original data flow involves the KornShell script orchestrating the execution of an Oracle SQL script.

**Original Flow:**
1.  `k_ausd_v_ta_bp_ref.ksh` initializes environment by sourcing several utility KornShell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  Parses command-line parameters (`p_JobKennung`, `p_EintragsNr`).
3.  Validates parameters and handles errors by calling `pruefeParameterGesetzt` and `DWMSG_MeldeFehler`.
4.  Sets up paths for the SQL script `d_ausd_v_ta_bp_ref.sql` and a temporary file.
5.  Invokes `starteSQLSkript` which is responsible for executing `d_ausd_v_ta_bp_ref.sql`. This likely involves connecting to an Oracle database and running the SQL.
6.  The SQL script `d_ausd_v_ta_bp_ref.sql` queries `isbert_schema.dwtk_meldungen` to determine `v_datum` and performs data manipulation. It connects to an external Oracle database named 'Carmen DB' via a DB-Link `@pcrs1`, truncates a local table `sof$ta_bp_ref` using a PL/SQL package call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`), and then inserts data into `sof$ta_bp_ref` from `cds$ta_bp_ref@pcrs1`.
7.  `k_ausd_v_ta_bp_ref.ksh` then reads the record count from a temporary file (populated by `starteSQLSkript`) into `v_records`.

**Target BigQuery Flow:**
1.  An orchestrator (e.g., Cloud Composer) or direct invocation will call the main BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as parameters.
2.  The BigQuery Stored Procedure `r_ausd_vertrag_control` will perform parameter validation using BigQuery scripting `IF` statements.
3.  Error handling will involve `INSERT` statements into the `error_log` table and potentially raising BigQuery exceptions (`SIGNAL SQLSTATE`).
4.  Job management (deactivating old jobs, registering new ones) will be handled by `UPDATE` and `INSERT` statements against the `job_control` table.
5.  The core data processing logic from `d_ausd_v_ta_bp_ref.sql` will be migrated to BigQuery SQL and executed within the stored procedure, possibly calling another nested stored procedure for modularity. The `TRUNCATE` and `INSERT...SELECT` operations will be translated to BigQuery DDL/DML.
6.  Record counts will be determined using BigQuery `COUNT(*)` queries on the target table and stored in the `process_result` table.

## 5. Transformation Logic
The transformation logic exists in two layers: the orchestration logic within the KornShell script and the data manipulation logic within the Oracle SQL script.

**KornShell Logic (`k_ausd_v_ta_bp_ref.ksh`):**
-   **Parameter Parsing (`getopts`):** Replaced by BigQuery Stored Procedure input parameters.
-   **Environment Loading (`.dw_init`, `BERT_DIR_ROOT`, `DW_DIR_UTL`):** Replaced by BigQuery stored procedure parameters, dataset/project configurations, or variables set by the orchestration layer.
-   **Utility Script Calls (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** Their functionalities will be re-implemented directly in BigQuery SQL scripting where applicable. For example, parameter validation routines (`pruefeParameterGesetzt`) become `IF` statements, and error messaging (`DWMSG_MeldeFehler`) becomes `INSERT` into an error log table. The `h_alis_sqlplus.ksh` and `starteSQLSkript` wrapper will be replaced by direct BigQuery DML/DDL execution.
-   **Temporary File Handling:** Reading `v_records` from a temporary file will be replaced by `SELECT COUNT(*)` queries on the BigQuery target table and assigning the result to a BigQuery scripting variable (`DECLARE v_records INT64; SET v_records = ...`).
-   **Job Control:** Updates to an implied job table for activation/deactivation will be translated to `UPDATE` and `INSERT` statements on a BigQuery `job_control` table.

**Oracle SQL Logic (`d_ausd_v_ta_bp_ref.sql`):**
-   **Variable Definitions (`DEFINE v_carmen`, `COLUMN s_datum new_value v_datum noprint`):** The `v_datum` logic (deriving date from `isbert_schema.dwtk_meldungen`) will be translated to a BigQuery `SELECT` query using `MAX(timecreated)` and `FORMAT_DATE('%Y%m%d',...)`. The `v_carmen` DB-Link indicates a cross-database query; this will be handled by configuring external connections or direct data ingestion from the source Oracle system.
-   **SQL*Plus Specific Commands (`prompt`, `START`, `SPOOL`, `WHENEVER SQLERROR`):** These will be replaced by BigQuery standard logging, error handling (e.g., `EXCEPTION` blocks, `SIGNAL SQLSTATE`), and output mechanisms (e.g., `SELECT` for messages, `INSERT` into log tables).
-   **PL/SQL Package Call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bp_ref')`):** This will be translated to a BigQuery `TRUNCATE TABLE` statement. If `DWPA_UTIL_SKRIPT.runstatement` has more complex logic, that logic needs to be extracted and migrated to a BigQuery Stored Procedure or equivalent.
-   **DML (`INSERT INTO ... SELECT ... FROM cds$ta_bp_ref &v_carmen`):** This core data transfer logic will be directly translated to BigQuery DML. The `WHERE` clause conditions involving `insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`, and `bp_ref_ty` will be maintained. The `cds$ta_bp_ref &v_carmen` refers to a source table in the Oracle 'Carmen DB' system. This data will need to be ingested or federated into BigQuery.
-   **`COMMIT;`:** BigQuery DML is typically auto-committed per statement unless explicit transactions are used within scripting.

## 6. External Dependencies
The job has the following external dependencies:

-   **Oracle Database ("Carmen DB"):** The `d_ausd_v_ta_bp_ref.sql` script explicitly uses an Oracle DB-Link `@pcrs1` to query `cds$ta_bp_ref`. This is a critical external source system.
    -   **Replacement Strategy:** Data from `cds$ta_bp_ref` and potentially `isbert_schema.dwtk_meldungen` will need to be ingested into BigQuery. Options include:
        -   **Batch Ingestion:** Using tools like Dataflow, Cloud Data Fusion, or Cloud Storage transfers to regularly load data from Oracle to BigQuery.
        -   **Change Data Capture (CDC):** For near real-time updates, using services like Debezium or GoldenGate to stream changes from Oracle to BigQuery.
        -   **Federated Queries (for read-only, small-scale access):** BigQuery Federated Queries can query external Oracle databases, but this is generally not recommended for large-scale data processing or write operations.
-   **Oracle Stored Procedures/Packages:** The call to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` indicates a dependency on Oracle PL/SQL.
    -   **Replacement Strategy:** The logic within this Oracle package/procedure will need to be re-implemented as a BigQuery Stored Procedure or directly integrated into the main BigQuery SQL logic.
-   **KornShell Utility Scripts:** (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    -   **Replacement Strategy:** As detailed in Section 5, the functionalities of these scripts will be absorbed into the BigQuery Stored Procedure's scripting logic. Environment variables will become procedure parameters or configuration values.

## 7. Unresolved / Risks
-   **`trace.sql.cfg` content:** The SQL script `d_ausd_v_ta_bp_ref.sql` `START`s `../trace.sql.cfg`. The content of this file is unknown and might contain further Oracle-specific settings or commands that need to be migrated.
-   **Complexity of Oracle SQL (`d_ausd_v_ta_bp_ref.sql`):** While the main `INSERT` statement is visible, `d_ausd_v_ta_bp_ref.sql` might contain more complex logic, views, or functions that require detailed analysis and specialized migration.
-   **`starteSQLSkript` Logic:** The actual implementation of `starteSQLSkript` (sourced via `h_alis_sqlplus.ksh`) is unknown. It likely handles the database connection, error handling during SQL execution, and the writing of the record count to the temporary file. This behavior needs to be fully understood to replicate it accurately in BigQuery and the orchestration layer.
-   **Oracle PL/SQL `DWPA_UTIL_SKRIPT`:** The exact logic within `DWPA_UTIL_SKRIPT.runstatement` is unknown. Assuming it's just a simple TRUNCATE, it's straightforward, but if it encapsulates more complex logic, that needs discovery.
-   **Data Latency/Freshness from Oracle:** The choice of data ingestion strategy from the "Carmen DB" will impact data freshness and could introduce latency compared to the existing DB-Link.
-   **Error Code Mapping:** The specific error codes (e.g., `ErrNr=193`, `ErrNr=192`) and their corresponding messages need to be consistently mapped to BigQuery error handling or a centralized logging mechanism.

## 8. Build Plan
The migration will follow these steps:

1.  **Analyze and Migrate `trace.sql.cfg`:** If `../trace.sql.cfg` contains relevant logic, analyze its content and incorporate its BigQuery equivalent into the overall design (e.g., BigQuery session variables or logging setup).
2.  **Schema Definition:**
    -   Create BigQuery datasets and tables for `job_control`, `error_log`, and `process_result`.
    -   Define the target BigQuery schema for `sof$ta_bp_ref` and `cds$ta_bp_ref` (after ingestion).
3.  **Data Ingestion from Oracle:**
    -   Implement a data ingestion pipeline to move data from `cds$ta_bp_ref` and `isbert_schema.dwtk_meldungen` in the Oracle "Carmen DB" to BigQuery. Choose an appropriate method (batch, CDC) based on data volume, frequency, and freshness requirements.
4.  **Migrate Core SQL Logic:**
    -   Translate `d_ausd_v_ta_bp_ref.sql` into BigQuery SQL. This will include:
        -   Converting Oracle-specific syntax (e.g., `NVL`, `TO_CHAR`, `SYSDATE`) to BigQuery equivalents.
        -   Replacing the `TRUNCATE` call via PL/SQL package with a direct BigQuery `TRUNCATE TABLE` statement.
        -   Adapting the `INSERT...SELECT` statement to BigQuery DML, ensuring proper handling of the `&v_datum` variable.
    -   Encapsulate this BigQuery SQL into a separate BigQuery Stored Procedure, if modularity is desired, or embed it directly into the main control procedure.
5.  **Migrate Orchestration Logic:**
    -   Create a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`).
    -   Implement parameter parsing and validation using BigQuery scripting.
    -   Replicate error handling by inserting into the `error_log` table and using `SIGNAL SQLSTATE` for critical errors.
    -   Translate job management logic (`UPDATE`/`INSERT` into `job_control`).
    -   Call the migrated core SQL logic (from step 4).
    -   Implement record counting and storage into the `process_result` table.
6.  **Orchestration Layer (if needed):**
    -   If an external orchestration layer is deemed necessary (e.g., for complex scheduling or external parameter injection), develop a Cloud Composer DAG or Cloud Workflow to call the BigQuery Stored Procedure.
7.  **Testing:**
    -   Develop comprehensive test cases to validate the migrated BigQuery stored procedure and associated data pipelines against the original job's behavior and outputs.