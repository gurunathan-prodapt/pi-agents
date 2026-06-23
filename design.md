# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_rn_da_vda_tk.ksh`, is a control script designed to orchestrate an ETL process. Its primary purpose is to:
- Initialize the execution environment.
- Parse and validate input parameters, including a job identifier, entry number, and a key date (`Stichtag`).
- Perform date format validation.
- Execute a core SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) which contains the main data processing logic.
- Capture the record count resulting from the SQL execution.
- It also contains commented-out sections for potential file-based data reformatting, deduplication, and joining, indicating a historical or alternative data processing path.
The overall job purpose is described as "Job assembled from 1 component(s); stage dist: medium=1".

## 2. Source Inventory
The job consists of a single primary source file, `k_ausd_bp_ta_rn_da_vda_tk.ksh`, which is a KornShell script.
- **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`
- **Technology**: KornShell (`ksh`)
- **Category**: shell
- **Purpose**: Control script that initializes the environment, parses parameters, performs date validation, and orchestrates the execution of a core SQL script for data processing.
- **Complexity Tier**: medium
- **Migration Bucket**: semi_auto (Automation Rate: 65%)
- **Estimated Effort**: medium

**Key components referenced by the script:**
- **Shell Scripts (sourced utilities)**:
    - `.dw_init` (environment setup)
    - `f_alis_msgerr.ksh` (error handling)
    - `h_alis_date.ksh` (date functions)
    - `h_alis_parameter.ksh` (parameter parsing)
    - `h_alis_sqlplus.ksh` (SQL*Plus invocation helper)
    - `gestern.ksh` (derives yesterday's and today's date)
- **SQL Script (core logic)**:
    - `d_ausd_bp_ta_rn_da_vda_tk.sql`
- **Database Table**:
    - `PoolBasisprodukt` (likely read from or written to by the SQL script)

## 3. Target Architecture
The target platform is Google BigQuery. The current shell script orchestrating an SQL script will be migrated to a BigQuery Stored Procedure written in BQSQL, leveraging BigQuery's scripting capabilities for control flow, parameter handling, and error management.

- **Orchestration**: The shell script's orchestration logic (parameter parsing, validation, date derivation, calling the SQL script, record counting, and logging) will be refactored into a BigQuery Stored Procedure.
- **Data Processing**: The core SQL logic from `d_ausd_bp_ta_rn_da_vda_tk.sql` will be translated into BQSQL and embedded either directly within the main stored procedure or as a separate, invoked stored procedure/query.
- **Logging and Error Handling**:
    - `DWMSG_MeldeFehler` will be replaced with BigQuery's `INSERT INTO project.dataset.error_log` and `SIGNAL SQLSTATE` for error reporting.
    - Job status updates (currently commented out `FOSJobErzeugeEintrag`) will be implemented as `INSERT INTO project.dataset.job_table`.
- **Date Handling**: Shell-based date calculations will be replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB`).
- **Temporary Data**: The use of a temporary file (`tmpFile`) for record counting will be replaced by direct `COUNT(*)` queries on the target BigQuery table within the stored procedure.
- **File Processing (if activated)**: If the currently commented-out `sed`, `sort`, and `join` operations become active, they would be re-implemented using standard BQSQL functions and constructs (e.g., `REPLACE`, `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`, `JOIN`).
- **Scheduling**: The overall job execution will be managed by a cloud-native scheduler like Cloud Composer (Airflow), Cloud Workflows, or Cloud Scheduler.

## 4. Data Flow & Lineage
Due to a lack of explicit `lineage_edges` records for this specific job, the data flow is inferred from the script's code.

The high-level data flow is:
1. **Input Parameters**: The job receives input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) via command-line arguments.
2. **Environment Setup & Utilities**: The script sources various utility scripts to set up the environment, handle errors, parse parameters, and prepare for SQL execution. It also derives "yesterday" and "today" dates using `gestern.ksh`.
3. **Validation**: Parameters and the `Stichtag` date are validated. Errors lead to early exit.
4. **Core SQL Execution**: The script invokes an external SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) using `starteSQLSkript`. This SQL script is expected to perform the primary data transformations and write to target tables.
5. **Record Counting**: The number of processed records is captured (implicitly from the SQL script's output, currently via a temporary file).
6. **Logging/Auditing**: A job entry is intended to be created (currently commented out) in a job-tracking table, and errors are logged via `DWMSG_MeldeFehler`.

**Inferred Data Interactions:**
- **Reads**: Input parameters, potentially `PoolBasisprodukt` table (via `d_ausd_bp_ta_rn_da_vda_tk.sql`).
- **Writes**: Temporary file (`tmpFile`), error log, and job table. The `d_ausd_bp_ta_rn_da_vda_tk.sql` script is the main writer of processed data to BigQuery tables.

## 5. Transformation Logic
The transformation logic is primarily contained within the `d_ausd_bp_ta_rn_da_vda_tk.sql` script, which is executed by this shell wrapper. Without the contents of the SQL script, the exact transformations cannot be detailed. However, based on the shell script's context, the SQL script likely:
- Processes data related to `PoolBasisprodukt` or similar business entities.
- Performs aggregations, filtering, or joins to prepare data for reporting or downstream systems.

The shell script itself handles:
- **Parameter Validation**: Ensures `Jobkennung`, `Stichtag`, and `EintragsNr` are provided.
- **Date Validation**: Checks if `p_Stichtag` is in `DDMMYYYY` format.
- **Defaulting**: Sets `p_wiederanlaufWert` to `0` if not provided.
- **Date Derivation**: Calculates `p_datum_heute` and `p_datum_gestern`.

**Potential Transformations (if commented-out sections are enabled):**
- **Whitespace Removal**: `sed s/\\ //g` (equivalent to `REPLACE(column, ' ', '')` in BQSQL).
- **Deduplication and Sorting**: `sort -u -k 1 -t ';'` (equivalent to `ROW_NUMBER() OVER (PARTITION BY SPLIT(column, ';')[OFFSET(0)] ORDER BY column) = 1` in BQSQL).
- **File Joining**: `join` operations (equivalent to `JOIN` clauses in BQSQL based on split key fields).

The migration will involve translating these control and potential data manipulation patterns into BQSQL stored procedures and standard SQL queries.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicates no external systems for this specific job (`external_systems: []`). However, the script itself reveals several implicit dependencies:

- **Shell Environment**:
    - `$HOME/.dw_init`: A standard environment initialization script. This will be replaced by environment variables or configuration within the BigQuery execution context (e.g., Cloud Composer environment variables, parameter store).
- **KornShell Utility Scripts**:
    - `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`: These are sourced or executed. Their functionalities will be integrated directly into the BigQuery stored procedure using BQSQL scripting for date calculations, parameter handling, and error logging. Generic utility functions can be implemented as BigQuery user-defined functions (UDFs) if reusable.
- **Oracle Database (Implied)**: Given the use of `SQL*Plus` via `h_alis_sqlplus.ksh` and the `PoolBasisprodukt` table, it is highly probable that the original system interacts with an Oracle database.
    - **Replacement**: Oracle data sources will be migrated to BigQuery tables. The `PoolBasisprodukt` table will become a BigQuery table, likely residing in a dedicated dataset.

## 7. Unresolved / Risks
- **Core SQL Script (`d_ausd_bp_ta_rn_da_vda_tk.sql`)**: The actual content of the main SQL script is critical but not analyzed here. This is the biggest unresolved item. Its complexity and specific transformations will heavily influence the BigQuery migration design. It needs separate detailed analysis.
- **`starteSQLSkript` function**: The exact implementation of this function is unknown. It's assumed to execute the SQL script. In BigQuery, this will translate to directly calling the migrated SQL logic (e.g., another stored procedure or a series of SQL statements).
- **`DWMSG_MeldeFehler`, `pruefeParameterGesetzt`, `DWDate_Datum_Check`**: These are shell functions. Their precise logic needs to be fully understood to accurately translate them into BQSQL scripting logic and error handling. The MCP tool provides a good starting point for `pruefeParameterGesetzt` and `DWDate_Datum_Check` but detailed logic is required.
- **Commented-out code**: The `sed`, `sort`, `join` sections are commented out but indicate a potential need for file-based processing. If these become active or are needed in the future, they would require re-implementation in BQSQL or Python (e.g., using Dataflow) for flat file handling in a cloud-native manner.
- **Hollow Lineage**: The `lineage_edges` queries for this job returned no specific data flow, necessitating inference from the script content. This indicates a potential gap in automated lineage capture for such shell-orchestrated jobs and adds a minor risk to fully understanding dependencies without manual code review.
- **Migration Bucket**: `semi_auto` implies some manual intervention is required, mainly in translating the shell control logic and verifying the migrated SQL logic.

## 8. Build Plan
The migration will involve creating a BigQuery Stored Procedure that encapsulates the orchestration logic and integrates the migrated core SQL.

**Build Steps (Ordered):**

1.  **Analyze and Migrate `d_ausd_bp_ta_rn_da_vda_tk.sql` (BQSQL)**:
    *   **Task**: Extract the content of `d_ausd_bp_ta_rn_da_vda_tk.sql`. Analyze its SQL statements (SELECTs, INSERTs, UPDATEs, DELETEs, DDL).
    *   **Language**: BQSQL
    *   **Output**: One or more BigQuery SQL scripts/stored procedures for the core data transformations.
    *   **Dependencies**: Requires DDL for source tables (e.g., `PoolBasisprodukt`) and target tables to be created in BigQuery.

2.  **Create BigQuery Stored Procedure for Orchestration (`k_ausd_bp_ta_rn_da_vda_tk` SP)**:
    *   **Task**: Develop a BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_bp_ta_rn_da_vda_tk`) that replicates the control flow of the original KornShell script.
        *   Implement parameter declaration and parsing logic.
        *   Translate `pruefeParameterGesetzt` logic into BQSQL `IF` statements and `SIGNAL SQLSTATE`.
        *   Translate `DWDate_Datum_Check` logic into BQSQL date parsing and validation.
        *   Implement date derivation logic (`gestern.ksh` equivalent) using BQSQL functions.
        *   Integrate the call to the migrated core SQL logic (e.g., `CALL project.dataset.d_ausd_bp_ta_rn_da_vda_tk(...)` or embed the SQL directly).
        *   Implement record counting using `SELECT COUNT(*)` on the target tables.
        *   Implement error logging into `project.dataset.error_log` table.
        *   Implement job status logging into `project.dataset.job_table`.
    *   **Language**: BQSQL
    *   **Output**: `k_ausd_bp_ta_rn_da_vda_tk.sql` (containing `CREATE OR REPLACE PROCEDURE ...` statement)

3.  **Define DDL for Control and Logging Tables**:
    *   **Task**: Create DDL for `project.dataset.error_log`, `project.dataset.job_table`, and any other internal control tables used.
    *   **Language**: BQSQL
    *   **Output**: `.sql` DDL files.

4.  **Develop Cloud Orchestration (e.g., Cloud Composer DAG)**:
    *   **Task**: Create an Airflow DAG to schedule and execute the BigQuery Stored Procedure. The DAG will handle parameter passing to the stored procedure.
    *   **Language**: Python
    *   **Output**: `k_ausd_bp_ta_rn_da_vda_tk_dag.py`

5.  **Data Ingestion (if external flat files are involved)**:
    *   **Task**: If the commented-out file processing sections become active, or if the `d_ausd_bp_ta_rn_da_vda_tk.sql` depends on flat files, set up Cloud Storage buckets and BigQuery external tables or data loading jobs (e.g., `LOAD DATA`) for ingesting these files into BigQuery.
    *   **Language**: GCS, BQSQL
    *   **Output**: GCS bucket setup, BigQuery external table definitions or `LOAD DATA` scripts.