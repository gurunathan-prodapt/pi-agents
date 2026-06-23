# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_vertrag_tmp.ksh`. This script serves as a control and orchestration component for data preparation. Its primary function is to handle environment setup, parse input parameters, manage errors, and trigger the execution of an underlying SQL script, `d_ausd_v_ta_vertrag_tmp.sql`, which in turn operates on the `ta_vertrag_tmp` table. The job also includes logic to ignore active jobs, manage job registration in a job table, and deactivate old active jobs. The script is marked for retirement (B0 migration bucket), implying its direct form will not be retained but its functionality will be reimplemented in the target platform.

## 2. Source Inventory
The job consists of a single primary source file:
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`
  - **Technology**: KornShell
  - **Tool**: KornShell
  - **Summary**: This ksh script acts as a control script for data preparation, handling environment setup, parameter parsing, error management, and orchestrating the execution of an SQL script that operates on the 'ta_vertrag_tmp' table.
  - **Complexity Tier**: Unknown (no `file_complexity` entry)
  - **Migration Flags**: Unknown (no `file_complexity` entry)
  - **Automation Bucket**: B0 (retire)
  - **Purpose Note**: Job assembled from 1 component(s); stage dist: medium=1

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data processing and storage, and BigQuery Stored Procedures for implementing the control and orchestration logic. The original KornShell script will be retired and its functionalities re-engineered.

- **Data Storage**: `ta_vertrag_tmp` and any associated tables will be migrated to BigQuery as standard tables within a dedicated dataset (e.g., `project.dataset.ta_vertrag_tmp`).
- **Control Logic**: The orchestration and parameter handling logic of `k_ausd_v_ta_vertrag_tmp.ksh` will be rewritten as a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag`).
- **Error Handling & Logging**: Dedicated BigQuery tables for error logging (e.g., `project.dataset.error_log`) and job run logging (e.g., `project.dataset.job_run_log`) will replace shell-based error messages and temporary file outputs.
- **SQL Execution**: The invoked SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) will be migrated to a separate BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_vertrag_tmp`) which will be called by the `r_ausd_vertrag` stored procedure.
- **Orchestration**: External scheduling and execution of the BigQuery Stored Procedure will be handled by a modern orchestrator like Cloud Composer (Apache Airflow), Cloud Workflows, or Cloud Scheduler, replacing the original shell script's runtime environment.

## 4. Data Flow & Lineage
The original shell script's data flow is primarily one of control and invocation.
1.  **Parameter Input**: The `k_ausd_v_ta_vertrag_tmp.ksh` script receives `JobKennung` and `EintragsNr` as command-line arguments.
2.  **Environment Setup**: It sources various helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Validation**: Inputs are validated, and errors are handled via the `f_alis_msgerr.ksh` helper.
4.  **SQL Script Invocation**: The script then calls an external SQL script, `d_ausd_v_ta_vertrag_tmp.sql`, which is expected to perform the core data manipulation on `ta_vertrag_tmp`. The shell script passes `p_EintragsNr` and `p_JobKennung` to this SQL script.
5.  **Record Count**: A temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_vertrag_tmp_$$.tmp`) is used to store and retrieve the number of records processed by the SQL script.
6.  **Job Completion**: Logs completion and exits.

In the BigQuery target, this flow will be:
1.  **Orchestration Trigger**: An external orchestrator (e.g., Airflow DAG) will initiate the `project.dataset.r_ausd_vertrag` BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as parameters.
2.  **Parameter Handling & Validation**: The `r_ausd_vertrag` stored procedure will perform parameter validation using BigQuery procedural language `IF` statements.
3.  **Error Logging**: Any validation errors or runtime issues will be logged into `project.dataset.error_log`.
4.  **Core SQL Logic Invocation**: The `r_ausd_vertrag` stored procedure will call another BigQuery Stored Procedure, `project.dataset.d_ausd_v_ta_vertrag_tmp`, to execute the core data processing. (Note: The actual content of `d_ausd_v_ta_vertrag_tmp.sql` is unknown and requires separate analysis for a complete migration plan.)
5.  **Record Count & Logging**: Instead of a temporary file, `r_ausd_vertrag` will query the target `project.dataset.ta_vertrag_tmp` table to get a record count (e.g., `SELECT COUNT(*)`) and log this information into `project.dataset.job_run_log`.

## 5. Transformation Logic
The transformation logic applies to the shell script's control flow and utility calls, not the actual data transformation within the SQL.

- **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`)**: Will be replaced by BigQuery stored procedure parameters, dataset/table names in SQL, or configuration variables managed by the orchestrator.
- **Helper Script Sourcing (`. $HOME/.dw_init`, etc.)**: The functionalities of these helper scripts need to be reimplemented or replaced:
    - `f_alis_msgerr.ksh`: Replaced by `INSERT` statements into `project.dataset.error_log` or a dedicated error handling BigQuery stored procedure.
    - `h_alis_date.ksh`: Replaced by BigQuery's native date/time functions.
    - `h_alis_parameter.ksh`: Functionality absorbed into the BigQuery stored procedure's parameter validation logic.
    - `h_alis_sqlplus.ksh`: Replaced by direct BigQuery SQL execution via the called `d_ausd_v_ta_vertrag_tmp` stored procedure.
- **Parameter Parsing (`getopts`)**: Replaced by direct input parameters to the BigQuery Stored Procedure (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`).
- **Conditional Logic (`if [ ! $ErrNr -eq 0 ]`)**: Replaced by BigQuery procedural `IF ... THEN ... END IF;` constructs.
- **Error Exit (`exit $ErrNr`)**: Replaced by `SIGNAL SQLSTATE` in BigQuery stored procedures, causing the procedure to abort with an error.
- **Temporary File Operations (`cat $tmpFile`, `eval "v_records=\`cat $tmpFile\`"`)**: Replaced by direct queries on BigQuery tables (e.g., `SELECT COUNT(*) FROM project.dataset.ta_vertrag_tmp`) and storing results in BigQuery procedure variables or a job logging table.
- **SQL Script Execution (`starteSQLSkript`)**: Replaced by `CALL project.dataset.d_ausd_v_ta_vertrag_tmp(p_EintragsNr, p_JobKennung);`.

**Key Assumption**: The actual data manipulation logic currently within `d_ausd_v_ta_vertrag_tmp.sql` will be migrated into the `project.dataset.d_ausd_v_ta_vertrag_tmp` BigQuery Stored Procedure, and will need separate analysis and potential conversion from its original SQL dialect (likely Oracle SQL based on `sqlplus` reference) to BigQuery SQL.

## 6. External Dependencies
- **Oracle Database**: The original script implicitly interacts with an Oracle database through the `sqlplus` wrapper. In BigQuery, this will be replaced by native BigQuery table operations. If `ta_vertrag_tmp` is sourced from an external Oracle database, a data ingestion pipeline (e.g., using Datastream, Fivetran, or custom ETL) will be required to bring the data into BigQuery.
- **Internal Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`)**: These are internal shell scripts providing utility functions. Their functionalities will be absorbed into BigQuery's native features or custom BigQuery stored procedures as described in Section 5.
- **Invoked SQL Script (`d_ausd_v_ta_vertrag_tmp.sql`)**: This is a critical dependency. While its invocation is clear, its contents are currently unanalyzed. Its migration to a BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_vertrag_tmp`) is essential.

No external systems (like SFTP, S3) were identified from `lineage_assembled_jobs.external_systems`.

## 7. Unresolved / Risks
- **Missing SQL Script Analysis**: The most significant unresolved item is the lack of analysis for `d_ausd_v_ta_vertrag_tmp.sql`. The core data transformation logic resides here. Without this code, a complete, implementation-ready design for data transformations cannot be finalized. This file must be analyzed and migrated separately.
- **`ta_vertrag_tmp` Table Details**: The schema, data types, and specific usage patterns of `ta_vertrag_tmp` are not available from the lineage. This information is crucial for proper BigQuery table design and data migration.
- **Complexity Tier and Migration Flags**: These were not found for the `ksh` script, which limits understanding of specific migration challenges identified during automated analysis.
- **Implicit Logic in Sourced Scripts**: Any hidden business logic within the sourced shell helper scripts not directly evident from the `ksh` script will need to be identified and re-implemented.

## 8. Build Plan
The build plan focuses on the re-engineering of the `ksh` script's orchestration logic into BigQuery components.

1.  **Define BigQuery Datasets**:
    -   `project.dataset` (main dataset for data and procedures)
2.  **Create BigQuery Tables**:
    -   `project.dataset.ta_vertrag_tmp` (target table for data, DDL derived from source system schema)
    -   `project.dataset.error_log` (table to log errors)
    -   `project.dataset.job_run_log` (table to log job execution details, including record counts)
3.  **Develop BigQuery Stored Procedure for Core SQL Logic (d_ausd_v_ta_vertrag_tmp)**:
    -   **Language**: BigQuery SQL
    -   **File**: `d_ausd_v_ta_vertrag_tmp.bqsql`
    -   **Content**: Conversion of original `d_ausd_v_ta_vertrag_tmp.sql` to BigQuery SQL, implementing data loading/transformation on `ta_vertrag_tmp`. *Requires analysis of the original SQL file.*
4.  **Develop BigQuery Stored Procedure for Orchestration (r_ausd_vertrag)**:
    -   **Language**: BigQuery SQL
    -   **File**: `k_ausd_v_ta_vertrag_tmp.bqsql` (or `r_ausd_vertrag.bqsql`)
    -   **Content**:
        ```sql
        -- BigQuery Stored Procedure pseudocode (from MCP analysis)
        CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag`(
          IN p_JobKennung STRING,
          IN p_EintragsNr STRING
        )
        BEGIN
          DECLARE ErrNr INT64 DEFAULT 0;
          DECLARE ErrArg STRING DEFAULT '';
          DECLARE v_TabName STRING DEFAULT 'ta_vertrag_tmp';
          DECLARE v_records INT64 DEFAULT 0;

          -- Parameter validation
          IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            SET ErrNr = 193;
            SET ErrArg = 'Jobkennung';
          END IF;

          IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
            SET ErrNr = 193;
            SET ErrArg = 'EintragsNr';
          END IF;

          IF ErrNr != 0 THEN
            INSERT INTO `project.dataset.error_log`
            (error_ts, module_name, error_nr, error_arg, message)
            VALUES
            (CURRENT_TIMESTAMP(), 'r_ausd_vertrag', ErrNr, ErrArg, 'Bitte ueber Rahmenscript aufrufen');

            SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS error_message;

            SIGNAL SQLSTATE '45000'
              SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
          END IF;

          -- Main SQL execution placeholder - Calls the migrated SQL script procedure
          CALL `project.dataset.d_ausd_v_ta_vertrag_tmp`(
            p_EintragsNr,
            p_JobKennung
          );

          -- Completion message
          SELECT ' ---------- ENDE Datenverarbeitung ----------' AS status_message;

          -- Replace temp file read with direct query or control table lookup
          SET v_records = (
            SELECT COUNT(*)
            FROM `project.dataset.ta_vertrag_tmp`
            -- Assuming 'entry_nr' is a relevant column for filtering
            WHERE entry_nr = p_EintragsNr
          );

          -- Persist record count if needed
          INSERT INTO `project.dataset.job_run_log`
          (run_ts, job_kennung, eintrags_nr, tab_name, records)
          VALUES
          (CURRENT_TIMESTAMP(), p_JobKennung, p_EintragsNr, v_TabName, v_records);
        END;
        ```
5.  **Orchestration Script (e.g., Airflow DAG)**:
    -   **Language**: Python
    -   **File**: `k_ausd_v_ta_vertrag_tmp_dag.py`
    -   **Content**: A Python script defining an Airflow DAG that calls the `project.dataset.r_ausd_vertrag` BigQuery Stored Procedure, passing required parameters.
6.  **Data Ingestion Pipeline**:
    -   **Language**: Varies (e.g., Python, SQL, Cloud Dataflow)
    -   **Content**: If `ta_vertrag_tmp` is not yet in BigQuery, a pipeline to ingest data from its source (e.g., Oracle) into `project.dataset.ta_vertrag_tmp`.
7.  **Testing Plan**: Develop unit and integration tests for both BigQuery stored procedures and the orchestration DAG to ensure functional equivalence.