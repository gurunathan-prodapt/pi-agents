# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_cntrct_crs.ksh`, serves as a control script for a data processing job. Its primary purpose is to orchestrate the execution of an SQL script (`d_ausd_v_ta_cntrct_crs.sql`) for processing data related to `ta_cntrct_crs`. The script handles job control, including ignoring currently active jobs, invoking the SQL script, making an entry in a job tracking table, and deactivating older active jobs. It reads runtime parameters, validates them, executes the database SQL script, and captures the count of processed records.

## 2. Source Inventory
The job consists of a single KornShell script.

| File Path                                                       | Technology | Category | Tool      | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------- | :--------- | :------- | :-------- | :-------------- | :---------------- |
| vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh | Shell      | shell    | KornShell | medium          | semi_auto         |

The script is a `semi_auto` migration candidate, indicating that it requires some manual intervention or review during the migration process.

## 3. Target Architecture
The target architecture on BigQuery will primarily involve:
- **BigQuery Stored Procedures**: The core orchestration logic of the KornShell script, including parameter validation, job control (activating/deactivating jobs), and error handling, will be migrated to a BigQuery Stored Procedure. This allows for native execution within BigQuery.
- **BigQuery SQL**: The underlying data processing logic contained in the `d_ausd_v_ta_cntrct_crs.sql` file will be directly translated into BigQuery SQL statements, likely within the same or a separate BigQuery Stored Procedure invoked by the control procedure.
- **Audit/Logging Tables**: The current script's error handling and job table entry mechanisms will be replaced by dedicated BigQuery tables for logging errors (`job_error_log`) and tracking job status/metadata (`job_table`, `job_audit_log`).
- **Data Tables**: Source and target data tables will reside in BigQuery datasets.

The migration will leverage BigQuery's native capabilities for scripting, data manipulation, and metadata management, minimizing the need for external components for the script's core functionality.

## 4. Data Flow & Lineage
The original script's logic flow is as follows:
1.  **Environment Loading**: Loads environment variables and helper scripts (e.g., `dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
2.  **Parameter Parsing & Validation**: Parses command-line parameters (`-j` for `JobKennung`, `-f` for `EintragsNr`) and validates their presence using `pruefeParameterGesetzt`. If validation fails, `DWMSG_MeldeFehler` is called, an error message is printed, and the script exits.
3.  **Job Control**:
    *   Sets the target table name `v_TabName` to `'ta_cntrct_crs'`.
    *   Inserts a new entry into a "job table" marking the current job as active.
    *   Updates older active jobs related to `ta_cntrct_crs` to an inactive status.
4.  **SQL Script Execution**: Defines the path to the main SQL script: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_crs.sql`. This SQL script is executed via the `starteSQLSkript` wrapper function, passing `p_EintragsNr` and `p_JobKennung`.
5.  **Record Count Capture**: The `starteSQLSkript` is assumed to write the number of processed records to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs_$$.tmp`). The script then reads this count into the `v_records` variable.
6.  **Completion**: Prints a completion message.

In BigQuery, this flow will be implemented within a stored procedure (`sp_ausd_v_ta_cntrct_crs`).
- **Input Parameters**: `p_JobKennung` and `p_EintragsNr` will be explicit `IN` parameters to the stored procedure.
- **Environment**: Environment variables will be replaced by BigQuery scripting variables or derived from configuration tables.
- **Parameter Validation**: `IF` conditions and `ASSERT` statements will handle parameter validation. Error logging will be directed to a BigQuery error log table.
- **Job Control**: `INSERT` and `UPDATE` statements against BigQuery `job_table` and `job_audit_log` tables will manage job status.
- **SQL Execution**: The logic of `d_ausd_v_ta_cntrct_crs.sql` will be embedded or called from within the BigQuery stored procedure.
- **Record Count**: `@@row_count` or explicit `COUNT(*)` queries will capture record counts, stored in BigQuery variables and updated in the `job_table`.

## 5. Transformation Logic
The shell script itself contains no direct data transformation logic. Its role is purely orchestrational. All data transformations and aggregations are assumed to be within the invoked SQL script, `d_ausd_v_ta_cntrct_crs.sql`. The migration of this SQL script will involve translating its specific DDL/DML operations to BigQuery SQL syntax and best practices. This could include:
-   Rewriting table joins, WHERE clauses, and column expressions.
-   Adapting any proprietary SQL functions to BigQuery equivalents.
-   Considering partitioning, clustering, and data types suitable for BigQuery for performance optimization.

## 6. External Dependencies
The original script has the following external dependencies:
-   **Local Shell Utilities**:
    -   `. $HOME/.dw_init`: Loads environment variables.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility functions.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing/validation utilities.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL execution wrapper.
-   **SQL Script**:
    -   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_cntrct_crs.sql`: The primary data processing SQL script.
-   **Temporary File**:
    -   `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_crs_$$.tmp`: Used for inter-process communication to store record counts.

**Replacement Strategy for BigQuery:**
-   **Shell Utilities**: These will be replaced by native BigQuery SQL scripting constructs (e.g., `DECLARE`, `SET`, `IF`, `ASSERT`) and potentially user-defined functions or auxiliary logging tables for error reporting.
-   **SQL Script**: The content of `d_ausd_v_ta_cntrct_crs.sql` will be directly translated into BigQuery SQL within the stored procedure.
-   **Temporary File**: The mechanism of writing to and reading from a temporary file will be replaced by BigQuery scripting variables (e.g., `DECLARE v_records INT64; SET v_records = @@row_count;`) or directly updating a record count column in the job control table.
-   **Job Table**: The existing "job table" concept will be migrated to a BigQuery table (e.g., `project.dataset.job_table`) to track job status, identifiers, and record counts.

## 7. Unresolved / Risks
-   **Details of `d_ausd_v_ta_cntrct_crs.sql`**: The specific SQL logic within `d_ausd_v_ta_cntrct_crs.sql` is not explicitly detailed in the current analysis. A thorough review and translation of this SQL script are critical. This may involve complex SQL constructs, specific database functions, or performance considerations that need careful adaptation to BigQuery.
-   **`dw_init` Environment**: The exact variables and configurations loaded by `. $HOME/.dw_init` are unknown. These need to be identified and mapped to BigQuery procedure parameters or BigQuery configuration tables.
-   **`starteSQLSkript` Implementation**: The `h_alis_sqlplus.ksh` script and `starteSQLSkript` function's internal workings (e.g., how it connects to the database, handles errors, and returns the record count) need to be understood to ensure faithful replication in BigQuery.
-   **"Aktive Jobs" Logic**: The precise definition and handling of "aktive Jobs" (active jobs) and their deactivation logic must be fully understood to correctly implement this concurrency control in BigQuery. This might involve timestamps, status flags, and unique job identifiers.
-   **"Purpose Note" details**: The `purpose_note` mentions "Job assembled from 1 component(s); stage dist: medium=1", which is aligned with the `medium` complexity tier. No other specific risks or unresolved items were identified from the metadata.

## 8. Build Plan

The migration will involve creating a BigQuery Stored Procedure that encapsulates the logic of the KornShell script and the invoked SQL script.

**Build Artifacts:**

1.  **BigQuery Stored Procedure (`sp_ausd_v_ta_cntrct_crs.sql`)**:
    *   **Language**: BigQuery SQL
    *   **Content**:
        ```sql
        -- BigQuery Stored Procedure: control wrapper for ta_cntrct_crs processing
        CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_cntrct_crs`(
          IN p_JobKennung STRING,
          IN p_EintragsNr STRING
        )
        BEGIN
          DECLARE v_TabName STRING DEFAULT 'ta_cntrct_crs';
          DECLARE v_records INT64 DEFAULT 0;
          DECLARE v_err_nr INT64 DEFAULT 0;
          DECLARE v_err_arg STRING DEFAULT '';
          DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

          -- Parameter validation
          IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            SET v_err_nr = 193;
            SET v_err_arg = 'Jobkennung';
          END IF;

          IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            IF v_err_nr = 0 THEN -- Only set if no prior error
                SET v_err_nr = 193;
                SET v_err_arg = 'EintragsNr';
            END IF;
          END IF;

          IF v_err_nr <> 0 THEN
            INSERT INTO `project.dataset.job_error_log`
              (event_ts, procedure_name, err_nr, err_arg, message)
            VALUES
              (v_now, 'sp_ausd_v_ta_cntrct_crs', v_err_nr, v_err_arg, 'Bitte ueber Rahmenscript aufrufen');

            SELECT ERROR(CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' ', v_err_arg));
          END IF;

          -- Job control: mark current job as active / insert job entry
          INSERT INTO `project.dataset.job_table`
            (job_kennung, eintrags_nr, tab_name, status, created_ts, updated_ts)
          VALUES
            (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', v_now, v_now);

          -- Deactivate older active jobs for same logical process
          UPDATE `project.dataset.job_table`
          SET status = 'INACTIVE',
              updated_ts = CURRENT_TIMESTAMP()
          WHERE tab_name = v_TabName
            AND status = 'ACTIVE'
            AND NOT (job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr);

          -- Core processing equivalent of d_ausd_v_ta_cntrct_crs.sql
          -- (*** Placeholder: Actual SQL logic from d_ausd_v_ta_cntrct_crs.sql goes here ***)
          -- Example pattern:
          INSERT INTO `project.dataset.target_output_table` -- Replace with actual target table
          SELECT
            * -- Replace with actual columns and transformations
          FROM `project.dataset.source_input_table` -- Replace with actual source table
          WHERE job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr; -- Example filtering, adapt as per original SQL

          SET v_records = @@row_count;

          -- Persist record count / completion status
          UPDATE `project.dataset.job_table`
          SET status = 'DONE',
              record_count = v_records,
              updated_ts = CURRENT_TIMESTAMP()
          WHERE job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr
            AND tab_name = v_TabName;

          -- Optional completion log
          INSERT INTO `project.dataset.job_audit_log`
            (event_ts, procedure_name, job_kennung, eintrags_nr, tab_name, record_count, message)
          VALUES
            (CURRENT_TIMESTAMP(), 'sp_ausd_v_ta_cntrct_crs', p_JobKennung, p_EintragsNr, v_TabName, v_records, 'ENDE Datenverarbeitung');

        END;
        ```

2.  **BigQuery Table DDLs**:
    *   **`project.dataset.job_table`**: DDL for a table to manage job status and metadata (e.g., `job_kennung STRING`, `eintrags_nr STRING`, `tab_name STRING`, `status STRING`, `record_count INT64`, `created_ts TIMESTAMP`, `updated_ts TIMESTAMP`).
    *   **`project.dataset.job_error_log`**: DDL for a logging table (e.g., `event_ts TIMESTAMP`, `procedure_name STRING`, `err_nr INT64`, `err_arg STRING`, `message STRING`).
    *   **`project.dataset.job_audit_log`**: DDL for an audit log table (e.g., `event_ts TIMESTAMP`, `procedure_name STRING`, `job_kennung STRING`, `eintrags_nr STRING`, `tab_name STRING`, `record_count INT64`, `message STRING`).
    *   DDLs for any source or target tables referenced in `d_ausd_v_ta_cntrct_crs.sql`.

**Execution Plan:**
1.  Create the necessary BigQuery datasets.
2.  Deploy the DDLs for `job_table`, `job_error_log`, `job_audit_log`, and any data tables.
3.  Translate the content of `d_ausd_v_ta_cntrct_crs.sql` into optimized BigQuery SQL and replace the placeholder in the `sp_ausd_v_ta_cntrct_crs` stored procedure.
4.  Deploy the `sp_ausd_v_ta_cntrct_crs` stored procedure.
5.  Set up a BigQuery-native scheduler (e.g., Cloud Composer, Cloud Workflows, or scheduled queries) to invoke `CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`('your_job_id', 'your_entry_nr');` as per the original scheduling requirements.