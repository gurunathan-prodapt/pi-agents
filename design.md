# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

## 1. Purpose & Scope
The KornShell script `f_alis_msgerr.ksh` serves as a utility library for error management and logging within an ETL framework. Its primary purpose is to provide standardized functions for:
- Handling and reporting errors during job execution.
- Managing the status (OK, ABORTED) of job entries in a message table.
- Generating unique identifiers for log entries.
- Creating new entries in a central message/log table.
- Appending additional information, such as timestamps and specific error details, to log entries.
- Constructing log file names based on job details and timestamps.

This script is not a standalone job but rather a collection of helper functions intended to be sourced and called by other KornShell ETL scripts. It interacts heavily with an Oracle database, specifically through `sqlplus` calls to PL/SQL procedures within the `BERT_MELDUNG` package.

The scope of this migration design is to translate these KornShell functions and their Oracle interactions into a BigQuery-native solution, primarily using BigQuery Stored Procedures, BigQuery Scripting, and BigQuery tables for persistent storage.

## 2. Source Inventory
The job consists of a single source file:
- **File Name:** `f_alis_msgerr.ksh`
- **Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
- **Technology:** KornShell (`ksh`)
- **Category:** shell
- **Tool:** KornShell
- **Complexity Tier:** medium
- **Migration Flags:** `[]` (None explicitly flagged by automation)
- **Automation Bucket:** semi_auto

**Key characteristics:**
- Defines several shell functions (e.g., `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler`).
- Uses `sqlplus` to execute Oracle PL/SQL stored procedures.
- Utilizes shell features like variable assignment, conditional logic (`if`, `test`), command substitution (`` ` `), and `eval`.
- Interacts with a temporary file system (`/tmp`) for inter-process communication with `sqlplus`.
- Uses Unix utilities (`cat`, `tr`, `rm`, `date`).
- Leverages environment variables like `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`.

## 3. Target Architecture
The migrated solution will primarily reside within Google Cloud Platform, leveraging BigQuery for data persistence and procedural logic.

**BigQuery Components:**
- **BigQuery Dataset:** A dedicated dataset (e.g., `your_project_id.your_dataset_name`) will house the message table and stored procedures.
- **Message Table:** A BigQuery table will replace the Oracle message table. This table will store log entries, statuses, error details, and other metadata.
  - Example Schema:
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_name.message_table` (
      entry_nr STRING,
      job_kennung STRING,
      programmname STRING,
      logdatei STRING,
      status STRING,
      fehler_typ STRING,
      fehler_nr STRING,
      zusatz1 STRING,
      zusatz2 STRING,
      zusatzinfos STRING,
      created_ts TIMESTAMP,
      updated_ts TIMESTAMP
    );
    ```
- **BigQuery Stored Procedures:** Each KornShell function that interacts with the database will be translated into a BigQuery Stored Procedure. These procedures will encapsulate the logic currently performed by the Oracle PL/SQL `BERT_MELDUNG` package.
- **Orchestration (Optional/Context Dependent):** While the utility functions themselves will be BigQuery Stored Procedures, the calling shell scripts (which are part of other assembled jobs) will need to be migrated to an orchestration tool like Cloud Composer (Apache Airflow), Cloud Workflows, or Cloud Functions. These orchestration tools will invoke the BigQuery Stored Procedures.

## 4. Data Flow & Lineage
The original script's data flow involves:
1. **Shell Script Execution:** An external shell script (the caller) invokes one of the `DWMSG_` functions.
2. **Parameter Passing:** Parameters are passed from the calling shell script to the `DWMSG_` function.
3. **Database Interaction:** The `DWMSG_` function constructs and executes `sqlplus` commands, often involving dynamic SQL scripts (`d_alis_spaufruf_p*.sql`, `d_al_is_ermittlenr.sql`) that interact with Oracle's `BERT_MELDUNG` package.
4. **Temporary File Usage (for `DWMSG_ErmittleNr`):** For retrieving a generated number, `sqlplus` writes the result to a temporary file, which the shell script then reads.
5. **Database Updates/Retrievals:** The Oracle PL/SQL procedures perform inserts, updates, or selects on the underlying message table.
6. **Return Values/Side Effects:** Functions either perform database updates or assign values to shell variables (`eval`).

**Migrated Data Flow:**
1. **Orchestration Layer Invocation:** The migrated calling job (e.g., an Airflow DAG task) will invoke the corresponding BigQuery Stored Procedure.
2. **Parameter Passing:** Parameters will be passed directly to the BigQuery Stored Procedure.
3. **BigQuery Stored Procedure Execution:** The stored procedure will execute SQL DML (INSERT, UPDATE) or DQL (SELECT) against the BigQuery message table.
4. **BigQuery Scripting:** Logic like conditional execution, variable assignment, and error handling will be handled natively within BigQuery Scripting in the stored procedures.
5. **Direct Table Interaction:** All database operations will directly interact with the BigQuery message table, eliminating the need for `sqlplus` or temporary files for inter-process communication.
6. **Return Values:** Stored procedures can use `OUT` parameters to return values if necessary, or the calling orchestration can query the updated message table.

## 5. Transformation Logic
The transformation logic will focus on replicating the behavior of each KornShell function as a BigQuery Stored Procedure.

**Original KornShell Function -> BigQuery Stored Procedure:**

-   **`DWMSG_Fehlerbehandlung`**:
    -   Original: Captures `$?`, calls `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`.
    -   Target: Will be a BigQuery Stored Procedure. Error code capture will be handled by BigQuery's `EXCEPTION WHEN ERROR` block in the calling context, which will then invoke `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_Fehlerbehandlung(entry_nr STRING)
        BEGIN
          -- Simulating error code capture and handling
          DECLARE fehler_nr INT64 DEFAULT 1; -- Placeholder, actual error will come from EXCEPTION
          DECLARE kUnerwFehler INT64 DEFAULT 10;
          CALL dataset.DWMSG_MeldeFehler(entry_nr, 'F', CAST(kUnerwFehler AS STRING), CONCAT('ErrorCode ist: ', CAST(fehler_nr AS STRING)), NULL);
          CALL dataset.DWMSG_SetzeStatusAbbruch(entry_nr);
        END;
        ```

-   **`DWMSG_SetzeStatusOK`**:
    -   Original: Calls `sqlplus` to execute `BERT_MELDUNG.SetzeStatusOk`.
    -   Target: BigQuery Stored Procedure. Updates the `status` column in `message_table`.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_SetzeStatusOK(entry_nr STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN
            RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben';
          END IF;
          UPDATE dataset.message_table SET status = 'OK', updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
        END;
        ```

-   **`DWMSG_SetzeStatusAbbruch`**:
    -   Original: Calls `sqlplus` to execute `BERT_MELDUNG.SetzeStatusAbbruch`.
    -   Target: BigQuery Stored Procedure. Updates the `status` column in `message_table`.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_SetzeStatusAbbruch(entry_nr STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN
            RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben';
          END IF;
          UPDATE dataset.message_table SET status = 'ABBRUCH', updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
        END;
        ```

-   **`DWMSG_ErmittleNr`**:
    -   Original: Calls `sqlplus` to execute `d_al_is_ermittlenr.sql`, which writes to a temp file; script reads the file and assigns to a variable.
    -   Target: BigQuery Stored Procedure. Will generate a UUID or use a sequence-like mechanism and return it via an `OUT` parameter.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_ErmittleNr(OUT var_name STRING)
        BEGIN
          SET var_name = GENERATE_UUID(); -- Or use a sequence generator if specific numbering is needed
        END;
        ```

-   **`DWMSG_ErzeugeEintrag`**:
    -   Original: Calls `sqlplus` to execute `BERT_MELDUNG.Erzeuge_Eintrag`.
    -   Target: BigQuery Stored Procedure. Inserts a new row into `message_table`.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_ErzeugeEintrag(entry_nr STRING, job_kennung STRING, programmname STRING, logdatei STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN
            RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben';
          END IF;
          INSERT INTO dataset.message_table (entry_nr, job_kennung, programmname, logdatei, status, created_ts, updated_ts)
          VALUES (entry_nr, job_kennung, programmname, logdatei, 'NEW', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        END;
        ```

-   **`DWMSG_MeldeFehler`**:
    -   Original: Dynamically selects a `sqlplus` script (`d_alis_spaufruf_p*.sql`) to execute `BERT_MELDUNG.Fehler` based on parameter count.
    -   Target: BigQuery Stored Procedure. Updates error-related columns in `message_table`. Dynamic parameter handling will be done via `IF/ELSEIF` within BigQuery Scripting.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_MeldeFehler(entry_nr STRING, typ STRING, fehler_nr STRING, zusatz1 STRING, zusatz2 STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN
            RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben';
          END IF;
          UPDATE dataset.message_table
          SET fehler_typ = typ, fehler_nr = fehler_nr, zusatz1 = zusatz1, zusatz2 = zusatz2, updated_ts = CURRENT_TIMESTAMP()
          WHERE entry_nr = entry_nr;
        END;
        ```

-   **`DWMSG_Logdateiname`**:
    -   Original: Concatenates environment variables and `date` command output to form a log file name.
    -   Target: BigQuery Stored Procedure. Will use BigQuery's string and timestamp formatting functions. The concept of a `logdatei` might be translated to a Cloud Storage URI if logs are stored there.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_Logdateiname(OUT var_name STRING, job_kennung STRING, entry_nr STRING)
        BEGIN
          SET var_name = CONCAT(
            '/protocol/', -- Re-evaluate this path for Cloud Storage integration
            job_kennung,
            '_',
            FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP()),
            '_',
            entry_nr,
            '.log'
          );
        END;
        ```

-   **`DWMSG_SetzeStichtagInfo`**:
    -   Original: Uses `sqlplus` heredoc to execute `BERT_MELDUNG.SetzeZusatzInfos` with `TO_DATE`.
    -   Target: BigQuery Stored Procedure. Updates `zusatzinfos` column using BigQuery date parsing functions like `PARSE_TIMESTAMP`.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_SetzeStichtagInfo(entry_nr STRING, stichtag STRING, stichtag_fmt STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben'; END IF;
          IF stichtag IS NULL OR stichtag = '' THEN RAISE USING MESSAGE = 'Argh!, keinen Stichtag angegeben!'; END IF;
          IF stichtag_fmt IS NULL OR stichtag_fmt = '' THEN RAISE USING MESSAGE = 'Argh!, Stichtagsangaben ohne Formatangaben knnen nicht verarbeitet werden!'; END IF;
          UPDATE dataset.message_table SET zusatzinfos = CAST(PARSE_TIMESTAMP(stichtag_fmt, stichtag) AS STRING), updated_ts = CURRENT_TIMESTAMP() WHERE entry_nr = entry_nr;
        END;
        ```

-   **`DWMSG_AppendTimingInfos`**:
    -   Original: Uses `sqlplus` heredoc to execute `BERT_MELDUNG.SetzeZusatzInfos` with `SYSDATE` and string concatenation.
    -   Target: BigQuery Stored Procedure. Appends to `zusatzinfos` using BigQuery string and timestamp formatting.
        ```sql
        CREATE OR REPLACE PROCEDURE dataset.DWMSG_AppendTimingInfos(entry_nr STRING, info_text STRING, date_format STRING)
        BEGIN
          IF entry_nr IS NULL OR entry_nr = '' THEN RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben'; END IF;
          IF date_format IS NULL OR date_format = '' THEN RAISE USING MESSAGE = 'Argh!, Formatangabe erforderlich!'; END IF;
          UPDATE dataset.message_table
          SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), info_text, ' ', FORMAT_TIMESTAMP(date_format, CURRENT_TIMESTAMP()), ' '),
              updated_ts = CURRENT_TIMESTAMP()
          WHERE entry_nr = entry_nr;
        END;
        ```

## 6. External Dependencies
The original script has the following external dependencies:

-   **Oracle Database:** This is the primary external dependency. The script makes extensive calls to Oracle PL/SQL procedures within the `BERT_MELDUNG` package.
    -   **Replacement:** The Oracle database will be replaced by a BigQuery dataset and BigQuery tables. The `BERT_MELDUNG` package and its procedures (`SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, `SetzeZusatzInfos`) will be re-implemented as BigQuery Stored Procedures operating on the BigQuery `message_table`.
-   **Oracle `sqlplus` client:** Used to connect to and execute commands against the Oracle database.
    -   **Replacement:** `sqlplus` will be eliminated. Direct calls to BigQuery Stored Procedures will be made from the orchestration layer (e.g., Cloud Composer) or other BigQuery scripting.
-   **Oracle SQL wrapper scripts (`d_alis_spaufruf_p1.sql`, etc.):** These scripts are intermediary files called by `sqlplus` to execute specific PL/SQL procedures.
    -   **Replacement:** These wrapper scripts will be eliminated. Their logic will be directly incorporated into the BigQuery Stored Procedures.
-   **Temporary Files (`/tmp/ErmittleNr_*.lst`):** Used for passing data (generated number) from `sqlplus` back to the shell script.
    -   **Replacement:** Eliminated. BigQuery Stored Procedures can return values directly via `OUT` parameters or by querying tables.
-   **Unix utilities (`cat`, `tr`, `rm`, `date`):** Used for file manipulation and date formatting.
    -   **Replacement:** Replaced by BigQuery's native string manipulation, timestamp formatting functions (`FORMAT_TIMESTAMP`, `CURRENT_TIMESTAMP()`), and `GENERATE_UUID()` for unique ID generation. File system operations will be replaced by direct BigQuery table operations.

## 7. Unresolved / Risks
-   **Absence of `file_purpose` in analysis:** The `file_purpose` for this script was empty in the `file_analysis` table. While the code clearly indicates a utility/logging purpose, this could be a risk if the LLM analysis missed other significant functions. This is mitigated by manual code review and the detailed output from the `shellscript_to_bqsql_design` tool.
-   **Exact `trap ERR` behavior:** The `trap ERR` mechanism in KornShell allows for custom error handling upon any command returning a non-zero exit code. Replicating this exact behavior in a BigQuery-only context requires the calling job to explicitly wrap its execution in `BEGIN...EXCEPTION WHEN ERROR THEN CALL DWMSG_Fehlerbehandlung(...) END;` blocks. This shifts error handling responsibility to the caller.
-   **`eval` usage:** The `eval` command is used for dynamic variable assignment (`eval "$VarName=$DWMSG_EintragsNr"`). This will be replaced by explicit `SET` statements for BigQuery variables or by returning values directly to the calling orchestration layer.
-   **Environment Variables:** The script relies on `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`. These will need to be configured as BigQuery job parameters or environment variables in the orchestration layer (e.g., Airflow variables or GCP Secret Manager).
-   **`to_date` and date formats:** Oracle's `TO_DATE` function has specific format string behaviors. Care must be taken to ensure BigQuery's `PARSE_TIMESTAMP`, `PARSE_DATE`, or `SAFE.PARSE_*` functions accurately replicate the expected date parsing logic, especially considering localization and implicit conversions in Oracle.
-   **Log File Path Convention:** The `DWMSG_Logdateiname` function constructs a path. The equivalent in BigQuery might be a path to a Cloud Storage bucket for external logs, or the `logdatei` column in the message table could store the log content itself or a reference to it. The current pseudocode assumes a generic `/protocol/` path; this needs refinement based on actual log storage strategy.
-   **Interfacing with other migrated components:** Since this is a utility script, its migration depends on the migration strategy of the scripts that invoke these `DWMSG_` functions. The integration points (API calls to BigQuery Stored Procedures) must be consistent.

## 8. Build Plan
The build plan will focus on creating the necessary BigQuery assets.

1.  **BigQuery Dataset Creation:**
    -   Create a BigQuery dataset for the logging and error management components if one does not already exist.
    -   **Language:** SQL (DDL)
    -   **File:** `bq_dataset_ddl.sql`
    -   **Content:**
        ```sql
        CREATE SCHEMA IF NOT EXISTS `your_project_id.your_dataset_name`
        OPTIONS (
          location = 'your_bq_region'
        );
        ```

2.  **BigQuery Message Table Creation:**
    -   Create the `message_table` within the designated BigQuery dataset.
    -   **Language:** SQL (DDL)
    -   **File:** `bq_message_table_ddl.sql`
    -   **Content:** (as provided in Section 3)
        ```sql
        CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_name.message_table` (
          entry_nr STRING,
          job_kennung STRING,
          programmname STRING,
          logdatei STRING,
          status STRING,
          fehler_typ STRING,
          fehler_nr STRING,
          zusatz1 STRING,
          zusatz2 STRING,
          zusatzinfos STRING,
          created_ts TIMESTAMP,
          updated_ts TIMESTAMP
        );
        ```

3.  **BigQuery Stored Procedure Definitions:**
    -   Create a separate SQL file for each `DWMSG_` function, defining it as a BigQuery Stored Procedure.
    -   **Language:** BigQuery SQL (DML/DDL for procedures)
    -   **Files:**
        -   `bq_sp_dwmsg_fehlerbehandlung.sql`
        -   `bq_sp_dwmsg_setzestatusok.sql`
        -   `bq_sp_dwmsg_setzestatusabbruch.sql`
        -   `bq_sp_dwmsg_ermittlenr.sql`
        -   `bq_sp_dwmsg_erzeugeeintrag.sql`
        -   `bq_sp_dwmsg_meldefehler.sql`
        -   `bq_sp_dwmsg_logdateiname.sql`
        -   `bq_sp_dwmsg_setzestichtaginfo.sql`
        -   `bq_sp_dwmsg_appendtiminginfos.sql`
    -   **Content:** (as provided in Section 5)

4.  **Orchestration Integration (Example: Cloud Composer/Airflow DAGs):**
    -   Update calling jobs (migrated from other KornShell scripts) to invoke these new BigQuery Stored Procedures.
    -   **Language:** Python (for Airflow DAGs)
    -   **Files:** (specific to each calling job)
    -   **Content (example for a calling job):**
        ```python
        from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
        from airflow.operators.dummy import DummyOperator
        from airflow import DAG
        from datetime import datetime

        with DAG(
            dag_id='example_calling_job',
            start_date=datetime(2023, 1, 1),
            schedule_interval=None,
            catchup=False
        ) as dag:
            start = DummyOperator(task_id='start')

            generate_entry_nr = BigQueryExecuteQueryOperator(
                task_id='generate_entry_nr',
                sql='CALL `your_project_id.your_dataset_name.DWMSG_ErmittleNr`(@entry_nr);',
                use_legacy_sql=False,
                gcp_conn_id='google_cloud_default',
                write_disposition='WRITE_TRUNCATE', # This depends on if output is written to a table
                do_xcom_push=True # To pass @entry_nr to subsequent tasks
            )

            # ... other tasks ...

            create_log_entry = BigQueryExecuteQueryOperator(
                task_id='create_log_entry',
                sql='''
                    DECLARE job_id STRING DEFAULT 'MY_JOB';
                    DECLARE program_name STRING DEFAULT 'MY_PROGRAM';
                    DECLARE log_path STRING DEFAULT 'gs://my-bucket/logs/';
                    CALL `your_project_id.your_dataset_name.DWMSG_ErzeugeEintrag`(
                        '{{ ti.xcom_pull(task_ids="generate_entry_nr", key="return_value") }}',
                        job_id,
                        program_name,
                        log_path
                    );
                ''',
                use_legacy_sql=False,
                gcp_conn_id='google_cloud_default'
            )

            # Example of error handling within a task group
            with BigQueryExecuteQueryOperator.partial(
                task_id="main_processing_logic",
                use_legacy_sql=False,
                gcp_conn_id='google_cloud_default',
            ).as_task_group():
                main_task = BigQueryExecuteQueryOperator(
                    task_id="run_main_sql",
                    sql="SELECT 1/0;", # Example of a failing query
                )
                success_handler = BigQueryExecuteQueryOperator(
                    task_id="set_status_ok",
                    sql='CALL `your_project_id.your_dataset_name.DWMSG_SetzeStatusOK`(\'{{ ti.xcom_pull(task_ids="generate_entry_nr", key="return_value") }}\');',
                )
                failure_handler = BigQueryExecuteQueryOperator(
                    task_id="set_status_aborted",
                    sql='CALL `your_project_id.your_dataset_name.DWMSG_Fehlerbehandlung`(\'{{ ti.xcom_pull(task_ids="generate_entry_nr", key="return_value") }}\');',
                    trigger_rule="all_done",
                )
                main_task >> [success_handler, failure_handler]
                # Further logic to determine which handler runs

            start >> generate_entry_nr >> create_log_entry >> main_processing_logic
        ```

This design provides a clear path for migrating the KornShell utility script to a BigQuery-native, cloud-friendly solution, handling both the procedural logic and database interactions.