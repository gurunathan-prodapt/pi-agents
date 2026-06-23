# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

## 1. Purpose & Scope
The purpose of this job, `r_ausd_v_ta_inv_assign.ksh`, is to synchronize or update the `ta_inv_assign` contract data table. It acts as a wrapper script for an underlying core script, `k_ausd_v_ta_inv_assign.ksh`, which in turn executes an Oracle SQL script, `d_ausd_v_ta_inv_assign.sql`. The primary business function is to maintain an up-to-date staging table (`sof$ta_inv_assign`) by extracting filtered contract assignment data from a source system (likely an Oracle database via a DB-Link named `CARMEN`) based on date-effectiveness and production flags. The job includes robust error handling, parameter validation, and logging mechanisms.

The scope of this migration involves moving the entire workflow from its current KornShell/Oracle SQL environment to Google Cloud's BigQuery platform. This includes replatforming the shell scripts, converting the Oracle SQL to BigQuery SQL, and replacing existing scheduling and logging frameworks with cloud-native alternatives.

## 2. Source Inventory
This job comprises three main components:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh`**
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Description:** This is the main wrapper script. It handles argument parsing (`getopts`), environment initialization (`. $HOME/.dw_init`), custom error handling and logging (`f_alis_msgerr.ksh`, `DWMSG_*` functions), and invokes the core script `k_ausd_v_ta_inv_assign.ksh` with specific job parameters. It also manages process traps for graceful exit.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh`**
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Description:** This is the core script invoked by the wrapper. It further validates parameters, includes utility scripts for date and SQL*Plus handling (`h_alis_date.ksh`, `h_alis_sqlplus.ksh`), and executes the Oracle SQL script `d_ausd_v_ta_inv_assign.sql` via `starteSQLSkript` function. It also aims to capture the number of processed records in a temporary file.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_assign.sql`**
    *   **Technology:** Oracle SQL*Plus Script
    *   **Description:** This script contains the primary data manipulation logic. It dynamically defines a database link (`&v_carmen`) and a cutoff date (`&v_datum`) from the `isbert_schema.dwtk_meldungen` table. It then truncates the `sof$ta_inv_assign` table and inserts data into it from `cds$ta_inv_assignment` (accessed via `&v_carmen` DB-Link), applying filters for `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production` against `&v_datum`.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services, primarily BigQuery, for data storage and processing, and potentially Cloud Composer (Apache Airflow) for orchestration.

*   **BigQuery Stored Procedure:** The core logic of the shell scripts and the Oracle SQL script will be encapsulated within a BigQuery Stored Procedure. This procedure will handle parameter passing, logging, and the data transformation logic.
*   **BigQuery Tables:**
    *   `project.dataset.sof_ta_inv_assign`: The target table for the contract assignment data, replacing the Oracle `sof$ta_inv_assign`.
    *   `project.dataset.cds_ta_inv_assignment`: Represents the source `cds$ta_inv_assignment` table. This data will either be ingested into BigQuery (e.g., via Data Transfer Service, batch loads) or accessed via BigQuery federated queries/external tables if the source Oracle system is still active.
    *   `project.dataset.dwtk_meldungen`: A BigQuery table to store the job metadata/messages, replacing the Oracle `isbert_schema.dwtk_meldungen`.
    *   `project.dataset.job_log`: A dedicated logging/audit table to replace the custom `DWMSG_*` logging framework and store job execution details, status, and record counts.
*   **Cloud Composer (Optional):** An Apache Airflow DAG could be used to orchestrate the execution of the BigQuery Stored Procedure, managing dependencies, scheduling, and potentially integrating with other GCP services for monitoring and alerts.
*   **Cloud Scheduler:** To trigger the Cloud Composer DAG or directly the BigQuery Stored Procedure on a schedule.

## 4. Data Flow & Lineage
The original data flow:
1.  **UC4 Scheduler:** Triggers `r_ausd_v_ta_inv_assign.ksh`.
2.  **`r_ausd_v_ta_inv_assign.ksh` (Wrapper):** Sets up the environment and passes control and parameters to `k_ausd_v_ta_inv_assign.ksh`. Logs job start.
3.  **`k_ausd_v_ta_inv_assign.ksh` (Core Script):** Prepares for SQL execution, including setting up SQL*Plus routines, and calls `d_ausd_v_ta_inv_assign.sql`.
4.  **`d_ausd_v_ta_inv_assign.sql` (Oracle SQL):**
    *   Reads the latest `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `isbert_schema.dwtk_meldungen` to determine `v_datum`.
    *   Truncates `sof$ta_inv_assign`.
    *   Reads from `cds$ta_inv_assignment` (potentially on a remote `CARMEN` database via `v_carmen` DB-Link).
    *   Filters data based on `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production` against `v_datum`.
    *   Inserts the filtered data into `sof$ta_inv_assign`.
    *   Commits the transaction.
5.  **`k_ausd_v_ta_inv_assign.ksh` (Core Script):** Captures the number of records processed.
6.  **`r_ausd_v_ta_inv_assign.ksh` (Wrapper):** Marks the job as successful and logs completion.

**Target BigQuery Data Flow:**
1.  **Cloud Scheduler/Cloud Composer:** Triggers the BigQuery Stored Procedure.
2.  **BigQuery Stored Procedure `r_ausd_v_ta_inv_assign`:**
    *   Receives `p_JobKennung` and `p_EintragsNr` as parameters.
    *   Validates parameters.
    *   Logs job start into `project.dataset.job_log`.
    *   Queries `project.dataset.dwtk_meldungen` to determine the cutoff date `v_datum`.
    *   Truncates `project.dataset.sof_ta_inv_assign`.
    *   Inserts data into `project.dataset.sof_ta_inv_assign` from `project.dataset.cds_ta_inv_assignment`, applying the same date and production filters as the original SQL.
    *   Captures the count of inserted records.
    *   Logs job completion and record count into `project.dataset.job_log`.
    *   Includes `EXCEPTION WHEN ERROR THEN` blocks for robust error handling and logging.

## 5. Transformation Logic
The core transformation logic resides within the `d_ausd_v_ta_inv_assign.sql` script and will be replicated in the BigQuery Stored Procedure.

*   **Date Cutoff (`v_datum`):** The `v_datum` (cutoff date) is determined by selecting the maximum `timecreated` from `isbert_schema.dwtk_meldungen` where `job_kennung` is `'BERT_DROP_TEMP_TABLE'`. This will be translated to a `SELECT MAX(timecreated) FROM project.dataset.dwtk_meldungen WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'` within the BigQuery Stored Procedure.
*   **Truncate and Load:** The `TRUNCATE TABLE sof$ta_inv_assign` followed by `INSERT INTO ... SELECT ...` pattern will be directly translated to BigQuery's `TRUNCATE TABLE project.dataset.sof_ta_inv_assign;` and an equivalent `INSERT INTO ... SELECT` statement.
*   **Filtering Conditions:** The `WHERE` clause conditions applied to `cds$ta_inv_assignment` will be directly migrated to the BigQuery `SELECT` statement:
    *   `ia.insert_at <= TO_DATE('&v_datum','YYYYMMDD')` -> `ia.insert_at <= TIMESTAMP(v_datum)`
    *   `(ia.modified_at IS NULL OR ia.modified_at > TO_DATE('&v_datum','YYYYMMDD'))` -> `(ia.modified_at IS NULL OR ia.modified_at > TIMESTAMP(v_datum))`
    *   `ia.valid_from <= TO_DATE('&v_datum','YYYYMMDD')` -> `ia.valid_from <= TIMESTAMP(v_datum)`
    *   `(ia.valid_to IS NULL OR ia.valid_to > TO_DATE('&v_datum','YYYYMMDD'))` -> `(ia.valid_to IS NULL OR ia.valid_to > TIMESTAMP(v_datum))`
    *   `ia.is_production = 1` remains `ia.is_production = 1` (assuming `is_production` is an integer/boolean type).

## 6. External Dependencies
*   **Oracle Database (Carmen DB):** The source table `cds$ta_inv_assignment` resides in a "Carmen DB" accessed via a DB-Link `@pcrs1`.
    *   **Replacement Strategy:** This will require data ingestion from the source Oracle system into BigQuery. Options include:
        *   **BigQuery Data Transfer Service:** For recurring, automated transfers of data from supported external sources.
        *   **Batch ETL:** Using custom Python/Java/Go applications with a tool like Cloud Dataflow or Dataproc to extract data from Oracle, transform if necessary, and load into BigQuery.
        *   **BigQuery Federated Queries / External Tables:** If the Oracle source can be directly exposed and accessed from BigQuery, though this is less common for large-scale migrations.
        The `cds_ta_inv_assignment` table will be a regular BigQuery table that contains the data from the Carmen DB.
*   **UC4 Scheduler:** Triggers the wrapper script.
    *   **Replacement Strategy:** Cloud Composer (Managed Apache Airflow) is the recommended replacement for UC4 for complex orchestration. For simple schedules, Cloud Scheduler can directly invoke the BigQuery Stored Procedure or a Cloud Function that triggers the procedure.
*   **Custom Logging and Error Handling Framework (`DWMSG_*` functions, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, etc.):** These shell and SQL*Plus specific utility scripts manage logging, error reporting, and SQL execution.
    *   **Replacement Strategy:** This framework will be replaced by a BigQuery `job_log` table, BigQuery Stored Procedure `EXCEPTION WHEN ERROR` blocks, and potentially integrated with Cloud Logging for monitoring and alerting. The `h_alis_sqlplus.ksh` functions will no longer be needed as SQL will be native BigQuery.
*   **`isbert_schema.dwtk_meldungen` Table:** Used to derive the `v_datum` cutoff date.
    *   **Replacement Strategy:** A dedicated BigQuery table, `project.dataset.dwtk_meldungen`, will be created to store this metadata. Its contents will need to be migrated from the source Oracle table.

## 7. Unresolved / Risks
*   **Dynamic Script Inclusion (`. $HOME/.dw_init`):** The content and dependencies of `.dw_init` are not fully known and may contain critical environment variables or functions that need to be explicitly migrated or re-configured in the BigQuery environment.
*   **Custom Shell Utilities (`pruefeParameterGesetzt`, `starteSQLSkript`):** These functions within the KornShell scripts are custom implementations. While the logic (`pruefeParameterGesetzt`) is translated to `IF` conditions in BQSP, the `starteSQLSkript` implies a mechanism for executing SQL dynamically and handling results, which will be replaced by direct SQL execution within the BigQuery Stored Procedure.
*   **`DWPA_UTIL_SKRIPT.runstatement`:** This Oracle package function is used for `TRUNCATE TABLE`. It needs to be confirmed if it performs any additional logic beyond just truncating the table. If not, a direct BigQuery `TRUNCATE TABLE` statement is sufficient.
*   **`tmpFile` for Record Count:** The `eval "v_records=`cat $tmpFile`"` mechanism is shell-specific. In BigQuery, this will be handled by querying `COUNT(*)` after the `INSERT` or using appropriate `ROW_COUNT` functions if available in the execution context.
*   **Metadata for `cds_ta_inv_assignment` and `dwtk_meldungen`:** The schema for these source tables needs to be fully captured to accurately create their BigQuery equivalents.
*   **Data Latency:** Depending on the chosen ingestion strategy for `cds_ta_inv_assignment` (e.g., batch vs. streaming), the latency of data availability in BigQuery might differ from the current Oracle DB-Link approach. This needs to be evaluated against business requirements.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **BigQuery DDL for Tables:**
    *   Create `project.dataset.sof_ta_inv_assign` DDL (BQSQL).
    *   Create `project.dataset.cds_ta_inv_assignment` DDL (BQSQL) - based on source schema.
    *   Create `project.dataset.dwtk_meldungen` DDL (BQSQL) - based on source schema.
    *   Create `project.dataset.job_log` DDL (BQSQL) - for logging and auditing.

2.  **Data Ingestion for Source Tables:**
    *   Develop and implement an ingestion pipeline (e.g., Data Transfer Service, Cloud Dataflow job) to regularly transfer data from the source Oracle `cds$ta_inv_assignment` to `project.dataset.cds_ta_inv_assignment`.
    *   Develop and implement an ingestion pipeline for `isbert_schema.dwtk_meldungen` to `project.dataset.dwtk_meldungen`.

3.  **BigQuery Stored Procedure Development:**
    *   Translate the combined logic of `r_ausd_v_ta_inv_assign.ksh`, `k_ausd_v_ta_inv_assign.ksh`, and `d_ausd_v_ta_inv_assign.sql` into a single BigQuery Stored Procedure: `project.dataset.r_ausd_v_ta_inv_assign(p_JobKennung STRING, p_EintragsNr INT64)`.
    *   Implement parameter validation and error handling using BigQuery's `IF`, `DECLARE`, and `EXCEPTION WHEN ERROR` constructs.
    *   Replace `DWMSG_*` calls with `INSERT` statements into `project.dataset.job_log`.
    *   Replace dynamic date determination and table truncation/load with BigQuery SQL equivalents.

4.  **Orchestration (Cloud Composer / Cloud Scheduler):**
    *   **Option A (Cloud Composer):** Develop an Airflow DAG (Python) that invokes the BigQuery Stored Procedure, handles scheduling, and provides monitoring.
    *   **Option B (Cloud Scheduler):** Configure a Cloud Scheduler job to directly trigger the BigQuery Stored Procedure, or trigger a Cloud Function that calls the procedure.

5.  **Testing:**
    *   Unit tests for the BigQuery Stored Procedure with various input parameters and data scenarios.
    *   Integration tests to verify the end-to-end data flow, including data ingestion and orchestration.
    *   Performance testing to ensure the BigQuery solution meets performance SLAs.

6.  **Deployment:**
    *   Deploy DDL for BigQuery tables.
    *   Deploy BigQuery Stored Procedure.
    *   Deploy ingestion pipelines.
    *   Deploy orchestration (Airflow DAG or Cloud Scheduler job).