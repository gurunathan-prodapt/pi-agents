# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

## 1. Purpose & Scope

This job is responsible for synchronizing contract data into the `ta_cntrct_crs3` table. It operates as a multi-component ETL workflow: an outer KornShell wrapper script (`r_ausd_v_ta_cntrct_crs3.ksh`) orchestrates a control script (`k_ausd_v_ta_cntrct_crs3.ksh`), which in turn executes an Oracle SQL script (`d_ausd_v_ta_cntrct_crs3.sql`). The SQL script performs a full refresh of the target table, truncating existing data and inserting new, transformed records based on contract information. The overall job is scheduled by a UC4 (Automic Workload Automation) job.

The scope of this migration design is to translate this entire workflow to the Google Cloud Platform, specifically leveraging BigQuery for data transformation and storage, and Airflow (Google Cloud Composer) for orchestration.

## 2. Source Inventory

This job is categorized as `semi_auto` for migration, indicating a need for some manual intervention in the migration process.

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh`
    *   **Technology**: KornShell (wrapper script)
    *   **Role**: Orchestrates the entire data synchronization process. Handles environment setup, parameter parsing, logging initialization, and calls the core control script.
    *   **Complexity Tier**: (Not available in `file_complexity` table, assumed Medium based on wrapper logic)
    *   **Automation Bucket**: Semi-Auto (B2)

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh`
    *   **Technology**: KornShell (control script)
    *   **Role**: Serves as a control script, handling parameters from the wrapper, loading SQL execution helpers, and invoking the core Oracle SQL script. Manages job identification and status.
    *   **Complexity Tier**: (Not available in `file_complexity` table, assumed Medium based on control logic)
    *   **Automation Bucket**: Semi-Auto (B2)

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql`
    *   **Technology**: Oracle SQL (DML script)
    *   **Role**: Core data transformation logic. Truncates the target `sof$ta_cntrct_crs3` table and inserts transformed data derived from `sof$ta_cntrct_crs2`, identifying "Twinbill" contracts.
    *   **Complexity Tier**: (Not available in `file_complexity` table, assumed Medium based on transformation complexity)
    *   **Automation Bucket**: Semi-Auto (B2)

## 3. Target Architecture

The migrated job will run entirely within Google Cloud, utilizing:

*   **Orchestration**: Google Cloud Composer (Airflow)
    *   An Airflow DAG will replace the UC4 scheduler and the KornShell wrapper/control scripts.
    *   The DAG will define tasks for logging, executing the BigQuery stored procedure, and handling success/failure states.
*   **Data Storage & Transformation**: Google BigQuery
    *   Source tables (`sof$ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`) will be migrated to BigQuery as `project.dataset.sof_ta_cntrct_crs2` and `project.dataset.dwtk_meldungen`.
    *   The target table (`sof$ta_cntrct_crs3`) will be migrated to BigQuery as `project.dataset.sof_ta_cntrct_crs3`.
    *   The core transformation logic from `d_ausd_v_ta_cntrct_crs3.sql` will be implemented as a BigQuery stored procedure: `project.dataset.r_ausd_v_ta_cntrct_crs3`.
    *   A new BigQuery audit log table (`project.dataset.job_audit_log`) will capture job execution details, replacing the `DWMSG_*` logging and `dwtk_meldungen` interactions for job status.

## 4. Data Flow & Lineage

The original data flow is as follows:
1.  **UC4 Job (DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml)** triggers `r_ausd_v_ta_cntrct_crs3.ksh`.
2.  **`r_ausd_v_ta_cntrct_crs3.ksh`** (Wrapper) sets up the environment, initializes logging, and invokes `k_ausd_v_ta_cntrct_crs3.ksh` with job parameters.
3.  **`k_ausd_v_ta_cntrct_crs3.ksh`** (Control) further processes parameters, loads SQL*Plus helper functions, and executes `d_ausd_v_ta_cntrct_crs3.sql` via SQL*Plus. It also reads temporary files for record counts.
4.  **`d_ausd_v_ta_cntrct_crs3.sql`** (Oracle SQL) connects to the Oracle database, reads `isbert_schema.dwtk_meldungen` to determine a date, truncates `sof$ta_cntrct_crs3`, and then performs an `INSERT ... SELECT` operation from `sof$ta_cntrct_crs2` into `sof$ta_cntrct_crs3`.
5.  Logging information is written to local log files and potentially to `dwtk_meldungen` via `DWMSG_*` functions.

In the BigQuery target architecture, the data flow will be:
1.  **Airflow DAG** (e.g., `dag_r_ausd_v_ta_cntrct_crs3`) is scheduled and triggered.
2.  The DAG initiates a **BigQuery stored procedure call** to `project.dataset.r_ausd_v_ta_cntrct_crs3`, passing any required parameters.
3.  The **BigQuery stored procedure** performs the following:
    *   Reads `project.dataset.dwtk_meldungen` (or an equivalent audit table) to determine a processing date.
    *   Inserts a "START" entry into `project.dataset.job_audit_log`.
    *   Executes a `TRUNCATE TABLE` statement on `project.dataset.sof_ta_cntrct_crs3`.
    *   Executes the `INSERT INTO ... SELECT` statement, reading data from `project.dataset.sof_ta_cntrct_crs2` and writing transformed data to `project.dataset.sof_ta_cntrct_crs3`.
    *   Captures the number of inserted rows.
    *   Inserts a "SUCCESS" or "ERROR" entry into `project.dataset.job_audit_log` with relevant details and record counts.

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_v_ta_cntrct_crs3.sql` and will be replicated in the BigQuery stored procedure.

**Source Tables**:
*   `sof$ta_cntrct_crs2`: Contains contract base data.
*   `isbert_schema.dwtk_meldungen`: Used to derive a processing date based on the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

**Target Table**:
*   `sof$ta_cntrct_crs3`: The destination table for the transformed contract data.

**Transformation Steps**:
1.  **Date Determination**: The processing date (`v_datum`) is derived by selecting the maximum `timecreated` from `isbert_schema.dwtk_meldungen` for a specific `job_kennung`. This will be converted using BigQuery's `COALESCE` and `FORMAT_DATE` functions.
2.  **Table Refresh**: The target table `sof$ta_cntrct_crs3` is fully refreshed. This translates directly to a `TRUNCATE TABLE` followed by an `INSERT INTO` statement in BigQuery.
3.  **Main Data Load (UNION)**: Data is loaded into `sof$ta_cntrct_crs3` from `sof$ta_cntrct_crs2` using a `UNION` of two `SELECT` statements, both involving a self-join on `sof$ta_cntrct_crs2` to identify parent-child contract relationships for "Twinbill" logic.
    *   **First `SELECT` (Primary Contracts)**: Selects contracts from `c` (`sof$ta_cntrct_crs2`) where `cntrct_ty` is *not* `10` (RV) or `20` (Mobilfunkzusatzvertrag). It then `LEFT JOIN`s to `ctb` (`sof$ta_cntrct_crs2`) on `c.cntrct_id = ctb.cntrct_parent` where `ctb.cntrct_ty = 20` to identify potential "Twinbill" children. `twinbill` is set to 'TB' if a matching child is found, otherwise NULL.
    *   **Second `SELECT` (Mobilfunkzusatzvertrag)**: Selects contracts from `ctb` (`sof$ta_cntrct_crs2`) where `ctb.cntrct_ty` *is* `20`. It `JOIN`s to `c` (`sof$ta_cntrct_crs2`) on `c.cntrct_id = ctb.cntrct_parent` where `c.cntrct_ty` is *not* `10` or `20`. Here, `twinbill` is always 'TB', and `twin_vertrag_id` is the parent contract ID (`c.cntrct_id`).
    *   The `UNION` combines these two result sets, effectively creating a comprehensive list of contracts with derived twinbill information.
4.  **Column Mapping**: All original columns from `sof$ta_cntrct_crs2` relevant to the `INSERT` statement are directly mapped to `sof$ta_cntrct_crs3`. Derived columns `twinbill` and `twin_vertrag_id` are populated as described above.

**BigQuery SQL Pseudocode (Stored Procedure)**:
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_cntrct_crs3`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64
)
BEGIN
  DECLARE v_datum STRING;
  DECLARE v_records INT64 DEFAULT 0;
  -- Error handling and logging tasks as outlined in the gen_design output.

  -- Derive date
  SET v_datum = (SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
                 FROM `project.dataset.dwtk_meldungen` m
                 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');

  -- Log Start
  INSERT INTO `project.dataset.job_audit_log` (...) VALUES (...);

  -- Truncate target
  TRUNCATE TABLE `project.dataset.sof_ta_cntrct_crs3`;

  -- Insert transformed data
  INSERT INTO `project.dataset.sof_ta_cntrct_crs3` (...)
  SELECT ...
  FROM (
      SELECT /* First branch: primary contracts */
             c.cntrct_id, ...,
             CASE WHEN ctb.cntrct_id IS NOT NULL THEN 'TB' END AS twinbill,
             ctb.cntrct_id AS twin_vertrag_id
      FROM `project.dataset.sof_ta_cntrct_crs2` c
      LEFT JOIN `project.dataset.sof_ta_cntrct_crs2` ctb
        ON c.cntrct_id = ctb.cntrct_parent AND ctb.cntrct_ty = 20
      WHERE c.cntrct_ty NOT IN (10, 20)

      UNION DISTINCT

      SELECT /* Second branch: Mobilfunkzusatzvertrag */
             ctb.cntrct_id, ...,
             'TB' AS twinbill,
             c.cntrct_id AS twin_vertrag_id
      FROM `project.dataset.sof_ta_cntrct_crs2` c
      JOIN `project.dataset.sof_ta_cntrct_crs2` ctb
        ON c.cntrct_id = ctb.cntrct_parent
      WHERE ctb.cntrct_ty = 20 AND c.cntrct_ty NOT IN (10, 20)
  );
  SET v_records = @@row_count;

  -- Log Success / Error
  INSERT INTO `project.dataset.job_audit_log` (...) VALUES (...);

EXCEPTION WHEN ERROR THEN
  -- Error logging and re-raise
END;
```

## 6. External Dependencies

*   **UC4 (Automic Workload Automation)**: This is the existing scheduler.
    *   **Replacement**: The scheduling will be handled by an Airflow DAG in Google Cloud Composer.
*   **Oracle Database**: Hosts `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, and `isbert_schema.dwtk_meldungen`.
    *   **Replacement**: These tables will be migrated to BigQuery. Initial data ingestion can be achieved using Google Cloud tools like Datastream for CDC (Change Data Capture) or batch loads (e.g., Dataflow, Storage Transfer Service) if `sof$ta_cntrct_crs2` is frequently updated or is a large table. For `dwtk_meldungen`, an equivalent audit log system in BigQuery will be established.
*   **Carmen DB (`@pcrs1` DB-Link)**: The `DEFINE v_carmen = "@pcrs1"` in the SQL script indicates a potential external Oracle system. However, `v_carmen` is not directly used in the `FROM` clause of the main `INSERT...SELECT`. If `sof$ta_cntrct_crs2` is sourced from Carmen, then that data source connection needs to be established. If `v_carmen` is a remnant or for other parts of a larger script, it might not need a direct replacement beyond ensuring all necessary source data is in BigQuery.
    *   **Replacement**: If `sof$ta_cntrct_crs2` and other necessary source tables ultimately originate from the Carmen DB, a robust data ingestion pipeline (e.g., Datastream for real-time or batch export/load) will be required to bring the Carmen data into BigQuery.
*   **Shell Utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`)**: These are shared shell scripts for environment setup, error handling, parameter parsing, and SQL*Plus execution.
    *   **Replacement**: Their functionalities will be absorbed by the Airflow DAG (for orchestration, parameter passing, and basic error handling) and the BigQuery stored procedure (for SQL execution and error trapping). A BigQuery audit table will replace the custom logging framework. Python operators within the Airflow DAG can handle any complex parameter logic if not directly convertible to BigQuery stored procedure parameters.

## 7. Unresolved / Risks

*   **Missing File Complexity Data**: The `file_complexity` table returned no rows for the component files. This means there's no official "tier" or `migration_flags` to provide insights into specific migration challenges. The assumed "Medium" complexity for each file is an estimate.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`**: The truncation uses a stored procedure. While `TRUNCATE TABLE` is straightforward in BigQuery, any complex logic within `DWPA_UTIL_SKRIPT` (if it does more than just truncate) needs to be analyzed and replicated if necessary. Assuming it solely performs a truncate here.
*   **Dynamic `v_datum` from `dwtk_meldungen`**: The logic to derive a processing date from a "job log" table (`dwtk_meldungen`) for `BERT_DROP_TEMP_TABLE` job ID might imply a dependency on a sequence of jobs or specific system events. This needs careful consideration to ensure the correct date is always picked in BigQuery.
*   **DB-Link to Carmen DB**: The presence of `@pcrs1` implies connectivity to another Oracle instance. While not directly used in the main `SELECT` of `d_ausd_v_ta_cntrct_crs3.sql`, its ultimate source of `sof$ta_cntrct_crs2` needs to be confirmed. If Carmen DB is the ultimate source, a dedicated ingestion pipeline from Carmen to BigQuery is crucial and might be a significant effort.
*   **Shell-specific features**: Aspects like shell `trap` commands, `getopts` for parameter parsing in shell, `print` / `tee` for logging, and temporary file usage for record counts will not have direct BigQuery equivalents. These will be handled by Airflow orchestration logic (e.g., Airflow's native error handling, passing parameters to BigQuery procedures, BigQuery's `@@row_count`, and audit logging).

## 8. Build Plan

This plan outlines the ordered steps to build the migrated job on Google Cloud.

1.  **Migrate Source Data to BigQuery**:
    *   **Action**: Create BigQuery datasets for source and target tables (e.g., `project.dataset`).
    *   **Action**: Ingest `sof$ta_cntrct_crs2` into `project.dataset.sof_ta_cntrct_crs2`. This might involve one-time batch load followed by a CDC stream or scheduled batch updates if needed.
    *   **Action**: Ingest `isbert_schema.dwtk_meldungen` into `project.dataset.dwtk_meldungen` or design an equivalent BigQuery audit table that captures `timecreated` and `job_kennung` for the date derivation logic.
    *   **Action**: Create the empty target table `project.dataset.sof_ta_cntrct_crs3` with the appropriate schema matching the Oracle source.
    *   **Dependency**: Requires access to the source Oracle databases.

2.  **Develop BigQuery Stored Procedure**:
    *   **Action**: Translate the logic from `d_ausd_v_ta_cntrct_crs3.sql` into a BigQuery stored procedure `project.dataset.r_ausd_v_ta_cntrct_crs3`.
    *   **Action**: Implement BigQuery-native error handling (e.g., `EXCEPTION WHEN ERROR THEN`) and logging to `project.dataset.job_audit_log`.
    *   **Action**: Replace Oracle-specific functions (e.g., `NVL` with `COALESCE`, `TO_CHAR` with `FORMAT_DATE`).
    *   **Action**: Adapt the `TRUNCATE TABLE` and `INSERT INTO ... SELECT` statements for BigQuery syntax.
    *   **Action**: Incorporate the date derivation logic from `dwtk_meldungen`.
    *   **Language**: BigQuery SQL.

3.  **Develop Airflow DAG**:
    *   **Action**: Create an Airflow DAG (Python script) to orchestrate the job.
    *   **Action**: Define a task using `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` to call `project.dataset.r_ausd_v_ta_cntrct_crs3`.
    *   **Action**: Implement logging within the DAG to capture job start/end times, parameters, and status, possibly writing to `project.dataset.job_audit_log` (if not fully handled by the BQ SP) or Cloud Logging.
    *   **Action**: Configure DAG scheduling to match the original UC4 job's schedule.
    *   **Language**: Python.

4.  **Testing**:
    *   **Action**: Unit test the BigQuery stored procedure with sample data.
    *   **Action**: Test the Airflow DAG end-to-end in a development environment, verifying correct execution, logging, and data output.
    *   **Action**: Perform data validation to ensure consistency between source Oracle and target BigQuery tables post-migration.

5.  **Deployment**:
    *   **Action**: Deploy the BigQuery stored procedure to the target BigQuery project.
    *   **Action**: Deploy the Airflow DAG to Google Cloud Composer.
    *   **Action**: Monitor the migrated job in production.