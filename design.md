# Migration Design — DW.BERT_AUSD_V_TA_P_DISCOUNT_RR

## 1. Purpose & Scope
This job's primary purpose is to prepare and enrich discount data for reporting. Specifically, it adds contract number and contract template details to existing discount information. The process involves orchestrating an Oracle SQL script via a series of KornShell scripts, with the entire workflow managed by a UC4 job scheduler. The output is a populated `sof$ta_p_discount_rr` table, which serves as a source for further reporting or data analysis.

The scope of this migration is to translate the existing UC4-orchestrated KornShell and Oracle SQL pipeline into a Google Cloud BigQuery-centric solution, utilizing Airflow for orchestration and BigQuery SQL for data transformation.

## 2. Source Inventory
The job is composed of four primary source files:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml`**
    *   **Technology:** UC4/Automic Job Definition (Unix Job)
    *   **Summary:** Orchestrates the execution of the main KornShell wrapper script. It sets environment variables and includes logging functionality.
    *   **Automation Bucket:** `semi_auto`
    *   **Complexity Tier:** Not available in metadata.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** This is a wrapper script that handles environment setup, error trapping, parameter parsing, logging, and orchestrates the execution of the core KornShell script `k_ausd_v_ta_p_discount_rr.ksh`.
    *   **Automation Bucket:** `semi_auto`
    *   **Complexity Tier:** Not available in metadata.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`**
    *   **Technology:** KornShell Script
    *   **Summary:** This control script is responsible for environment setup, parameter handling, error checking, and executing the Oracle SQL*Plus script `d_ausd_v_ta_p_discount_rr.sql`. It passes relevant job and entry number parameters to the SQL script.
    *   **Automation Bucket:** `semi_auto`
    *   **Complexity Tier:** Not available in metadata.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql`**
    *   **Technology:** Oracle SQL*Plus
    *   **Summary:** This script truncates the target table `sof$ta_p_discount_rr` and populates it by joining data from `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`. It enriches discount information with contract details and defines a `v_carmen` DB-Link.
    *   **Automation Bucket:** `semi_auto`
    *   **Complexity Tier:** Not available in metadata.

## 3. Target Architecture
The migrated job will run on Google Cloud Platform, leveraging the following components:

*   **Orchestration:** Apache Airflow DAG hosted on Cloud Composer.
*   **Data Transformation:** BigQuery SQL for all data processing logic.
*   **Data Storage:** Google BigQuery tables for source and target data.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring integrated with Airflow.
*   **Environment Management:** Python-based scripts/operators within Airflow to handle environment variables and parameter passing.

## 4. Data Flow & Lineage
The original job executes in a sequential manner:
1.  **UC4 Job `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR`**: Initiates the process.
2.  **`r_ausd_v_ta_p_discount_rr.ksh`**: This wrapper script is invoked by the UC4 job. It performs initial setup and calls `k_ausd_v_ta_p_discount_rr.ksh`.
3.  **`k_ausd_v_ta_p_discount_rr.ksh`**: This control script is called by `r_ausd_v_ta_p_discount_rr.ksh`. It prepares parameters and executes the `d_ausd_v_ta_p_discount_rr.sql` script.
4.  **`d_ausd_v_ta_p_discount_rr.sql`**: This Oracle SQL script is executed by `k_ausd_v_ta_p_discount_rr.ksh`. It reads data from source Oracle tables:
    *   `sof$ta_discount_rr`
    *   `sof$ta_cntrct_crs`
    *   `sof$ta_cntrct_templ`
    *   `isbert_schema.dwtk_meldungen` (for `v_datum`)
    It then truncates and inserts into the target table `sof$ta_p_discount_rr`.

**Migrated Data Flow (Airflow DAG):**
The Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr` will have a linear flow:

*   **`start` task (EmptyOperator)**
*   **`main_data_processing` task (BigQueryOperator or PythonOperator + BigQuery client)**: This task will encapsulate the transformed logic of `d_ausd_v_ta_p_discount_rr.sql`. It will:
    *   Truncate the target BigQuery table `ta_p_discount_rr`.
    *   Insert data by selecting and joining from the corresponding BigQuery source tables (`ta_discount_rr`, `ta_cntrct_crs`, `ta_cntrct_templ`).
    *   Handle the `v_datum` logic by querying the equivalent BigQuery logging/metadata table.
*   **`post_processing_log` task (PythonOperator or EmptyOperator)**: This will replace the `:inc DW.BERT_LESE_LOG` functionality, potentially writing to Cloud Logging or a specific logging table in BigQuery.
*   **`end` task (EmptyOperator)**

The execution order in Airflow will mirror the original: `start` -> `main_data_processing` -> `post_processing_log` -> `end`.

## 5. Transformation Logic

**UC4 Orchestration (`DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml`):**
*   **Legacy:** UNIX job calling a KSH script, setting job-specific identifiers.
*   **Target:** Migrated to an Airflow DAG (`dw_bert_ausd_v_ta_p_discount_rr`). The schedule will be determined during the build phase (likely a daily or scheduled trigger). Environment initialization (`. $HOME/.dw_init`) will be managed through Airflow environment variables or Python operators. Job-specific identifiers will be passed as Airflow task parameters.

**KornShell Scripts (`r_ausd_v_ta_p_discount_rr.ksh`, `k_ausd_v_ta_p_discount_rr.ksh`):**
*   **Legacy:** These scripts primarily handle job orchestration, environment sourcing, parameter passing (e.g., `p_JobKennung`, `p_EintragsNr`), error handling (using `f_alis_msgerr.ksh`), and execution of the SQL script.
*   **Target:** The core orchestration logic (sequencing, error handling, parameter management) will be absorbed by the Airflow DAG structure and Python operators.
    *   Environment sourcing (`. $HOME/.dw_init`, `BERT_DIR_ROOT`) will be handled by Airflow task environment variables or within Python operators.
    *   Parameter parsing (`getopts`) will be replaced by Airflow task parameters.
    *   Error logging and messaging (`DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`) will be replaced by Airflow's built-in logging and potentially custom Python operators writing to BigQuery logging tables or Cloud Logging.
    *   The function `starteSQLSkript` will be replaced by a `BigQueryOperator` or a Python operator executing BigQuery SQL.

**Oracle SQL*Plus Script (`d_ausd_v_ta_p_discount_rr.sql`):**
*   **Legacy:**
    *   `WHENEVER SQLERROR CONTINUE/EXIT FAILURE`: Error handling.
    *   `SET TIMING ON`, `SET SERVEROUTPUT ON`: SQL*Plus specific commands.
    *   `DEFINE v_carmen = "@pcrs1"`: Defines a DB-Link for source systems.
    *   `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'`: Dynamic date calculation.
    *   `TRUNCATE TABLE sof$ta_p_discount_rr`: Clears the target table.
    *   `INSERT INTO sof$ta_p_discount_rr (...) SELECT ... FROM sof$ta_discount_rr da, sof$ta_cntrct_crs c, sof$ta_cntrct_templ ct WHERE ...`: Main data transformation logic, joining three source tables and populating the target.
    *   `COMMIT`: Transaction commit.
*   **Target (BigQuery SQL):**
    *   Error handling will be managed by Airflow (retries, alerts).
    *   SQL*Plus commands will be removed.
    *   DB-Link usage (`@pcrs1`) implies fetching data from an external Oracle source. This will need to be replaced with federated queries (if possible and performant), data replication into BigQuery, or BigQuery External Tables. Assuming data is replicated to BigQuery, the `FROM` clause will directly reference BigQuery tables.
    *   The dynamic date calculation from `dwtk_meldungen` will be translated to a BigQuery SQL query against a corresponding metadata/logging table in BigQuery.
    *   `TRUNCATE TABLE` will map to `TRUNCATE TABLE` in BigQuery or `DELETE FROM <table_name> WHERE 1=1` for partitioned tables.
    *   The `INSERT ... SELECT` statement will be directly translatable to BigQuery SQL, assuming `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, and `sof$ta_p_discount_rr` are mapped to BigQuery tables. The `/*+ parallel(da,4) parallel(c,4) parallel(ct,4) */` hint will be removed as BigQuery automatically handles parallelism.
    *   `COMMIT` is not needed in BigQuery as operations are typically atomic.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Oracle Database (`@pcrs1` / `DATABASE`):** This is the primary source of operational data, referenced in the SQL script (`v_carmen = "@pcrs1"`) and indirectly by the table schemas (`sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`).
    *   **Replacement Strategy:** Data from the source Oracle database(s) must be ingested into BigQuery. This can be achieved through:
        *   **Batch Replication:** Using tools like Dataflow, Datastream, or Fivetran to replicate source tables into BigQuery on a scheduled basis. This is the recommended approach for performance and reliability.
        *   **BigQuery External Tables:** If real-time or near real-time access to Oracle is required, and latency/performance are acceptable, external tables querying through Cloud SQL Federation could be considered (though less common for high-volume ETL).
*   **`isbert_schema.dwtk_meldungen` (Oracle Table):** Used to determine the `v_datum` variable.
    *   **Replacement Strategy:** A corresponding logging or metadata table should exist in BigQuery. The query will be re-written to query this BigQuery table.
*   **`DWHDWH1P` (Host):** Referenced in the UC4 XML as `HostDst`.
    *   **Replacement Strategy:** This host environment will be replaced by the Cloud Composer environment hosting Airflow and the BigQuery processing environment.
*   **`DW.UNIX.ISBERT` (Login):** Referenced in the UC4 XML.
    *   **Replacement Strategy:** Replaced by Google Cloud service accounts with appropriate IAM roles for BigQuery and Cloud Composer access.
*   **Shell Utilities (`f_alis_msgerr.ksh`, `h_alis_job.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These KornShell utility scripts are sourced and used by the main job scripts.
    *   **Replacement Strategy:** The functionality of these utilities will be replaced by native Python functions within Airflow operators, or by standard Airflow features for logging, error handling, date calculations, and parameter management.

## 7. Unresolved / Risks
*   **`complexity_signals` and `file_purpose`:** These fields were empty in `file_analysis`, indicating a potential lack of detailed automated analysis for these files. This may necessitate more manual review during implementation.
*   **`file_complexity` data:** The `file_complexity` table had no entries for these files, so no complexity tier or migration flags were available. This means the semi-auto migration bucket is a general classification, and specific challenges might emerge during detailed design.
*   **Dynamic `v_datum` logic:** The dynamic date retrieval from `isbert_schema.dwtk_meldungen` needs careful mapping to a BigQuery equivalent. Ensuring the `BERT_DROP_TEMP_TABLE` job_kennung has a corresponding logging mechanism in BigQuery is crucial.
*   **Oracle to BigQuery Type Mapping:** Implicit data type conversions in Oracle SQL*Plus need to be carefully handled when translating to BigQuery SQL to prevent data loss or unexpected behavior.
*   **Performance Tuning:** The original SQL uses `parallel` hints. While BigQuery handles parallelism automatically, performance should be monitored and optimized post-migration.
*   **Data Latency:** The migration assumes data from Oracle source systems will be available in BigQuery. The chosen data ingestion method (e.g., batch replication) will define the data freshness and latency characteristics in the target environment.

## 8. Build Plan
The build plan will involve translating the legacy components into their BigQuery and Airflow equivalents, with careful attention to data type compatibility, performance, and operational aspects.

1.  **BigQuery Schema Definition (DDL):**
    *   Create target BigQuery table `sof_ta_p_discount_rr` (or `ta_p_discount_rr` in the target dataset) based on the schema derived from `d_ausd_v_ta_p_discount_rr.sql` and the source Oracle tables.
    *   Create BigQuery tables for source data: `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ` (assuming data replication to BigQuery).
    *   Create a BigQuery logging/metadata table to replace `isbert_schema.dwtk_meldungen`, if not already present.

2.  **Data Ingestion Pipeline (Oracle to BigQuery):**
    *   Implement a data ingestion pipeline (e.g., Dataflow, Datastream, or a third-party tool) to replicate `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, and `isbert_schema.dwtk_meldungen` from Oracle to BigQuery.

3.  **BigQuery SQL Transformation Script:**
    *   Translate `d_ausd_v_ta_p_discount_rr.sql` into a `bq_d_ausd_v_ta_p_discount_rr.sql` script (or a series of SQL statements within a Python script) that executes in BigQuery.
        *   Replace `TRUNCATE TABLE` and `INSERT INTO ... SELECT` with BigQuery equivalents.
        *   Translate Oracle-specific functions or syntax.
        *   Adjust table names to reference the BigQuery target tables.
        *   Implement the `v_datum` logic using the BigQuery metadata table.

4.  **Airflow DAG Construction:**
    *   Create an Airflow DAG file `dw_bert_ausd_v_ta_p_discount_rr.py`.
    *   Define the DAG properties (`dag_id`, `schedule`, `start_date`, `default_args`).
    *   Implement the `main_data_processing` task using a `BigQueryOperator` to run the translated BigQuery SQL.
    *   Implement the `post_processing_log` task as an `EmptyOperator` or `PythonOperator` for logging.
    *   Establish task dependencies as `start >> main_data_processing >> post_processing_log >> end`.
    *   Configure Airflow connections and variables for BigQuery project, dataset, and table names.

5.  **Parameter Management:**
    *   Define Airflow Variables or XComs to handle parameters that were previously managed by KornShell scripts.

6.  **Error Handling and Monitoring:**
    *   Leverage Airflow's native error handling, retries, and alerting mechanisms.
    *   Ensure Cloud Logging and Cloud Monitoring are configured for the Airflow DAG and BigQuery operations.

7.  **Testing and Validation:**
    *   Develop unit and integration tests for the BigQuery SQL transformation.
    *   Conduct end-to-end testing of the Airflow DAG to ensure data integrity and correct execution.

This build plan will result in the following new artifacts:
*   BigQuery DDL scripts for target and source tables.
*   BigQuery SQL script for the data transformation.
*   Python Airflow DAG definition (`.py` file).
*   Data ingestion pipeline definition (e.g., Dataflow template, Datastream configuration).