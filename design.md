# Migration Design — DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Purpose & Scope
The original UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` is designed to orchestrate the initial provisioning and preparation of selected basic products related to MSISDN data for the BERT system. This involves executing a series of KornShell scripts that ultimately run an Oracle SQL script to extract, transform, and load data into a target table.

The scope of this migration is to re-platform this ETL workflow from its legacy UC4/KornShell/Oracle environment to a modern Google Cloud Platform (GCP) architecture. This includes converting the UC4 job into an Apache Airflow Directed Acyclic Graph (DAG) for orchestration, translating the KornShell script logic into Python, and migrating the Oracle SQL data transformation to BigQuery SQL.

## 2. Source Inventory
The job is composed of four main components: an UC4 orchestration file, two KornShell wrapper scripts, and a core Oracle SQL script.

*   **UC4 Orchestration Job:**
    *   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`
    *   **Technology:** UC4/Automic (JOBS_UNIX type)
    *   **Migration Bucket:** `semi_auto`
    *   **Description:** This XML defines a UC4 Unix job named `DW.BERT_AUSD_BP_TA_BCP_MSISDN`. It is responsible for calling an external KornShell script (`r_ausd_bp_ta_bcp_msisdn.ksh`) on a Unix host (`DWHDWH2P`) using a specific login (`DW.UNIX.ISBERT`). It also includes UC4 variables/includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) and sets a job identifier (`&DWH_JOB_KENNUNG`).
*   **Main KornShell Wrapper Script:**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh`
    *   **Technology:** KornShell
    *   **Migration Bucket:** `semi_auto`
    *   **Description:** This script serves as the primary wrapper. It initializes the environment (`. $HOME/.dw_init`), sets up error handling (`f_alis_msgerr.ksh`), parses command-line arguments (e.g., `Stichtag`, `Wiederanlaufwert`), determines processing dates, and then invokes the `k_ausd_bp_ta_bcp_msisdn.ksh` script with collected parameters. It also includes logging mechanisms (`DWMSG_...`).
*   **Inner KornShell Wrapper Script:**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`
    *   **Technology:** KornShell
    *   **Migration Bucket:** `semi_auto` (inferred)
    *   **Description:** This script is called by `r_ausd_bp_ta_bcp_msisdn.ksh`. It further processes parameters, includes SQL*Plus utility scripts (`h_alis_sqlplus.ksh`), and crucially, executes the core Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) using a `starteSQLSkript` function (likely a wrapper for SQL*Plus execution). It manages temporary files and logs the record count.
*   **Core Oracle SQL Script:**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql`
    *   **Technology:** Oracle SQL
    *   **Migration Bucket:** `manual`
    *   **Description:** This script contains the primary data transformation logic. It truncates the target table `sof$ta_bcp_msisdn` and then populates it by performing a `DISTINCT` join between `sof$ta_bpr_bcp` and `sof$ta_rn_vertrag` tables on `cntrct_id_ref`. It also retrieves a `v_datum` variable from the `isbert_schema.dwtk_meldungen` metadata table and utilizes Oracle-specific PL/SQL calls (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) and hints (`/*+ full(...) parallel(...) */`).

## 3. Target Architecture
The migrated job will leverage GCP services for orchestration, data processing, and storage.

*   **Orchestration:** Apache Airflow on Cloud Composer.
    *   The UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` will be replaced by an Airflow DAG named `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Data Processing:** Python on Google Cloud Dataproc.
    *   The logic contained within `r_ausd_bp_ta_bcp_msisdn.ksh` and `k_ausd_bp_ta_bcp_msisdn.ksh` (parameter handling, logging, environment setup, SQL execution wrapper) will be reimplemented in a single Python script (e.g., `r_ausd_bp_ta_bcp_msisdn.py`).
    *   This Python script will execute the translated BigQuery SQL using BigQuery client libraries.
    *   The Airflow DAG will trigger this Python script via a `DataprocSubmitJobOperator`, running it as a PySpark job on a Dataproc cluster.
*   **Data Storage:** Google BigQuery.
    *   The Oracle source tables `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, and the target table `sof$ta_bcp_msisdn` will be migrated to BigQuery as `sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, and `sof_ta_bcp_msisdn` respectively (using appropriate GCP project and dataset naming conventions).
    *   The metadata table `isbert_schema.dwtk_meldungen` will also be migrated to BigQuery as `isbert_schema.dwtk_meldungen`.

## 4. Data Flow & Lineage
The end-to-end data flow in the target architecture will be as follows:

1.  **Airflow DAG Trigger:** The `dw_bert_ausd_bp_ta_bcp_msisdn` Airflow DAG is triggered on Cloud Composer. As no schedule was specified in the source UC4 XML, this will initially be an unscheduled DAG requiring manual or external invocation, or a schedule will need to be defined.
2.  **Dataproc Job Submission:** The Airflow DAG's single task, implemented as a `DataprocSubmitJobOperator` (e.g., `run_dw_bert_ausd_bp_ta_bcp_msisdn`), submits a PySpark job to a configured Dataproc cluster.
3.  **Python Script Execution:** The PySpark job executes the Python script `r_ausd_bp_ta_bcp_msisdn.py`. This script will:
    *   Receive and process parameters (e.g., Stichtag) passed from Airflow.
    *   Retrieve the `v_datum` value by querying the `isbert_schema.dwtk_meldungen` table in BigQuery.
    *   Execute a BigQuery DDL statement to `TRUNCATE` the target table `sof_ta_bcp_msisdn`.
    *   Execute a BigQuery SQL DML statement to `INSERT` data into `sof_ta_bcp_msisdn` by joining `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` tables.
    *   Manage logging and error reporting using Python's standard libraries, which will integrate with Cloud Logging.
4.  **Target Update:** Upon successful execution of the BigQuery SQL, the `sof_ta_bcp_msisdn` table in BigQuery is updated.

**Lineage (Conceptual):**
`Airflow DAG (dw_bert_ausd_bp_ta_bcp_msisdn)`
  `-> DataprocSubmitJobOperator (run_dw_bert_ausd_bp_ta_bcp_msisdn)`
    `-> Python Script (r_ausd_bp_ta_bcp_msisdn.py)`
      `-> Reads: BigQuery (isbert_schema.dwtk_meldungen)`
      `-> Truncates: BigQuery (sof_ta_bcp_msisdn)`
      `-> Reads: BigQuery (sof_ta_bpr_bcp)`
      `-> Reads: BigQuery (sof_ta_rn_vertrag)`
      `-> Writes: BigQuery (sof_ta_bcp_msisdn)`

## 5. Transformation Logic
The core data transformation is an `INSERT...SELECT` operation, preceded by a `TRUNCATE` and a date variable retrieval.

*   **Original Oracle SQL (`d_ausd_bp_ta_bcp_msisdn.sql`):**
    ```sql
    -- 1. Retrieve date variable
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
      FROM isbert_schema.dwtk_meldungen m
     WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- 2. Truncate target table
    begin
    isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_msisdn REUSE STORAGE');
    end;
    /

    -- 3. Insert transformed data
    INSERT INTO sof$ta_bcp_msisdn
    (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN )
    SELECT /*+ full(bp) parallel(bp,4) full(rn) parallel(rn,4) */
              distinct
              bp.cntrct_id,
              bp.bpr_id,
              bp.cntrct_id_ref,
              rn.tn_tel_msisdn
    FROM      sof$ta_bpr_bcp  bp,
              sof$ta_rn_vertrag  rn
    WHERE     bp.cntrct_id_ref = rn.cntrct_id
    ;
    COMMIT;
    ```

*   **Proposed BigQuery SQL Translation:**
    1.  **Date Variable Retrieval:** The Python script will handle this.
        *   `COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')` from `your_gcp_project.isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  **Truncate Target Table:**
        ```sql
        TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`;
        ```
        The Oracle PL/SQL wrapper (`DWPA_UTIL_SKRIPT.runstatement`) will be replaced by direct BigQuery DDL execution via the Python client.
    3.  **Insert Transformed Data:**
        ```sql
        INSERT INTO `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)
        SELECT DISTINCT
               bp.cntrct_id,
               bp.bpr_id,
               bp.cntrct_id_ref,
               rn.tn_tel_msisdn
        FROM `your_gcp_project.your_bigquery_dataset.sof_ta_bpr_bcp` AS bp
        JOIN `your_gcp_project.your_bigquery_dataset.sof_ta_rn_vertrag` AS rn
        ON bp.cntrct_id_ref = rn.cntrct_id;
        ```
        Oracle-specific hints (`/*+ full(...) parallel(...) */`) will be removed as BigQuery's query optimizer automatically handles execution plans and parallelism. `COMMIT` is implicit in BigQuery transactions.

*   **Shell Script to Python Conversion:**
    *   All shell environment sourcing (`. $HOME/.dw_init`), parameter parsing (`getopts`), logging (`DWMSG_...`), and error handling (`trap`, `f_alis_msgerr.ksh`) will be reimplemented in Python using libraries such as `argparse` and the standard `logging` module.
    *   Date utility functions will be replaced by Python's `datetime` module.
    *   The `starteSQLSkript` wrapper will be replaced by direct invocation of BigQuery client methods to execute the translated SQL queries.

## 6. External Dependencies
*   **UC4/Automic Scheduler:** This external dependency will be replaced by Apache Airflow running on Cloud Composer.
*   **Oracle Database:**
    *   All referenced Oracle tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, `isbert_schema.dwtk_meldungen`) will be migrated to BigQuery.
    *   Oracle-specific functions/procedures like `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` will be re-implemented using native BigQuery DDL commands executed via Python.
    *   Any implicit database link usage indicated by `v_carmen = "@pcrs1"` will be replaced by direct BigQuery table references within the Python script.
*   **UNIX Operating System and Environment:** The execution environment provided by the UNIX host (`DWHDWH2P`) and user login (`DW.UNIX.ISBERT`) will be replaced by a Dataproc cluster (for script execution) and GCP service accounts with appropriate IAM roles and permissions.
*   **KornShell Utilities:** Standard shell commands and custom KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will be re-implemented in Python or replaced by equivalent BigQuery/Airflow features. For example, local file operations like `sed`, `sort`, `join` (if active) would be converted to Python data processing or BigQuery DML.

## 7. Unresolved / Risks
*   **Manual SQL Conversion:** The `manual` migration bucket for `d_ausd_bp_ta_bcp_msisdn.sql` indicates that certain Oracle SQL/PL/SQL constructs, particularly the `DWPA_UTIL_SKRIPT.runstatement` call, will require manual analysis and re-engineering to ensure correct and efficient translation to BigQuery SQL and Python.
*   **Scheduler Configuration:** The original UC4 job XML did not specify a schedule (no `EVNT_TIME` file). A definitive schedule for the Airflow DAG will need to be established (e.g., a cron expression or triggered by an upstream event).
*   **Environment Variables & Configuration:** The exact details of environment variables set by `. $HOME/.dw_init` and how `BERT_DIR_ROOT` is resolved need to be fully understood to ensure proper configuration of the Dataproc environment and the Python script. These will need to be managed through Airflow Variables, environment variables on Dataproc, or configuration files.
*   **Parameter Default Values and Edge Cases:** While parameter parsing is identified, the full range of default values, validation rules, and error conditions in the shell scripts must be accurately replicated in the Python implementation.
*   **Logging and Alerting Integration:** The legacy `DWMSG_...` logging and `trap` mechanisms need to be carefully mapped to Cloud Logging and Cloud Monitoring alerts to ensure operational visibility and incident response capabilities are maintained or improved.
*   **Completeness of Job Definition:** The analysis noted that only a single `JOBS_UNIX` object was provided, not a complete UC4 workflow. This implies potential missing upstream or downstream dependencies or related jobs that might be part of a larger business process. This migration addresses only the provided job as a standalone entity.

## 8. Build Plan
1.  **BigQuery Schema Definition (DDL):**
    *   Define and deploy BigQuery DDL for the target tables: `sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `sof_ta_bcp_msisdn`, and `isbert_schema.dwtk_meldungen` within the designated GCP project and dataset.
    *   **Language:** BigQuery DDL
2.  **Historical Data Migration:**
    *   Perform a one-time migration of historical data from the Oracle source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, `isbert_schema.dwtk_meldungen`) to their corresponding BigQuery tables. This is a prerequisite for functional testing.
    *   **Language:** Data Migration Tooling (e.g., Database Migration Service, Cloud Dataflow)
3.  **Python Script Development:**
    *   Develop `r_ausd_bp_ta_bcp_msisdn.py`:
        *   Re-implement the parameter parsing, validation, and date calculation logic from `r_ausd_bp_ta_bcp_msisdn.ksh` and `k_ausd_bp_ta_bcp_msisdn.ksh` using Python's `argparse` and `datetime` libraries.
        *   Translate the core SQL logic from `d_ausd_bp_ta_bcp_msisdn.sql` into BigQuery SQL statements. Implement BigQuery client calls (using `google-cloud-bigquery` library) for executing the `TRUNCATE` and `INSERT...SELECT` operations, and for retrieving the `v_datum` variable.
        *   Integrate Python's standard `logging` module for robust logging.
        *   Implement error handling and retry logic as appropriate, consistent with Airflow best practices.
    *   **Language:** Python 3
4.  **Airflow DAG Development:**
    *   Create `dw_bert_ausd_bp_ta_bcp_msisdn.py` Airflow DAG:
        *   Define the DAG structure with appropriate `dag_id`, `start_date`, `catchup` settings.
        *   Include a `DataprocSubmitJobOperator` task (`run_dw_bert_ausd_bp_ta_bcp_msisdn`) that points to the `r_ausd_bp_ta_bcp_msisdn.py` script on a GCS bucket.
        *   Configure required Dataproc parameters (project ID, region, cluster name) and GCS bucket for the script.
        *   Define `default_args` including the owner (`DW.UNIX.ISBERT`).
        *   Determine and implement the desired schedule (e.g., `schedule_interval='@daily'`, or `None` for manual/external triggers).
    *   **Language:** Python 3
5.  **Deployment to GCP:**
    *   Deploy the BigQuery tables.
    *   Upload the `r_ausd_bp_ta_bcp_msisdn.py` Python script to the designated GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/`).
    *   Deploy the `dw_bert_ausd_bp_ta_bcp_msisdn.py` Airflow DAG to Cloud Composer.
6.  **Testing and Validation:**
    *   Conduct comprehensive unit, integration, and end-to-end testing to verify functional equivalence, data accuracy, performance, and robustness in the GCP environment.
    *   Monitor Cloud Logging and Cloud Monitoring for successful execution and anomaly detection.