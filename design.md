# Migration Design — DW.BERT_AUSD_BP_TA_BCP_MSISDN

## 1. Purpose & Scope

This migration design document outlines the re-platforming of the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job from its legacy Oracle/UNIX/UC4 environment to Google Cloud Platform, utilizing BigQuery for data processing and Airflow for orchestration.

The job's primary purpose is the "preparation of instantiated basic products" related to MSISDN (Mobile Subscriber Integrated Services Digital Network Number) data for the BERT system. It involves an extract, transform, and load (ETL) process where data from source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`) is joined, enriched, and loaded into a target table (`sof$ta_bcp_msisdn`). The process is controlled by KornShell scripts and scheduled by a UC4 job.

## 2. Source Inventory

The `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job consists of the following source files:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`**
    *   **Technology:** UC4/Automic Job Definition (XML)
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Summary:** Defines the scheduling and execution parameters for the main KornShell script. It specifies the host (`DWHDWH2P`), login (`DW.UNIX.ISBERT`), and initiates the `r_ausd_bp_ta_bcp_msisdn.ksh` script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh`**
    *   **Technology:** KornShell Script
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Summary:** This script acts as the main entry point for the shell-based logic. It sources environment initialization (`. $HOME/.dw_init`), sets up error handling, parses command-line parameters (`-s` for Stichtag, `-l` for Wiederanlaufwert), determines the execution date, and ultimately calls `k_ausd_bp_ta_bcp_msisdn.ksh` with resolved parameters and logging configured.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`**
    *   **Technology:** KornShell Script
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Summary:** A control script invoked by `r_ausd_bp_ta_bcp_msisdn.ksh`. It further processes parameters, performs date validation (`DWDate_Datum_Check`), sources SQL execution utilities (`h_alis_sqlplus.ksh`), and crucially calls the `d_ausd_bp_ta_bcp_msisdn.sql` script via a `starteSQLSkript` function. It also handles basic record counting and job status updates.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql`**
    *   **Technology:** Oracle SQL
    *   **Tier:** Complex
    *   **Automation Bucket:** Manual
    *   **Summary:** Contains the core data transformation logic. It first determines a `v_datum` from the `isbert_schema.dwtk_meldungen` table. Then, it truncates the `sof$ta_bcp_msisdn` table and repopulates it by joining `sof$ta_bpr_bcp` and `sof$ta_rn_vertrag` tables based on `cntrct_id_ref = cntrct_id`, selecting `cntrct_id`, `bpr_id`, `cntrct_id_ref`, and `tn_tel_msisdn`.

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services:

*   **Scheduler/Orchestration:** The UC4 job and KornShell orchestration logic will be re-implemented as an **Airflow DAG (Directed Acyclic Graph)**. Python operators within the DAG will manage parameter passing, date calculations, and execution flow.
*   **Data Storage:** All Oracle tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, `isbert_schema.dwtk_meldungen`) will be migrated to **BigQuery tables**. A dedicated dataset (e.g., `bert_raw` or `bert_staging` within `your_project_id`) will house these tables.
*   **Data Transformation:** The Oracle SQL script will be translated into **BigQuery Standard SQL**. Data manipulation will occur directly within BigQuery.
*   **Environment & Utilities:** Legacy shell utilities will be replaced by standard Python libraries or Airflow features. Logging will leverage Airflow's native capabilities, integrated with Cloud Logging.

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow Scheduler:** Triggers the `bert_ausd_bp_ta_bcp_msisdn_dag` based on its defined schedule.
2.  **`init_parameters_task` (PythonOperator):**
    *   Retrieves job parameters (e.g., `Stichtag`, `Wiederanlaufwert`) from Airflow configurations or DAG run parameters.
    *   Performs date validation and prepares the execution context.
    *   This task encapsulates the logic from `r_ausd_bp_ta_bcp_msisdn.ksh` and parameter handling from `k_ausd_bp_ta_bcp_msisdn.ksh`.
3.  **`determine_metadata_date_task` (PythonOperator/BigQueryOperator):**
    *   Queries `your_project.bert_raw.dwtk_meldungen` to determine the metadata `v_datum` (similar to the `SELECT MAX(m.timecreated)` in the original SQL).
    *   Passes this `v_datum` to subsequent tasks via Airflow XComs.
4.  **`transform_and_load_data_task` (BigQueryOperator):**
    *   Executes the translated BigQuery SQL.
    *   **Input:** Reads from `your_project.bert_raw.sof_ta_bpr_bcp` and `your_project.bert_raw.sof_ta_rn_vertrag`.
    *   **Output:** Truncates and inserts into `your_project.bert_raw.sof_ta_bcp_msisdn`.
    *   This task directly replaces the logic in `d_ausd_bp_ta_bcp_msisdn.sql`.
5.  **`post_processing_task` (PythonOperator):**
    *   Performs any final logging, metrics capture, or status updates within Airflow.
    *   Corresponds to the post-processing and logging in `k_ausd_bp_ta_bcp_msisdn.ksh`.

## 5. Transformation Logic

*   **UC4 Job (`DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`) to Airflow DAG:**
    *   The UC4 job's scheduling will be replaced by the Airflow DAG's `schedule_interval`.
    *   The `Login` (DW.UNIX.ISBERT) and `HostDst` (DWHDWH2P) will be managed by Airflow connections to Google Cloud, typically using a service account for BigQuery and other GCP services.
    *   The invocation of `r_ausd_bp_ta_bcp_msisdn.ksh` will be replaced by the Python entry point of the Airflow DAG.

*   **KornShell Scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) to Python Operators:**
    *   **Environment Setup:** The `. $HOME/.dw_init` sourcing will be replaced by Airflow's environment configuration (e.g., setting variables in the DAG or using Airflow Variables).
    *   **Parameter Parsing:** `getopts` logic will be translated into Python code that retrieves parameters from Airflow's context or DAG run configuration.
    *   **Date Handling:** Shell utilities like `DWDate_Datum_Check`, `DWDate_Gib_Zeitraum`, and `gestern.ksh` will be replaced by Python's `datetime` module for date calculations and validations.
    *   **Error Handling & Logging:** Legacy scripts' `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK` will be replaced by Airflow's native logging mechanisms and Python's `try-except` blocks for error management.
    *   **SQL Execution:** The `starteSQLSkript` function (which wraps `sqlplus`) will be replaced by a BigQueryOperator or a Python function that uses the `google-cloud-bigquery` client library to execute BigQuery SQL.

*   **Oracle SQL (`d_ausd_bp_ta_bcp_msisdn.sql`) to BigQuery SQL:**
    *   **Variable Definition (`v_carmen`, `v_datum`):** `v_carmen` is an Oracle-specific connection alias and will be removed. The `v_datum` derivation from `dwtk_meldungen` will be translated into BigQuery SQL, potentially executed in a separate task or integrated into the main query using a subquery or UDF. `NVL` becomes `IFNULL`, `TO_CHAR(..., 'YYYYMMDD')` becomes `FORMAT_DATE('%Y%m%d', ...)`.
    *   **`TRUNCATE` Statement:** `TRUNCATE TABLE sof$ta_bcp_msisdn REUSE STORAGE;` becomes `TRUNCATE TABLE \`your_project.bert_raw.sof_ta_bcp_msisdn\`;` in BigQuery.
    *   **`INSERT` Statement:** The `INSERT INTO ... SELECT` statement will be translated directly.
        *   Oracle hints like `/*+ full(bp) parallel(bp,4) ... */` are not applicable in BigQuery and will be removed, as BigQuery automatically handles query optimization and parallelism.
        *   Table names will be fully qualified: `sof$ta_bpr_bcp` becomes ``your_project.bert_raw.sof_ta_bpr_bcp```.
        *   `COMMIT;` is not required in BigQuery as DML operations are atomic.

## 6. External Dependencies

*   **Oracle Database:**
    *   **Legacy:** Source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`) and target table (`sof$ta_bcp_msisdn`) reside in an Oracle database.
    *   **Target:** All these tables will be migrated to BigQuery. An initial bulk load or continuous data replication mechanism (e.g., Cloud Data Migration Service, custom ETL) will be established to populate these tables in BigQuery. Subsequent runs of the migrated job will operate purely within BigQuery.
*   **UNIX Host (`DWHDWH2P|HOST`):**
    *   **Legacy:** The KornShell scripts execute on a specific UNIX host.
    *   **Target:** Airflow workers/executors running on Google Cloud infrastructure (e.g., GKE, GCE) will execute the Python-based DAG. Access to GCP resources will be managed via service accounts.
*   **Local File System (`$HOME`, `${BERT_DIR_ROOT}`):**
    *   **Legacy:** Used for storing scripts, utilities, and temporary files.
    *   **Target:** All scripts will be part of the Airflow DAG definition or Python modules. Temporary data can be stored in Cloud Storage if needed, or handled in-memory. Configuration will utilize Airflow Variables or configuration files in Cloud Storage.
*   **External Job Management System (FOS - `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`):**
    *   **Legacy:** Calls to a FOS (Forderungsscoring) job management system are commented out in the source.
    *   **Target:** Since these calls are commented out, they are assumed to be inactive and will not be migrated. However, if future requirements necessitate this functionality, a separate integration component (e.g., Cloud Functions, another Airflow DAG) would need to be developed to interface with the target FOS system or its replacement.

## 7. Unresolved / Risks

*   **`isbert_schema.dwtk_meldungen` Table Usage:** The full lifecycle and update mechanism of `dwtk_meldungen` are not visible within this job. Ensuring the integrity and availability of this metadata table in BigQuery, including how it is populated and maintained, is critical.
*   **`starteSQLSkript` Complexity:** The `starteSQLSkript` function (defined in `h_alis_sqlplus.ksh`) is a wrapper for `sqlplus`. Its exact implementation is not provided. If it contains complex logic beyond simple SQL execution (e.g., dynamic SQL generation, intricate error parsing, or environment manipulation), this logic will require careful analysis and replication in Python.
*   **Date Logic Nuances:** While Python's `datetime` can replace `gestern.ksh` and other date utilities, specific edge cases (e.g., leap years, time zones, or custom holiday logic) might exist in the legacy scripts that need explicit testing and verification in the new implementation.
*   **Error Code Mapping:** The legacy error handling uses numeric error codes (`ErrNr=193`, `ErrNr=192`). A mapping or strategy for how these legacy error codes translate to Airflow/Python exceptions and notifications should be defined.
*   **Restart/Recovery Mechanism (`p_wiederanlaufWert`):** The `p_wiederanlaufWert` indicates a potential restart/recovery mechanism. BigQuery's transactional nature for DML and Airflow's retry mechanisms often simplify this, but the specific logic around how `p_wiederanlaufWert` is used to prevent duplicate processing or resume partially completed work needs to be fully understood and replicated if necessary.

## 8. Build Plan

1.  **BigQuery Schema and Data Migration (Initial Phase):**
    *   Create `bert_raw` (or similar) dataset in BigQuery.
    *   Define BigQuery schemas for `sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `sof_ta_bcp_msisdn`, and `dwtk_meldungen` to mirror the Oracle structures.
    *   Perform a one-time migration of historical data from Oracle to BigQuery for these tables.
    *   Establish a continuous data synchronization mechanism (if required for `dwtk_meldungen` or other dynamically updated tables) from Oracle to BigQuery.
    *   **Language:** DDL for schema, Data Migration Service, custom Python/Go scripts.

2.  **SQL Translation (`d_ausd_bp_ta_bcp_msisdn.sql`):**
    *   Translate `d_ausd_bp_ta_bcp_msisdn.sql` from Oracle SQL to BigQuery Standard SQL.
    *   Store the translated SQL in `bigquery/d_ausd_bp_ta_bcp_msisdn.sql`.
    *   **Language:** BigQuery SQL.

3.  **Airflow DAG Development (`bert_ausd_bp_ta_bcp_msisdn_dag.py`):**
    *   Create a new Airflow DAG file: `dags/bert_ausd_bp_ta_bcp_msisdn_dag.py`.
    *   **Define DAG Structure:**
        *   `schedule_interval` (from UC4 schedule).
        *   `default_args` including retries, email notifications.
    *   **`init_parameters_task`:**
        *   Implement parameter parsing and date calculation logic from `r_ausd_bp_ta_bcp_msisdn.ksh` and `k_ausd_bp_ta_bcp_msisdn.ksh` in Python.
        *   Use Airflow Variables for configuration (`BERT_DIR_ROOT` equivalent).
        *   Use Python's `datetime` for date operations.
        *   **Language:** Python (Airflow PythonOperator).
    *   **`determine_metadata_date_task`:**
        *   Implement the `v_datum` determination logic.
        *   **Language:** Python (Airflow PythonOperator with BigQuery client) or BigQueryOperator.
    *   **`transform_and_load_data_task`:**
        *   Use `BigQueryOperator` to execute the translated `d_ausd_bp_ta_bcp_msisdn.sql`.
        *   Pass `v_datum` (if needed in the SQL) as a template parameter.
        *   **Language:** Python (Airflow BigQueryOperator), BigQuery SQL.
    *   **`post_processing_task`:**
        *   Implement logging and status updates.
        *   **Language:** Python (Airflow PythonOperator).
    *   **Task Dependencies:** Chain tasks appropriately (e.g., `init_parameters_task >> determine_metadata_date_task >> transform_and_load_data_task >> post_processing_task`).
    *   **Language:** Python.

4.  **Airflow Configuration:**
    *   Create Airflow Connections for BigQuery (e.g., `google_cloud_default`).
    *   Define any necessary Airflow Variables (e.g., `dwtk_meldungen_job_kennung_filter`, `bigquery_project_id`, `bigquery_dataset_id`).
    *   **Language:** Airflow CLI/UI or Terraform.

5.  **Testing and Validation:**
    *   Unit test Python logic.
    *   Integration test Airflow DAG with BigQuery.
    *   Validate data output against legacy system results.
    *   **Language:** Python, SQL, Testing Frameworks.