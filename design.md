# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

## 1. Purpose & Scope
This job orchestrates the initial provisioning of selected basic products for the BERT system. Its primary function is to generate a snapshot extraction of contract cache/base product data for a given cutoff date and load it into a target table. The process involves parameter parsing, date determination, and robust error handling. The job records job/log metadata and supports restart/resume capabilities via a designated restart value, filtering out already processed contract IDs. The core data preparation is delegated to a SQL script.

## 2. Source Inventory
The job is composed of three primary files:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh**
    *   **Technology:** KornShell
    *   **Complexity Tier:** N/A (no tier information available)
    *   **Automation Bucket:** semi_auto
    *   **Description:** The primary orchestrator script, responsible for command-line parameter parsing, date setup, error handling, and invoking the `k_ausd_bp_ta_bpr_optionen.ksh` control script.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh**
    *   **Technology:** KornShell
    *   **Complexity Tier:** N/A (no tier information available)
    *   **Automation Bucket:** semi_auto
    *   **Description:** A control script invoked by `r_ausd_bp_ta_bpr_optionen.ksh`. It further processes parameters, validates dates, and executes the `d_ausd_bp_ta_bpr_optionen.sql` script via an SQL runner.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_optionen.sql**
    *   **Technology:** Oracle PL/SQL
    *   **Complexity Tier:** N/A (no tier information available)
    *   **Automation Bucket:** semi_auto
    *   **Description:** The core data processing script. It determines a date variable from a metadata table, truncates a target table (`sof$ta_bpr_optionen`), and loads data into it from a source table (`sof$ta_bpr_instance`).

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services, with BigQuery as the central data warehouse and storage, orchestrated by Cloud Composer (Airflow).

*   **Orchestration:** Google Cloud Composer (Airflow) will replace the existing UC4 scheduler. An Airflow DAG will be developed to manage the workflow, passing parameters and monitoring execution.
*   **Data Warehouse:** Google BigQuery will host all migrated tables and stored procedures.
    *   **Tables:** Legacy Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_optionen`, `sof$ta_bpr_instance`) will be migrated to corresponding BigQuery tables.
    *   **Logging:** A dedicated `project.dataset.job_audit_log` BigQuery table will be created to record job status, parameters, and error messages.
*   **Transformation Logic:** BigQuery Stored Procedures will encapsulate the data transformation logic previously contained in the ksh and Oracle SQL scripts.

## 4. Data Flow & Lineage
The end-to-end data flow in the target BigQuery environment will be:

1.  **Trigger:** An Airflow DAG (migrated from the UC4 job `DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml`) is triggered.
2.  **Orchestration (Airflow):** The Airflow DAG will:
    *   Extract necessary parameters (Stichtag, Wiederanlaufwert).
    *   Call the main BigQuery Stored Procedure, `project.dataset.r_ausd_bp_ta_bpr_optionen`.
    *   Log job start and completion status, and any errors, into the `project.dataset.job_audit_log` table.
3.  **Main Stored Procedure (`r_ausd_bp_ta_bpr_optionen`):**
    *   Receives `p_stichtag` and `p_wiederanlaufWert` as input parameters.
    *   Determines `v_sysdate` and sets `v_stichtag` (defaulting to system date if not provided).
    *   Validates the `p_stichtag` format.
    *   Logs the job start in `job_audit_log`.
    *   **Executes Data Processing:** Calls an internal logic block (or another stored procedure) that performs the data manipulation.
    *   Logs success or failure to `job_audit_log`.
4.  **Data Processing Logic (within `r_ausd_bp_ta_bpr_optionen`):**
    *   Queries a BigQuery metadata table (migrated from `isbert_schema.dwtk_meldungen`) to determine a `v_datum` based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Truncates** the target BigQuery table: `project.dataset.sof_ta_bpr_optionen`.
    *   **Inserts** data into `project.dataset.sof_ta_bpr_optionen` from the source BigQuery table `project.dataset.sof_ta_bpr_instance`.
    *   Applies a filter `DWH_VERTRAG_ID > v_wiederanlaufWert` based on the restart parameter.
    *   Counts inserted records and logs to `job_audit_log`.

## 5. Transformation Logic
The transformation logic will be primarily within BigQuery Stored Procedures, with orchestration and some utility functions handled by Airflow.

*   **Parameter Handling:** `getopts` logic from ksh will be directly translated to BigQuery Stored Procedure parameters (e.g., `p_stichtag STRING`, `p_wiederanlaufWert INT64`).
*   **Date Determination:** Shell-based date calculations (`DWDate_Gib_Zeitraum`, `gestern.ksh`) will be replaced by BigQuery SQL functions like `CURRENT_DATE()`, `FORMAT_DATE('%d%m%Y', ...)` and `PARSE_DATE('%d%m%Y', ...)`.
*   **Error Handling and Logging:**
    *   KornShell `trap` commands and custom `DWMSG_*` error functions will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks.
    *   All logging (`DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) will be consolidated into inserts into the `project.dataset.job_audit_log` BigQuery table.
*   **Data Truncation:** The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_optionen REUSE STORAGE')` will be directly translated to `TRUNCATE TABLE `project.dataset.sof_ta_bpr_optionen`;` in BigQuery.
*   **Data Insertion:** The Oracle `INSERT INTO sof$ta_bpr_optionen (CNTRCT_ID, BPR_ID) SELECT /*+ full(bp) parallel(bp,4) */ bp.cntrct_id, bp.bpr_id FROM sof$ta_bpr_instance bp;` statement will be converted to:
    ```sql
    INSERT INTO `project.dataset.sof_ta_bpr_optionen` (CNTRCT_ID, BPR_ID)
    SELECT
      bp.CNTRCT_ID,
      bp.BPR_ID
    FROM `project.dataset.sof_ta_bpr_instance` bp
    WHERE
      bp.DWH_VERTRAG_ID > p_wiederanlaufWert -- Applying restart logic
      -- Optional: Reintroduce date-based filtering if confirmed to be required
      -- AND bp.GUELTIG_VON <= PARSE_DATE('%d%m%Y', v_stichtag)
      -- AND PARSE_DATE('%d%m%Y', v_stichtag) < bp.GUELTIG_BIS
      -- AND bp.LADEDATUM < PARSE_DATE('%d%m%Y', v_stichtag)
    ;
    ```
    Oracle-specific hints (`full(bp) parallel(bp,4)`) are removed as BigQuery automatically handles query optimization and parallelism.
*   **Metadata Date Derivation:** The Oracle SQL for determining `v_datum` from `isbert_schema.dwtk_meldungen` will be translated to its BigQuery SQL equivalent, assuming `isbert_schema.dwtk_meldungen` is migrated.

## 6. External Dependencies
*   **UC4 Scheduler:** Replaced by Google Cloud Composer (Airflow) for job orchestration and scheduling.
*   **Oracle Database:** All Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_optionen`, `sof$ta_bpr_instance`) and the `isbert_schema.DWPA_UTIL_SKRIPT` stored procedure will be migrated to BigQuery tables and a BigQuery stored procedure respectively.
*   **KornShell Utilities:** Custom `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` scripts will be absorbed into BigQuery stored procedures (for logic) or Airflow DAGs (for environment setup, parameter handling, error handling).
*   **`v_carmen = "@pcrs1"`:** This dependency needs further investigation. If it represents a database link to another Oracle instance, the data source must be identified and either replicated to BigQuery or accessed via BigQuery's federated query capabilities if real-time access is critical. Its current usage in `d_ausd_bp_ta_bpr_optionen.sql` is only as a `DEFINE` variable not used in the provided SQL body, so its migration impact might be minimal if it's unused.

## 7. Unresolved / Risks
*   **`v_carmen = "@pcrs1"` Resolution:** The exact purpose and usage of this variable need to be determined to ensure all data sources are accounted for in the BigQuery migration.
*   **Functionality of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** Assuming this is a simple DDL execution wrapper. If it contains complex logic beyond `TRUNCATE TABLE`, that logic needs to be extracted and migrated.
*   **`DWH_VERTRAG_ID` in Restart Logic:** The `WHERE bp.DWH_VERTRAG_ID > v_wiederanlaufWert` filter was added based on the parameter's name and common patterns. It needs to be explicitly confirmed whether `sof$ta_bpr_instance` contains this column and if this is the correct column for restart logic. The original SQL did not include this filter.
*   **Complete Functionality of KornShell Utility Scripts:** While major functionalities like date handling and error logging are outlined for migration, a detailed analysis of all `.ksh` utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) is required to ensure no nuanced logic is missed during translation to BigQuery/Airflow.
*   **Metadata Table `isbert_schema.dwtk_meldungen`:** Confirm that this table and its `job_kennung` column are properly migrated to BigQuery and accessible for date derivation.
*   **Orchestration of Multiple Jobs:** The `lineage_edges` showed that the original UC4 job invoked multiple sub-jobs. The current design focuses on the specific data flow for `r_ausd_bp_ta_bpr_optionen.ksh`. A broader Airflow DAG design would encompass all sub-jobs invoked by the main UC4 process.

## 8. Build Plan
1.  **BigQuery Environment Setup:**
    *   Create the target BigQuery dataset (`project.dataset`).
    *   Define schemas and create tables for `dwtk_meldungen`, `sof_ta_bpr_optionen`, and `sof_ta_bpr_instance` in BigQuery.
    *   Create the `job_audit_log` table in BigQuery.
2.  **Data Migration:**
    *   Migrate historical data from Oracle `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_optionen`, `sof$ta_bpr_instance` to their respective BigQuery tables using batch loading tools (e.g., Data Transfer Service, custom ETL).
3.  **BigQuery Stored Procedure Development (Language: BigQuery SQL):**
    *   **`r_ausd_bp_ta_bpr_optionen` Stored Procedure:** Develop the main stored procedure to handle parameter parsing, date logic, job logging, truncation, and the `INSERT...SELECT` statement.
        *   Include `CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_optionen`(p_stichtag STRING, p_wiederanlaufWert INT64)`
        *   Implement parameter validation and default logic.
        *   Integrate error handling using `BEGIN...EXCEPTION` blocks and `job_audit_log`.
        *   Translate the `isbert_schema.dwtk_meldungen` date query.
        *   Implement `TRUNCATE TABLE `project.dataset.sof_ta_bpr_optionen`;`.
        *   Implement `INSERT INTO ... SELECT ... FROM `project.dataset.sof_ta_bpr_instance` ... WHERE DWH_VERTRAG_ID > p_wiederanlaufWert;`.
    *   **(Optional) `k_ausd_bp_ta_bpr_optionen` Wrapper Stored Procedure:** If modularity of the two-layer ksh structure is desired, create a wrapper BigQuery stored procedure that calls `r_ausd_bp_ta_bpr_optionen`.
        *   `CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_optionen`(...)`
        *   Calls `project.dataset.r_ausd_bp_ta_bpr_optionen`.
4.  **Airflow DAG Development (Language: Python):**
    *   Create an Airflow DAG for the job.
    *   Define tasks to call the BigQuery stored procedure(s) using the `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator`.
    *   Configure parameters for the DAG, mapping to the stored procedure parameters.
    *   Implement scheduling equivalent to the original UC4 job.
    *   Add Airflow tasks for pre- and post-execution checks or additional logging if needed.
    *   Ensure proper error handling and retry mechanisms within the DAG.
5.  **External Dependency Integration:**
    *   For `@pcrs1`, based on its determined purpose:
        *   If it's a static data source, replicate its data to BigQuery.
        *   If it's a dynamic or real-time source, configure BigQuery federated queries or create a data pipeline to continuously sync the data.
6.  **Testing and Validation:**
    *   Unit test BigQuery stored procedures.
    *   Integration test the Airflow DAG with BigQuery.
    *   Perform data validation to ensure consistency between legacy and target systems.