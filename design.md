# Migration Design — DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Purpose & Scope

This job, `DW.BERT_AUSD_BP_TA_BCP_ICCID`, is responsible for preparing instantiated basic products for BERT's demand scoring system. It involves an orchestration layer (UC4 and KornShell scripts) that drives a core Oracle SQL*Plus script. The SQL script's primary function is to populate or refresh the `sof$ta_bcp_iccid` table by joining data from `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag`, with a date parameter derived from `isbert_schema.dwtk_meldungen`. The job handles parameters such as a key date (`Stichtag`) and a restart value, and includes logging and error handling.

The scope of this migration is to re-implement the existing UC4, KornShell, and Oracle SQL*Plus job into a BigQuery-native solution, leveraging Apache Airflow for orchestration and BigQuery Standard SQL for data transformation.

## 2. Source Inventory

The following source files comprise this job, along with their analyzed characteristics:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`**
    *   **Technology:** UC4/Automic XML
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Purpose:** Scheduler/Orchestrator. Defines a UNIX job that invokes a KornShell script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Purpose:** Main orchestrator script. Parses parameters, handles logging and error trapping, and calls the core processing script (`k_ausd_bp_ta_bcp_iccid.ksh`).

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-auto
    *   **Purpose:** Core processing script. Executes the Oracle SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`) after setting up the environment and parameters.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`**
    *   **Technology:** Oracle PL/SQL (SQL*Plus script)
    *   **Tier:** Medium
    *   **Automation Bucket:** Retire
    *   **Purpose:** Data transformation. Truncates and loads the `sof$ta_bcp_iccid` table by joining `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag`. Determines a date variable from `isbert_schema.dwtk_meldungen`.

## 3. Target Architecture

The migrated job will run on Google Cloud Platform, utilizing the following components:

*   **Google Cloud Storage (GCS):** For staging any intermediate files if necessary, although direct BigQuery operations are preferred.
*   **BigQuery:** As the primary data warehouse, hosting all source and target tables.
    *   `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` (Target table)
    *   `PROJECT_ID.DATASET_ID.TA_BPR_BCP` (Source table, migrated from Oracle)
    *   `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG` (Source table, migrated from Oracle)
    *   `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` (Source table, migrated from Oracle, used for date lookup)
*   **Cloud Composer (Apache Airflow):** For scheduling, orchestration, monitoring, and error handling. A single DAG will encapsulate the entire workflow.
*   **Python:** For implementing the orchestration logic previously handled by KornShell scripts, parameter handling, and BigQuery interaction within Airflow tasks.

## 4. Data Flow & Lineage

The data flow will be as follows:

1.  **Airflow DAG Trigger:** The Airflow DAG, `dw_bert_ausd_bp_ta_bcp_iccid`, is triggered (scheduled or manually).
2.  **Date Parameter Retrieval:** An Airflow task (PythonOperator) fetches the `v_datum` value by querying `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` in BigQuery, analogous to the Oracle SQL*Plus script.
3.  **Truncate Target Table:** An Airflow task (BigQueryOperator) executes a `TRUNCATE TABLE` statement on `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`.
4.  **Insert Data into Target Table:** An Airflow task (BigQueryOperator) executes the transformed BigQuery SQL. This SQL will:
    *   Read from `PROJECT_ID.DATASET_ID.TA_BPR_BCP` and `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`.
    *   Join these tables on `CNTRCT_ID_REF = CNTRCT_ID`.
    *   Insert the distinct `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, and `TN_IMSI_HLR` into `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`.
    *   Utilize the `v_datum` parameter obtained in step 2 if the logic requires dynamic date filtering during the insert (currently, the `v_datum` is only used for `BERT_DROP_TEMP_TABLE` job, so its direct application to the insert statement itself is not evident in the source SQL, but parameterization is good practice).
5.  **Logging and Monitoring:** Airflow's native logging and monitoring capabilities will replace the custom KornShell logging mechanisms.

**Execution Order:**
UC4 (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) (scheduling and invocation)
↓
`r_ausd_bp_ta_bcp_iccid.ksh` (orchestration, parameter parsing)
↓
`k_ausd_bp_ta_bcp_iccid.ksh` (SQL execution wrapper)
↓
`d_ausd_bp_ta_bcp_iccid.sql` (data transformation logic)
    - Reads from `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`
    - Writes to `sof$ta_bcp_iccid`

## 5. Transformation Logic

### `DW.BERT_AUSD_BP_TA_BCP_ICCID.xml` (UC4)
*   **Original:** UC4 job definition.
*   **Target:** Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid.py`. The schedule and any external dependencies defined in UC4 will be translated to Airflow DAG parameters (e.g., `schedule_interval`).

### `r_ausd_bp_ta_bcp_iccid.ksh` (KornShell Orchestration)
*   **Original:** Parameter parsing (`-s Stichtag`, `-l Wiederanlaufwert`), environment setup (`. $HOME/.dw_init`), sourcing helper scripts, error handling (`f_alis_msgerr.ksh`), date determination (`DWDate_Gib_Zeitraum`), and invoking `k_ausd_bp_ta_bcp_iccid.ksh`.
*   **Target:** Python tasks within the Airflow DAG.
    *   Parameters will be managed as Airflow DAG parameters or XComs.
    *   Environment variables will be managed via Airflow connections or environment variables within the Airflow environment.
    *   Helper script logic (error handling, date utilities) will be re-implemented in Python functions or leverage existing Python libraries.
    *   The invocation of `k_ausd_bp_ta_bcp_iccid.ksh` will be replaced by direct calls to BigQuery operators or Python functions executing BigQuery SQL.

### `k_ausd_bp_ta_bcp_iccid.ksh` (KornShell SQL Execution Wrapper)
*   **Original:** Receives parameters, sources helper scripts (`h_alis_sqlplus.ksh`), constructs the SQL script path, and executes the Oracle SQL*Plus script `d_ausd_bp_ta_bcp_iccid.sql` via `starteSQLSkript`.
*   **Target:** The functionality will be integrated into the Airflow DAG's Python tasks. The BigQueryOperator will directly execute the transformed SQL. Parameter passing to the SQL will use BigQuery's query parameterization.

### `d_ausd_bp_ta_bcp_iccid.sql` (Oracle PL/SQL Data Transformation)
*   **Original:**
    *   `DEFINE v_carmen = "@pcrs1"` (This is unused in the provided snippet)
    *   `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` (Sets `v_datum`)
    *   `begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE'); end;` (Truncates table)
    *   `INSERT INTO sof$ta_bcp_iccid (...) SELECT /*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */ distinct bp.cntrct_id, bp.bpr_id, bp.cntrct_id_ref, ic.tn_iccid, ic.tn_imsi_hlr FROM sof$ta_bpr_bcp bp, sof$ta_iccid_vertrag ic WHERE bp.cntrct_id_ref = ic.cntrct_id;` (Inserts data)
    *   `COMMIT;`
*   **Target (BigQuery Standard SQL):**
    *   The `v_carmen` definition will be removed.
    *   **Date Parameter:** A separate Python task in Airflow will query `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` to determine the date. This date will then be passed as a templated parameter (`ds` or a custom parameter) to the SQL query if needed for filtering.
    *   **Truncate:**
        ```sql
        TRUNCATE TABLE `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
        ```
    *   **Insert:**
        ```sql
        INSERT INTO `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
        SELECT
            DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            ic.tn_iccid,
            ic.tn_imsi_hlr
        FROM
            `PROJECT_ID.DATASET_ID.TA_BPR_BCP` bp
        INNER JOIN
            `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG` ic
        ON
            bp.cntrct_id_ref = ic.cntrct_id;
        ```
    *   `/*+ ... */` (Oracle hints) will be removed as they are not applicable to BigQuery.
    *   `COMMIT;` is not necessary in BigQuery as DML operations are atomic.

## 6. External Dependencies

*   **Oracle Database:** The source of all tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and the target `sof$ta_bcp_iccid`).
    *   **Replacement:** All these tables will be migrated to BigQuery. The BigQuery tables will be the definitive source and target for this job.
*   **UNIX Host (DWHDWH2P):** Where the KornShell scripts execute.
    *   **Replacement:** Replaced by Cloud Composer (Airflow) worker nodes.
*   **DW.UNIX.ISBERT Login:** Unix user account for execution.
    *   **Replacement:** Service accounts on Google Cloud, associated with Airflow/BigQuery for authentication and authorization.
*   **Local KornShell Helper Scripts:** `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`.
    *   **Replacement:** Their functionality will be reimplemented in Python as part of the Airflow DAG, leveraging Python's standard library for date/time, parameter handling, and logging.

## 7. Unresolved / Risks

*   **`d_ausd_bp_ta_bcp_iccid.sql` "Retire" Migration Bucket:** The `retire` migration bucket for the core SQL script is a significant flag. This design assumes the *logic* of the SQL script is still required, even if the script itself is not directly converted but its functionality re-implemented in BigQuery SQL. If "retire" means the business logic itself is no longer needed, then this job could potentially be decommissioned instead of migrated. Clarification is required on the meaning of "retire" for this specific file. For now, the design proceeds with the assumption that the logic needs to be migrated.
*   **Helper Script Complexity:** The sourced KornShell helper scripts need to be thoroughly analyzed to ensure all critical functionalities are correctly translated to Python.
*   **`v_datum` usage:** While the SQL script defines `v_datum`, its direct usage in the `INSERT` statement's `WHERE` clause is not present in the provided snippet. If `v_datum` was intended for filtering the source tables, this needs to be captured in the BigQuery SQL. The current logic determines `v_datum` from `dwtk_meldungen.timecreated` for `BERT_DROP_TEMP_TABLE` job. This implies `dwtk_meldungen` table should be available and accessible in BigQuery.
*   **Error Handling and Logging:** The `DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, etc., functions from the original KornShell scripts need to be fully mapped to Airflow's robust logging and alerting mechanisms.

## 8. Build Plan

The following is an ordered list of components to be built for the migration:

1.  **Migrate Oracle Source Tables to BigQuery:**
    *   `PROJECT_ID.DATASET_ID.TA_BPR_BCP`
    *   `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`
    *   `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN`
    *   `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` (Target table creation)
    *   **Language/Tool:** BigQuery DDL, Data Migration Service, or ETL tools. (Prerequisite for this job migration).

2.  **Develop Airflow DAG (`dw_bert_ausd_bp_ta_bcp_iccid.py`):**
    *   **Python:** Define the DAG structure, schedule, and default arguments.
    *   **Task 1: `fetch_stichtag_task`** (PythonOperator)
        *   Queries `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` to determine the `v_datum` (Stichtag) and pushes it to XCom.
    *   **Task 2: `truncate_target_table_task`** (BigQueryOperator)
        *   SQL: `TRUNCATE TABLE \`PROJECT_ID.DATASET_ID.TA_BCP_ICCID\`;`
    *   **Task 3: `insert_data_task`** (BigQueryOperator)
        *   SQL:
            ```sql
            INSERT INTO `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
            (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)
            SELECT
                DISTINCT
                bp.cntrct_id,
                bp.bpr_id,
                bp.cntrct_id_ref,
                ic.tn_iccid,
                ic.tn_imsi_hlr
            FROM
                `PROJECT_ID.DATASET_ID.TA_BPR_BCP` bp
            INNER JOIN
                `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG` ic
            ON
                bp.cntrct_id_ref = ic.cntrct_id;
            ```
            (Note: If `v_datum` is used for filtering, it will be added here via JINJA templating, e.g., `WHERE some_date_column = '{{ ti.xcom_pull(task_ids="fetch_stichtag_task", key="stichtag_date") }}'`)
    *   **Error Handling & Logging:** Implement robust error handling and integrate with Airflow's logging.
    *   **Schedule and Dependencies:** Configure the DAG to match the original UC4 job's schedule and dependencies.

3.  **Python Utility Functions:**
    *   **Python:** Re-implement the logic from the KornShell helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) as Python functions for use within the Airflow DAG if their generic utility is needed beyond this specific DAG. Otherwise, integrate the required logic directly into the DAG tasks.

4.  **Security and Access Configuration:**
    *   **GCP IAM:** Configure service accounts with appropriate roles for BigQuery access and Cloud Composer operations.