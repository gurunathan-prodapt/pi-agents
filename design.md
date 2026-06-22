# Migration Design — DW.BERT_AUSD_BP_TA_P_BASISPROD

## 1. Purpose & Scope

This job, `DW.BERT_AUSD_BP_TA_P_BASISPROD`, is responsible for the daily preparation and provisioning of selected "Basisprodukte" (base products) for the BERT system. It generates a snapshot of contract cache data from the Data Warehouse (DWH) and makes it available for "Forderungsscoring" (demand scoring). The job orchestrates the execution of KornShell scripts which in turn execute an Oracle SQLPlus script. The SQL script populates the `SOF$TA_P_BASISPROD` table by joining various `sof$ta_` tables. The scope of this migration is to re-implement this entire workflow on Google Cloud Platform, using BigQuery for data storage and transformations, and Cloud Composer (Apache Airflow) for orchestration.

## 2. Source Inventory

| File Name                                                                                                               | Technology    | Tier      | Automation Bucket | Description                                                                                                                                                                                                                                  |
| :---------------------------------------------------------------------------------------------------------------------- | :------------ | :-------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`                                                                                    | UC4/Automic   | medium    | semi_auto         | UC4 job definition. Orchestrates the execution of `r_ausd_bp_ta_p_basisprod.ksh` on a UNIX host `DWHDWH2P` under login `DW.UNIX.ISBERT`. Handles job parameters, environment setup, and logging (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`).                                                                                                                                |
| `k_ausd_bp_ta_p_basisprod.ksh`                                                                                          | KornShell     | medium    | semi_auto         | Control script. Parses job parameters (`j`, `f`, `s`, `l`), performs date checks using `h_alis_date.ksh`, includes error handling (`f_alis_msgerr.ksh`), and executes the core SQLPlus script `d_ausd_bp_ta_p_basisprod.sql` via `starteSQLSkript` (from `h_alis_sqlplus.ksh`). It manages a temporary file for record counts (`$DW_DIR_UTL/bert_k_ausd_bp_ta_p_basisprod.tmp`).                                                                                                                                                                                                                                  |
| `r_ausd_bp_ta_p_basisprod.ksh`                                                                                          | KornShell     | medium    | semi_auto         | Main orchestration script. Sets up the environment (`. $HOME/.dw_init`), includes error handling (`f_alis_msgerr.ksh`), parameter parsing (`h_alis_parameter.ksh`), and date handling (`h_alis_date.ksh`). It determines the `p_stichtag` (reference date) and invokes `k_ausd_bp_ta_p_basisprod.ksh` with derived parameters. It also logs job status and errors using `DWMSG_*` functions.                                                                                                                                                                                                                                                |
| `d_ausd_bp_ta_p_basisprod.sql`                                                                                          | Oracle SQLPlus| complex   | manual            | Core data transformation logic. This script first determines a date variable `v_datum` from `isbert_schema.dwtk_meldungen`. It then truncates the target table `sof$ta_p_basisprod` using `isbert_schema.dwpa_util_skript.runstatement`. The main operation is a complex `INSERT /*+ APPEND */ INTO sof$ta_p_basisprod ... SELECT ...` statement. This SELECT statement joins several `sof$ta_` tables: `sof$ta_cntrct_dist`, `sof$ta_bcp_iccid`, `sof$ta_bcp_msisdn`, `sof$ta_cntrct_evn`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`. It includes numerous column selections, renames, and a `decode` for the `apn` field, along with Oracle-specific hints like `FULL` and `parallel`. |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:

*   **Orchestration**: Google Cloud Composer (Apache Airflow) for scheduling and managing the end-to-end workflow.
*   **Data Storage**: Google BigQuery will serve as the target data warehouse. All source `sof$ta_` tables will be migrated to BigQuery datasets and tables. The target table `sof$ta_p_basisprod` will also reside in BigQuery.
*   **Transformation**: The complex Oracle SQL will be translated into BigQuery SQL.
*   **Scripting/Utilities**: Python scripts will replace KornShell scripts for environmental setup, parameter handling, and triggering BigQuery jobs.

**BigQuery Dataset & Table Layout:**

*   **Dataset**: `bert_dwh_prod` (or similar, reflecting the source system and environment)
*   **Tables**:
    *   `sof_ta_p_basisprod` (target table, replacing `sof$ta_p_basisprod`)
    *   `sof_ta_cntrct_dist` (source table, replacing `sof$ta_cntrct_dist`)
    *   `sof_ta_bcp_iccid` (source table, replacing `sof$ta_bcp_iccid`)
    *   `sof_ta_bcp_msisdn` (source table, replacing `sof$ta_bcp_msisdn`)
    *   `sof_ta_cntrct_evn` (source table, replacing `sof$ta_cntrct_evn`)
    *   `sof_ta_iccid_vertrag` (source table, replacing `sof$ta_iccid_vertrag`)
    *   `sof_ta_rn_vertrag` (source table, replacing `sof$ta_rn_vertrag`)
    *   `sof_ta_rn_da_vda_tk` (source table, replacing `sof$ta_rn_da_vda_tk`)
    *   `sof_ta_tarifoption` (source table, replacing `sof$ta_tarifoption`)
    *   `sof_ta_apn_vertrag` (source table, replacing `sof$ta_apn_vertrag`)
    *   `dwtk_meldungen` (source table for date variable, replacing `isbert_schema.dwtk_meldungen`)

All table names will be converted to lowercase and use underscores instead of dollar signs for BigQuery compatibility.

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow DAG (`bert_ausd_bp_ta_p_basisprod_dag.py`)**:
    *   This DAG will be the main entry point, replacing the UC4 job (`DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`).
    *   It will define the sequence of tasks.
    *   **Task 1: Parameter Setup & Date Determination**: A PythonOperator will consolidate the logic from `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh` for environment setup, parameter parsing, and calculating the `stichtag` (reference date). This will involve reading from `bert_dwh_prod.dwtk_meldungen` to determine `v_datum` if required by the logic.
    *   **Task 2: Truncate Target Table**: A BigQueryOperator will execute a `TRUNCATE TABLE` statement on `bert_dwh_prod.sof_ta_p_basisprod`. This directly replaces the `isbert_schema.dwpa_util_skript.runstatement` call in the Oracle script.
    *   **Task 3: Execute BigQuery Transformation**: A BigQueryOperator will execute the translated SQL from `d_ausd_bp_ta_p_basisprod.sql`. This task will read from the various `bert_dwh_prod.sof_ta_*` source tables and insert into `bert_dwh_prod.sof_ta_p_basisprod`.
    *   **Task 4: Logging & Status Update**: A PythonOperator will handle logging and status updates, writing to a designated BigQuery logging table or leveraging Cloud Logging (Stackdriver). This replaces the functionality of the `DWMSG_*` functions in the KornShell scripts.

**Execution Order (Migrated):**

Cloud Composer DAG (`bert_ausd_bp_ta_p_basisprod_dag.py`)
  -> Python Operator (Parameter Setup & Date Determination)
  -> BigQuery Operator (TRUNCATE `bert_dwh_prod.sof_ta_p_basisprod`)
  -> BigQuery Operator (INSERT INTO `bert_dwh_prod.sof_ta_p_basisprod` FROM:
     `bert_dwh_prod.sof_ta_cntrct_dist`, `bert_dwh_prod.sof_ta_bcp_iccid`, `bert_dwh_prod.sof_ta_bcp_msisdn`, `bert_dwh_prod.sof_ta_cntrct_evn`, `bert_dwh_prod.sof_ta_iccid_vertrag`, `bert_dwh_prod.sof_ta_rn_vertrag`, `bert_dwh_prod.sof_ta_rn_da_vda_tk`, `bert_dwh_prod.sof_ta_tarifoption`, `bert_dwh_prod.sof_ta_apn_vertrag`, `bert_dwh_prod.dwtk_meldungen`)
  -> Python Operator (Logging & Status Update)

## 5. Transformation Logic

The core transformation logic from `d_ausd_bp_ta_p_basisprod.sql` will be translated into BigQuery SQL.

**Key Translation Considerations:**

*   **Table Naming**: Oracle table names like `sof$ta_TABLE_NAME` will be converted to `bert_dwh_prod.sof_ta_table_name`. `isbert_schema.dwtk_meldungen` will become `bert_dwh_prod.dwtk_meldungen`.
*   **Data Types**: Oracle data types will be mapped to appropriate BigQuery data types (e.g., `NUMBER` to `INT64`/`BIGNUMERIC`, `VARCHAR2` to `STRING`, `DATE` to `DATE`/`TIMESTAMP`).
*   **Functions**:
    *   `NVL(...)` -> `COALESCE(...)`
    *   `TO_CHAR(date, 'YYYYMMDD')` -> `FORMAT_DATE('%Y%m%d', date_expression)`
    *   `DECODE(condition, value1, result1, ..., else_result)` -> `CASE WHEN condition = value1 THEN result1 ... ELSE else_result END`
    *   Concatenation operator `||` will remain the same.
*   **Oracle Hints**: Oracle-specific hints like `/*+ APPEND */`, `/*+ ORDERED */`, `/*+ FULL */`, `/*+ parallel */` will be removed, as BigQuery's query optimizer handles these aspects automatically.
*   **SQL Structure**: The `INSERT ... SELECT` structure is directly transferable. Oracle's `OUTER JOIN` syntax `(+)` will be explicitly translated to `LEFT JOIN` in BigQuery.
*   **User-defined Procedures**: The call to `isbert_schema.dwpa_util_skript.runstatement` for truncating the target table will be replaced by a direct `TRUNCATE TABLE` DDL statement executed via a BigQueryOperator.
*   **Commented Code**: The commented-out `TRUNCATE TABLE` and `DROP TABLE` statements in the original SQL will not be migrated unless explicitly required as part of the new design's cleanup.

**Example SQL Translation Snippet (Illustrative for a `LEFT JOIN` and `DECODE`):**

```sql
-- Original Oracle SQL snippet:
-- SELECT decode(av.apn, null,av.apn, av.apn||','||av.apn_cntrct) as apn, cn.cntrct_id
-- FROM sof$ta_cntrct_dist cn, sof$ta_apn_vertrag av
-- WHERE cn.cntrct_id = av.cntrct_id (+);

-- Translated BigQuery SQL snippet:
SELECT
    CASE
        WHEN av.apn IS NULL THEN av.apn
        ELSE CONCAT(av.apn, ',', av.apn_cntrct)
    END AS apn,
    cn.cntrct_id
FROM
    `project_id.bert_dwh_prod.sof_ta_cntrct_dist` AS cn
LEFT JOIN
    `project_id.bert_dwh_prod.sof_ta_apn_vertrag` AS av
ON
    cn.cntrct_id = av.cntrct_id;
```

The logic embedded in the KornShell scripts for parameter handling, date calculations, and logging will be reimplemented in Python code within the Airflow DAG or as separate Python modules invoked by the DAG.

## 6. External Dependencies

*   **Oracle Database (Source)**: The primary external dependency is the Oracle database instance hosting the `sof$ta_` tables and the `isbert_schema.dwtk_meldungen` table.
    *   **Replacement Strategy**: These source tables must be migrated to BigQuery prior to migrating this job. This will involve:
        1.  A one-time historical data load from Oracle to BigQuery.
        2.  Implementing a Change Data Capture (CDC) or batch replication mechanism (e.g., Google Cloud Database Migration Service, Dataflow, or custom ETL) to ensure the BigQuery source tables remain synchronized with the operational Oracle system.
*   **UNIX Host (`DWHDWH2P`)**: The original UC4 job and KornShell scripts execute on a UNIX host.
    *   **Replacement Strategy**: The execution environment will shift to Google Cloud Composer, which provides a managed Apache Airflow service running on Google Kubernetes Engine. All shell script logic will be replaced by Python code or BigQuery operations.
*   **UC4/Automic Scheduler**: The UC4 job currently schedules and monitors the workflow.
    *   **Replacement Strategy**: Cloud Composer (Apache Airflow) will take over the scheduling, orchestration, and monitoring responsibilities.

## 7. Unresolved / Risks

*   **Inferred Lineage**: The absence of detailed `lineage_edges` data necessitated inferring inter-script and script-to-table dependencies from code analysis. While confident in the current assessment, it's a general risk for less explicit logic.
*   **`starteSQLSkript` Functionality**: The exact implementation of `starteSQLSkript` (likely from `h_alis_sqlplus.ksh`) is unknown. This function might contain crucial logic beyond simply invoking SQLPlus, such as advanced error handling, transaction management, or specific output parsing. A thorough review of `h_alis_sqlplus.ksh` is required during implementation to ensure full functional equivalence.
*   **`isbert_schema.dwpa_util_skript.runstatement` Details**: Assuming `runstatement` is a simple wrapper for DDL execution for `TRUNCATE TABLE`. If it involves more complex business logic, logging, or state management, that additional logic must be identified and migrated.
*   **Comprehensive Date Calculation (`Stichtag`)**: The logic in `r_ausd_bp_ta_p_basisprod.ksh` for determining `p_stichtag` (especially the `MIN(sysdate, maxladedatum)` part, potentially involving `FOSHoleLadedatum`) needs to be precisely translated into Python to guarantee correct date parameter generation in the migrated workflow.
*   **Error Handling and Logging (`DWMSG_*`)**: The `DWMSG_*` functions (e.g., `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`) define the legacy job's error handling and logging standards. Their full implementation needs to be understood to ensure that the migrated Python logging and error handling aligns with business requirements and integrates effectively with Cloud Logging.
*   **UC4 Inc. Statements (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)**: The content and functionality of these included UC4 objects are not detailed. These might involve environment variable setup, path definitions, or specific log reading/handling. This functionality needs to be analyzed and replicated within the Airflow DAG's context or supporting Python scripts.
*   **Inactive Code Branches**: The commented-out `sed`, `sort`, `join` operations in `k_ausd_bp_ta_p_basisprod.ksh` represent functionality that is currently disabled. While not part of the active migration scope, their existence indicates potential future requirements or past data processing patterns that could be resurrected. This should be documented and reviewed with stakeholders.

## 8. Build Plan

1.  **BigQuery Schema Migration (DDL)**:
    *   Create the `bert_dwh_prod` dataset.
    *   Define and create all necessary source tables (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, `sof_ta_cntrct_evn`, `sof_ta_iccid_vertrag`, `sof_ta_rn_vertrag`, `sof_ta_rn_da_vda_tk`, `sof_ta_tarifoption`, `sof_ta_apn_vertrag`, `dwtk_meldungen`) and the target table (`sof_ta_p_basisprod`) in BigQuery. Ensure correct data type mapping from Oracle.
    *   **Language**: BigQuery DDL.

2.  **Initial Data Load & Ongoing Synchronization**:
    *   Perform a one-time migration of historical data from the Oracle source tables into their corresponding BigQuery tables.
    *   Establish an ongoing data synchronization mechanism (CDC or batch) to keep BigQuery source tables updated from Oracle.
    *   **Language**: Google Cloud Database Migration Service, Dataflow, or custom Python/SQL scripts.

3.  **Python Utility Development**:
    *   Develop Python modules to replace KornShell functionalities:
        *   Environment setup (`. $HOME/.dw_init` equivalent).
        *   Parameter parsing (replacing `h_alis_parameter.ksh`).
        *   Date utilities (replacing `h_alis_date.ksh` and date calculation logic in `r_ausd_bp_ta_p_basisprod.ksh`).
        *   Error handling and logging (replacing `f_alis_msgerr.ksh` and `DWMSG_*` functions, integrating with Cloud Logging/BigQuery logging table).
        *   SQL execution wrapper logic from `h_alis_sqlplus.ksh` if it contains more than simple SQLPlus invocation.
    *   **Language**: Python.

4.  **BigQuery SQL Transformation Script Development**:
    *   Translate `d_ausd_bp_ta_p_basisprod.sql` into a BigQuery-compatible SQL script (`d_ausd_bp_ta_p_basisprod_bq.sql`). This includes adapting syntax, functions, and removing Oracle-specific hints.
    *   **Language**: BigQuery SQL.

5.  **Airflow DAG Development (`bert_ausd_bp_ta_p_basisprod_dag.py`)**:
    *   Create a new Airflow DAG.
    *   Integrate the Python utility modules developed in step 3.
    *   Define the sequence of tasks:
        *   `parameter_setup_task`: PythonOperator for parameter and date determination.
        *   `truncate_target_table_task`: BigQueryOperator executing `TRUNCATE TABLE bert_dwh_prod.sof_ta_p_basisprod`.
        *   `execute_transformation_task`: BigQueryOperator executing `d_ausd_bp_ta_p_basisprod_bq.sql`.
        *   `logging_task`: PythonOperator for final logging/status updates.
    *   Configure appropriate task dependencies.
    *   **Language**: Python.

6.  **Testing and Validation**:
    *   **Unit Testing**: Test individual Python modules and BigQuery SQL transformation for correctness.
    *   **Integration Testing**: Verify the Airflow DAG executes correctly, passing parameters and data between tasks as expected.
    *   **Data Validation**: Compare the output data in `bert_dwh_prod.sof_ta_p_basisprod` with the expected results from the legacy Oracle system for various test cases and date parameters.
    *   **Performance Testing**: Benchmark the BigQuery job performance against the legacy Oracle job.
    *   **Language**: Python (Pytest), SQL, manual inspection.