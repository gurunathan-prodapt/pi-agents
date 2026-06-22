# Migration Design — DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Purpose & Scope

This job, `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG`, is responsible for preparing aggregated SIM card (ICCID) data at the contract level. It processes individual ICCID records from the `SOF$TA_ICCID_EINZELN` Oracle table, groups them by contract ID, and pivots various ICCID types into distinct columns within the `SOF$TA_ICCID_VERTRAG` Oracle table.

The process is initiated by an Automic (UC4) job, which orchestrates a series of KornShell scripts. These scripts handle parameter parsing, date validation, and the execution of the core Oracle SQL transformation.

**Business Purpose:** To consolidate and structure SIM card identification data per contract, providing a comprehensive view of associated ICCIDs for reporting or further downstream processing.

## 2. Source Inventory

This job is composed of four primary source files, orchestrating a data transformation from an Oracle source to an Oracle target.

| File Name (Relative Path)                                                                                                    | Technology    | Tier    | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| :--------------------------------------------------------------------------------------------------------------------------- | :------------ | :------ | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_ICCID_VERTRAG.xml` | UC4/Automic   | medium  | semi_auto         | UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_ICCID_VERTRAG, responsible for preparing instantiated base products by executing a shell script. It acts as the scheduler for the entire workflow.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`                                    | KornShell     | medium  | semi_auto         | This KornShell script acts as an orchestrator. It parses command-line arguments for a 'Stichtag' (key date) and a 'Wiederanlaufwert' (restart value). It then executes a core data preparation script (`k_ausd_bp_ta_iccid_vertrag.ksh`) with these parameters and includes robust error handling and logging mechanisms through common utility scripts.                                                                                                                                                                                                        |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh`                                    | KornShell     | medium  | semi_auto         | This KornShell script acts as a control script. It parses parameters passed from the orchestrator script, performs date validation, and executes the main Oracle SQL script (`d_ausd_bp_ta_iccid_vertrag.sql`) via a wrapper function. It also handles post-execution steps including job status logging and record counting. Commented-out sections suggest potential for shell-based data reformatting and joining, though these are not currently active. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_iccid_vertrag.sql`                                    | Oracle SQL    | complex | retire            | This Oracle SQL script is the core data transformation component. It truncates the `SOF$TA_ICCID_VERTRAG` table and populates it by aggregating ICCID (SIM card ID) data from `SOF$TA_ICCID_EINZELN`, grouping by `CNTRCT_ID` and pivoting multiple ICCID types (e.g., TN, TC, TB, MS1-MS10) into separate columns. **Note: This file is marked for retirement.**                                                                                                                                                                                                                                                                                                                                   |

## 3. Target Architecture

The migration target platform is BigQuery. The existing Oracle tables will be migrated to BigQuery tables. The orchestration logic currently managed by UC4 and KornShell will be migrated to Google Cloud's orchestration tools, likely Cloud Composer (Apache Airflow) for scheduling and Python for scripting.

**Target Components:**
*   **Scheduling**: Google Cloud Composer (Apache Airflow) DAG.
*   **Data Transformation**: BigQuery SQL script.
*   **Data Sources**:
    *   `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` (from original `SOF$TA_ICCID_EINZELN` Oracle table).
    *   `PROJECT_ID.DATASET.DWTK_MELDUNGEN` (from original `isbert_schema.dwtk_meldungen` Oracle table, if still relevant for logging/metadata).
*   **Data Target**:
    *   `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` (from original `SOF$TA_ICCID_VERTRAG` Oracle table).

## 4. Data Flow & Lineage

The execution and data flow will be as follows in the BigQuery environment:

1.  **Airflow DAG (`dag_dw_bert_ausd_bp_ta_iccid_vertrag`)**:
    *   Triggered by schedule (equivalent to UC4 job schedule).
    *   **Task 1: Orchestrator (`r_ausd_bp_ta_iccid_vertrag_task`)**: A PythonOperator or BashOperator will simulate the logic of `r_ausd_bp_ta_iccid_vertrag.ksh`. This task will primarily determine parameters like `stichtag` (key date) and `wiederanlaufwert` (restart value), adapting the logic from the shell script's `.dw_init`, date, and parameter handling utilities. It will then pass these parameters to the next task.
    *   **Task 2: Control & SQL Execution (`k_ausd_bp_ta_iccid_vertrag_task`)**: This task, potentially a PythonOperator, will encapsulate the logic of `k_ausd_bp_ta_iccid_vertrag.ksh`.
        *   It will receive parameters from the orchestrator task.
        *   It will construct and execute a BigQuery SQL query based on the logic in `d_ausd_bp_ta_iccid_vertrag.sql`.
        *   It will handle BigQuery-specific error logging.

2.  **BigQuery SQL Transformation (`d_ausd_bp_ta_iccid_vertrag.sql` logic)**:
    *   The SQL executed in BigQuery will perform the following steps:
        *   **TRUNCATE/DELETE**: Truncate or overwrite `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`.
        *   **INSERT/SELECT**: Insert data into `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` by selecting from `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`.
        *   **Transformation Logic**: Group by `CNTRCT_ID` and apply `MAX()` aggregate function to pivot the various ICCID and related fields (TN, TC, TB, MS1-MS10 ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, STATUS, VALID_TO, E_ID, CARD_TYPE_NAME).

**Lineage:**
`Airflow DAG` -> `r_ausd_bp_ta_iccid_vertrag_task` -> `k_ausd_bp_ta_iccid_vertrag_task` -> BigQuery SQL (Reads `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`) -> BigQuery SQL (Writes `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`)

## 5. Transformation Logic

The core data transformation logic resides within `d_ausd_bp_ta_iccid_vertrag.sql`.

**Source (Oracle SQL):**

```sql
TRUNCATE TABLE sof$ta_iccid_vertrag REUSE STORAGE;

INSERT INTO sof$ta_iccid_vertrag
(CNTRCT_ID, TN_ICCID, TN_IMSI_MCC, ..., MS10_VALID_TO)
SELECT /*+ full(rp) parallel(rp,4) */
        cntrct_id,
        max(TN_ICCID) TN_ICCID ,
        max(TN_IMSI_MCC) TN_IMSI_MCC,
        -- ... (other max aggregates for TN, TC, TB, MS1-MS10 fields)
FROM    sof$ta_iccid_einzeln rp
group by cntrct_id;
```

**Target (BigQuery SQL):**

The Oracle SQL can be directly translated to BigQuery SQL with minor adjustments for syntax and `TRUNCATE` equivalent.

```sql
-- Truncate or overwrite the target table
TRUNCATE TABLE `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`;
-- OR
-- CREATE OR REPLACE TABLE `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` AS

INSERT INTO `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`
(CNTRCT_ID,
   TN_ICCID,   TN_IMSI_MCC,   TN_IMSI_MNC,   TN_IMSI_HLR,   TN_IMSI_SI,   TN_STATUS,   TN_VALID_TO,
   TC_ICCID,   TC_IMSI_MCC,   TC_IMSI_MNC,   TC_IMSI_HLR,   TC_IMSI_SI,   TC_STATUS,   TC_VALID_TO,
   TB_ICCID,   TB_IMSI_MCC,   TB_IMSI_MNC,   TB_IMSI_HLR,   TB_IMSI_SI,   TB_STATUS,   TB_VALID_TO,
   MS1_ICCID,  MS1_IMSI_MCC,  MS1_IMSI_MNC,  MS1_IMSI_HLR,  MS1_IMSI_SI,  MS1_STATUS,  MS1_VALID_TO,
   MS2_ICCID,  MS2_IMSI_MCC,  MS2_IMSI_MNC,  MS2_IMSI_HLR,  MS2_IMSI_SI,  MS2_STATUS,  MS2_VALID_TO,
   TN_E_ID, TN_CARD_TYPE_NAME, TC_E_ID, TC_CARD_TYPE_NAME, TB_E_ID, TB_CARD_TYPE_NAME,
   MS1_E_ID, MS1_CARD_TYPE_NAME, MS2_E_ID, MS2_CARD_TYPE_NAME,
   MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_IMSI_MCC, MS3_IMSI_MNC, MS3_IMSI_HLR, MS3_IMSI_SI, MS3_STATUS, MS3_VALID_TO,
   MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_IMSI_MCC, MS4_IMSI_MNC, MS4_IMSI_HLR, MS4_IMSI_SI, MS4_STATUS, MS4_VALID_TO,
   MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_IMSI_MCC, MS5_IMSI_MNC, MS5_IMSI_HLR, MS5_IMSI_SI, MS5_STATUS, MS5_VALID_TO,
   MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_IMSI_MCC, MS6_IMSI_MNC, MS6_IMSI_HLR, MS6_IMSI_SI, MS6_STATUS, MS6_VALID_TO,
   MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_IMSI_MCC, MS7_IMSI_MNC, MS7_IMSI_HLR, MS7_IMSI_SI, MS7_STATUS, MS7_VALID_TO,
   MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_IMSI_MCC, MS8_IMSI_MNC, MS8_IMSI_HLR, MS8_IMSI_SI, MS8_STATUS, MS8_VALID_TO,
   MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_IMSI_MCC, MS9_IMSI_MNC, MS9_IMSI_HLR, MS9_IMSI_SI, MS9_STATUS, MS9_VALID_TO,
   MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_IMSI_MCC, MS10_IMSI_MNC, MS10_IMSI_HLR, MS10_IMSI_SI, MS10_STATUS, MS10_VALID_TO
)
SELECT
        cntrct_id,
        MAX(TN_ICCID) AS TN_ICCID,
        MAX(TN_IMSI_MCC) AS TN_IMSI_MCC,
        MAX(TN_IMSI_MNC) AS TN_IMSI_MNC,
        MAX(TN_IMSI_HLR) AS TN_IMSI_HLR,
        MAX(TN_IMSI_SI) AS TN_IMSI_SI,
        MAX(TN_STATUS) AS TN_STATUS,
        MAX(TN_VALID_TO) AS TN_VALID_TO,
        MAX(TC_ICCID) AS TC_ICCID,
        MAX(TC_IMSI_MCC) AS TC_IMSI_MCC,
        MAX(TC_IMSI_MNC) AS TC_IMSI_MNC,
        MAX(TC_IMSI_HLR) AS TC_IMSI_HLR,
        MAX(TC_IMSI_SI) AS TC_IMSI_SI,
        MAX(TC_STATUS) AS TC_STATUS,
        MAX(TC_VALID_TO) AS TC_VALID_TO,
        MAX(TB_ICCID) AS TB_ICCID,
        MAX(TB_IMSI_MCC) AS TB_IMSI_MCC,
        MAX(TB_IMSI_MNC) AS TB_IMSI_MNC,
        MAX(TB_IMSI_HLR) AS TB_IMSI_HLR,
        MAX(TB_IMSI_SI) AS TB_IMSI_SI,
        MAX(TB_STATUS) AS TB_STATUS,
        MAX(TB_VALID_TO) AS TB_VALID_TO,
        MAX(MS1_ICCID) AS MS1_ICCID,
        MAX(MS1_IMSI_MCC) AS MS1_IMSI_MCC,
        MAX(MS1_IMSI_MNC) AS MS1_IMSI_MNC,
        MAX(MS1_IMSI_HLR) AS MS1_IMSI_HLR,
        MAX(MS1_IMSI_SI) AS MS1_IMSI_SI,
        MAX(MS1_STATUS) AS MS1_STATUS,
        MAX(MS1_VALID_TO) AS MS1_VALID_TO,
        MAX(MS2_ICCID) AS MS2_ICCID,
        MAX(MS2_IMSI_MCC) AS MS2_IMSI_MCC,
        MAX(MS2_IMSI_MNC) AS MS2_IMSI_MNC,
        MAX(MS2_IMSI_HLR) AS MS2_IMSI_HLR,
        MAX(MS2_IMSI_SI) AS MS2_IMSI_SI,
        MAX(MS2_STATUS) AS MS2_STATUS,
        MAX(MS2_VALID_TO) AS MS2_VALID_TO,
        MAX(TN_E_ID) AS TN_E_ID,
        MAX(TN_CARD_TYPE_NAME) AS TN_CARD_TYPE_NAME,
        MAX(TC_E_ID) AS TC_E_ID,
        MAX(TC_CARD_TYPE_NAME) AS TC_CARD_TYPE_NAME,
        MAX(TB_E_ID) AS TB_E_ID,
        MAX(TB_CARD_TYPE_NAME) AS TB_CARD_TYPE_NAME,
        MAX(MS1_E_ID) AS MS1_E_ID,
        MAX(MS1_CARD_TYPE_NAME) AS MS1_CARD_TYPE_NAME,
        MAX(MS2_E_ID) AS MS2_E_ID,
        MAX(MS2_CARD_TYPE_NAME) AS MS2_CARD_TYPE_NAME,
        MAX(MS3_ICCID) AS MS3_ICCID,
        MAX(MS3_E_ID) AS MS3_E_ID,
        MAX(MS3_CARD_TYPE_NAME) AS MS3_CARD_TYPE_NAME,
        MAX(MS3_IMSI_MCC) AS MS3_IMSI_MCC,
        MAX(MS3_IMSI_MNC) AS MS3_IMSI_MNC,
        MAX(MS3_IMSI_HLR) AS MS3_IMSI_HLR,
        MAX(MS3_IMSI_SI) AS MS3_IMSI_SI,
        MAX(MS3_STATUS) AS MS3_STATUS,
        MAX(MS3_VALID_TO) AS MS3_VALID_TO,
        MAX(MS4_ICCID) AS MS4_ICCID,
        MAX(MS4_E_ID) AS MS4_E_ID,
        MAX(MS4_CARD_TYPE_NAME) AS MS4_CARD_TYPE_NAME,
        MAX(MS4_IMSI_MCC) AS MS4_IMSI_MCC,
        MAX(MS4_IMSI_MNC) AS MS4_IMSI_MNC,
        MAX(MS4_IMSI_HLR) AS MS4_IMSI_HLR,
        MAX(MS4_IMSI_SI) AS MS4_IMSI_SI,
        MAX(MS4_STATUS) AS MS4_STATUS,
        MAX(MS4_VALID_TO) AS MS4_VALID_TO,
        MAX(MS5_ICCID) AS MS5_ICCID,
        MAX(MS5_E_ID) AS MS5_E_ID,
        MAX(MS5_CARD_TYPE_NAME) AS MS5_CARD_TYPE_NAME,
        MAX(MS5_IMSI_MCC) AS MS5_IMSI_MCC,
        MAX(MS5_IMSI_MNC) AS MS5_IMSI_MNC,
        MAX(MS5_IMSI_HLR) AS MS5_IMSI_HLR,
        MAX(MS5_IMSI_SI) AS MS5_IMSI_SI,
        MAX(MS5_STATUS) AS MS5_STATUS,
        MAX(MS5_VALID_TO) AS MS5_VALID_TO,
        MAX(MS6_ICCID) AS MS6_ICCID,
        MAX(MS6_E_ID) AS MS6_E_ID,
        MAX(MS6_CARD_TYPE_NAME) AS MS6_CARD_TYPE_NAME,
        MAX(MS6_IMSI_MCC) AS MS6_IMSI_MCC,
        MAX(MS6_IMSI_MNC) AS MS6_IMSI_MNC,
        MAX(MS6_IMSI_HLR) AS MS6_IMSI_HLR,
        MAX(MS6_IMSI_SI) AS MS6_IMSI_SI,
        MAX(MS6_STATUS) AS MS6_STATUS,
        MAX(MS6_VALID_TO) AS MS6_VALID_TO,
        MAX(MS7_ICCID) AS MS7_ICCID,
        MAX(MS7_E_ID) AS MS7_E_ID,
        MAX(MS7_CARD_TYPE_NAME) AS MS7_CARD_TYPE_NAME,
        MAX(MS7_IMSI_MCC) AS MS7_IMSI_MCC,
        MAX(MS7_IMSI_MNC) AS MS7_IMSI_MNC,
        MAX(MS7_IMSI_HLR) AS MS7_IMSI_HLR,
        MAX(MS7_IMSI_SI) AS MS7_IMSI_SI,
        MAX(MS7_STATUS) AS MS7_STATUS,
        MAX(MS7_VALID_TO) AS MS7_VALID_TO,
        MAX(MS8_ICCID) AS MS8_ICCID,
        MAX(MS8_E_ID) AS MS8_E_ID,
        MAX(MS8_CARD_TYPE_NAME) AS MS8_CARD_TYPE_NAME,
        MAX(MS8_IMSI_MCC) AS MS8_IMSI_MCC,
        MAX(MS8_IMSI_MNC) AS MS8_IMSI_MNC,
        MAX(MS8_IMSI_HLR) AS MS8_IMSI_HLR,
        MAX(MS8_IMSI_SI) AS MS8_IMSI_SI,
        MAX(MS8_STATUS) AS MS8_STATUS,
        MAX(MS8_VALID_TO) AS MS8_VALID_TO,
        MAX(MS9_ICCID) AS MS9_ICCID,
        MAX(MS9_E_ID) AS MS9_E_ID,
        MAX(MS9_CARD_TYPE_NAME) AS MS9_CARD_TYPE_NAME,
        MAX(MS9_IMSI_MCC) AS MS9_IMSI_MCC,
        MAX(MS9_IMSI_MNC) AS MS9_IMSI_MNC,
        MAX(MS9_IMSI_HLR) AS MS9_IMSI_HLR,
        MAX(MS9_IMSI_SI) AS MS9_IMSI_SI,
        MAX(MS9_STATUS) AS MS9_STATUS,
        MAX(MS9_VALID_TO) AS MS9_VALID_TO,
        MAX(MS10_ICCID) AS MS10_ICCID,
        MAX(MS10_E_ID) AS MS10_E_ID,
        MAX(MS10_CARD_TYPE_NAME) AS MS10_CARD_TYPE_NAME,
        MAX(MS10_IMSI_MCC) AS MS10_IMSI_MCC,
        MAX(MS10_IMSI_MNC) AS MS10_IMSI_MNC,
        MAX(MS10_IMSI_HLR) AS MS10_IMSI_HLR,
        MAX(MS10_IMSI_SI) AS MS10_IMSI_SI,
        MAX(MS10_STATUS) AS MS10_STATUS,
        MAX(MS10_VALID_TO) AS MS10_VALID_TO
FROM    `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`
GROUP BY cntrct_id;
```

**Script Logic Transformation (`r_ausd_bp_ta_iccid_vertrag.ksh` & `k_ausd_bp_ta_iccid_vertrag.ksh`):**
The shell script logic will be refactored into Python code within an Airflow DAG.

*   **Parameter Parsing**: The `getopts` logic for `Stichtag` (`-s`) and `Wiederanlaufwert` (`-l`) will be implemented using Python's `argparse` or similar, or directly as Airflow DAG parameters.
*   **Environment Initialization**: `. $HOME/.dw_init` and other `.ksh` includes represent environment setup and utility functions. These will need to be translated into Python functions or Airflow Variable lookups.
*   **Date Handling**: `DWDate_Datum_Check` and `DWDate_Gib_Zeitraum` will be replaced with Python's `datetime` module functionalities.
*   **SQL Execution Wrapper**: The `starteSQLSkript` function will be replaced by a BigQuery hook/operator in Airflow that executes the translated BigQuery SQL.
*   **Logging**: The `DWMSG_*` error and status logging will be replaced by Airflow's native logging mechanisms and potentially custom Cloud Logging integrations.
*   **Record Count**: Reading from `$tmpFile` will be replaced by querying BigQuery for the row count after the insert.

## 6. External Dependencies

| Original System / Component                                  | Replacement in GCP                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Notes                                                                                                                                                                                                                                  |
| :----------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **UC4/Automic** (Scheduler)                                  | Google Cloud Composer (Apache Airflow)                                                                                                                                                                                                                                                                                                                                                                                                                                                               | The existing UC4 job definition will be translated into an Airflow DAG (`dag_dw_bert_ausd_bp_ta_iccid_vertrag`) responsible for scheduling and orchestrating the BigQuery transformation and related tasks.                             |
| **Oracle Database** (housing `SOF$TA_ICCID_EINZELN`, `SOF$TA_ICCID_VERTRAG`, `isbert_schema.dwtk_meldungen`) | Google BigQuery                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | The source and target Oracle tables will be migrated to BigQuery tables (`PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`, `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`, `PROJECT_ID.DATASET.DWTK_MELDUNGEN`). Data will need to be migrated using tools like Cloud Dataflow or DMS. |
| **UNIX Host `DWHDWH2P`**                                     | Google Cloud Compute Engine (for specific needs if any scripts cannot be Pythonized) or generally, Python execution within Airflow tasks.                                                                                                                                                                                                                                                                                                                                                              | The current execution environment for KornShell scripts. Most logic will be re-implemented in Python within Airflow, removing the dependency on a specific UNIX host. If any specialized shell commands are indispensable, they might run in a Docker container or a dedicated GCE instance. |
| **Login `DW.UNIX.ISBERT`**                                   | Google Cloud Service Accounts                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Authentication to Google Cloud resources (BigQuery, Cloud Storage, etc.) will be handled using service accounts with appropriate IAM roles, adhering to the principle of least privilege.                                            |
| **Oracle DB Link `@pcrs1` (implied by `DEFINE v_carmen = "@pcrs1"`)** | If `pcrs1` is another Oracle database that provides source data, it will need to be migrated to BigQuery as well, or a federated query/external table in BigQuery might be set up to access it from the source Oracle database if it cannot be migrated immediately. The `isbert_schema.dwtk_meldungen` table would also need to be accessible, likely also migrated to BigQuery. | The presence of a DB link suggests external data access. The exact nature and necessity of this link need to be thoroughly investigated to determine the appropriate BigQuery-native alternative or direct migration of the linked data. |
| **Common KornShell Utilities** (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) | Python functions / Airflow Hooks & Operators                                                                                                                                                                                                                                                                                                                                                                                           | These common utilities will be re-implemented in Python as helper functions or custom Airflow operators/hooks, providing equivalent functionality for environment setup, error handling, date calculations, and BigQuery interaction. |

## 7. Unresolved / Risks

*   **SQL Script Retirement Status (B0 - Retire):** The `d_ausd_bp_ta_iccid_vertrag.sql` script is marked for retirement. This is the most critical risk. A decision needs to be made:
    *   **Option A (Migrate as designed):** If the "retire" status is provisional or business requirements dictate keeping the logic, then the migration proceeds as outlined above. However, the business should re-evaluate the need for this specific transformation.
    *   **Option B (Redesign/Decommission):** If the script is truly meant to be retired, then this entire job should be decommissioned or fully redesigned based on new requirements, rather than a direct migration. The current design assumes Option A for completeness.
*   **Completeness of Utility Scripts:** The KornShell scripts rely on several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While their purpose is generally understood, their full implementation details are not available from this lineage run. These will require detailed analysis and re-implementation in Python or verification of existing Airflow equivalents.
*   **`starteSQLSkript` function:** The `starteSQLSkript` function within `k_ausd_bp_ta_iccid_vertrag.ksh` is a wrapper for SQL execution. Its full behavior (e.g., error handling, logging, connection management beyond basic `sqlplus`) needs to be fully understood to ensure accurate replication in Airflow.
*   **`DEFINE v_carmen = "@pcrs1"`**: The Oracle SQL script defines a variable `v_carmen` that points to an Oracle DB link `@pcrs1`. The specific use of this variable and the `pcrs1` database needs to be understood. If `pcrs1` is a source of data, it will also need to be migrated or connected to BigQuery.
*   **Historical Commented-out Logic:** The `k_ausd_bp_ta_iccid_vertrag.ksh` contains commented-out sections involving `sed`, `sort`, and `join` for data manipulation. While not active, it's good to note their existence. They should not be part of the migration scope unless explicitly reactivated by business.
*   **Data Volume and Performance:** The `parallel(rp,4)` hint in the Oracle SQL suggests performance considerations. This should be taken into account when designing BigQuery tables (partitioning, clustering) and Airflow task resource allocation to ensure equivalent or better performance.

## 8. Build Plan

The migration will involve the following ordered steps:

1.  **Migrate Oracle Tables to BigQuery:**
    *   Migrate `SOF$TA_ICCID_EINZELN` to `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`. (Technology: DMS, Dataflow, or other ETL)
    *   Migrate `SOF$TA_ICCID_VERTRAG` to `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`. (Technology: DMS, Dataflow, or other ETL)
    *   Migrate `isbert_schema.dwtk_meldungen` to `PROJECT_ID.DATASET.DWTK_MELDUNGEN` (if required for logging/metadata). (Technology: DMS, Dataflow, or other ETL)
    *   Investigate and migrate/connect `pcrs1` if it's an active data source. (Technology: DMS, Federated Query, or other ETL)

2.  **Translate SQL Transformation Logic:**
    *   Translate `d_ausd_bp_ta_iccid_vertrag.sql` into BigQuery-compatible SQL.
        *   **Output**: `bigquery_sql/d_ausd_bp_ta_iccid_vertrag.sql` (Language: BigQuery SQL)

3.  **Develop Airflow DAG for Orchestration:**
    *   Create an Airflow DAG (`dag_dw_bert_ausd_bp_ta_iccid_vertrag.py`) that encapsulates the scheduling and orchestration logic.
        *   **Task**: `r_ausd_bp_ta_iccid_vertrag_python_task` (PythonOperator/BashOperator)
            *   Re-implement parameter parsing logic from `r_ausd_bp_ta_iccid_vertrag.ksh`.
            *   Re-implement date handling and environment setup logic.
            *   Pass parameters to the next task.
        *   **Task**: `execute_bq_sql_task` (BigQueryOperator)
            *   Receive parameters (e.g., `stichtag`, `wiederanlaufwert`).
            *   Execute the `bigquery_sql/d_ausd_bp_ta_iccid_vertrag.sql` using the BigQueryOperator.
            *   Implement BigQuery-native error handling and logging.
            *   (Optional) If `k_ausd_bp_ta_iccid_vertrag.ksh` has complex logic not covered by BigQueryOperator, create a PythonOperator `k_ausd_bp_ta_iccid_vertrag_python_task` to build and execute the BigQuery SQL dynamically.
        *   **Output**: `dags/dag_dw_bert_ausd_bp_ta_iccid_vertrag.py` (Language: Python)

4.  **Re-implement Common Utilities:**
    *   Create a Python module for common utility functions (e.g., date calculations, parameter handling, BigQuery interaction wrappers, custom logging).
        *   **Output**: `utils/bert_utilities.py` (Language: Python)

5.  **Deployment and Testing:**
    *   Deploy the Airflow DAG and Python utilities to Cloud Composer.
    *   Perform thorough unit, integration, and user acceptance testing, especially verifying the data transformation accuracy and job scheduling.