# Migration Design — DW.BERT_AUSD_BP_TA_P_BASISPROD

## 1. Purpose & Scope

The job `DW.BERT_AUSD_BP_TA_P_BASISPROD` is an ETL process responsible for the preparation of "instantiierten Basisprodukte" (instantiated base products) for the BERT system. This involves extracting data from several source tables, applying complex transformations and joins, and loading the result into a target table, `SOF$TA_P_BASISPROD`.

The overall workflow is orchestrated by a UC4 Job Plan `DW.BERT_P_BASISPRODUKT_JP` (though this specific job is a sub-component), which invokes a UC4 UNIX job `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`. This UC4 job then executes a KornShell script (`r_ausd_bp_ta_p_basisprod.ksh`), which in turn calls another KornShell wrapper (`k_ausd_bp_ta_p_basisprod.ksh`). The ultimate data transformation logic resides within an Oracle SQLPlus script (`d_ausd_bp_ta_p_basisprod.sql`) executed by the KornShell wrappers.

The scope of this migration design is to convert this existing Oracle-based ETL job to run on Google Cloud Platform, utilizing Airflow for orchestration and BigQuery for data storage and transformation, with Python (PySpark) wrappers where necessary.

## 2. Source Inventory

The following files constitute the source components of this job:

| File Path | Technology | Category | Tool | Summary | Tier | Automation Bucket |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------- | :------ | :------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` | UC4 XML     | `uc4`   | `UC4/Automic` | UC4 job definition for a UNIX job responsible for preparing instantiated base products by executing a shell script. | `medium`     | `semi_auto`       |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_P_BASISPRODUKT_JP.xml` | UC4 XML     | `uc4`   | `UC4/Automic` | This UC4 Job Plan orchestrates the execution of multiple child jobs related to product master data processing, including conditional branching and retry logic. (Parent Job Plan) | `very_complex` | `redesign`        |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh` | KornShell   | `shell` | `KornShell`   | This ksh script acts as a control wrapper for an SQL script, handling parameter parsing, environment setup, date validation, and execution of the main SQL logic. | `medium`     | `semi_auto`       |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_p_basisprod.ksh` | KornShell   | `shell` | `KornShell`   | This KornShell script orchestrates the initial provisioning of selected 'Basisprodukte' (base products) for BERT. It generates a snapshot of contract cache from the Data Warehouse (DWH) and makes it available for 'Forderungsscoring' by calling a core script. | `medium`     | `semi_auto`       |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql` | Oracle SQL  | `sql`   | `Oracle SQLPlus` | This SQLPlus script orchestrates the population of the `SOF$TA_P_BASISPROD` table. It first determines a date variable, then calls a stored procedure to truncate the target table, and finally inserts data by joining multiple source tables. | `complex`    | `manual`          |

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform (GCP) services:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Transformation:** Google BigQuery for SQL execution.
*   **Wrapper Logic/Parameter Handling:** Python scripts executed via Dataproc or directly in Airflow (PythonOperator). For this job, DataprocSubmitJobOperator is indicated by the UC4 conversion tool.
*   **Data Storage:** BigQuery will host the migrated `SOF$TA_P_BASISPROD` table and all source tables currently in Oracle.

**Target BigQuery Components:**
*   **Dataset:** A dedicated BigQuery dataset (e.g., `bert_dwh`) will house the migrated tables.
*   **Tables:**
    *   `bert_dwh.SOF_TA_P_BASISPROD` (target table)
    *   `bert_dwh.SOF_TA_CNTRCT_DIST` (source table)
    *   `bert_dwh.SOF_TA_CNTRCT_EVN` (source table)
    *   `bert_dwh.SOF_TA_ICCID_VERTRAG` (source table)
    *   `bert_dwh.SOF_TA_RN_VERTRAG` (source table)
    *   `bert_dwh.SOF_TA_RN_DA_VDA_TK` (source table)
    *   `bert_dwh.SOF_TA_TARIFOPTION` (source table)
    *   `bert_dwh.SOF_TA_APN_VERTRAG` (source table)
    *   `bert_dwh.SOF_TA_BCP_ICCID` (source table)
    *   `bert_dwh.SOF_TA_BCP_MSISDN` (source table)
    *   `isbert_schema.dwtk_meldungen` (source/utility table, assuming `isbert_schema` maps to a BigQuery dataset)

## 4. Data Flow & Lineage

The migrated data flow will be as follows:

1.  **Airflow DAG Trigger:** The Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` (derived from `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`) is triggered based on its schedule (to be defined, as no EVNT_TIME was available).
2.  **PySpark Job Submission:** The DAG submits a PySpark job (derived from `r_ausd_bp_ta_p_basisprod.ksh`) to a Dataproc cluster. This PySpark script will act as the primary wrapper.
3.  **Parameter Handling:** The PySpark script will parse parameters (e.g., `JobKennung`, `Stichtag`, `EintragsNr`, `Wiederanlaufwert`) as defined in the original `k_ausd_bp_ta_p_basisprod.ksh` and `r_ausd_bp_ta_p_basisprod.ksh` scripts. It will also handle date validation and environment setup.
4.  **BigQuery SQL Execution:** The PySpark script will then execute the transformed BigQuery SQL script (derived from `d_ausd_bp_ta_p_basisprod.sql`).
5.  **Data Transformation and Load:** The BigQuery SQL will:
    *   Determine `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   Truncate the target table `bert_dwh.SOF_TA_P_BASISPROD`.
    *   Insert data into `bert_dwh.SOF_TA_P_BASISPROD` by joining various `bert_dwh.SOF_TA_*` source tables.
    *   Perform column remappings and transformations as specified in the original SQL.
6.  **Logging and Status Update:** The PySpark script will manage logging and update job status, similar to the original KornShell's interaction with the error handling and logging utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, etc.).

**Execution Order:**

*   **Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`**
    *   `start` task
    *   `run_bert_ausd_bp_ta_p_basisprod` (DataprocSubmitJobOperator executing `r_ausd_bp_ta_p_basisprod.py`)
        *   (Internal to PySpark script):
            *   Initialize parameters
            *   Execute BQSQL to populate `bert_dwh.SOF_TA_P_BASISPROD`
            *   Handle logging/status
    *   `end` task

## 5. Transformation Logic

### 5.1 Orchestration (UC4 XML to Airflow DAG)

The `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` will be converted into an Airflow DAG named `dw_bert_ausd_bp_ta_p_basisprod`. This DAG will consist of a single `DataprocSubmitJobOperator` task that executes the Python equivalent of the KornShell scripts.

**Key Mapping:**
*   UC4 Job `DW.BERT_AUSD_BP_TA_P_BASISPROD` -> Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
*   UC4 `SCRIPT` section -> Triggers a PySpark script.
*   UC4 host `|DWHDWH2P|HOST` -> GCP Dataproc Cluster (placeholder `YOUR_DATAPROC_CLUSTER_NAME`).
*   UC4 login `DW.UNIX.ISBERT` -> Informational, handled by GCP service accounts/permissions.
*   Restartability (UC4 `DOCU_Doku`) -> Configured in Airflow task retries if explicit policy is determined. Current default is 0 retries.

### 5.2 Shell Script Logic (KornShell to Python/PySpark)

The KornShell scripts `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh` will be combined and converted into a single Python script, `r_ausd_bp_ta_p_basisprod.py`, designed to run as a PySpark job on Dataproc.

**Key Mappings:**
*   **Environment Setup:** `. $HOME/.dw_init` and other `inc` files will be replaced by Python environment setup, module imports, and potentially Airflow Variables/Connections.
*   **Parameter Parsing:** `getopts` logic will be translated to Python's `argparse` module to handle `-j`, `-f`, `-s`, `-l` parameters.
*   **Date Handling:** KornShell `h_alis_date.ksh` functions (`DWDate_Datum_Check`, `DWDate_Gib_Zeitraum`) will be replaced by Python's `datetime` module.
*   **SQL Execution:** The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) will be replaced by a Python function that uses a BigQuery client library to execute the `d_ausd_bp_ta_p_basisprod.sql` (now converted to BQSQL).
*   **Logging:** KornShell error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, etc.) will be replaced by Python logging mechanisms, possibly integrating with Cloud Logging.
*   **Temporary Files:** `tmpFile` and associated `cat`/`eval` operations will be handled using in-memory variables or temporary storage on GCS.
*   **Commented-out Sections:** The commented-out `sed`, `sort`, `join` commands in `k_ausd_bp_ta_p_basisprod.ksh` indicate potential file-based processing that was either deprecated or unused. These will not be migrated unless explicitly required.

### 5.3 Data Transformation (Oracle SQLPlus to BigQuery SQL)

The `d_ausd_bp_ta_p_basisprod.sql` script will be converted to BigQuery Standard SQL.

**Key Mappings:**
*   **Table Naming:** Oracle schema-qualified tables (e.g., `isbert_schema.dwtk_meldungen`, `sof$ta_p_basisprod`) will be mapped to BigQuery `project.dataset.table` format (e.g., `` `your_gcp_project.bert_dwh.dwtk_meldungen` ``, `` `your_gcp_project.bert_dwh.SOF_TA_P_BASISPROD` ``). `$` in table names will be replaced with `_` or removed (`SOF$TA_P_BASISPROD` -> `SOF_TA_P_BASISPROD`).
*   **Outer Join Syntax:** Oracle `(+)` outer join will be converted to `LEFT JOIN`.
*   **Conditional Logic:** Oracle `DECODE` function will be converted to BigQuery `CASE WHEN` statements.
*   **Null Handling:** Oracle `NVL` function will be converted to BigQuery `IFNULL` or `COALESCE`.
*   **Date Formatting:** Oracle `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` will be converted to BigQuery `FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated))` or `FORMAT_DATETIME` / `FORMAT_DATE` depending on data types.
*   **Table Truncation:** Oracle `TRUNCATE TABLE ... REUSE STORAGE` will be converted to BigQuery `TRUNCATE TABLE`.
*   **Procedural Calls:** The Oracle PL/SQL block calling `isbert_schema.dwpa_util_skript.runstatement` will need to be re-evaluated. If `runstatement` performs generic DDL, it might be replaced by direct DDL execution. If it contains business logic, that logic needs to be migrated to BigQuery Stored Procedures or Python. For now, it's assumed to be part of the truncation process and handled by the overall script.
*   **Optimizer Hints:** Oracle optimizer hints (e.g., `/*+ APPEND */`, `/*+ ORDERED ... */`) will be removed as BigQuery's optimizer handles query planning automatically.
*   **`DEFINE` variables:** Oracle `DEFINE` variables (e.g., `v_carmen`) will be converted to BigQuery `DECLARE` statements or passed as script parameters.
*   **`COLUMN new_value`:** This mechanism to store query result into a SQLPlus variable will be handled by the Python wrapper that executes the BigQuery SQL, extracting the `v_datum` value.
*   **`COMMIT`:** Explicit `COMMIT` statements are not needed in BigQuery's auto-committing DML operations.
*   **`spool` and `prompt`:** These SQLPlus client commands will be removed; logging will be handled by the Python wrapper and Airflow.

## 6. External Dependencies

The initial analysis reported no explicit external systems for this job. However, the original environment involved UNIX hosts and an Oracle database.

*   **UNIX Host (`|DWHDWH2P|HOST`):** This will be replaced by the GCP Dataproc cluster and Airflow (Cloud Composer) environment.
*   **Oracle Database:** All Oracle tables (`sof$ta_p_basisprod`, `isbert_schema.dwtk_meldungen`, etc.) will be migrated to Google BigQuery. Data will be ingested from Oracle into BigQuery through a separate data migration process (e.g., Datastream, DMS, batch loads).

## 7. Unresolved / Risks

*   **Parent Job Plan (`DW.BERT_P_BASISPRODUKT_JP.xml`):** This job plan is marked as `very_complex` and `redesign`. While this document focuses on a sub-component, a full redesign of the overall job plan hierarchy and dependencies will be necessary to achieve an optimal migration. The current design assumes this specific `DW.BERT_AUSD_BP_TA_P_BASISPROD` job will be invoked as a standalone Airflow DAG.
*   **KornShell Utility Scripts:** The KornShell scripts depend on several shared utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). The functionalities of these utilities (e.g., error handling, parameter parsing, date manipulation) will need to be re-implemented in Python or leveraged from existing Airflow/GCP capabilities.
*   **Oracle Stored Procedure `isbert_schema.dwpa_util_skript.runstatement`:** The exact functionality of this procedure needs to be understood. If it performs critical business logic, it will require migration to a BigQuery Stored Procedure or Python. If it's purely for DDL execution, direct BigQuery DDL statements are sufficient.
*   **Parameter Defaults and Validation:** The KornShell scripts have logic for default values (e.g., `p_wiederanlaufWert`, `p_stichtag`) and parameter validation. This logic needs to be faithfully replicated in the Python wrapper.
*   **Data Type Mismatches:** Although the SQL conversion tool attempts to map data types, careful review is required to ensure no precision loss or unexpected behavior occurs, especially with various ICCID/MSISDN/status/validity fields and the newly added `MS3` through `MS10` MultiSIM fields.

## 8. Build Plan

The migration will involve generating the following artifacts:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_p_basisprod.py`):**
    *   **Language:** Python
    *   **Purpose:** Orchestrates the execution of the PySpark transformation script.
    *   **Generation Tool:** `uc4_to_airflow_dag_design` (already used for design, will be used for build).
    *   **Dependencies:** `r_ausd_bp_ta_p_basisprod.py` (PySpark script).

2.  **PySpark Transformation Script (`r_ausd_bp_ta_p_basisprod.py`):**
    *   **Language:** Python (PySpark)
    *   **Purpose:** Acts as the main application logic. It parses runtime parameters, sets up the environment, and executes the BigQuery SQL transformation. It encapsulates the logic from both `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh`.
    *   **Generation Method:** Manual implementation based on shell script logic and Python BigQuery client.
    *   **Dependencies:** `d_ausd_bp_ta_p_basisprod.bqsql` (BigQuery SQL script).

3.  **BigQuery SQL Transformation Script (`d_ausd_bp_ta_p_basisprod.bqsql`):**
    *   **Language:** BigQuery Standard SQL
    *   **Purpose:** Performs the core data extraction, transformation, and loading into `bert_dwh.SOF_TA_P_BASISPROD`.
    *   **Generation Tool:** `hql_sql_to_bqsql_design` (already used for design, will be used for build).
    *   **Dependencies:** `bert_dwh.SOF_TA_CNTRCT_DIST`, `bert_dwh.SOF_TA_CNTRCT_EVN`, `bert_dwh.SOF_TA_ICCID_VERTRAG`, `bert_dwh.SOF_TA_RN_VERTRAG`, `bert_dwh.SOF_TA_RN_DA_VDA_TK`, `bert_dwh.SOF_TA_TARIFOPTION`, `bert_dwh.SOF_TA_APN_VERTRAG`, `bert_dwh.SOF_TA_BCP_ICCID`, `bert_dwh.SOF_TA_BCP_MSISDN`, `bert_dwh.dwtk_meldungen`.

**Ordered Build Steps:**

1.  **Migrate Oracle Schemas/Tables to BigQuery:** This foundational step involves creating the target BigQuery tables and ingesting data from the source Oracle database. This is a prerequisite for the ETL job migration.
2.  **Generate `d_ausd_bp_ta_p_basisprod.bqsql`:** Use the `hql_sql_to_bqsql_design` tool with `target_language='bqsql'` and the source `d_ausd_bp_ta_p_basisprod.sql` content. Review and manually refine the generated BQSQL, especially considering the `manual` migration bucket flag.
3.  **Implement `r_ausd_bp_ta_p_basisprod.py`:** Manually develop this Python (PySpark) script. It should:
    *   Accept parameters from Airflow.
    *   Replicate environment variable setups (e.g., `BERT_DIR_ROOT`).
    *   Implement date calculation and validation logic from `h_alis_date.ksh`.
    *   Read and execute `d_ausd_bp_ta_p_basisprod.bqsql` against BigQuery, passing any required runtime parameters.
    *   Implement logging and error handling similar to `f_alis_msgerr.ksh`.
4.  **Generate `dw_bert_ausd_bp_ta_p_basisprod.py` (Airflow DAG):** Use the `uc4_to_airflow_dag_design` tool (or `airflow_dag_build` if given the design output) with the `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` content. Configure the `DataprocSubmitJobOperator` to point to the newly created `r_ausd_bp_ta_p_basisprod.py` script on GCS. Define a suitable schedule.
5.  **Deployment:** Deploy the Airflow DAG to Cloud Composer and the `r_ausd_bp_ta_p_basisprod.py` script to a GCS bucket accessible by Dataproc.

This detailed plan addresses the full migration of `DW.BERT_AUSD_BP_TA_P_BASISPROD`, from orchestration to data transformation, while acknowledging the complexities and manual intervention required for certain components.