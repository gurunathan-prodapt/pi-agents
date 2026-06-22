# Migration Design — DW.BERT_AUSD_BP_TA_P_BASISPROD

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_BP_TA_P_BASISPROD`, is responsible for the initial provisioning and preparation of selected "Basisprodukte" (base products) data for the BERT system. The primary business purpose is to generate a snapshot of contract cache from the Data Warehouse (DWH) and make it available for "Forderungsscoring" (demand scoring). The process involves extracting contract-related information, enriching it with MultiSIM details and other options, and then loading it into a target table, `sof$ta_p_basisprod`. This is a daily refresh process, overwriting the target table with the latest snapshot.

The scope of this migration covers the conversion of the UC4 job orchestration, KornShell wrapper scripts, and the core Oracle SQLPlus script to run on the BigQuery platform, orchestrated by Airflow.

## 2. Source Inventory

| File Path | Technology | Tier | Automation Bucket | Summary |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` | UC4/Automic | medium | semi_auto | UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_P_BASISPROD, responsible for preparing instantiated base products by executing a shell script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_p_basisprod.ksh` | KornShell | medium | semi_auto | This KornShell script orchestrates the initial provisioning of selected 'Basisprodukte'. It generates a snapshot of contract cache and makes it available for 'Forderungsscoring' by calling a core script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh` | KornShell | medium | semi_auto | This ksh script acts as a control wrapper for an SQL script, handling parameter parsing, environment setup, date validation, and execution of the main SQL logic. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql` | Oracle SQLPlus | complex | manual | This SQLPlus script orchestrates the population of the SOF$TA_P_BASISPROD table. It first determines a date variable, then calls a stored procedure to truncate the target table, and finally inserts data by joining multiple source tables with extensive column remapping and transformations. |

## 3. Target Architecture
The target platform is Google Cloud Platform (GCP) with BigQuery for data storage and transformation, and Airflow for workflow orchestration.

*   **Orchestration:** Apache Airflow DAG.
*   **Transformation:** PySpark job running on Dataproc for the shell script logic, and BigQuery SQL for the core SQL logic.
*   **Data Storage:** BigQuery tables.

The core data processing logic, currently implemented in Oracle SQL, will be converted to BigQuery SQL. The shell scripts acting as wrappers will be converted into a PySpark application to manage the execution flow, parameter passing, and logging, similar to their current role.

## 4. Data Flow & Lineage

The original job execution flow is as follows:
1.  The UC4 job `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` (a `JOBS_UNIX` object) is triggered (scheduling details are currently unknown, as no `EVNT_TIME` object was provided).
2.  The UC4 job executes the KornShell script `r_ausd_bp_ta_p_basisprod.ksh`.
3.  `r_ausd_bp_ta_p_basisprod.ksh` initializes the environment, parses parameters (like `p_stichtag` and `p_wiederanlaufWert`), handles error trapping, and then invokes `k_ausd_bp_ta_p_basisprod.ksh` with the resolved parameters.
4.  `k_ausd_bp_ta_p_basisprod.ksh` further initializes the environment, performs date checks, and then executes the Oracle SQLPlus script `d_ausd_bp_ta_p_basisprod.sql` using a `starteSQLSkript` function (likely a utility to run SQL*Plus with specific parameters).
5.  `d_ausd_bp_ta_p_basisprod.sql` performs the main data transformation:
    *   Reads `timecreated` from `isbert_schema.dwtk_meldungen` to determine `v_datum`.
    *   Truncates the target table `sof$ta_p_basisprod` using `isbert_schema.dwpa_util_skript.runstatement`.
    *   Inserts data into `sof$ta_p_basisprod` by joining:
        *   `sof$ta_cntrct_dist` (`cn`)
        *   `sof$ta_bcp_iccid` (`bc`)
        *   `sof$ta_bcp_msisdn` (`bcm`)
        *   `sof$ta_cntrct_evn` (`ev`)
        *   `sof$ta_iccid_vertrag` (`icc`)
        *   `sof$ta_rn_vertrag` (`msi`)
        *   `sof$ta_rn_da_vda_tk` (`msd`)
        *   `sof$ta_tarifoption` (`opt`)
        *   `sof$ta_apn_vertrag` (`av`)
    *   The join involves `cntrct_id` across most tables. The query uses Oracle-specific `/*+ APPEND */` and parallel hints (`parallel(cn,4)`), `decode`, `NVL` functions, and left outer joins (`(+)`).
6.  The SQL script commits the changes.
7.  Control returns to `k_ausd_bp_ta_p_basisprod.ksh` and subsequently to `r_ausd_bp_ta_p_basisprod.ksh` for logging and exit.

**Migrated Data Flow (Airflow + BigQuery):**
*   **Airflow DAG:** `dw_bert_ausd_bp_ta_p_basisprod`
*   **Task:** `run_bert_ausd_bp_ta_p_basisprod` (DataprocSubmitJobOperator)
*   **PySpark Script:** `k_ausd_bp_ta_p_basisprod.py` (replacing `r_` and `k_` shell scripts)
    *   This script will handle environment setup, parameter parsing, and logging.
    *   It will construct and execute the BigQuery SQL equivalent of `d_ausd_bp_ta_p_basisprod.sql`.
*   **BigQuery SQL:** Equivalent of `d_ausd_bp_ta_p_basisprod.sql`
    *   Reads from BigQuery tables corresponding to:
        *   `isbert_schema.dwtk_meldungen`
        *   `sof$ta_cntrct_dist`
        *   `sof$ta_bcp_iccid`
        *   `sof$ta_bcp_msisdn`
        *   `sof$ta_cntrct_evn`
        *   `sof$ta_iccid_vertrag`
        *   `sof$ta_rn_vertrag`
        *   `sof$ta_rn_da_vda_tk`
        *   `sof$ta_tarifoption`
        *   `sof$ta_apn_vertrag`
    *   Writes to BigQuery table corresponding to `sof$ta_p_basisprod`.
    *   Table truncate will be replaced by a `TRUNCATE TABLE` or `DELETE FROM` statement in BigQuery or a `WRITE_TRUNCATE` disposition in the BigQuery load job.

## 5. Transformation Logic

The core transformation logic resides within `d_ausd_bp_ta_p_basisprod.sql`. This script performs a full refresh of the target table `sof$ta_p_basisprod` by:
1.  **Determining reference date:** `v_datum` is derived from `MAX(m.timecreated)` in `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This logic needs to be preserved, potentially as a subquery or a preceding step in PySpark/BQSQL.
2.  **Truncation:** The `isbert_schema.dwpa_util_skript.runstatement(0,'TRUNCATE TABLE sof$ta_p_basisprod REUSE STORAGE')` call will be replaced by BigQuery's `TRUNCATE TABLE <dataset>.<target_table>` or by configuring the BigQuery load job with `WRITE_TRUNCATE` disposition if using an `INSERT OVERWRITE` pattern.
3.  **Data Insertion (SELECT statement):** The complex `SELECT` statement joining numerous tables and performing extensive column remapping and transformations needs to be translated directly to BigQuery SQL.
    *   **Join Conditions:** The `(+)` syntax for Oracle outer joins will be converted to explicit `LEFT OUTER JOIN`.
    *   **Functions:** `NVL`, `TO_CHAR`, `decode` will be translated to BigQuery equivalents (`IFNULL` or `COALESCE`, `FORMAT_DATE`, `CASE` statements respectively).
    *   **Parallel/Optimizer Hints:** Oracle-specific hints like `/*+ APPEND */`, `/*+ ORDERED ... parallel(...) NO_SWAP_JOIN_INPUTS(...) FULL(...) */` will be removed as BigQuery's optimizer handles these automatically.
    *   **Column Mappings:** The script maps a large number of source columns to target columns, often with `as` aliases. This mapping structure will be preserved. Columns like `MS1_ICCID` through `MS10_ICCID`, `MS1_E_ID` through `MS10_E_ID`, etc., indicate a highly denormalized structure or extensive multi-value attributes.
    *   **Nested SELECT for `bccm`:** The subquery defining `bccm` will be retained as a CTE (Common Table Expression) for readability and maintainability in BigQuery SQL.
    *   **APN decoding:** The `decode(av.apn, null,av.apn, av.apn||','||av.apn_cntrct)` logic for `apn` requires careful translation. It seems to concatenate `apn` and `apn_cntrct` if `apn` is not null. This would become `CASE WHEN av.apn IS NOT NULL THEN CONCAT(av.apn, ',', av.apn_cntrct) ELSE av.apn END` in BigQuery.

The KornShell scripts (`r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh`) will be rewritten in PySpark to manage the execution flow. This PySpark script will:
*   Handle argument parsing for `Stichtag` and `Wiederanlaufwert`.
*   Connect to BigQuery.
*   Execute the translated BigQuery SQL.
*   Manage logging and error handling, similar to the `DWMSG` functions in the original shell scripts.

## 6. External Dependencies

| Original System / Component | Reference in Source | Proposed Target / Replacement | Notes |
| :-------------------------- | :------------------ | :---------------------------- | :---- |
| **UC4 Scheduler**           | `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml` | Airflow DAG (`dw_bert_ausd_bp_ta_p_basisprod`) | The UC4 job will be replaced by an Airflow DAG. Scheduling frequency needs to be explicitly defined, as no `EVNT_TIME` object was provided in the source. |
| **Oracle Database**         | `isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_dist`, `sof$ta_bcp_iccid`, `sof$ta_bcp_msisdn`, `sof$ta_cntrct_evn`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`, `sof$ta_p_basisprod` | BigQuery Tables (e.g., `isbert_schema.dwtk_meldungen_bq`, `dw_dwh_prod.sof_ta_p_basisprod`) | All Oracle source and target tables will be migrated to BigQuery. Appropriate datasets and table names will be established in BigQuery. |
| **Oracle Stored Procedure** | `isbert_schema.dwpa_util_skript.runstatement` | BigQuery SQL `TRUNCATE TABLE` or `DELETE` statement within the PySpark script/BQSQL. | This utility call is specifically for truncating tables. BigQuery SQL provides direct `TRUNCATE TABLE` functionality. |
| **KornShell Interpreter**   | `r_ausd_bp_ta_p_basisprod.ksh`, `k_ausd_bp_ta_p_basisprod.ksh` | PySpark application executed on Dataproc. | Shell script logic for orchestration, parameter handling, and external utility calls will be re-implemented in Python using PySpark. |
| **`$HOME/.dw_init`**        | Included in both KSH scripts. | Environment variables managed in Airflow/Dataproc or PySpark script. | The initialization logic will be translated to Python environment setup or directly integrated into the PySpark application. |
| **Utility Scripts (KSH)**   | `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh` | Python modules/functions within the PySpark application or standard Python libraries. | These utility functions will be replaced by appropriate Python logic or standard library calls. Error handling and logging will be managed by a Python logging framework. |
| **Temporary Files**         | `tmpFile="$DW_DIR_UTL/bert_k_ausd_bp_ta_p_basisprod.tmp"` | In-memory variables or temporary GCS/BigQuery tables in PySpark. | Management of temporary files will be handled within the PySpark script, leveraging Python's capabilities for temporary data storage or BigQuery temporary tables. |

## 7. Unresolved / Risks

*   **Scheduling and Dependencies:** The UC4 scheduling (`EVNT_TIME`, `JOBP`, `JSCH`) was not available. The Airflow DAG will be initially created with a placeholder schedule (`None`), and proper scheduling (e.g., cron expression) and upstream/downstream dependencies must be identified and configured during the build phase.
*   **Oracle-specific SQL Features:** While most Oracle SQL can be translated to BigQuery SQL, complex PL/SQL constructs, specific Oracle functions, or intricate query hints may require manual review and potentially different BigQuery approaches (e.g., using UDFs or alternative SQL constructs). The `manual` migration bucket for `d_ausd_bp_ta_p_basisprod.sql` flags this as a high-risk area requiring careful attention from SQL experts.
*   **Stored Procedure `isbert_schema.dwpa_util_skript.runstatement`:** This stored procedure's full logic is unknown beyond the `TRUNCATE TABLE` call. If it contains additional complex logic, that logic must be identified and migrated to BigQuery as part of the data ingestion strategy.
*   **Parameter `v_carmen = "@pcrs1"`:** The purpose and usage of this parameter (`v_carmen`) defined in the SQL script are unclear from the provided context. Its relevance in the BigQuery environment needs to be assessed.
*   **Error Handling and Logging:** The shell script's custom error handling (`DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`) and logging (`DWMSG_Logdateiname`) will need to be re-implemented using Airflow's native logging capabilities and Python's logging framework.
*   **MultiSIM and Other Options Logic:** The extensive column set related to MultiSIM (`MS1_ICCID` to `MS10_VALID`) and various options (`DATA_OPTION_REIN`, `VOICE_OPTION_REIN`, etc.) suggests potentially complex business rules that need to be accurately reflected in the BigQuery SQL.
*   **Commented-out `sed`, `sort`, `join`:** The `k_ausd_bp_ta_p_basisprod.ksh` script contains commented-out sections for file-based processing using `sed`, `sort`, and `join`. It's crucial to confirm if this logic is indeed obsolete or if it represents an alternative/dormant processing path that needs to be considered. Assuming it's obsolete for now.

## 8. Build Plan

**Overall Strategy:**
The migration will involve creating an Airflow DAG for orchestration. The shell script logic will be encapsulated in a PySpark job, which will then execute the translated BigQuery SQL.

**Ordered List of Files to Generate:**

1.  **BigQuery SQL Script (`d_ausd_bp_ta_p_basisprod_bq.sql`)**
    *   **Language:** BigQuery SQL
    *   **Description:** Translation of `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_p_basisprod.sql`.
    *   **Key Changes:**
        *   Convert Oracle `(+)` outer joins to explicit `LEFT OUTER JOIN`.
        *   Translate `NVL` to `IFNULL`/`COALESCE`.
        *   Translate `TO_CHAR` to `FORMAT_DATE`.
        *   Translate `decode` to `CASE` statements.
        *   Remove Oracle-specific hints (`/*+ APPEND */`, `PARALLEL`).
        *   Replace `isbert_schema.dwpa_util_skript.runstatement(0,'TRUNCATE TABLE sof$ta_p_basisprod REUSE STORAGE')` with `TRUNCATE TABLE \`<PROJECT_ID>.<DATASET_ID>.sof_ta_p_basisprod\``.
        *   Ensure all source and target table names are fully qualified with BigQuery project and dataset IDs (e.g., `\`<PROJECT_ID>.<DATASET_ID>.isbert_dwtk_meldungen\``).
        *   Address `v_carmen` definition if it impacts the SQL.

2.  **PySpark Application (`k_ausd_bp_ta_p_basisprod.py`)**
    *   **Language:** Python (PySpark)
    *   **Description:** Re-implementation of the logic from `r_ausd_bp_ta_p_basisprod.ksh` and `k_ausd_bp_ta_p_basisprod.ksh`.
    *   **Key Functionality:**
        *   Parse command-line arguments for `stichtag` (key date) and `wiederanlaufwert` (restart value).
        *   Determine `sysdate` (current date) in Python.
        *   Load the BigQuery SQL from `d_ausd_bp_ta_p_basisprod_bq.sql`.
        *   Execute the BigQuery SQL, passing necessary parameters (like `stichtag`).
        *   Implement robust logging (e.g., using Python's `logging` module) to replace `DWMSG` functions.
        *   Integrate error handling and exit codes.
        *   Handle environment variable setup (e.g., `BERT_DIR_ROOT`) through Airflow variables or Dataproc cluster initialization.

3.  **Airflow DAG (`dw_bert_ausd_bp_ta_p_basisprod_dag.py`)**
    *   **Language:** Python
    *   **Description:** Orchestrates the PySpark job.
    *   **Key Components:**
        *   `dag_id`: `dw_bert_ausd_bp_ta_p_basisprod`
        *   `schedule`: To be defined based on business requirements (currently `None`).
        *   `start_date`: Set appropriately.
        *   `DataprocSubmitJobOperator`: To submit the `k_ausd_bp_ta_p_basisprod.py` PySpark job to a Dataproc cluster.
            *   Configure `main_python_file_uri` to point to the PySpark script in GCS (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/k_ausd_bp_ta_p_basisprod.py`).
            *   Pass `stichtag` and `wiederanlaufwert` as arguments to the PySpark job, potentially using Airflow `{{ ds }}` for `stichtag`.
        *   Error handling and retries as per Airflow best practices (default retries 0, no retry delay for this initial design).

**Deployment Strategy:**
1.  Upload `d_ausd_bp_ta_p_basisprod_bq.sql` (or embed it in the PySpark script) and `k_ausd_bp_ta_p_basisprod.py` to a GCS bucket (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/`).
2.  Deploy `dw_bert_ausd_bp_ta_p_basisprod_dag.py` to the Airflow environment.
3.  Ensure Dataproc cluster is provisioned and configured for PySpark job execution.
4.  BigQuery target tables (`sof_ta_p_basisprod`) and source tables are created and populated.