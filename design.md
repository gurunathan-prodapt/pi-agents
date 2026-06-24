# Migration Design — DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_BP_TA_BCP_ICCID`, is responsible for preparing and extracting instantiated basic products related to ICCID (Integrated Circuit Card IDentifier) data for the BERT (Business Event Response Tracker) demand scoring system. The process involves orchestration, parameter handling, date validation, and finally, data transformation and loading into a target table. Specifically, it populates the `SOF$TA_BCP_ICCID` table with enriched data by joining `SOF$TA_BPR_BCP` and `SOF$TA_ICCID_VERTRAG` tables, considering date variables from `DWTK_MELDUNGEN`.

## 2. Source Inventory

| File Path                                                                                                   | Technology         | Tier   | Automation Bucket | Summary                                                                                                                                                                                                            |
| :---------------------------------------------------------------------------------------------------------- | :----------------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml` | UC4/Automic        | medium | semi_auto         | UC4 job definition for a UNIX job named DW.BERT_AUSD_BP_TA_BCP_ICCID, which orchestrates the preparation of instantiated base products by executing a ksh script.                                                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`                       | KornShell          | medium | semi_auto         | This KornShell script acts as a control script, handling parameter parsing, date validation, and orchestrating the execution of a core SQL script.                                                                     |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`                       | KornShell          | medium | semi_auto         | This ksh script serves as an orchestration layer to prepare selected 'Basisprodukte' (basic products) for BERT's demand scoring system. It handles snapshot date and restart value parameters, sets up the environment, and calls a core processing script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`                       | Oracle PL/SQL (SQL*Plus) | (null) | retire            | This SQL*Plus script truncates the SOF$TA_BCP_ICCID table and then loads it with enriched data by joining SOF$TA_BPR_BCP and SOF$TA_ICCID_VERTRAG tables. It also defines a date variable from DWTK_MELDUNGEN.            |

## 3. Target Architecture
The target platform is Google BigQuery.
-   **Orchestration:** The UC4 job will be migrated to Google Cloud Composer (Apache Airflow).
-   **Shell Scripts:** The KornShell scripts will be translated into Python scripts, leveraging Google Cloud services for environmental variables, logging, and potentially Cloud Functions for smaller, triggered tasks if applicable.
-   **SQL/PLSQL:** The Oracle SQL/PLSQL logic will be converted to BigQuery SQL. The current `TRUNCATE` and `INSERT` pattern will be replaced with standard BigQuery DML operations (e.g., `TRUNCATE TABLE` or `DELETE` followed by `INSERT INTO` or `MERGE` statements).

**BigQuery Datasets and Tables:**
-   **`DWTK_MELDUNGEN`**: This metadata table from Oracle (`isbert_schema.dwtk_meldungen`) will be migrated to a BigQuery table, e.g., `isbert_schema.dwtk_meldungen_bq`.
-   **`SOF$TA_BPR_BCP`**: This source table will be migrated to BigQuery, e.g., `sof_schema.ta_bpr_bcp_bq`.
-   **`SOF$TA_ICCID_VERTRAG`**: This source table will be migrated to BigQuery, e.g., `sof_schema.ta_iccid_vertrag_bq`.
-   **`SOF$TA_BCP_ICCID`**: This target table will be migrated to BigQuery, e.g., `sof_schema.ta_bcp_iccid_bq`.

## 4. Data Flow & Lineage

The original job execution flow is as follows:
1.  The UC4 job (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) acts as the top-level scheduler, initiating the process.
2.  The UC4 job invokes `r_ausd_bp_ta_bcp_iccid.ksh`.
3.  `r_ausd_bp_ta_bcp_iccid.ksh` is an orchestration layer that sets up parameters and the environment, then invokes `k_ausd_bp_ta_bcp_iccid.ksh`.
4.  `k_ausd_bp_ta_bcp_iccid.ksh` is a control script that handles parameter parsing, date validation, and executes the core SQL script `d_ausd_bp_ta_bcp_iccid.sql`.
5.  `d_ausd_bp_ta_bcp_iccid.sql` performs the primary data transformation:
    *   Reads `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen` where `m.job_kennung = 'BERT_DROP_TEMP_TABLE'` to define `v_datum`.
    *   Truncates `sof$ta_bcp_iccid`.
    *   Inserts into `sof$ta_bcp_iccid` by joining `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` on `bp.cntrct_id_ref = ic.cntrct_id`.

**Target Data Flow:**
1.  **Airflow DAG:** A new Airflow DAG will replace the UC4 job.
2.  **Python Operators:** Python operators within the Airflow DAG will replace the KornShell scripts (`r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh`). These Python scripts will handle:
    *   Parameter parsing (e.g., snapshot date, restart value).
    *   Date validation.
    *   Environment setup.
    *   Invoking the BigQuery SQL transformation.
3.  **BigQueryOperator:** A BigQueryOperator will execute the migrated SQL logic. This SQL will:
    *   Retrieve the relevant date variable from `isbert_schema.dwtk_meldungen_bq`.
    *   Perform a `TRUNCATE` or `DELETE` on `sof_schema.ta_bcp_iccid_bq`.
    *   Execute an `INSERT INTO` or `MERGE` statement joining `sof_schema.ta_bpr_bcp_bq` and `sof_schema.ta_iccid_vertrag_bq` to populate `sof_schema.ta_bcp_iccid_bq`.

## 5. Transformation Logic

**UC4 XML (`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml`) to Airflow DAG:**
-   The scheduling and dependency management logic from the UC4 XML will be translated into an Airflow DAG definition, written in Python.
-   The UNIX `Login` credential `DW.UNIX.ISBERT` and `HostDst` `DWHDWH2P` will be managed by Airflow connections and operators that execute tasks on appropriate compute resources.
-   The script invocation `. $HOME/.dw_init` and `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh` will be replaced by a PythonOperator calling the migrated `r_ausd_bp_ta_bcp_iccid.py` script.

**KornShell Scripts (`r_ausd_bp_ta_bcp_iccid.ksh` and `k_ausd_bp_ta_bcp_iccid.ksh`) to Python:**
-   **Environment Setup:** The `. $HOME/.dw_init` and sourcing of other utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) will be reimplemented in Python using environment variables, configuration files, or direct Python library calls.
-   **Parameter Parsing:** The `getopts` logic for `s` (Stichtag/snapshot date) and `l` (Wiederanlaufwert/restart value) will be translated to Python's `argparse` module or passed directly as Airflow parameters.
-   **Date Validation:** `DWDate_Datum_Check` will be replaced by Python's `datetime` module.
-   **SQL Script Execution:** The `starteSQLSkript` function will be replaced by BigQuery client calls within Python, executing the migrated BigQuery SQL.
-   **Logging and Error Handling:** The `DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` calls will be replaced by standard Python logging, integrated with Google Cloud Logging.
-   The currently commented-out `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, and `FOSHoleLadedatum` suggest a job management system. If these functionalities are still required, they will need to be re-evaluated and migrated or replaced with BigQuery-native metadata management or a separate Google Cloud service.

**Oracle PL/SQL (`d_ausd_bp_ta_bcp_iccid.sql`) to BigQuery SQL:**
-   **Variable Definition:** `DEFINE v_carmen = "@pcrs1"` and `COLUMN s_datum new_value v_datum noprint SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
    *   The `v_carmen` definition might be a connection string or alias; this will be managed by Airflow connections or direct BigQuery table references.
    *   The `v_datum` derivation will be translated to a `SELECT` statement in BigQuery that can be executed prior to the main `INSERT`, or incorporated into the main `INSERT` using a subquery.
    *   BigQuery equivalent: `SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM isbert_schema.dwtk_meldungen_bq AS m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'`.
-   **Temporary Table Deletion/Truncation:** `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE');`
    *   This will be a direct BigQuery `TRUNCATE TABLE sof_schema.ta_bcp_iccid_bq;` statement.
-   **Data Enrichment and Loading:** `INSERT INTO sof$ta_bcp_iccid (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR) SELECT /*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */ distinct bp.cntrct_id, bp.bpr_id, bp.cntrct_id_ref, ic.tn_iccid, ic.tn_imsi_hlr FROM sof$ta_bpr_bcp bp, sof$ta_iccid_vertrag ic WHERE bp.cntrct_id_ref = ic.cntrct_id;`
    *   This will be a BigQuery `INSERT INTO` statement.
    *   `/*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */` are Oracle-specific hints and will be removed as BigQuery's query optimizer handles parallelism automatically.
    *   BigQuery equivalent: `INSERT INTO sof_schema.ta_bcp_iccid_bq (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR) SELECT DISTINCT bp.cntrct_id, bp.bpr_id, bp.cntrct_id_ref, ic.tn_iccid, ic.tn_imsi_hlr FROM sof_schema.ta_bpr_bcp_bq AS bp JOIN sof_schema.ta_iccid_vertrag_bq AS ic ON bp.cntrct_id_ref = ic.cntrct_id;`
-   **`COMMIT;`**: BigQuery operates on an auto-commit model, so explicit `COMMIT` statements are not needed.

## 6. External Dependencies
The current external dependencies are Oracle database tables.
-   **Oracle Database:** All referenced Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, `sof$ta_bcp_iccid`) will be migrated to BigQuery tables. The migration strategy for these tables (e.g., one-time load, CDC, batch replication) will be defined as part of the overall data migration plan.
-   **Filesystem (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/...`):** These file-based dependencies will be replaced by:
    *   Google Cloud Storage for any configuration files or lookup data.
    *   Python modules or functions within the migrated Python scripts for utility logic.
    *   Airflow variables or connections for sensitive information or common parameters.

## 7. Unresolved / Risks
-   **`d_ausd_bp_ta_bcp_iccid.sql` in `retire` bucket:** The most significant unresolved item is that the core SQL script, `d_ausd_bp_ta_bcp_iccid.sql`, is flagged with a `retire` migration bucket. This implies a recommendation to decommission or significantly redesign this component rather than a direct migration. A decision needs to be made whether to truly retire this functionality or if a redesign is required to meet current business needs. If retired, the downstream impacts must be understood. If a redesign is chosen, the transformation logic described above would serve as a starting point, but a more in-depth analysis and new design would be necessary. For the purpose of this document, we assume a redesign/reimplementation to BigQuery will occur if the functionality is still needed.
-   **Missing `file_purpose` for most files:** While the summaries provide context, having explicit `file_purpose` values would offer clearer insights into the intended role of each script (e.g., `etl`, `utility`, `orchestrator`). This information could help refine the target component mapping.
-   **Commented-out FOS Job Management:** The KornShell scripts contain commented-out lines referring to `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, and `FOSHoleLadedatum`. These suggest interaction with a job management or metadata system. It's unclear if this functionality is still active or needed. If required, its migration or replacement in BigQuery needs to be addressed.
-   **SQL*Plus Specifics:** Some `SQL*Plus` specific commands (e.g., `prompt`, `start`, `spool`, `whenever sqlerror continue/exit failure`) will need to be removed or adapted. Error handling will be managed at the Python script and Airflow operator level.

## 8. Build Plan

The migration will involve building the following components in Python and BigQuery SQL:

1.  **Migrate Oracle Tables to BigQuery (Data Migration):**
    *   `isbert_schema.dwtk_meldungen` -> `isbert_schema.dwtk_meldungen_bq`
    *   `sof$ta_bpr_bcp` -> `sof_schema.ta_bpr_bcp_bq`
    *   `sof$ta_iccid_vertrag` -> `sof_schema.ta_iccid_vertrag_bq`
    *   `sof$ta_bcp_iccid` -> `sof_schema.ta_bcp_iccid_bq`
    *   *(Language: Data Migration Tools, BigQuery DDL)*

2.  **`d_ausd_bp_ta_bcp_iccid.sql` to BigQuery SQL:**
    *   Create `d_ausd_bp_ta_bcp_iccid_bq.sql` containing the BigQuery-compliant SQL for truncating and inserting into `sof_schema.ta_bcp_iccid_bq`.
    *   *(Language: BigQuery SQL)*

3.  **`k_ausd_bp_ta_bcp_iccid.ksh` to Python:**
    *   Create `k_ausd_bp_ta_bcp_iccid.py` to handle parameter parsing, date validation, and execution of `d_ausd_bp_ta_bcp_iccid_bq.sql` via BigQuery client.
    *   Replace `.dw_init` and other shell script inclusions with Python equivalents or Airflow configurations.
    *   Implement Python logging for progress and error reporting.
    *   *(Language: Python)*

4.  **`r_ausd_bp_ta_bcp_iccid.ksh` to Python:**
    *   Create `r_ausd_bp_ta_bcp_iccid.py` as the main orchestration script.
    *   Translate `getopts` for snapshot date and restart value.
    *   Implement logic for date determination if `p_stichtag` is not set.
    *   Invoke `k_ausd_bp_ta_bcp_iccid.py`.
    *   Integrate with Google Cloud Logging for error handling and status updates.
    *   *(Language: Python)*

5.  **`DW.BERT_AUSD_BP_TA_BCP_ICCID.xml` to Airflow DAG:**
    *   Create `dw_bert_ausd_bp_ta_bcp_iccid_dag.py` to define the Airflow DAG.
    *   Define tasks:
        *   Task for executing `r_ausd_bp_ta_bcp_iccid.py` (e.g., using `PythonOperator` or `BashOperator` if `gcloud` command is used to run Python script).
        *   Dependencies between tasks as per the original UC4 flow.
    *   Configure Airflow connections for BigQuery access.
    *   *(Language: Python (Airflow DAG))*