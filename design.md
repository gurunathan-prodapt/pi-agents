# Migration Design — DW.BERT_AUSD_BP_TA_BCP_ICCID

## 1. Purpose & Scope

This document outlines the migration design for the `DW.BERT_AUSD_BP_TA_BCP_ICCID` job to Google Cloud Platform, specifically BigQuery.

The original job's purpose, as indicated by its title and script summaries, is to prepare selected "Basisprodukte" (basic products) for BERT's demand scoring system. It involves extracting and enriching data related to contracts and ICCIDs (Integrated Circuit Card ID) into a target table.

The job workflow originates from a UC4 job definition, which orchestrates KornShell scripts. These scripts manage parameters, environment setup, and execute a core Oracle SQL*Plus script. The SQL script performs a truncate-and-load operation on the `sof$ta_bcp_iccid` table, populating it with data derived from a join of `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` tables, and utilizes a date from `isbert_schema.dwtk_meldungen`.

The scope of this migration includes translating the UC4 orchestration, KornShell scripting logic, and the Oracle SQL transformation logic to equivalent BigQuery compatible components. The migration bucket for the core SQL transformation is `retire`, which suggests a strong consideration for simplifying or redesigning the logic, or even deprecating it if its business value is low.

## 2. Source Inventory

The job `DW.BERT_AUSD_BP_TA_BCP_ICCID` consists of the following key components:

| File Path                                                                                                                          | Technology    | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                        |
| :--------------------------------------------------------------------------------------------------------------------------------- | :------------ | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_ICCID.xml` | UC4/Automic   | medium | semi_auto         | UC4 job definition for a UNIX job that orchestrates the preparation of instantiated base products by executing a ksh script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh`                                            | KornShell     | medium | semi_auto         | Orchestration layer for preparing 'Basisprodukte' for BERT. Handles snapshot date and restart value parameters, sets up the environment, and calls a core processing script.                                                                                                                                                                                          |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`                                            | KornShell     | medium | semi_auto         | Control script handling parameter parsing, date validation, and orchestrating the execution of a core SQL script.                                                                                                                                                                                                                                              |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`                                            | Oracle PL/SQL | medium | retire            | SQL*Plus script that truncates the `SOF$TA_BCP_ICCID` table and loads it with enriched data by joining `SOF$TA_BPR_BCP` and `SOF$TA_ICCID_VERTRAG` tables. It also defines a date variable from `DWTK_MELDUNGEN`.                                                                                                                                       |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services:

*   **Orchestration**: The UC4 job and KornShell scripts will be migrated to an **Airflow DAG (Composer)**. This DAG will manage the overall workflow, parameter passing, and execution of the data transformation.
*   **Data Storage & Transformation**: The Oracle tables will be migrated to **BigQuery**. The core SQL logic will be translated into standard BigQuery SQL. Given the `retire` migration bucket for the SQL, the opportunity to simplify, optimize, or even eliminate this transformation if no longer needed by the business should be evaluated.
*   **Logging & Monitoring**: Google Cloud's native logging (Cloud Logging) and monitoring (Cloud Monitoring) will be used.
*   **External Data Sources**: Any external systems currently accessed by the Oracle database or scripts will need to be re-evaluated for BigQuery integration (e.g., through BigQuery External Tables, Cloud Storage, or direct connectors).

## 4. Data Flow & Lineage

The legacy data flow is sequential:

1.  **UC4 Job `DW.BERT_AUSD_BP_TA_BCP_ICCID`**: This is the top-level scheduler, triggering the execution.
2.  **KornShell Wrapper Script `r_ausd_bp_ta_bcp_iccid.ksh`**: Invoked by the UC4 job. It performs initial parameter parsing (Stichtag, Wiederanlaufwert) and environmental setup. It then calls the core KornShell script.
3.  **KornShell Control Script `k_ausd_bp_ta_bcp_iccid.ksh`**: Invoked by the wrapper script. It performs further parameter validation and environment setup, including sourcing SQL*Plus utility functions. It then executes the core SQL script using a `starteSQLSkript` function (implying SQL*Plus execution).
4.  **Oracle SQL Script `d_ausd_bp_ta_bcp_iccid.sql`**: Executed by the control script.
    *   **Source Data**:
        *   `isbert_schema.dwtk_meldungen`: Reads `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to determine a date variable (`v_datum`).
        *   `sof$ta_bpr_bcp` (aliased as `bp`): Provides core "Basisprodukt" data.
        *   `sof$ta_iccid_vertrag` (aliased as `ic`): Provides ICCID contract data.
    *   **Transformation**: Truncates the target table `sof$ta_bcp_iccid`. Inserts distinct records by joining `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` on `cntrct_id_ref`. Selects `CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR`.
    *   **Target Data**: `sof$ta_bcp_iccid` (Truncated and loaded).

**Target Data Flow:**

1.  **Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid`**: Replaces the UC4 job.
    *   **Task 1: Parameter & Environment Setup**: A Python operator in the DAG will manage date and restart value parameters, similar to the `r_ausd_bp_ta_bcp_iccid.ksh` script. It will set XComs for these values to be used by downstream tasks.
    *   **Task 2: Get Date Variable**: A BigQuery operator or Python BigQuery client will query the `dwtk_meldungen` equivalent in BigQuery to retrieve the `v_datum`.
    *   **Task 3: BigQuery Transformation**: A BigQuery operator will execute the translated BigQuery SQL.
        *   **Source BigQuery Tables**:
            *   `<project>.<dataset>.dwtk_meldungen`
            *   `<project>.<dataset>.sof_ta_bpr_bcp`
            *   `<project>.<dataset>.sof_ta_iccid_vertrag`
        *   **Transformation**:
            *   Equivalent of `TRUNCATE TABLE sof$ta_bcp_iccid` will be `TRUNCATE TABLE <project>.<dataset>.sof_ta_bcp_iccid`.
            *   The `INSERT INTO ... SELECT DISTINCT` statement will be translated to BigQuery SQL, possibly leveraging CTAS (Create Table As Select) or INSERT OVERWRITE if the target table is partitioned/clustered.
        *   **Target BigQuery Table**: `<project>.<dataset>.sof_ta_bcp_iccid`

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_bp_ta_bcp_iccid.sql`.

**Legacy Oracle SQL:**

```sql
-- Get v_datum
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate target table
begin
 isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE');
end;
/

-- Insert data
INSERT INTO sof$ta_bcp_iccid
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_ICCID,
  TN_IMSI_HLR)
SELECT /*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */
          distinct
          bp.cntrct_id,
          bp.bpr_id,
          bp.cntrct_id_ref,
          ic.tn_iccid,
          ic.tn_imsi_hlr
FROM      sof$ta_bpr_bcp bp,
          sof$ta_iccid_vertrag ic
WHERE     bp.cntrct_id_ref = ic.cntrct_id
;
COMMIT;
```

**Proposed BigQuery SQL:**

```sql
-- Step 1: Retrieve the max timecreated for v_datum.
-- This can be done in a separate Airflow task or within the SQL if needed for filtering.
-- For direct SQL translation, assuming v_datum is passed as a parameter or derived.
-- Example if v_datum is needed for filtering in the main query:
DECLARE v_datum STRING;
SET v_datum = (SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
               FROM `<project>.<dataset>.dwtk_meldungen`
               WHERE job_kennung = 'BERT_DROP_TEMP_TABLE');

-- Step 2: Truncate and Load (or INSERT OVERWRITE)
-- Option A: TRUNCATE TABLE and INSERT (Requires separate DDL for TRUNCATE)
TRUNCATE TABLE `<project>.<dataset>.sof_ta_bcp_iccid`;

INSERT INTO `<project>.<dataset>.sof_ta_bcp_iccid`
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_ICCID,
  TN_IMSI_HLR)
SELECT DISTINCT
    bp.cntrct_id,
    bp.bpr_id,
    bp.cntrct_id_ref,
    ic.tn_iccid,
    ic.tn_imsi_hlr
FROM `<project>.<dataset>.sof_ta_bpr_bcp` AS bp
JOIN `<project>.<dataset>.sof_ta_iccid_vertrag` AS ic
  ON bp.cntrct_id_ref = ic.cntrct_id;

-- Option B: INSERT OVERWRITE (if the target table is not a partitioned table requiring specific partition overwrite)
-- This is often preferred in BigQuery for full table refreshes.
/*
INSERT OVERWRITE `<project>.<dataset>.sof_ta_bcp_iccid`
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_ICCID,
  TN_IMSI_HLR)
SELECT DISTINCT
    bp.cntrct_id,
    bp.bpr_id,
    bp.cntrct_id_ref,
    ic.tn_iccid,
    ic.tn_imsi_hlr
FROM `<project>.<dataset>.sof_ta_bpr_bcp` AS bp
JOIN `<project>.<dataset>.sof_ta_iccid_vertrag` AS ic
  ON bp.cntrct_id_ref = ic.cntrct_id;
*/
```

**Notes on Transformation:**
*   Oracle hints `/*+ full(bp) parallel(bp,4) ... */` will be removed as BigQuery's execution engine automatically handles parallelism.
*   The `NVL` function translates to `IFNULL` in BigQuery.
*   `TO_CHAR(..., 'YYYYMMDD')` translates to `FORMAT_DATE('%Y%m%d', ...)` in BigQuery.
*   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call for truncation will be replaced by a direct `TRUNCATE TABLE` statement in BigQuery or an `INSERT OVERWRITE` operation.
*   The `COMMIT` statement is not needed in BigQuery as it operates on an eventual consistency model and transactions are typically handled implicitly per statement or within scripting.

## 6. External Dependencies

| Legacy External System | Type     | Notes                                                    | Migration Strategy                                                                                                           |
| :--------------------- | :------- | :------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------- |
| `DWHDWH2P`             | Host     | Referenced in UC4 for job execution.                   | No direct migration needed. Airflow will run within GCP Composer.                                                            |
| `DW.UNIX.ISBERT`       | Login    | UNIX login for executing scripts.                      | Replaced by service account credentials within Airflow/GCP for BigQuery access.                                              |
| Oracle Database        | Database | Source of `dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and target `sof$ta_bcp_iccid`. | All relevant tables will be migrated to BigQuery. Initial data load (backfill) will be required. Incremental updates via CDC or similar methods will be evaluated if necessary. |
| Utility Scripts        | Files    | `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` | These utility functions (environment setup, error handling, date formatting, parameter parsing) will need to be re-implemented in Python as part of the Airflow DAG or as custom operators. |

## 7. Unresolved / Risks

*   **`retire` Migration Bucket for SQL**: The `retire` bucket for the SQL script (`d_ausd_bp_ta_bcp_iccid.sql`) is a significant point. A thorough business analysis is required to confirm if this job can be truly retired or if its functionality needs to be absorbed into another process or drastically simplified. If retirement is confirmed, the migration effort will be minimal. If not, the transformation needs to be implemented. For this design, we assume it will be migrated but with the strong recommendation to re-evaluate.
*   **Parameter `p_wiederanlaufWert` (Restart Value)**: The `r_ausd_bp_ta_bcp_iccid.ksh` script uses a restart value (`-l`). This functionality needs to be carefully replicated in Airflow, potentially using a combination of Airflow XComs and idempotent BigQuery operations (e.g., filtering on an equivalent of `DWH_VERTRAG_ID > Wiederanlaufwert`).
*   **Error Handling and Logging**: The legacy scripts use custom error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`). This will be replaced by Airflow's native error handling, alerting, and Cloud Logging integration.
*   **`starteSQLSkript` function**: The exact implementation of `starteSQLSkript` from `h_alis_sqlplus.ksh` is unknown but likely involves executing SQL*Plus with the provided arguments. The Airflow migration will replace this with a BigQuery operator directly executing the SQL.
*   **Data Volume for Distinct**: The `SELECT DISTINCT` clause in the SQL might process large volumes of data. Performance of this in BigQuery should be monitored and optimized if necessary (e.g., using partitioning or clustering on the target table).

## 8. Build Plan

The migration will be implemented as an Airflow DAG orchestrating BigQuery SQL.

1.  **BigQuery Table DDL Generation**:
    *   **Source Tables**: Ensure DDLs are generated for `dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag` in BigQuery, reflecting the Oracle schema (data types, nullability).
    *   **Target Table**: Generate DDL for `sof_ta_bcp_iccid` in BigQuery. Consider partitioning and clustering strategies for performance, especially for the `CNTRCT_ID_REF` column used in the join.

2.  **Airflow DAG Development (Python)**:
    *   **DAG Definition**: Create a Python file (`dw_bert_ausd_bp_ta_bcp_iccid_dag.py`) for the Airflow DAG.
    *   **Parameters**: Implement a mechanism to accept run-time parameters (`stichtag`, `wiederanlaufwert`) for the DAG, similar to the legacy ksh script arguments.
    *   **Task 1: Retrieve `v_datum`**:
        *   Language: Python (BigQuery client) or BigQuery SQL Operator.
        *   Code: SQL query to get `MAX(timecreated)` from `dwtk_meldungen`. Store the result in an XCom variable.
    *   **Task 2: Truncate/Load Transformation**:
        *   Language: BigQuery SQL Operator.
        *   Code: The translated BigQuery SQL (see Section 5), either using `TRUNCATE TABLE` followed by `INSERT INTO` or `INSERT OVERWRITE`.
        *   The task should use the `v_datum` obtained from Task 1 if needed in the WHERE clause or other logic.
        *   Parameters like `p_wiederanlaufWert` will be directly used in the BigQuery SQL to filter records.

3.  **Utility Function Replacement**:
    *   Analyze the required functionality of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` and re-implement necessary logic directly within the Airflow DAG's Python tasks or as Python utility functions. Error logging should conform to GCP standards (Cloud Logging).

4.  **Testing**:
    *   **Unit Tests**: For Python components of the DAG.
    *   **Integration Tests**: Test the full DAG execution with sample data in BigQuery, verifying data accuracy and adherence to business logic.
    *   **Performance Testing**: Ensure the BigQuery transformations meet performance SLAs.

5.  **Deployment**:
    *   Deploy the Airflow DAG to a Cloud Composer environment.
    *   Configure necessary service accounts and permissions for BigQuery access.