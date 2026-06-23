# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

## 1. Purpose & Scope
This job, "Bereitstellung Basisprodukte BERT", is responsible for the initial provisioning and ongoing extraction of selected base product contract cache data from the Data Warehouse (DWH). It prepares this data for "Forderungsscoring" (demand scoring). The script handles date-based extractions, supports a restart mechanism (`Wiederanlaufwert`), and manages job logging and error handling. The core functionality involves selecting and inserting data into a target table, `sof$ta_bcp_msisdn`.

## 2. Source Inventory
The job is composed of three interconnected KornShell scripts, with the core data manipulation in an Oracle SQL script.

| File Path                                                                   | Technology   | Purpose           | Migration Bucket |
| :-------------------------------------------------------------------------- | :----------- | :---------------- | :--------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh` | KornShell    | Orchestrator      | semi_auto        |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh` | KornShell    | Executor / Wrapper| -                |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql` | Oracle SQL   | Core Logic        | -                |

**Sourced Helper Scripts (implicitly part of the inventory for functional completeness):**
*   `$HOME/.dw_init` (Environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus execution utility)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` (Date utility for yesterday/today)

## 3. Target Architecture
The migrated solution will reside on Google Cloud Platform, leveraging BigQuery for data storage and transformation, and Airflow for orchestration.

*   **Orchestration:** The `r_ausd_bp_ta_bcp_msisdn.ksh` script and its direct invocation of `k_ausd_bp_ta_bcp_msisdn.ksh` will be replaced by an Airflow DAG written in Python. This DAG will handle parameter parsing, date logic, and coordinate the execution of BigQuery operations.
*   **Data Storage:** All source and target Oracle tables will be migrated to BigQuery datasets and tables.
    *   `isbert_schema.dwtk_meldungen` → `isbert_schema.dwtk_meldungen_bq`
    *   `sof$ta_bpr_bcp` → `sof_schema.ta_bpr_bcp_bq`
    *   `sof$ta_rn_vertrag` → `sof_schema.ta_rn_vertrag_bq`
    *   `sof$ta_bcp_msisdn` (Target) → `sof_schema.ta_bcp_msisdn_bq`
*   **Transformation Logic:** The SQL code within `d_ausd_bp_ta_bcp_msisdn.sql` will be converted to BigQuery Standard SQL and executed as a BigQuery operator within the Airflow DAG.
*   **Logging and Error Handling:** Legacy `DWMSG_*` functions will be replaced by Airflow's native logging capabilities, and standard Python error handling for robust operation.

## 4. Data Flow & Lineage

The data flow can be summarized as:

1.  **Trigger:** The Airflow DAG (migrated from `r_ausd_bp_ta_bcp_msisdn.ksh`) is executed, potentially with parameters for `stichtag` (cutoff date) and `wiederanlaufwert` (restart value).
2.  **Date and Parameter Handling:** The Airflow DAG will manage the date calculations and parameter validation, mirroring the logic in `r_ausd_bp_ta_bcp_msisdn.ksh` and `k_ausd_bp_ta_bcp_msisdn.ksh`.
3.  **Metadata Retrieval:** A BigQuery SQL task in the DAG queries `isbert_schema.dwtk_meldungen_bq` to determine the `s_datum` value for job control, replacing the `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'` statement.
4.  **Target Table Truncation:** A BigQuery operator executes a `TRUNCATE TABLE` command on `sof_schema.ta_bcp_msisdn_bq`, replacing the `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call.
5.  **Core Data Transformation:** A BigQuery operator executes the main `INSERT INTO ... SELECT ...` statement, joining `sof_schema.ta_bpr_bcp_bq` and `sof_schema.ta_rn_vertrag_bq`, and inserting the results into `sof_schema.ta_bcp_msisdn_bq`.
6.  **Completion:** The Airflow DAG logs successful completion or errors.

**Lineage from source scripts (as inferred from code):**

*   `r_ausd_bp_ta_bcp_msisdn.ksh` INVOKES `k_ausd_bp_ta_bcp_msisdn.ksh`
*   `k_ausd_bp_ta_bcp_msisdn.ksh` INVOKES `d_ausd_bp_ta_bcp_msisdn.sql`
*   `d_ausd_bp_ta_bcp_msisdn.sql` READS `isbert_schema.dwtk_meldungen`
*   `d_ausd_bp_ta_bcp_msisdn.sql` READS `sof$ta_bpr_bcp`
*   `d_ausd_bp_ta_bcp_msisdn.sql` READS `sof$ta_rn_vertrag`
*   `d_ausd_bp_ta_bcp_msisdn.sql` WRITES `sof$ta_bcp_msisdn`
*   `d_ausd_bp_ta_bcp_msisdn.sql` CALLS Stored Procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`

## 5. Transformation Logic

The core data transformation is an `INSERT ... SELECT` statement within `d_ausd_bp_ta_bcp_msisdn.sql`:

```sql
INSERT INTO sof$ta_bcp_msisdn
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_TEL_MSISDN )
SELECT
          distinct
          bp.cntrct_id,
          bp.bpr_id,
          bp.cntrct_id_ref,
          rn.tn_tel_msisdn
FROM      sof$ta_bpr_bcp  bp,
          sof$ta_rn_vertrag  rn
WHERE     bp.cntrct_id_ref = rn.cntrct_id;
```

This logic will be directly translated to BigQuery Standard SQL:

```sql
INSERT INTO `project_id.sof_schema.ta_bcp_msisdn_bq`
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_TEL_MSISDN )
SELECT
          DISTINCT
          bp.CNTRCT_ID,
          bp.BPR_ID,
          bp.CNTRCT_ID_REF,
          rn.TN_TEL_MSISDN
FROM      `project_id.sof_schema.ta_bpr_bcp_bq`  AS bp
INNER JOIN `project_id.sof_schema.ta_rn_vertrag_bq`  AS rn
ON     bp.CNTRCT_ID_REF = rn.CNTRCT_ID;
```

**Pre-transformation steps:**
*   Retrieve `s_datum` from `isbert_schema.dwtk_meldungen_bq`.
*   Truncate `sof_schema.ta_bcp_msisdn_bq`.

**Post-transformation steps:**
*   (Original commented-out `sed`, `sort`, `join` operations are ignored for now, assuming they are not active or required).
*   Log record count (currently from a temporary file in source, will need to be adapted for BigQuery, e.g., using `num_affected_rows` or a `COUNT(*)` in a separate task).

## 6. External Dependencies

| Original System / Component             | Description                                     | Replacement in GCP / BigQuery                                    |
| :-------------------------------------- | :---------------------------------------------- | :--------------------------------------------------------------- |
| Oracle Database                         | Hosts `isbert_schema`, `sof$ta` tables          | Migrated to BigQuery datasets and tables (`project_id.isbert_schema.*`, `project_id.sof_schema.*`) |
| `$HOME/.dw_init`                        | Environment initialization                      | Airflow DAG configuration, environment variables, or Python code |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/*` | Helper scripts for error handling, date, params, SQL*Plus | Python functions within Airflow DAG, or dedicated Python utility modules |
| `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` | Date utility                                    | Python date/time functions within Airflow DAG                |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` | Oracle stored procedure for DDL commands        | Direct BigQuery SQL DDL commands (e.g., `TRUNCATE TABLE`)      |
| `v_carmen = "@pcrs1"`                   | Oracle database link / service name             | Needs clarification. If it points to another Oracle database that provides source data, that source data needs to be identified and migrated to BigQuery as well. If it's internal to the current system, it will be abstracted away by the BigQuery data model. This is a potential risk/unresolved item if the connection implies a separate external system not covered by current lineage. |

## 7. Unresolved / Risks

*   **Kernel Script `k_ausd_bp_ta_bcp_msisdn.ksh` and Helper Scripts:** The content of the sourced helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) was not available for detailed analysis. Their full functionality needs to be understood and accurately translated into Python for the Airflow DAG.
*   **`v_carmen = "@pcrs1"`:** The exact purpose and target of this Oracle database link/service name need to be thoroughly investigated. If it represents an external data source, that source must be identified and included in the migration scope, or a strategy for its replacement in BigQuery must be developed.
*   **Commented-out `sed`, `sort`, `join`:** While currently inactive, these commented sections in `k_ausd_bp_ta_bcp_msisdn.ksh` suggest a potential for file-based post-processing. A review with the business or source system owners is recommended to confirm these steps are indeed obsolete and not required in the migrated system.
*   **`FOSJobErzeugeEintrag` / `FOSJobDeaktivate`:** The commented-out calls in `k_ausd_bp_ta_bcp_msisdn.ksh` suggest interaction with a "FOS-Jobverwaltung" (FOS Job Management). This system needs to be identified, and its interaction points with this ETL job need to be understood. If it's a critical external system, its migration or integration strategy with Airflow must be defined.

## 8. Build Plan

1.  **Migrate Source Data:**
    *   Migrate `isbert_schema.dwtk_meldungen` to BigQuery table `project_id.isbert_schema.dwtk_meldungen_bq`.
    *   Migrate `sof$ta_bpr_bcp` to BigQuery table `project_id.sof_schema.ta_bpr_bcp_bq`.
    *   Migrate `sof$ta_rn_vertrag` to BigQuery table `project_id.sof_schema.ta_rn_vertrag_bq`.
2.  **Create Target Table:**
    *   Create empty BigQuery table `project_id.sof_schema.ta_bcp_msisdn_bq` with the appropriate schema.
3.  **Develop Airflow DAG (Python):**
    *   **File:** `dags/bert_bp_ta_bcp_msisdn_dag.py`
    *   **Language:** Python
    *   **Content:**
        *   Define DAG parameters (e.g., `stichtag`, `wiederanlaufwert`).
        *   Implement parameter parsing and validation logic from `r_ausd_bp_ta_bcp_msisdn.ksh` and `k_ausd_bp_ta_bcp_msisdn.ksh`.
        *   Implement date calculation logic.
        *   Task 1: Retrieve `s_datum` from `dwtk_meldungen_bq` using `BigQueryOperator`.
        *   Task 2: Truncate `ta_bcp_msisdn_bq` using `BigQueryOperator` (using a `TRUNCATE TABLE` DDL statement).
        *   Task 3: Execute the main `INSERT INTO ... SELECT ...` statement (converted to BigQuery SQL) using `BigQueryOperator`.
        *   Implement logging and error handling using Airflow's native mechanisms.
        *   Include logic for restart value (`p_wiederanlaufWert`).
4.  **Unit Testing:** Develop unit tests for the Airflow DAG and BigQuery SQL transformations.
5.  **Integration Testing:** Test the end-to-end data flow with sample data.
6.  **Deployment:** Deploy the Airflow DAG and BigQuery resources to the production environment.