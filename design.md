# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

## 1. Purpose & Scope
This job, orchestrated by the KornShell script `r_ausd_rechempf.ksh`, is responsible for the initial provisioning (snapshot) of the contract cache for demand scoring (FOS). It involves extracting and transforming data from various source tables in an Oracle environment and loading it into a set of intermediate and final snapshot tables (`sof$ta_means_of_pay`, `sof$ta_bank`, `sof$ta_bank_verb`, `sof$ta_bank_zuord`, `sof$ta_p_rech_empf`, `sof$ta_p_d1_vpn`). The process is driven by a reference date (Stichtag) and includes robust logging and error handling. The core data manipulation logic resides within the `d_ausd_rechempf.sql` script, executed via an intermediary KornShell script `k_ausd_rechempf.ksh`.

## 2. Source Inventory
This job comprises three main components:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`
    *   **Technology:** KornShell
    *   **Summary:** Orchestrates the job, parses parameters, sets up the environment, handles logging, and invokes the core processing script.
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-auto

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_rechempf.ksh`
    *   **Technology:** KornShell
    *   **Summary:** Intermediate control script. Parses parameters from `r_ausd_rechempf.ksh`, performs data checks, and executes the `d_ausd_rechempf.sql` script using SQL*Plus.
    *   **Complexity Tier:** Inherited from main script (Medium)
    *   **Automation Bucket:** Inherited from main script (Semi-auto)

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_rechempf.sql`
    *   **Technology:** Oracle SQL*Plus
    *   **Summary:** Contains the core data extraction and transformation logic, populating several `sof$` tables. Involves multiple `INSERT...SELECT` statements with complex `CASE` logic and joins across various tables, including some accessed via a database link.
    *   **Complexity Tier:** Inherited from main script (Medium)
    *   **Automation Bucket:** Inherited from main script (Semi-auto)

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) for orchestration.

*   **BigQuery Datasets/Tables:**
    *   Source tables like `isbert_schema.dwtk_meldungen`, `sof$ta_e_reach_re`, `sof$ta_e_business_re`, `sof$ta_e_regulierer`, `dwh$vi_s_ibasisprodukt` will be ingested into BigQuery.
    *   Tables accessed via the `&v_carmen` DB link (e.g., `bpd$ta_means_of_payment`, `bpd$ta_bank`, `bpd$ta_bank_international`) will need to be similarly migrated to BigQuery, likely into a dedicated dataset (e.g., `carmen_bpd`).
    *   Target tables `sof$ta_means_of_pay`, `sof$ta_bank`, `sof$ta_bank_verb`, `sof$ta_bank_zuord`, `sof$ta_p_rech_empf`, `sof$ta_p_d1_vpn` will be recreated in BigQuery. These will likely reside in a `fos_snapshots` dataset.
*   **Cloud Composer (Apache Airflow):**
    *   The orchestration logic currently handled by `r_ausd_rechempf.ksh` and `k_ausd_rechempf.ksh` will be translated into an Airflow DAG.
    *   The DAG will manage parameter passing, execution order, logging, and error handling.
*   **BigQuery Stored Procedures/SQL Scripts:**
    *   The core SQL logic from `d_ausd_rechempf.sql` will be converted to BigQuery SQL and executed as a series of BigQuery operations within the Airflow DAG. It could be encapsulated in BigQuery stored procedures or executed as individual SQL tasks.

## 4. Data Flow & Lineage
The original lineage for this job is:
`UC4 Job (DW.BERT_P_RECH_EMPF.xml)` -> `r_ausd_rechempf.ksh` -> `k_ausd_rechempf.ksh` -> `d_ausd_rechempf.sql`

In the target BigQuery/Airflow environment, the data flow will be as follows:

1.  **Trigger:** An Airflow DAG (e.g., `dag_r_ausd_rechempf`) will be scheduled or triggered (replacing the UC4 invocation).
2.  **Parameter Acquisition:** The Airflow DAG will handle the `Stichtag` and `Wiederanlaufwert` parameters, similar to how `r_ausd_rechempf.ksh` parses them. The `v_datum` derivation from `isbert_schema.dwtk_meldungen` will be translated into a BigQuery SQL query executed by Airflow.
3.  **Table Truncation:** The DAG will execute BigQuery `TRUNCATE TABLE` statements for the target `sof$` tables, similar to Step02 in the SQL script.
4.  **Data Transformation (Step 03 - 06):** The converted BigQuery SQL for each `INSERT INTO` statement (Steps 03, 04, 05, 06) will be executed sequentially as BigQuery operators within the Airflow DAG.
    *   Data will be read from:
        *   `isbert_schema.dwtk_meldungen` (for `v_datum`)
        *   `carmen_bpd.ta_means_of_payment` (source for `sof$ta_means_of_pay`)
        *   `carmen_bpd.ta_bank` and `carmen_bpd.ta_bank_international` (source for `sof$ta_bank`)
        *   `fos_source.sof_ta_e_reach_re`, `fos_source.sof_ta_e_business_re`, `fos_source.sof_ta_e_regulierer`
        *   `dwh_view.vi_s_ibasisprodukt`
    *   Data will be written to:
        *   `fos_snapshots.sof_ta_means_of_pay`
        *   `fos_snapshots.sof_ta_bank`
        *   `fos_snapshots.sof_ta_bank_verb`
        *   `fos_snapshots.sof_ta_bank_zuord`
        *   `fos_snapshots.sof_ta_p_rech_empf`
        *   `fos_snapshots.sof_ta_p_d1_vpn`
5.  **Logging & Error Handling:** Airflow's native logging and error handling mechanisms will replace the custom KornShell functions (`DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `trap`).

## 5. Transformation Logic
The core transformation logic from `d_ausd_rechempf.sql` has been converted to BigQuery SQL as follows:

```sql
-- BigQuery SQL equivalent of d_ausd_rechempf.sql

DECLARE v_carmen STRING DEFAULT '@pcrs1'; -- Placeholder for external source reference
DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 02: truncate temp tables
TRUNCATE TABLE `fos_snapshots.sof_ta_means_of_pay`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank_verb`;
TRUNCATE TABLE `fos_snapshots.sof_ta_bank_zuord`;
TRUNCATE TABLE `fos_snapshots.sof_ta_p_rech_empf`;
TRUNCATE TABLE `fos_snapshots.sof_ta_p_d1_vpn`;

-- Step 03: create temp. rechnungsdefinitionen (means_of_pay)
INSERT INTO `fos_snapshots.sof_ta_means_of_pay`
(
  BP_ID, MEANS_OF_PAYMENT_ID, OBJ_VERSION, INSERT_AT, MOP_TY, ACCOUNT_INT_BP_ID, ACCOUNT_INT_MOP_ID,
  BANK_ID_ACC, ACCOUNT_NUMBER_ACC, BANK_INTERNATIONAL_ID, MANDATE_VAR_CV, MANDATE_ST, MOP_ST, CHECK_ST,
  STATUS_REASON, IBAN, MANDATE_REFERENCE_NO, MANDATE_MIGRATED, MANDATE_CITY, MANDATE_DATE, VALID_FROM,
  VALID_TO, INSERT_BY, MODIFIED_AT, MODIFIED_BY, MODIFY_REASON, IS_IN_ARCHIVE, ROW_VERSION,
  REDUNDANT_RB_DOMAIN_PATH, REDUNDANT_RB_PROC_PATH, IS_PRODUCTION, RB_PARTITION_ID$
)
SELECT
  mop.BP_ID, mop.MEANS_OF_PAYMENT_ID, mop.OBJ_VERSION, mop.INSERT_AT, mop.MOP_TY, mop.ACCOUNT_INT_BP_ID, mop.ACCOUNT_INT_MOP_ID,
  mop.BANK_ID_ACC, mop.ACCOUNT_NUMBER_ACC, mop.BANK_INTERNATIONAL_ID, mop.MANDATE_VAR_CV, mop.MANDATE_ST, mop.MOP_ST, mop.CHECK_ST,
  mop.STATUS_REASON, mop.IBAN, mop.MANDATE_REFERENCE_NO, mop.MANDATE_MIGRATED, mop.MANDATE_CITY, mop.MANDATE_DATE, mop.VALID_FROM,
  mop.VALID_TO, mop.INSERT_BY, mop.MODIFIED_AT, mop.MODIFIED_BY, mop.MODIFY_REASON, mop.IS_IN_ARCHIVE, mop.ROW_VERSION,
  mop.REDUNDANT_RB_DOMAIN_PATH, mop.REDUNDANT_RB_PROC_PATH, mop.IS_PRODUCTION, mop.RB_PARTITION_ID$
FROM `carmen_bpd.ta_means_of_payment` AS mop -- Renamed from bpd$ta_means_of_payment
WHERE
  (DATE(mop.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (mop.modified_at IS NULL OR DATE(mop.modified_at) > PARSE_DATE('%Y%m%d', v_datum)))
  AND
  (DATE(mop.valid_from) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (mop.valid_to IS NULL OR DATE(mop.valid_to) > PARSE_DATE('%Y%m%d', v_datum)))
  AND mop.is_production = 1;

-- Step 03 continued: bank data
INSERT INTO `fos_snapshots.sof_ta_bank`
(
  BANK_ID, INSERT_AT, COUNTRY_CODE, BANK_SORT_NAME, BANK_NAME, INSERT_BY, MODIFIED_AT, MODIFIED_BY,
  MODIFY_REASON, IS_IN_ARCHIVE, ROW_VERSION, BIC, BANK_INTERNATIONAL_ID
)
SELECT
  ba.BANK_ID, ba.INSERT_AT, ba.COUNTRY_CODE, ba.BANK_SORT_NAME, ba.BANK_NAME, ba.INSERT_BY, ba.MODIFIED_AT, ba.MODIFIED_BY,
  ba.MODIFY_REASON, ba.IS_IN_ARCHIVE, ba.ROW_VERSION, CAST(NULL AS STRING) AS BIC, CAST(NULL AS STRING) AS BANK_INTERNATIONAL_ID
FROM `carmen_bpd.ta_bank` AS ba -- Renamed from BPD$TA_BANK
WHERE
  (DATE(ba.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (ba.modified_at IS NULL OR DATE(ba.modified_at) > PARSE_DATE('%Y%m%d', v_datum)))
UNION ALL
SELECT
  -99999 AS BANK_ID, bi.INSERT_AT, bi.COUNTRY_CODE, CAST(NULL AS STRING) AS BANK_SORT_NAME, bi.BANK_NAME,
  bi.INSERT_BY, bi.MODIFIED_AT, bi.MODIFIED_BY, bi.MODIFY_REASON, bi.IS_IN_ARCHIVE, bi.ROW_VERSION,
  bi.BIC, bi.BANK_INTERNATIONAL_ID
FROM `carmen_bpd.ta_bank_international` AS bi -- Renamed from BPD$TA_BANK_INTERNATIONAL
WHERE
  (DATE(bi.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
   AND (bi.modified_at IS NULL OR DATE(bi.modified_at) > PARSE_DATE('%Y%m%d', v_datum)));

-- Step 04: bank_verb
INSERT INTO `fos_snapshots.sof_ta_bank_verb`
(
  MEANS_OF_PAYMENT_ID, BP_ID, ACCOUNT_NUMBER_ACC, BANK_NAME, BANK_SORT_NAME, IBAN, BIC
)
SELECT
  mp.MEANS_OF_PAYMENT_ID, mp.BP_ID, mp.ACCOUNT_NUMBER_ACC, ba.BANK_NAME, ba.BANK_SORT_NAME, mp.IBAN, ba.BIC
FROM `fos_snapshots.sof_ta_means_of_pay` AS mp
JOIN `fos_snapshots.sof_ta_bank` AS ba
  ON mp.BANK_ID_ACC = ba.BANK_ID
  OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

-- Step 04 continued: bank_zuord
INSERT INTO `fos_snapshots.sof_ta_bank_zuord`
(
  INV_DEF_MOPREF_ID, ACCOUNT_NUMBER_ACC, BANK_NAME, BANK_SORT_NAME, IBAN, BIC
)
SELECT
  za.inv_def_mopref_id, ba.account_number_acc, ba.bank_name, ba.bank_sort_name, ba.iban, ba.bic
FROM `fos_snapshots.sof_ta_bank_verb` AS ba
JOIN `fos_source.sof_ta_e_regulierer` AS za
  ON za.means_of_payment_id = ba.means_of_payment_id
 AND za.mop_bp_id = ba.bp_id;

-- Step 05: rech_empf
INSERT INTO `fos_snapshots.sof_ta_p_rech_empf`
(
  KUNDENKONTO, RECHDEF_ID, DPPS_KONTONUMMER, RECHNUNGSEMPFAENGER, QUELLE, AKAD_TITEL, FIRMA,
  VORNAME, NACHNAME, ZUSATZ_1, ZUSATZ_2, STRASSE, PLZ, WOHNORT, LAND, BANKNAME,
  BANK_KONTONUMMER, BLZ, ORGANISATIONSEINHEIT, MWST_KENNZEICHEN, KUN_NR_RECH_EMPF, IBAN, BIC
)
SELECT
  '0' AS kundenkonto,
  re.inv_def_invrec_id AS rechdef_id,
  '0' AS dpps_kontonummer,
  CASE
    WHEN re.corp_unit IS NULL AND bp.organisation_name IS NULL THEN
      CASE
        WHEN re.surname_s IS NULL THEN CONCAT(bp.first_name, ' ', bp.surname)
        ELSE CONCAT(re.first_name_g, ' ', re.surname_s)
      END
    ELSE
      CASE
        WHEN re.corp_unit IS NULL THEN bp.organisation_name
        ELSE re.corp_unit
      END
  END AS rechnungsempfaenger,
  'C' AS quelle,
  CASE
    WHEN re.surname_s IS NULL THEN bp.title
    ELSE ''
  END AS akad_titel,
  CASE
    WHEN re.corp_unit IS NULL THEN bp.organisation_name
    ELSE re.corp_unit
  END AS firma,
  CASE
    WHEN re.first_name_g IS NULL THEN bp.first_name
    ELSE re.first_name_g
  END AS vorname,
  CASE
    WHEN re.surname_s IS NULL THEN bp.surname
    ELSE bp.surname
  END AS nachname,
  re.for_the_attention_of AS zusatz_1,
  re.address_attachment AS zusatz_2,
  CASE
    WHEN re.street IS NULL THEN
      CASE
        WHEN re.pobox IS NULL THEN ''
        ELSE CONCAT('Postfach ', CAST(re.pobox AS STRING))
      END
    ELSE CONCAT(re.street, ' ', CAST(re.house_nr AS STRING))
  END AS strasse,
  re.zip_code AS plz,
  re.city AS wohnort,
  re.land_sd AS land,
  ba.bank_name AS bankname,
  ba.account_number_acc AS bank_kontonummer,
  ba.bank_sort_name AS blz,
  re.address_attachment_org AS organisationseinheit,
  bp.sales_tax_freed AS mwst_kennzeichen,
  bp.tm_customerid AS kun_nr_rech_empf,
  ba.iban,
  ba.bic
FROM `fos_source.sof_ta_e_reach_re` AS re
JOIN `fos_source.sof_ta_e_business_re` AS bp
  ON re.bp_id = bp.bp_id
LEFT JOIN `fos_snapshots.sof_ta_bank_zuord` AS ba
  ON re.inv_def_invrec_id = ba.inv_def_mopref_id;

-- Step 06: d1_vpn
INSERT INTO `fos_snapshots.sof_ta_p_d1_vpn`
(
  VERTRAGS_ID, VPN_ID
)
SELECT
  bp.vertrags_id, bp.vpn_id
FROM `dwh_view.vi_s_ibasisprodukt` AS bp
WHERE
  bp.vpn_id IS NOT NULL
  AND bp.basisprodukt_id IN (2828, 2831);
```

**Key Transformations:**
*   Oracle `TO_DATE` and `NVL` functions replaced with BigQuery `PARSE_DATE` and `IFNULL`/`COALESCE`.
*   Oracle `COLUMN ... NEW_VALUE` for variable definition replaced with BigQuery `DECLARE` and `SELECT` statement for `v_datum`.
*   Oracle outer join syntax `(+)` replaced with explicit `LEFT JOIN`.
*   Oracle `parallel` hints are removed as BigQuery handles parallelism automatically.
*   Schema and table names are adjusted to fit BigQuery conventions (e.g., `isbert_schema.dwtk_meldungen` becomes `isbert_schema.dwtk_meldungen`, `bpd$ta_means_of_payment@pcrs1` becomes `carmen_bpd.ta_means_of_payment`).
*   String concatenations `||` are converted to `CONCAT()`.
*   `DESC` statements for existence checks are removed as they are not directly portable; BigQuery table existence is handled at a schema management level.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Oracle Database (`@pcrs1` DB Link):** This links to `bpd$ta_means_of_payment`, `bpd$ta_bank`, and `bpd$ta_bank_international`.
    *   **Replacement:** These source tables will be migrated to BigQuery into a dedicated dataset (e.g., `carmen_bpd`). Data ingestion can be set up using a batch loading mechanism (e.g., Dataflow, Cloud Storage transfers) or CDC tools to keep them synchronized.
*   **KornShell (ksh) Utility Scripts:**
    *   `.dw_init`: Likely environment setup.
    *   `f_alis_msgerr.ksh`: Error handling and messaging.
    *   `h_alis_parameter.ksh`: Parameter parsing.
    *   `h_alis_date.ksh`: Date handling.
    *   `h_alis_sqlplus.ksh`: SQL*Plus execution wrapper.
    *   `gestern.ksh`: Date calculation (yesterday).
    *   **Replacement:** These functionalities will be absorbed by the Airflow DAG. Airflow provides robust mechanisms for environment variables, parameter passing, logging, and error handling. Date calculations will use Python's `datetime` module within the DAG.
*   **UC4 Scheduler:** The job is invoked by a UC4 XML definition.
    *   **Replacement:** The job will be orchestrated by an Apache Airflow DAG in Cloud Composer, providing scheduling, monitoring, and dependency management.

## 7. Unresolved / Risks
*   **`v_datum` from `dwtk_meldungen`:** The exact `isbert_schema.dwtk_meldungen` table and its contents will need to be accurately migrated to BigQuery to ensure `v_datum` is derived correctly.
*   **Custom Shell Functions:** The exact implementation of helper functions like `starteSQLSkript` in `h_alis_sqlplus.ksh` might have subtle behaviors (e.g., specific error codes, logging formats) that need careful replication in Python/Airflow.
*   **Data Types:** While the design addresses general data type conversions, a detailed schema mapping will be required for all source tables to ensure precise BigQuery data types (e.g., `NUMERIC` for financial values, `TIMESTAMP` vs `DATETIME` vs `DATE` for dates).
*   **Performance:** Oracle `/*+ parallel(4) */` hints indicate performance tuning. BigQuery handles parallelism automatically, but query performance needs to be validated after migration.
*   **Missing Table Descriptions (DESC):** The original script uses `DESC` statements for existence checks. In BigQuery, this can be replaced by querying `INFORMATION_SCHEMA` or ensuring table creation as part of the deployment.
*   **Character Encoding:** Potential issues with German umlauts (e.g., `Lbbers` -> `Lübbers`) in comments/prompts, though this is less critical for functional migration.

## 8. Build Plan
The migration will follow these steps:

1.  **BigQuery Schema Definition (SQL/DDL):**
    *   Define the target BigQuery schemas for `carmen_bpd`, `fos_source`, `dwh_view`, and `fos_snapshots` datasets.
    *   Create all source and target tables in BigQuery with appropriate data types based on a detailed schema mapping document.
    *   _Language: BigQuery DDL_

2.  **Data Ingestion (Python/Dataflow/Cloud Storage):**
    *   Set up a data pipeline to ingest initial and incremental data from the source Oracle database for `bpd$ta_means_of_payment`, `bpd$ta_bank`, `bpd$ta_bank_international`, `isbert_schema.dwtk_meldungen`, `sof$ta_e_reach_re`, `sof$ta_e_business_re`, `sof$ta_e_regulierer`, and `dwh$vi_s_ibasisprodukt` into their respective BigQuery tables.
    *   _Language: Python (Dataflow) or Cloud Storage Transfer Service config_

3.  **BigQuery SQL Script (SQL):**
    *   Create a single BigQuery SQL script (`d_ausd_rechempf_bq.sql`) containing the entire transformed SQL logic from Section 5. This script will include the `DECLARE` statements and the sequential `TRUNCATE` and `INSERT` operations.
    *   _Language: BigQuery SQL_

4.  **Airflow DAG Development (Python):**
    *   Develop an Airflow DAG (`r_ausd_rechempf_dag.py`) in Python to orchestrate the job.
    *   The DAG will define tasks for:
        *   Deriving `v_datum` using a `BigQueryExecuteQueryOperator`.
        *   Executing the BigQuery `TRUNCATE TABLE` statements.
        *   Executing each `INSERT INTO` statement as a separate `BigQueryExecuteQueryOperator` or a single `BigQueryExecuteQueryOperator` running the entire `d_ausd_rechempf_bq.sql` script.
        *   Handling logging and error notifications using Airflow's built-in features.
        *   Parameterizing the `Stichtag` and `Wiederanlaufwert`.
    *   _Language: Python_

5.  **Testing and Validation:**
    *   Implement unit and integration tests for the BigQuery SQL and the Airflow DAG.
    *   Validate data accuracy and completeness against the legacy system.

6.  **Deployment:**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Deploy BigQuery schemas and SQL scripts.