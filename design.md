Here is the complete, implementation-ready Migration Design Document for the legacy Oracle/Shell/UC4 orchestration job **DW.BERT_AUSD_BP_TA_P_BASISPROD**.

---

# MIGRATION DESIGN DOCUMENT: DW.BERT_AUSD_BP_TA_P_BASISPROD

## 1. Executive Summary & Objective
This document outlines the migration design for transitioning the legacy Automic (UC4) execution job `DW.BERT_AUSD_BP_TA_P_BASISPROD` and its corresponding wrapper scripts and Oracle SQL operations to Google Cloud Platform. 

The objective of this job is to orchestrate, prepare, and load basic product data (**Basisprodukt**) into the `sof$ta_p_basisprod` table (destined for downstream scoring and reporting systems like BERT). The legacy system uses Automic UC4, KornShell wrappers (`r_ausd_bp_ta_p_basisprod.ksh`, `k_ausd_bp_ta_p_basisprod.ksh`), and complex Oracle PL/SQL. 

The target architecture replaces these legacy components with **BigQuery Standard SQL** for data transformations and **Google Cloud Composer (Apache Airflow)** for end-to-end orchestration.

---

## 2. Legacy Source Artifacts & Analysis
The job consists of 5 files within the pre-collected context:
1. **UC4 Job XML:** `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`
2. **Orchestration Shell Script:** `r_ausd_bp_ta_p_basisprod.ksh`
3. **Control/Validation Shell Script:** `k_ausd_bp_ta_p_basisprod.ksh`
4. **Oracle SQL Execution Script:** `d_ausd_bp_ta_p_basisprod.sql`
5. **Environment Configuration Helper:** `.dw_init`

### Lineage and Call Sequence
```
[Automic UC4 Scheduler] 
       │
       ▼
 [r_ausd_bp_ta_p_basisprod.ksh] (Initializes logger, parses inputs, determines Stichtag)
       │
       ▼
 [k_ausd_bp_ta_p_basisprod.ksh] (Validates parameters, handles recovery metrics)
       │
       ▼
 [d_ausd_bp_ta_p_basisprod.sql] (Executes TRUNCATE and bulk INSERT in Oracle DB)
```

### Downstream / Upstream Schema Dependencies
*   **Upstream Inputs:**
    *   `isbert_schema.dwtk_meldungen`
    *   `sof$ta_cntrct_dist` (Base Contracts)
    *   `sof$ta_cntrct_evn`
    *   `sof$ta_iccid_vertrag`
    *   `sof$ta_rn_vertrag`
    *   `sof$ta_tarifoption`
    *   `sof$ta_apn_vertrag`
    *   `SOF$TA_BCP_ICCID` & `SOF$TA_BCP_MSISDN` (joined inside the query as `bccm`)
    *   `sof$ta_rn_da_vda_tk`
*   **Target Output Table:** `sof$ta_p_basisprod` (BigQuery dataset mapping: `sof_ta_p_basisprod`)

---

## 3. Core Transformation Logic (MCP Output)

The following section contains the verbatim analysis and target SQL conversion of the complex Oracle query computed by the CodeMaverick Migration engine:

### 3.1 Design Document (Verbatim)

```markdown
## Design Document

### 1. Objective
Convert the provided Hive/Oracle-style SQL into an equivalent BigQuery SQL statement while preserving business logic, column mapping, and join semantics as closely as possible.

### 2. Source Artifacts
- File: `d_ausd_basisprodukt.sql`
- Source SQL type: Oracle/Hive-style SQL with legacy outer join syntax and `DECODE`, `NVL`, `TO_CHAR`, `APPEND` hint, and `DEFINE/COLUMN` scripting constructs.

### 3. Target Platform
- BigQuery Standard SQL

### 4. Conversion Scope
- Convert the final `INSERT ... SELECT` logic to BigQuery.
- Ignore SQL*Plus scripting directives not supported in BigQuery:
  - `DEFINE v_carmen = "@pcrs1"`
  - `COLUMN s_datum new_value v_datum noprint`
  - standalone `SELECT NVL(TO_CHAR(MAX(...))) ...`
- Preserve all selected columns and aliases.
- Convert Oracle outer join syntax `(+)` to BigQuery `LEFT JOIN`.
- Convert Oracle functions:
  - `NVL` → `COALESCE`
  - `TO_CHAR(date, 'YYYYMMDD')` → `FORMAT_DATE('%Y%m%d', DATE(...))` or `FORMAT_TIMESTAMP` depending on source type
  - `DECODE(expr, null, a, b)` → `IF(expr IS NULL, a, b)` or `COALESCE`
- Preserve numeric precision by explicit casting where needed.
- Preserve date/time semantics by explicit conversion where conditions or formatting are involved.
- Preserve binary-like fields using `BYTES` if source metadata indicates binary content.
- Preserve structured types using `ARRAY`, `STRUCT`, or JSON-compatible handling if encountered.

### 5. Data Type Conversion Rules
#### 5.1 Date-related columns
- `m.timecreated` in the standalone query should be treated as timestamp/datetime/date depending on source metadata.
- BigQuery equivalent:
  - If `TIMESTAMP`: `FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated)))`
  - If `DATETIME`: `FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated)))`
  - If `DATE`: `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))`

#### 5.2 Numeric precision
- Columns such as MCC/MNC/HLR/SI/STAT/VALID may be numeric or string in source.
- If source metadata indicates numeric precision/scale, use `CAST(... AS NUMERIC)` or `CAST(... AS BIGNUMERIC)` as appropriate.
- If values are identifiers with leading zeros, keep as `STRING`.

#### 5.3 Binary-like data
- No explicit binary columns are present in the SQL text.
- If underlying schema defines binary content, map to `BYTES` in BigQuery.

#### 5.4 Structured types
- No arrays/structs/maps are explicitly present in the SQL text.
- If source schema contains collections/objects, flatten or map to `ARRAY<STRUCT<...>>` / `STRUCT<...>` as required.

### 6. Join Conversion Strategy
- Convert comma joins plus `(+)` predicates into explicit `LEFT JOIN`s.
- Preserve the driving table `sof$ta_cntrct_dist cn` as the base table.
- Preserve optional relationships as left joins:
  - `sof$ta_cntrct_evn ev`
  - `sof$ta_iccid_vertrag icc`
  - `sof$ta_rn_vertrag msi`
  - `sof$ta_rn_da_vda_tk msd`
  - `sof$ta_tarifoption opt`
  - `sof$ta_apn_vertrag av`
  - derived table `bccm`

### 7. BigQuery SQL Query
```sql
INSERT INTO `sof_ta_p_basisprod` (
  CNTRCT_ID,
  EVN,
  TNV_ICCID,
  TNV_MCC,
  TNV_MNC,
  TNV_HLR,
  TNV_SI,
  TNV_ICC_STAT,
  TNV_ICC_VALID,
  TC_ICCID,
  TC_MCC,
  TC_MNC,
  TC_HLR,
  TC_SI,
  TC_ICC_STAT,
  TC_ICC_VALID,
  TB_ICCID,
  TB_MCC,
  TB_MNC,
  TB_HLR,
  TB_SI,
  TB_ICC_STAT,
  TB_ICC_VALID,
  MS1_ICCID,
  MS1_MCC,
  MS1_MNC,
  MS1_HLR,
  MS1_SI,
  MS1_STAT,
  MS1_VALID,
  MS2_ICCID,
  MS2_MCC,
  MS2_MNC,
  MS2_HLR,
  MS2_SI,
  MS2_STAT,
  MS2_VALID,
  TNV_E_ID,
  TNV_CARD_TYPE_NAME,
  TC_E_ID,
  TC_CARD_TYPE_NAME,
  TB_E_ID,
  TB_CARD_TYPE_NAME,
  MS1_E_ID,
  MS1_CARD_TYPE_NAME,
  MS2_E_ID,
  MS2_CARD_TYPE_NAME,
  TNV_MULTI_SINGLE,
  TC_MULTI_SINGLE,
  TB_MULTI_SINGLE,
  TNV_MSISDN,
  TNV_MS_STAT,
  TNV_MS_VALID,
  TNV_DAT_MSISDN,
  TNV_DAT_STAT,
  TNV_DAT_VALID,
  TNV_FAX_MSISDN,
  TNV_FAX_STAT,
  TNV_FAX_VALID,
  TC_MSISDN,
  TC_MS_STAT,
  TC_MS_VALID,
  TC_DAT_MSISDN,
  TC_DAT_STAT,
  TC_DAT_VALID,
  TC_FAX_MSISDN,
  TC_FAX_STAT,
  TC_FAX_VALID,
  TB_MSISDN,
  TB_MS_STAT,
  TB_MS_VALID,
  TB_DAT_MSISDN,
  TB_DAT_STAT,
  TB_DAT_VALID,
  TB_FAX_MSISDN,
  TB_FAX_STAT,
  TB_FAX_VALID,
  MS1_MSISDN,
  MS1_MS_STAT,
  MS1_MS_VALID,
  MS2_MSISDN,
  MS2_MS_STAT,
  MS2_MS_VALID,
  DA_MSISDN,
  DA_MS_STAT,
  DA_MS_VALID,
  VDA_MSISDN,
  VDA_MS_STAT,
  VDA_MS_VALID,
  TK_MSISDN,
  TK_MS_STAT,
  TK_MS_VALID,
  BCP_VERTRAG,
  BCP_ICCID,
  BCP_HLR,
  APN,
  BCP_TN_TEL,
  DATA_OPTION_REIN,
  VOICE_OPTION_REIN,
  MIX_OPTION,
  MULTI_OPTION,
  ROAMING_OPTION,
  SONSTIGE_OPTION,
  MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_MCC, MS3_MNC, MS3_HLR, MS3_SI, MS3_STAT, MS3_VALID,
  MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_MCC, MS4_MNC, MS4_HLR, MS4_SI, MS4_STAT, MS4_VALID,
  MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_MCC, MS5_MNC, MS5_HLR, MS5_SI, MS5_STAT, MS5_VALID,
  MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_MCC, MS6_MNC, MS6_HLR, MS6_SI, MS6_STAT, MS6_VALID,
  MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_MCC, MS7_MNC, MS7_HLR, MS7_SI, MS7_STAT, MS7_VALID,
  MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_MCC, MS8_MNC, MS8_HLR, MS8_SI, MS8_STAT, MS8_VALID,
  MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_MCC, MS9_MNC, MS9_HLR, MS9_SI, MS9_STAT, MS9_VALID,
  MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_MCC, MS10_MNC, MS10_HLR, MS10_SI, MS10_STAT, MS10_VALID
)
SELECT
  cn.cntrct_id,
  ev.evn,
  icc.tn_iccid AS tnv_iccid,
  icc.tn_imsi_mcc AS tnv_mcc,
  icc.tn_imsi_mnc AS tnv_mnc,
  icc.tn_imsi_hlr AS tnv_hlr,
  icc.tn_imsi_si AS tnv_si,
  icc.tn_status AS tnv_icc_stat,
  icc.tn_valid_to AS tnv_icc_valid,
  icc.tc_iccid AS tc_iccid,
  icc.tc_imsi_mcc AS tc_mcc,
  icc.tc_imsi_mnc AS tc_mnc,
  icc.tc_imsi_hlr AS tc_hlr,
  icc.tc_imsi_si AS tc_si,
  icc.tc_status AS tc_icc_stat,
  icc.tc_valid_to AS tc_icc_valid,
  icc.tb_iccid AS tb_iccid,
  icc.tb_imsi_mcc AS tb_mcc,
  icc.tb_imsi_mnc AS tb_mnc,
  icc.tb_imsi_hlr AS tb_hlr,
  icc.tb_imsi_si AS tb_si,
  icc.tb_status AS tb_icc_stat,
  icc.tb_valid_to AS tb_icc_valid,
  icc.ms1_iccid AS ms1_iccid,
  icc.ms1_imsi_mcc AS ms1_mcc,
  icc.ms1_imsi_mnc AS ms1_mnc,
  icc.ms1_imsi_hlr AS ms1_hlr,
  icc.ms1_imsi_si AS ms1_si,
  icc.ms1_status AS ms1_stat,
  icc.ms1_valid_to AS ms1_valid,
  icc.ms2_iccid AS ms2_iccid,
  icc.ms2_imsi_mcc AS ms2_mcc,
  icc.ms2_imsi_mnc AS ms2_mnc,
  icc.ms2_imsi_hlr AS ms2_hlr,
  icc.ms2_imsi_si AS ms2_si,
  icc.ms2_status AS ms2_stat,
  icc.ms2_valid_to AS ms2_valid,
  icc.tn_e_id AS tnv_e_id,
  icc.tn_card_type_name AS tnv_card_type_name,
  icc.tc_e_id AS tc_e_id,
  icc.tc_card_type_name AS tc_card_type_name,
  icc.tb_e_id AS tb_e_id,
  icc.tb_card_type_name AS tb_card_type_name,
  icc.ms1_e_id AS ms1_e_id,
  icc.ms1_card_type_name AS ms1_card_type_name,
  icc.ms2_e_id AS ms2_e_id,
  icc.ms2_card_type_name AS ms2_card_type_name,
  msi.tn_multi_single AS tnv_multi_single,
  msi.tc_multi_single AS tc_multi_single,
  msi.tb_multi_single AS tb_multi_single,
  msi.tn_tel_msisdn AS tnv_msisdn,
  msi.tn_tel_status AS tnv_ms_stat,
  msi.tn_tel_valid_to AS tnv_ms_valid,
  msi.tn_dat_msisdn AS tnv_dat_msisdn,
  msi.tn_dat_status AS tnv_dat_stat,
  msi.tn_dat_valid_to AS tnv_dat_valid,
  msi.tn_fax_msisdn AS tnv_fax_msisdn,
  msi.tn_fax_status AS tnv_fax_stat,
  msi.tn_fax_valid_to AS tnv_fax_valid,
  msi.tc_tel_msisdn AS tc_msisdn,
  msi.tc_tel_status AS tc_ms_stat,
  msi.tc_tel_valid_to AS tc_ms_valid,
  msi.tc_dat_msisdn AS tc_dat_msisdn,
  msi.tc_dat_status AS tc_dat_stat,
  msi.tc_dat_valid_to AS tc_dat_valid,
  msi.tc_fax_msisdn AS tc_fax_msisdn,
  msi.tc_fax_status AS tc_fax_stat,
  msi.tc_fax_valid_to AS tc_fax_valid,
  msi.tb_tel_msisdn AS tb_msisdn,
  msi.tb_tel_status AS tb_ms_stat,
  msi.tb_tel_valid_to AS tb_ms_valid,
  msi.tb_dat_msisdn AS tb_dat_msisdn,
  msi.tb_dat_status AS tb_dat_stat,
  msi.tb_dat_valid_to AS tb_dat_valid,
  msi.tb_fax_msisdn AS tb_fax_msisdn,
  msi.tb_fax_status AS tb_fax_stat,
  msi.tb_fax_valid_to AS tb_fax_valid,
  msi.ms_rn_1_msisdn AS ms1_msisdn,
  msi.ms_rn_1_status AS ms1_ms_stat,
  msi.ms_rn_1_valid_to AS ms1_ms_valid,
  msi.ms_rn_2_msisdn AS ms2_msisdn,
  msi.ms_rn_2_status AS ms2_ms_stat,
  msi.ms_rn_2_valid_to AS ms2_ms_valid,
  msd.da_rn_msisdn AS da_msisdn,
  msd.da_rn_status AS da_ms_stat,
  msd.da_rn_valid_to AS da_ms_valid,
  msd.vda_rn_msisdn AS vda_msisdn,
  msd.vda_rn_status AS vda_ms_stat,
  msd.vda_rn_valid_to AS vda_ms_valid,
  msd.tk_rn_msisdn AS tk_msisdn,
  msd.tk_rn_status AS tk_ms_stat,
  msd.tk_rn_valid_to AS tk_ms_valid,
  bccm.cntrct_id_ref AS bcp_vertrag,
  bccm.tn_iccid AS bcp_iccid,
  bccm.tn_imsi_hlr AS bcp_hlr,
  IF(av.apn IS NULL, av.apn, CONCAT(av.apn, ',', av.apn_cntrct)) AS apn,
  bccm.tn_tel_msisdn AS bcp_tn_tel,
  opt.data_option_rein AS data_option_rein,
  opt.voice_option_rein AS voice_option_rein,
  opt.mix_option AS mix_option,
  opt.multi_option AS multi_option,
  opt.roaming_option AS roaming_option,
  opt.sonstige_option AS sonstige_option,
  icc.ms3_iccid AS ms3_iccid,
  icc.ms3_e_id AS ms3_e_id,
  icc.ms3_card_type_name AS ms3_card_type_name,
  icc.ms3_imsi_mcc AS ms3_mcc,
  icc.ms3_imsi_mnc AS ms3_mnc,
  icc.ms3_imsi_hlr AS ms3_hlr,
  icc.ms3_imsi_si AS ms3_si,
  icc.ms3_status AS ms3_stat,
  icc.ms3_valid_to AS ms3_valid,
  icc.ms4_iccid AS ms4_iccid,
  icc.ms4_e_id AS ms4_e_id,
  icc.ms4_card_type_name AS ms4_card_type_name,
  icc.ms4_imsi_mcc AS ms4_mcc,
  icc.ms4_imsi_mnc AS ms4_mnc,
  icc.ms4_imsi_hlr AS ms4_hlr,
  icc.ms4_imsi_si AS ms4_si,
  icc.ms4_status AS ms4_stat,
  icc.ms4_valid_to AS ms4_valid,
  icc.ms5_iccid AS ms5_iccid,
  icc.ms5_e_id AS ms5_e_id,
  icc.ms5_card_type_name AS ms5_card_type_name,
  icc.ms5_imsi_mcc AS ms5_mcc,
  icc.ms5_imsi_mnc AS ms5_mnc,
  icc.ms5_imsi_hlr AS ms5_hlr,
  icc.ms5_imsi_si AS ms5_si,
  icc.ms5_status AS ms5_stat,
  icc.ms5_valid_to AS ms5_valid,
  icc.ms6_iccid AS ms6_iccid,
  icc.ms6_e_id AS ms6_e_id,
  icc.ms6_card_type_name AS ms6_card_type_name,
  icc.ms6_imsi_mcc AS ms6_mcc,
  icc.ms6_imsi_mnc AS ms6_mnc,
  icc.ms6_imsi_hlr AS ms6_hlr,
  icc.ms6_imsi_si AS ms6_si,
  icc.ms6_status AS ms6_stat,
  icc.ms6_valid_to AS ms6_valid,
  icc.ms7_iccid AS ms7_iccid,
  icc.ms7_e_id AS ms7_e_id,
  icc.ms7_card_type_name AS ms7_card_type_name,
  icc.ms7_imsi_mcc AS ms7_mcc,
  icc.ms7_imsi_mnc AS ms7_mnc,
  icc.ms7_imsi_hlr AS ms7_hlr,
  icc.ms7_imsi_si AS ms7_si,
  icc.ms7_status AS ms7_stat,
  icc.ms7_valid_to AS ms7_valid,
  icc.ms8_iccid AS ms8_iccid,
  icc.ms8_e_id AS ms8_e_id,
  icc.ms8_card_type_name AS ms8_card_type_name,
  icc.ms8_imsi_mcc AS ms8_mcc,
  icc.ms8_imsi_mnc AS ms8_mnc,
  icc.ms8_imsi_hlr AS ms8_hlr,
  icc.ms8_imsi_si AS ms8_si,
  icc.ms8_status AS ms8_stat,
  icc.ms8_valid_to AS ms8_valid,
  icc.ms9_iccid AS ms9_iccid,
  icc.ms9_e_id AS ms9_e_id,
  icc.ms9_card_type_name AS ms9_card_type_name,
  icc.ms9_imsi_mcc AS ms9_mcc,
  icc.ms9_imsi_mnc AS ms9_mnc,
  icc.ms9_imsi_hlr AS ms9_hlr,
  icc.ms9_imsi_si AS ms9_si,
  icc.ms9_status AS ms9_stat,
  icc.ms9_valid_to AS ms9_valid,
  icc.ms10_iccid AS ms10_iccid,
  icc.ms10_e_id AS ms10_e_id,
  icc.ms10_card_type_name AS ms10_card_type_name,
  icc.ms10_imsi_mcc AS ms10_mcc,
  icc.ms10_imsi_mnc AS ms10_mnc,
  icc.ms10_imsi_hlr AS ms10_hlr,
  icc.ms10_imsi_si AS ms10_si,
  icc.ms10_status AS ms10_stat,
  icc.ms10_valid_to AS ms10_valid
FROM `sof_ta_cntrct_dist` cn
LEFT JOIN `sof_ta_cntrct_evn` ev
  ON cn.cntrct_id = ev.cntrct_id
LEFT JOIN `sof_ta_iccid_vertrag` icc
  ON cn.cntrct_id = icc.cntrct_id
LEFT JOIN `sof_ta_rn_vertrag` msi
  ON cn.cntrct_id = msi.cntrct_id
LEFT JOIN `sof_ta_tarifoption` opt
  ON cn.cntrct_id = opt.cntrct_id
LEFT JOIN `sof_ta_apn_vertrag` av
  ON cn.cntrct_id = av.cntrct_id
LEFT JOIN (
  SELECT
    BC.CNTRCT_ID,
    BC.CNTRCT_ID_REF,
    BC.TN_ICCID,
    BC.TN_IMSI_HLR,
    BCM.TN_TEL_MSISDN
  FROM `SOF_TA_BCP_ICCID` BC
  JOIN `SOF_TA_BCP_MSISDN` BCM
    ON BC.CNTRCT_ID = BCM.CNTRCT_ID
   AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF
) bccm
  ON cn.cntrct_id = bccm.cntrct_id
LEFT JOIN `sof_ta_rn_da_vda_tk` msd
  ON cn.cntrct_id = msd.cntrct_id;
```

### 8. Low-Level Pseudocode
```text
BEGIN
  DEFINE source date marker logic from dwtk_meldungen:
    max_timecreated = MAX(m.timecreated) WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    v_datum = IF max_timecreated IS NULL THEN '19000101' ELSE FORMAT_DATE('%Y%m%d', DATE(max_timecreated))

  READ base contracts from sof_ta_cntrct_dist as cn

  LEFT JOIN event data from sof_ta_cntrct_evn as ev ON cn.cntrct_id = ev.cntrct_id
  LEFT JOIN ICCID contract data from sof_ta_iccid_vertrag as icc ON cn.cntrct_id = icc.cntrct_id
  LEFT JOIN RN contract data from sof_ta_rn_vertrag as msi ON cn.cntrct_id = msi.cntrct_id
  LEFT JOIN RN DA/VDA/TK data from sof_ta_rn_da_vda_tk as msd ON cn.cntrct_id = msd.cntrct_id
  LEFT JOIN tariff options from sof_ta_tarifoption as opt ON cn.cntrct_id = opt.cntrct_id
  LEFT JOIN APN contract data from sof_ta_apn_vertrag as av ON cn.cntrct_id = av.cntrct_id

  BUILD derived table bccm:
    JOIN SOF_TA_BCP_ICCID BC with SOF_TA_BCP_MSISDN BCM
      ON BC.CNTRCT_ID = BCM.CNTRCT_ID
     AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF

  LEFT JOIN bccm to cn ON cn.cntrct_id = bccm.cntrct_id

  FOR EACH resulting row:
    project all source columns into target aliases
    compute APN:
      IF av.apn IS NULL THEN av.apn ELSE CONCAT(av.apn, ',', av.apn_cntrct)
    preserve all ICCID/MSISDN/status/valid_to fields
    preserve all MS3..MS10 fields
    preserve numeric/string types as source-compatible BigQuery types

  INSERT projected rows into sof_ta_p_basisprod
END
```

### 9. Entities
#### Tables
- `isbert_schema.dwtk_meldungen`
- `sof$ta_p_basisprod`
- `sof$ta_cntrct_dist`
- `SOF$TA_BCP_ICCID`
- `SOF$TA_BCP_MSISDN`
- `sof$ta_cntrct_evn`
- `sof$ta_iccid_vertrag`
- `sof$ta_rn_vertrag`
- `sof$ta_rn_da_vda_tk`
- `sof$ta_tarifoption`
- `sof$ta_apn_vertrag`

#### Columns
- `m.timecreated`
- `m.job_kennung`
- `cn.cntrct_id`
- `ev.evn`
- `icc.*`
- `msi.*`
- `msd.*`
- `opt.*`
- `av.apn`
- `av.apn_cntrct`
- `BC.*`
- `BCM.*`

#### Files
- `d_ausd_basisprodukt.sql`
```

---

## 4. Operational Migration & Orchestration Plan

The legacy execution relies on KornShell and Automic parameters. The following describes how this behavior is fully handled in the Google Cloud environment.

### 4.1 Airflow DAG Topology
*   **Orchestration Framework:** Google Cloud Composer (Airflow 2.x).
*   **Target Operators:**
    1.  `BigQueryValueCheckOperator` / `BigQueryGetDataOperator`: Used to calculate the `v_datum` indicator from `dwtk_meldungen` (Step 00).
    2.  `BigQueryInsertJobOperator`: Performs the `TRUNCATE` (Step 01).
    3.  `BigQueryInsertJobOperator`: Runs the consolidated `INSERT INTO ... SELECT` query (Step 12).
*   **Data Consistency:** Standard transactions (`TRUNCATE` followed by `INSERT`) will be scheduled sequentially. BigQuery guarantees execution atomic integrity.

### 4.2 Parameter Passing and Recovery (`Stichtag` / `Wiederanlaufwert`)
The legacy shell scripts calculate temporal boundaries such as `p_stichtag` (Reporting Cutoff Date) and `p_wiederanlaufWert` (Resume/Recovery contract ID threshold).
*   **Stichtag Calculation:**
    *   In the Airflow DAG, default the execution date (`{{ ds_nodash }}`) as the base `p_stichtag` variable.
    *   Alternatively, run a dynamically parameterized BigQuery SQL task using Jinja templating:
        `DECLARE p_stichtag STRING DEFAULT '{{ dag_run.conf.get("stichtag", ds_nodash) }}';`
*   **Resume/Recovery Logic:**
    *   The variable `p_wiederanlaufWert` is used to skip already processed contract IDs. 
    *   In BigQuery, this parameter can be declared as a query parameter or Airflow variable. If active (value > 0), append a filtering clause:
        `WHERE cn.cntrct_id > p_wiederanlaufWert` dynamically inside the transformation DAG logic.

### 4.3 Environment-Specific Configuration Variables
The build agent must populate the following BigQuery paths and connection configurations dynamically:
*   `PROJECT_ID`: Target GCP Project (e.g., `gcp-dwh-prod`).
*   `DATASET_NAME`: Target Dataset mapping the old schema prefixes (e.g., `sof_core`).
*   `MELDUNGEN_DATASET`: Target Dataset hosting logging events (`isbert_schema_dwtk`).

### 4.4 Target File Plan
To build this job, the Build Agent needs to generate the following relative path components:

| Target File Path | Language | Source Legacy Component | Description |
| :--- | :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_p_basisprod.py` | Python (Airflow) | `DW.BERT_AUSD_BP_TA_P_BASISPROD.xml`, `r_ausd_*.ksh`, `k_ausd_*.ksh` | Orchestrates table truncation, dynamically computes runtime variables (`Stichtag`), and executes the transformation. |
| `sql/d_ausd_bp_ta_p_basisprod.sql` | SQL (BigQuery) | `d_ausd_bp_ta_p_basisprod.sql` | Houses the fully rewritten conversion query mapping all left outer joins, `COALESCE` handling, and APN calculations. |

---

## 5. Risk Assessment & manual Review Points
1.  **Late-Arriving Data (`dwtk_meldungen`):** The legacy query relies heavily on the state of `BERT_DROP_TEMP_TABLE`. If there is high variability in upstream job sequences, this indicator might return an outdated date. Add dependency checks inside Cloud Composer to ensure upstream metadata updates have completed.
2.  **Schema Mismatch on Multi-SIM Columns:** Columns `ms3_*` to `ms10_*` were added during "sim evolution" in the legacy database. The BigQuery schemas for `sof_ta_iccid_vertrag` and `sof_ta_p_basisprod` must explicitly contain all columns up to suffix 10 before initiating this query, otherwise the write will fail.