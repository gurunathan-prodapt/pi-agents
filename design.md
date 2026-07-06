# Migration Design Document

**Seed Job Name:** `DW.BERT_P_ADRESSEN`  
**Seed Type:** JOB  
**Source Root:** `/home/gurunathan_t/test_lineage_data`  
**Target Platform:** BigQuery  

---

## 1. System Lineage and Context
The `DW.BERT_P_ADRESSEN` job is orchestrated by Automic UC4, using KornShell scripts to wrap and execute database-level PL/SQL processing in Oracle. 

### 1.1 Lineage Relationships
- **Upstream Producers (Dependencies):**
  - Synchronization locks exist against `DW.BERT_P_GESCHAEFTSP` and `DW.BERT_P_RECHEMPF`.
  - Input tables are read from Carmen schemas (`cds$ta_bp_ref`, `cds$ta_inv_definition`), global lookups (`glv$ta_country`, `glv$ta_description`), and business partner details (`bpd$ta_reachability`, `bpd$ta_business_partner`).
- **Downstream Consumers:**
  - Standard reporting and downstream staging layers that depend on address, reachability, and business partner configurations.
- **Shared Objects & Packages:**
  - PL/SQL wrapper functions from `DWPA_UTIL_SKRIPT` (e.g., dynamic table truncation).
  - Common shell includes: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.

---

## 2. Equivalent BigQuery SQL Query (VERBATIM MCP OUTPUT)

```sql
-- BigQuery Script Parameters
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

DECLARE d_datum DATE DEFAULT PARSE_DATE('%Y%m%d', v_datum);

-- Step 01: Truncate target/temp tables
TRUNCATE TABLE `sof.ta_bp_ref_gp`;
TRUNCATE TABLE `sof.ta_bp_ref_re`;
TRUNCATE TABLE `sof.ta_bp_ref_ev`;
TRUNCATE TABLE `sof.ta_bp_ref_dn`;
TRUNCATE TABLE `sof.ta_bp_ref_gp_nodp`;
TRUNCATE TABLE `sof.ta_bp_ref_re_nodp`;
TRUNCATE TABLE `sof.ta_bp_ref_ev_nodp`;
TRUNCATE TABLE `sof.ta_bp_ref_dn_nodp`;
TRUNCATE TABLE `sof.ta_reachability`;
TRUNCATE TABLE `sof.ta_business_pt`;
TRUNCATE TABLE `sof.ta_country`;
TRUNCATE TABLE `sof.ta_country_desc`;
TRUNCATE TABLE `sof.ta_laender_kng`;
TRUNCATE TABLE `sof.ta_e_reach_gp`;
TRUNCATE TABLE `sof.ta_e_reach_re`;
TRUNCATE TABLE `sof.ta_e_reach_dn`;
TRUNCATE TABLE `sof.ta_e_reach_ev`;
TRUNCATE TABLE `sof.ta_e_business_gp`;
TRUNCATE TABLE `sof.ta_e_business_re`;
TRUNCATE TABLE `sof.ta_e_business_dn`;
TRUNCATE TABLE `sof.ta_e_business_ev`;
TRUNCATE TABLE `sof.ta_e_regulierer`;

-- Step 02a
INSERT INTO `sof.ta_bp_ref_gp`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 4
  AND bpr.address_ref_ty = 6;

-- Step 02b
INSERT INTO `sof.ta_bp_ref_re`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 5

UNION ALL

SELECT
  id.rdndnt_cp2_bp_id AS bp_id,
  id.rdndnt_cp2_reachability_id AS reachability_id,
  NULL AS cntrct_cp2_id,
  id.inv_definition_id AS inv_def_invrec_id,
  NULL AS bpr_inst_evnrec_id,
  NULL AS bpr_inst_srvusr_id
FROM `cds.ta_inv_definition` id
WHERE id.insert_at <= d_datum
  AND (id.modified_at IS NULL OR id.modified_at > d_datum)
  AND id.valid_from <= d_datum
  AND (id.valid_to IS NULL OR id.valid_to > d_datum)
  AND id.is_production = 1
  AND id.rdndant_invrec = 0;

-- Step 02c
INSERT INTO `sof.ta_bp_ref_ev`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 7;

-- Step 02d
INSERT INTO `sof.ta_bp_ref_dn`
  (BP_ID, REACHABILITY_ID, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID, BPR_INST_SRVUSR_ID)
SELECT
  bpr.bp_id,
  bpr.reachability_id,
  bpr.cntrct_cp2_id,
  bpr.inv_def_invrec_id,
  bpr.bpr_inst_evnrec_id,
  bpr.bpr_inst_srvusr_id
FROM `cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 1
  AND bpr.address_ref_ty = 8;

-- Step 03a
INSERT INTO `sof.ta_country`
  (COUNTRY_CODE, DESCRIPTION_ID, PARENT_COUNTRY_CODE, EU_INDICATOR, SAP_CODE, CORR_CODE, VALID)
SELECT
  country.country_code,
  country.description_id,
  country.parent_country_code,
  country.eu_indicator,
  country.sap_code,
  country.corr_code,
  country.valid
FROM `glv.ta_country` country;

-- Step 03b
INSERT INTO `sof.ta_country_desc`
  (DESCRIPTION_ID, LANGUAGE, SHORT_DESCRIPTION, DESCRIPTION, LONG_DESCRIPTION)
SELECT
  des.description_id,
  des.language,
  des.short_description,
  des.description,
  des.long_description
FROM `glv.ta_description` des;

-- Step 03c
INSERT INTO `sof.ta_laender_kng`
  (COUNTRY_CODE, DESCRIPTION_ID, LANGUAGE, SHORT_DESCRIPTION, DESCRIPTION, LONG_DESCRIPTION)
SELECT
  co.country_code,
  de.description_id,
  de.language,
  de.short_description,
  de.description,
  de.long_description
FROM `sof.ta_country` co
JOIN `sof.ta_country_desc` de
  ON co.description_id = de.description_id
WHERE co.valid = 1;

-- Step 03e
INSERT INTO `sof.ta_reachability`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru
FROM `bpd.ta_reachability` re
WHERE re.insert_at <= d_datum
  AND (re.modified_at IS NULL OR re.modified_at > d_datum)
  AND re.valid_from <= d_datum
  AND (re.valid_to IS NULL OR re.valid_to > d_datum)
  AND re.is_production = 1;

-- Step 03f
INSERT INTO `sof.ta_e_reach_gp`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM `sof.ta_bp_ref_gp` br
JOIN `sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03g
INSERT INTO `sof.ta_e_reach_re`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM `sof.ta_bp_ref_re` br
JOIN `sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03h
INSERT INTO `sof.ta_e_reach_ev`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM `sof.ta_bp_ref_ev` br
JOIN `sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 03i
INSERT INTO `sof.ta_e_reach_dn`
  (BP_ID, REACHABILITY_ID, OBJ_VERSION, COUNTRY_CODE, FOR_THE_ATTENTION_OF, ADDRESS_ATTACHMENT,
   ADDRESS_ATTACHMENT_ORG, CORP_UNIT, SURNAME_S, FIRST_NAME_G, ZIP_CODE, CITY, POBOX, STREET,
   HOUSE_NR, PUBLIC_AREA_A, PRIVATE_AREA_P, CORP_UNIT_OU1, ADDRESS_LINE_1, ADDRESS_LINE_2,
   REACHABLE_FROM, REACHABLE_THRU, CNTRCT_CP2_ID, INV_DEF_INVREC_ID, BPR_INST_EVNREC_ID,
   BPR_INST_SRVUSR_ID, LAND_SD)
SELECT
  re.bp_id,
  re.reachability_id,
  re.obj_version,
  re.country_code,
  re.for_the_attention_of,
  re.address_attachment,
  re.address_attachment_org,
  re.corp_unit,
  re.surname_s,
  re.first_name_g,
  re.zip_code,
  re.city,
  re.pobox,
  re.street,
  re.house_nr,
  re.public_area_a,
  re.private_area_p,
  re.corp_unit_ou1,
  re.address_line_1,
  re.address_line_2,
  re.reachable_from,
  re.reachable_thru,
  br.cntrct_cp2_id,
  br.inv_def_invrec_id,
  br.bpr_inst_evnrec_id,
  br.bpr_inst_srvusr_id,
  SUBSTR(lk.short_description, 1, 3) AS land_sd
FROM `sof.ta_bp_ref_dn` br
JOIN `sof.ta_reachability` re
  ON br.bp_id = re.bp_id
 AND br.reachability_id = re.reachability_id
LEFT JOIN `sof.ta_laender_kng` lk
  ON re.country_code = lk.country_code;

-- Step 04a
INSERT INTO `sof.ta_business_pt`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `bpd.ta_business_partner` bp
WHERE bp.insert_at <= d_datum
  AND (bp.modified_at IS NULL OR bp.modified_at > d_datum);

-- Step 04b
INSERT INTO `sof.ta_bp_ref_gp_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `sof.ta_bp_ref_gp`;

INSERT INTO `sof.ta_e_business_gp`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `sof.ta_bp_ref_gp_nodp` br
JOIN `sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04d
INSERT INTO `sof.ta_bp_ref_re_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `sof.ta_bp_ref_re`;

INSERT INTO `sof.ta_e_business_re`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `sof.ta_bp_ref_re_nodp` br
JOIN `sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04f
INSERT INTO `sof.ta_bp_ref_ev_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `sof.ta_bp_ref_ev`;

INSERT INTO `sof.ta_e_business_ev`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `sof.ta_bp_ref_ev_nodp` br
JOIN `sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 04h
INSERT INTO `sof.ta_bp_ref_dn_nodp`
  (BP_ID)
SELECT DISTINCT bp_id
FROM `sof.ta_bp_ref_dn`;

INSERT INTO `sof.ta_e_business_dn`
  (BP_ID, ORGANISATION_NAME, TITLE, SURNAME, FIRST_NAME, SALES_TAX_FREED, TM_CUSTOMERID)
SELECT
  bp.bp_id,
  bp.organisation_name,
  bp.title,
  bp.surname,
  bp.first_name,
  bp.sales_tax_freed,
  bp.tm_customerid
FROM `sof.ta_bp_ref_dn_nodp` br
JOIN `sof.ta_business_pt` bp
  ON br.bp_id = bp.bp_id;

-- Step 05
INSERT INTO `sof.ta_e_regulierer`
  (INV_DEF_MOPREF_ID, MOP_BP_ID, MEANS_OF_PAYMENT_ID)
SELECT
  bpr.inv_def_mopref_id,
  bpr.mop_bp_id,
  bpr.means_of_payment_id
FROM `cds.ta_bp_ref` bpr
WHERE bpr.insert_at <= d_datum
  AND (bpr.modified_at IS NULL OR bpr.modified_at > d_datum)
  AND bpr.valid_from <= d_datum
  AND (bpr.valid_to IS NULL OR bpr.valid_to > d_datum)
  AND bpr.is_production = 1
  AND bpr.bp_ref_ty = 2
  AND bpr.mop_ref_ty = 1;
```

---

## 3. High-Level Architecture & Translation of Orchestration

### 3.1 Orchestration Replacement (UC4 to Airflow)
The original UC4 XML (`DW.BERT_P_ADRESSEN.xml`) contains execution control that calls KornShell wrapper scripts. In the target environment, this job will be consolidated into an Airflow DAG.

```
       +---------------------------------------------+
       |             Airflow DAG/Task                |
       +---------------------+-----------------------+
                             |
                             v
       +---------------------------------------------+
       |   Python Operator (BigQuery Client/Script)  |
       +---------------------+-----------------------+
                             | (Executes parsed SQL)
                             v
       +---------------------------------------------+
       |           Target BigQuery Dataset           |
       +---------------------------------------------+
```

### 3.2 Target File Plan
The complex multi-file legacy call chain will be resolved and simplified into a structured target set of components:

| Target File Path | Target Tech | Sources Compiled From | Description |
| :--- | :--- | :--- | :--- |
| `dags/dw_bert_p_adressen_dag.py` | Python (Airflow) | `DW.BERT_P_ADRESSEN.xml`, `r_ausd_adressen.ksh`, `k_ausd_adressen.ksh` | Main workflow orchestrator DAG. Handles parameter setup (key date parsing) and calls SQL script execution. |
| `sql/d_ausd_adressen.sql` | BigQuery SQL Script | `d_ausd_adressen.sql` | Executable BigQuery procedural script containing DDL/DML processing steps. |

---

## 4. Environment-Specific Mapping and Configurations

### 4.1 Schema Mappings
To conform to Google Cloud best practices, legacy schema hierarchies are routed into explicit BigQuery datasets:

- `isbert_schema.` $\rightarrow$ `gcp_project_id.isbert_schema`
- `cds$` prefixed tables $\rightarrow$ `gcp_project_id.cds`
- `glv$` prefixed tables $\rightarrow$ `gcp_project_id.glv`
- `bpd$` prefixed tables $\rightarrow$ `gcp_project_id.bpd`
- `sof$` prefixed tables $\rightarrow$ `gcp_project_id.sof`

### 4.2 Handling of Database Links & Variables
- The dynamic database link placeholder `&v_carmen` (defined in Oracle as `@pcrs1`) is eliminated. Under BigQuery, federation or scheduled replication handles the sync of CARMEN data to the local GCP project datasets.
- Parameter passing (Stichtag / `v_datum` value) is orchestrated dynamically using Airflow template variables (e.g. `{{ ds_nodash }}`) mapped into BigQuery script parameters.

---

## 5. Risks & Manual Intervention Checklist
1. **DWTK_MELDUNGEN Parameterization**: The fallback date lookup uses a table called `dwtk_meldungen`. If this metadata table is not actively populated in BigQuery, the fallback logic should be replaced with Airflow logical dates.
2. **Intermediate/Temp Table Performance**: The procedural script relies heavily on intermediate tables (such as `sof.ta_bp_ref_gp_nodp`). If execution latency or cost is a concern, several of these single-use tables can be refactored into persistent temporary tables (`CREATE TEMP TABLE`) or common table expressions (CTEs) within larger SQL queries.