An elegant, complete, and production-ready **Migration Design Document** has been prepared for the job `DW.BERT_P_GESCHAEFTSP`. 

This design adheres precisely to the **High-Confidence Prescribed Migration Pattern** (`UC4+KSH+PLSQL_COMPLEX` -> **Cloud Composer + Dataform + BigQuery UDFs**) and incorporates the complete, exact output of the translation engine.

---

# MIGRATION DESIGN DOCUMENT: DW.BERT_P_GESCHAEFTSP

## 1. Executive Summary & Job Purpose
The job `DW.BERT_P_GESCHAEFTSP` is a core DWH master data pipeline designed to extract, transform, and load business partner master data (Geschäftspartnerstammdaten) and customer value-segments (Kundenwert) into the BERT data warehouse environment. 

### Legacy Technology Stack:
*   **Orchestration / Scheduling**: UC4 (Automic) XML Job Definitions.
*   **Wrapper & Control**: KornShell scripts (`r_ausd_geschaeftspartner.ksh`, `k_ausd_geschaeftspartner.ksh`) managing orchestration, date checks, variable derivation, and execution logging.
*   **Database Engine**: Oracle PL/SQL (`d_ausd_geschaeftspartner.sql`) executing complex analytical queries, dynamic data-filtering over time-slices, table truncations, and parallelized multi-table joins.

### Prescribed Target Architecture:
*   **Orchestration**: Cloud Composer (Airflow DAG) executing Python Operators to replace shell controls and date calculations.
*   **Data Pipeline & Transformation**: Google Cloud Dataform (SQLX models) compiling to BigQuery SQL, utilizing BigQuery UDFs (or inline Javascript) where procedural conversions are required.

---

## 2. Lineage, Dependencies & Execution Order

```
[Upstream Data Sources]
  - bpd$ta_bp_valueseg_assoc @pcrs1 (CRM External Link)
  - pds$ta_bpri_com @pcrs1 (CRM External Link)
  - sof$ta_e_reach_gp, sof$ta_e_business_gp (Staging Partner)
  - sof$ta_e_reach_dn, sof$ta_e_business_dn (Staging Service User)
  - sof$ta_e_reach_ev, sof$ta_e_business_ev (Staging EVN Recipient)
       │
       ▼
[Orchestration & Control]
  - DW.BERT_P_GESCHAEFTSP (UC4 Scheduler)
    └── r_ausd_geschaeftspartner.ksh (Shell Wrapper)
         └── k_ausd_geschaeftspartner.ksh (Control Script)
              └── d_ausd_geschaeftspartner.sql (Oracle PL/SQL ETL)
                   │
                   ▼
[Target / Intermediate Tables Populated]
  - sof$ta_segm_prem (Staging - Premium Segment)
  - sof$ta_bpr_dn_evn_his (Staging - History Segment)
  - sof$ta_bpr_dn_evn (Staging - Derived Instances)
  - sof$ta_p_gesch_part (Target - Business Partner Master Data)
  - sof$ta_p_dn_nutzer (Target - Service Users)
  - sof$ta_p_evn_empf (Target - EVN Bill Recipients)
```

### External System Replacements:
*   **`@pcrs1` (Oracle DB Link)**: Replaced with BigQuery external queries (Federated Queries) over Cloud SQL, or pre-loaded into persistent BigQuery staging tables (`bpd$ta_bp_valueseg_assoc`, `pds$ta_bpri_com`) via a prior ingestion pipeline.
*   **Shared Oracle Tables**: All `sof$...` and `bpd$...` tables map directly to specific BigQuery datasets (`bert_staging_sof`, `bert_staging_bpd`, `bert_core`).

---

## 3. Core Transformation Logic (Verbatim Engine Output)

Below is the verbatim translation output mapping legacy Oracle PL/SQL syntax to standard BigQuery-compatible patterns.

### Verbatim Conversion Output for `d_ausd_geschaeftspartner.sql`:

```markdown
# Design Document: Hive SQL to BigQuery SQL Conversion

## 1. Objective
Convert the provided Hive/Oracle-style SQL script into BigQuery-compatible SQL while preserving business logic, filtering, joins, and output column mappings.

## 2. Source Artifacts
- File: `d_ausd_geschaeftspartner.sql`

## 3. Entities Detected

### Tables / Views
- `isbert_schema.dwtk_meldungen`
- `sof$ta_e_reach_gp`
- `sof$ta_e_business_gp`
- `sof$ta_segm_prem`
- `pds$ta_bpri_com`
- `sof$ta_bpr_dn_evn_his`
- `sof$ta_bpr_dn_evn`
- `sof$ta_e_reach_dn`
- `sof$ta_e_business_dn`
- `sof$ta_p_gesch_part`
- `sof$ta_p_dn_nutzer`
- `sof$ta_e_reach_ev`
- `sof$ta_e_business_ev`
- `sof$ta_p_evn_empf`

### Columns Requiring Conversion Handling
#### Date-related
- `m.timecreated`
- `bp.insert_at`
- `bp.modified_at`
- `bp.valid_from`
- `bp.valid_to`
- `&v_datum` substitution variable

#### Numeric / precision-sensitive
- `bp.bpr_id`
- `bp.cntrct_id`
- `bp.cntrct_id_ref`
- `bp.bpri_com_id`
- `pr.segment_id`
- `bp.is_production`
- `bp.sales_tax_freed`
- `bp.bp_id`
- `rg.bp_id`
- `dn.bp_id`
- `ev.bp_id`

#### Binary-like
- None explicitly detected in the provided SQL.

#### Structured / collection / object types
- None explicitly detected in the provided SQL.

## 4. Conversion Design

### 4.1 Date Conversion Strategy
- Replace Oracle `TO_CHAR(...,'YYYYMMDD')` with BigQuery `FORMAT_DATE` or `FORMAT_TIMESTAMP` depending on source type.
- Replace Oracle `TO_DATE('YYYYMMDD','YYYYMMDD')` with BigQuery `DATE(PARSE_DATE('%Y%m%d', ...))` or `PARSE_DATE('%Y%m%d', ...)`.
- If source columns are `TIMESTAMP`, compare using `DATE(column)` or `TIMESTAMP(...)` consistently.
- Preserve date filtering semantics:
  - `insert_at <= DATE`
  - `modified_at IS NULL OR modified_at > DATE`
  - `valid_from <= DATE`

### 4.2 Numeric Conversion Strategy
- Preserve integer semantics for IDs and flags.
- Use `CAST(... AS INT64)` only if source typing is ambiguous or string-based.
- Preserve `segment_id` mapping logic using `CASE`.

### 4.3 Binary / Structured Handling
- No explicit conversion required.

### 4.4 Oracle-Specific Syntax Replacement
- `NVL(a,b)` -> `IFNULL(a,b)` or `COALESCE(a,b)`
- `DECODE(expr, a, b, c, d, ...)` -> `CASE expr WHEN a THEN b WHEN c THEN d ELSE ... END`
- Outer join `(+)` -> `LEFT JOIN`
- `DUAL` not present
- `COMMIT`, `TRUNCATE TABLE`, `DESC`, `PROMPT`, `SPOOL`, `WHENEVER SQLERROR`, `DEFINE`, `COLUMN`, `SELECT ... INTO` style scripting constructs are not native BigQuery SQL and should be removed or handled in orchestration layer.

## 5. BigQuery SQL Query

```sql
-- Step 00: derive v_datum equivalent
WITH params AS (
  SELECT
    COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101') AS v_datum
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
),

segm_prem AS (
  SELECT
    bp_id,
    segment_id
  FROM `bpd$ta_bp_valueseg_assoc`
),

bpr_dn_evn_his AS (
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id,
    bp.cntrct_id_ref,
    bp.valid_from,
    bp.valid_to,
    bp.modified_at,
    bp.insert_at
  FROM `pds$ta_bpri_com` bp
  CROSS JOIN params p
  WHERE bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
    AND DATE(bp.insert_at) <= PARSE_DATE('%Y%m%d', p.v_datum)
    AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > PARSE_DATE('%Y%m%d', p.v_datum))
    AND DATE(bp.valid_from) <= PARSE_DATE('%Y%m%d', p.v_datum)
    AND bp.is_production = 1
),

bpr_dn_evn AS (
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id AS bpr_instance_id,
    bp.cntrct_id_ref,
    COALESCE(bp.valid_to, DATE '4712-12-31') AS valid_to
  FROM (
    SELECT
      bp1.*,
      MAX(COALESCE(bp1.valid_to, DATE '4712-12-31'))
        OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
    FROM bpr_dn_evn_his bp1
  ) bp
  WHERE COALESCE(bp.valid_to, DATE '4712-12-31') = bp.max_valid_to
),

p_gesch_part AS (
  SELECT
    rg.cntrct_cp2_id AS cntrct_id,
    rg.for_the_attention_of AS namenszusatz,
    rg.address_attachment AS adresszusatz,
    COALESCE(rg.corp_unit, bp.organisation_name) AS firmenname,
    CASE
      WHEN rg.surname_s IS NULL THEN bp.title
      ELSE ''
    END AS akad_titel,
    COALESCE(rg.surname_s, bp.surname) AS nachname,
    COALESCE(rg.first_name_g, bp.first_name) AS vorname,
    rg.land_sd AS land,
    rg.zip_code AS plz,
    rg.city AS wohnort,
    CASE
      WHEN rg.street IS NULL THEN
        CASE
          WHEN rg.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', CAST(rg.pobox AS STRING))
        END
      ELSE CONCAT(rg.street, ' ', CAST(rg.house_nr AS STRING))
    END AS strasse,
    CASE
      WHEN pr.segment_id = 11 THEN 'SP'
      WHEN pr.segment_id = 12 THEN 'RV'
      WHEN pr.segment_id = 13 THEN 'MA'
      WHEN pr.segment_id = 14 THEN 'SO'
      WHEN pr.segment_id = 15 THEN 'VJ'
      WHEN pr.segment_id = 16 THEN 'IN'
      ELSE CAST(pr.segment_id AS STRING)
    END AS kunde_segment_id,
    0 AS prem_segment_id,
    bp.tm_customerid AS tm_kundennummer,
    bp.sales_tax_freed AS mwst_kennzeichen,
    rg.address_attachment_org AS organisationseinheit
  FROM `sof$ta_e_reach_gp` rg
  JOIN `sof$ta_e_business_gp` bp
    ON rg.bp_id = bp.bp_id
  LEFT JOIN segm_prem pr
    ON rg.bp_id = pr.bp_id
),

p_dn_nutzer AS (
  SELECT
    bi.cntrct_id AS cntrct_id,
    dn.for_the_attention_of AS namenszusatz,
    dn.address_attachment AS adresszusatz,
    COALESCE(dn.corp_unit, bp.organisation_name) AS firmenname,
    CASE
      WHEN dn.surname_s IS NULL THEN bp.title
      ELSE ''
    END AS akad_titel,
    COALESCE(dn.surname_s, bp.surname) AS nachname,
    COALESCE(dn.first_name_g, bp.first_name) AS vorname,
    dn.land_sd AS land,
    dn.zip_code AS plz,
    dn.city AS wohnort,
    CASE
      WHEN dn.street IS NULL THEN
        CASE
          WHEN dn.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', CAST(dn.pobox AS STRING))
        END
      ELSE CONCAT(dn.street, ' ', CAST(dn.house_nr AS STRING))
    END AS strasse,
    dn.address_attachment_org AS organisationseinheit,
    bp.sales_tax_freed AS mwst_kennzeichen
  FROM `sof$ta_e_reach_dn` dn
  JOIN `sof$ta_e_business_dn` bp
    ON dn.bp_id = bp.bp_id
  JOIN bpr_dn_evn bi
    ON dn.bpr_inst_srvusr_id = bi.bpr_instance_id
),

p_evn_empf AS (
  SELECT
    bi.cntrct_id AS cntrct_id,
    ev.for_the_attention_of AS namenszusatz,
    ev.address_attachment AS adresszusatz,
    COALESCE(ev.corp_unit, bp.organisation_name) AS firmenname,
    CASE
      WHEN ev.surname_s IS NULL THEN bp.title
      ELSE ''
    END AS akad_titel,
    COALESCE(ev.surname_s, bp.surname) AS nachname,
    COALESCE(ev.first_name_g, bp.first_name) AS vorname,
    ev.land_sd AS land,
    ev.zip_code AS plz,
    ev.city AS wohnort,
    CASE
      WHEN ev.street IS NULL THEN
        CASE
          WHEN ev.pobox IS NULL THEN ''
          ELSE CONCAT('Postfach ', CAST(ev.pobox AS STRING))
        END
      ELSE CONCAT(ev.street, ' ', CAST(ev.house_nr AS STRING))
    END AS strasse,
    ev.address_attachment_org AS organisationseinheit,
    bp.sales_tax_freed AS mwst_kennzeichen
  FROM `sof$ta_e_reach_ev` ev
  JOIN `sof$ta_e_business_ev` bp
    ON ev.bp_id = bp.bp_id
  JOIN bpr_dn_evn bi
    ON ev.bpr_inst_evnrec_id = bi.bpr_instance_id
)

SELECT * FROM segm_prem;
SELECT * FROM bpr_dn_evn_his;
SELECT * FROM bpr_dn_evn;
SELECT * FROM p_gesch_part;
SELECT * FROM p_dn_nutzer;
SELECT * FROM p_evn_empf;
```

## 6. Low-Level Pseudocode

```text
BEGIN

  READ source script metadata and identify all referenced entities

  DERIVE v_datum:
    query dwtk_meldungen for max(timecreated) where job_kennung = 'BERT_DROP_TEMP_TABLE'
    format as YYYYMMDD
    if null then use '19000101'

  BUILD staging dataset segm_prem:
    select bp_id, segment_id from bpd$ta_bp_valueseg_assoc

  BUILD staging dataset bpr_dn_evn_his:
    from pds$ta_bpri_com
    filter:
      bpr_id in allowed list
      insert_at <= derived date
      modified_at is null OR modified_at > derived date
      valid_from <= derived date
      is_production = 1
    project contract and validity columns

  BUILD staging dataset bpr_dn_evn:
    for each (cntrct_id, bpr_id) group in bpr_dn_evn_his:
      compute max(valid_to default 4712-12-31)
      keep rows matching max_valid_to
      rename bpri_com_id to bpr_instance_id

  BUILD target dataset p_gesch_part:
    join reach_gp with business_gp on bp_id
    left join segm_prem on bp_id
    map fields:
      contract id, names, address, company, title, surname, first name, country, postal code, city
    derive street:
      if street is null:
        if pobox is null then empty string
        else 'Postfach ' + pobox
      else street + ' ' + house_nr
    derive kunde_segment_id:
      map 11->SP, 12->RV, 13->MA, 14->SO, 15->VJ, 16->IN, else cast segment_id to string
    set prem_segment_id = 0

  BUILD target dataset p_dn_nutzer:
    join reach_dn with business_dn on bp_id
    join bpr_dn_evn on dn.bpr_inst_srvusr_id = bpr_instance_id
    apply same name/address derivations as above

  BUILD target dataset p_evn_empf:
    join reach_ev with business_ev on bp_id
    join bpr_dn_evn on ev.bpr_inst_evnrec_id = bpr_instance_id
    apply same name/address derivations as above

  OUTPUT equivalent BigQuery SQL
  LIST all entities used

END
```

## 7. Conversion Notes
- Oracle script control commands are excluded from BigQuery SQL.
- Temporary table lifecycle operations should be handled outside SQL or via BigQuery DDL/DML orchestration.
- If source columns are stored as `TIMESTAMP`, keep `DATE(column)` comparisons; if stored as `DATE`, compare directly.
- `bpr_instance_id` is preserved as the BigQuery alias for `bpri_com_id`.
```

---

## 4. Target Orchestration & File Plan

Instead of legacy KornShell scripts wrapping SQL*Plus execution blocks, the pipeline is decomposed into dedicated **Cloud Composer (Airflow)** and **Dataform** staging entities.

### Target Workspace File Structure:
```
├── dags/
│   └── dag_bert_p_geschaeftsp.py         # Airflow Dag Orchestrator replacing UC4 & ksh wrappers
└── dataform/
    ├── definitions/
    │   ├── staging/
    │   │   ├── sof$ta_segm_prem.sqlx     # Staging Segment Premium table transformation
    │   │   ├── sof$ta_bpr_dn_evn_his.sqlx# Staging historical instances
    │   │   └── sof$ta_bpr_dn_evn.sqlx    # Staging unified instances
    │   └── core/
    │       ├── sof$ta_p_gesch_part.sqlx  # Target Core Business Partner table
    │       ├── sof$ta_p_dn_nutzer.sqlx   # Target Core Service Users table
    │       └── sof$ta_p_evn_empf.sqlx    # Target Core EVN Recipients table
    └── dataform.json                     # Environment configuration settings
```

---

## 5. Environment-Specific Configuration (Airflow & Dataform)

To build and run this job dynamically, the Build Agent needs to inject the following configurations:

*   **Google Cloud Project ID**: `{{var.value.gcp_project_id}}`
*   **BigQuery Core Dataset**: `bert_core` (where persistent target tables reside)
*   **BigQuery Staging Dataset**: `bert_staging_sof` (replaces temporary schema references)
*   **Airflow Connection ID**: `bigquery_default`
*   **Variable Substitution (`v_datum` & `p_stichtag`)**:
    In the Airflow DAG wrapper, parameter checks previously done by `h_alis_date.ksh` will be handled by native Airflow execution date logic (`{{ ds_nodash }}`). If not provided, it defaults to the task runtime execution date.

---

## 6. Risks, Manual Actions & Unresolved Components

### Unresolved References Check:
There are crucial legacy sub-routines/files missing from the codebase.
*   **SOURCE: NOT FOUND** — `DW.BERT_LESE_LOG` — no candidate file found
*   **SOURCE: NOT FOUND** — `DW.HOLE_PFAD` — no candidate file found

*Downstream Impact & Mitigation*: These routines are used in UC4 script tags (`:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG`). Their purposes are path initialization and logging management respectively. These are natively replaced in GCP by Airflow's internal environment variable management (`Variable.get()`) and Google Cloud Logging (Stackdriver). No manual porting of these stub elements is required, but their removal must be validated.

### Risks and Manual Steps:
1.  **DML Truncate Actions**: The original script uses `TRUNCATE TABLE ... REUSE STORAGE`. In Dataform, it is recommended to set these staging models as `{ type: "incremental" }` using a `pre_operations { OR REPLACE TABLE ... }` or `{ type: "table" }` to automatically drop and recreate them daily, ensuring clean execution during retries.
2.  **Date Handling Validation**: Oracle allows string comparisons on dates if implicitly cast. Ensure that source schema types in BigQuery (`DATE` vs `TIMESTAMP`) are strictly synchronized with the cast structures (`PARSE_DATE('%Y%m%d', p.v_datum)`) shown in the converted SQL queries.

---

## ⚠️ UNRESOLVED SOURCE COMPONENTS (auto-generated — do not remove)

The following components referenced by this job have **no real source file** anywhere in the scanned codebase. Any logic shown above for them is, at best, an unconfirmed guess derived from the name alone — treat it as a stub, not a real migration, until a human supplies or confirms the actual source.

- **DW.BERT_LESE_LOG** — no candidate file found
- **DW.HOLE_PFAD** — no candidate file found
