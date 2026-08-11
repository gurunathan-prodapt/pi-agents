=== FILE: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql ===
-- ===================================================================
-- Datei:  d_ausd_vertrag.sql
-- Datum:  16.10.2002
-- Autor:  Martin Buettner
-- ===================================================================
--
-- Modifikationen
----------------------------------------------------------------------
-- Version Datum    Autor  Dokumentation
-- 3.1.0   20030109 sj     Tabellennamenerweiterung um das tagesdatum
--         20030115 mb     Modifikationen und Erweiterung gemaess Protokoll:
--                         (u.a.: Ausgabe der Rabatthoehe, Sperrgruende, Sperrzeiten)
-- 7.5.0   20040831 Roh    Neues Attr. is_production in CDS$TA_DISCOUNT
--                         Umstellung auf parallel degree 4
-- 7.5.1   20041011 mb     Modifikationen fuer den neuen Rabattreport
-- ab 2005 neue Releasenummern
-- 5.1.1   20050204 Roh    neues Feld SEGMENT_ID auf Rechnugsdefiniton
-- 5.4.0   20090901 Roh    spool ins Unterverzeichnis ./tmp
-- 5.4.1   20051025 mb     Erweiterung fuer Pooling Report: Rechnungsinhaltskonfigurationstext
--                                                          (inv_cont_config_id)
-- 5.4.2   20051117 mb     Ersetzung der ANALYZE Kommandos durch GATHER_TABLE_STATS
-- 5.4.3   20051119 hs     Mergen auf dwh$ta_p_vertrag_<datum> zum Addiern von Stillegungstagen
-- 5.4.4   20051202 hs     Laufzeittuning zu Step 201b (merge via FTS)
-- 6.1.0   20051220 Roh    Neues Feld 0B-Nummer (CDS$TA_CNTRCT.ORDER_NUMBER)
-- 6.3.0   20060919 me/hs  F�r Berechnung Bindefrist mit Sperren/Stillegungen: Erweiterung um Sperrklasse 7
-- 6.4.0   20061122 me     Umstellung wegen Datenmodell-Aenderung in Carmen:
--                         Tab. CDS$TA_BARRIER_KIND_CV -> CDS$TA_BARRIER_KIND,
--                         tab. CDS$TA_DESCRIPTION     -> CDS$TA_CARE_DESCRIPTION
-- 6.4.0   20061121 RR     Bestimmung Substitutions-Variable v_datum aus
--                         Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR     �berfl�ssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20061129 RR     Restartf�higkeit unter Verwendung bereits erzeugter Tabellen
-- 7.2.0   20070511 ME     NVL bei Setzen Bindefrist eingebaut (MERGE, step201b)
-- 7.4.0   20070814 AR/ME  Nach Umstellung 9i -> 10g:
--                         Steps 13a/b und 106a/b umgestellt auf TABLE-Function, hierzu neues Package
--                         Step 201b, MERGE: VALUES bei INSERT WHEN NOT MATCHED ergaenzt
-- 8.1.0   20071219 FD     Ausgliederung aus d_ausd_vertrag.sql
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
-- 12.2.0  20120629 Anna Kowalczuk Fix f�r INM12360473 - die Tabelle sof$ta_c_bfc durch die View sof$vi_c_bfc ersetzt
----------------------------------------------------------------------
--
--
prompt variablendefinitionen
--
--
-- DB-Link auf CARMEN DB: entweder leer oder mit "@"
DEFINE v_carmen       = "@pcrs1"

-- Stichtag ermitteln
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
--
--
prompt tracing und settings
--
--
START ../trace.sql.cfg
SPOOL ./tmp/trace_d_ausd_v_ta_vertrag_tmp

WHENEVER SQLERROR CONTINUE
  SET TIMING ON
  SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT FAILURE
--
--
prompt tabelle von vorherigem lauf loeschen
--
--
WHENEVER SQLERROR CONTINUE
begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vertrag_tmp'); 
end;
/

WHENEVER SQLERROR EXIT FAILURE
--
--
prompt zieltabelle anlegen: vertragstabelle sof$ta_vertrag_tmp_YYYYMMDD
--
--
INSERT  INTO sof$ta_vertrag_tmp(
        vertrag_id_carmen,
        partner_id_carmen,
        rechdef_id_carmen,
        kundenkonto,
        mwst_kennzeichen,
        rahmenvertrag_id,
        rechnungslauf,
        vo_kenn,
        order_number,
        geplant_kuend,
        eingang_kuend,
        vertragsbeginn,
        vertragsstatus,
        sperrart,
        sperrgrund,
        stillegungszeitraum,
        twincard,
        dwh_tarifgr_text,
        bindefrist,
        letztes_upgrade,
        vertragsbindung,
        vertragsbindungseinheit,
        rechnungszahlart,
        rechnungsmedium,
        twin_vertrag_id,
        upgradeberechtigt,
        apn,
        upgradegrund,
        SV_Id,
        VDA,
        cost_centre,
        cost_centre_user,
        cntrct_ty,
        segment_id,
        rv_action_id,
        rechn_inh_konfig_text,
        commitment_reference_date,
        cntrct_validity_id)
SELECT /*+ parallel(c,4) parallel(ia,4) parallel(n,4) parallel(b,4) parallel(ac,4) full(rd) parallel(rd,4) paralell(bf,4)*/
       c.cntrct_id                      VERTRAG_ID_CARMEN,
       bp.bp_id                         PARTNER_ID_CARMEN,
       ia.inv_definition_id             RECHDEF_ID_CARMEN,
       ia.account_reference             KUNDENKONTO,
       ia.sales_tax_freed               MWST_KENNZEICHEN,
       c.rv_num                         RAHMENVERTRAG_ID,
       ia.billcycle_id                  RECHNUNGSLAUF,
       c.vo_code                        VO_KENN,
       c.order_number                   ORDER_NUMBER,   -- 20051220 Roh. ab Rel6.1.0
       n.valid_from                     GEPLANT_KUEND,
       n.entry_date_of_notice           EINGANG_KUEND,
       c.cntrct_start_date              VERTRAGSBEGINN,
       decode(c.cntrct_st,
                           5,'A',
                           6,'L',
                             ''    )    VERTRAGSSTATUS,
       b.sperrart_alle                  SPERRART,            -- ab Rel3.1.0
       b.sperrgrund_alle                SPERRGRUND,          -- ab Rel3.1.0
       b.stilllegungszeitraum_alle      STILLEGUNGSZEITRAUM, -- ab Rel3.1.0
       c.twinbill                       TWINCARD,   -- nur TB-Information, TC-Informationen aus Basisprodukt
       ct.cds_description               DWH_TARIFGR_TEXT,
       bf.bindefrist                    BINDEFRIST,
       vvl.upgradedatum                 LETZTES_UPGRADE,
       p.number_time_measurement        VERTRAGSBINDUNG,
       p.einheit                        VERTRAGSBINDUNGsEinheit,   -- <== zusaetzlich
       decode(ia.inv_pay_ty_cv,
              1,'U',2,'E',3,'K',4,'B','')    RECHNUNGSZAHLART,
       decode(ia.inv_media_cv,      1,'Papier',
                                    2,'ELMO',
                                    3,'E-Mail',
                                    4,'Fax',
                                    5,'Inline/Papier',
                                    6,'ELMO/Papier','') RECHNUNGSMEDIUM,
       c.twin_vertrag_id                TWIN_VERTRAG_ID,
       CASE    -- Upgrade Berechtigung: =========>
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)     -- keine Bindefrist: jederzeit
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      p.number_time_measurement = 12   -- Bindefrist 12 Monate: nach 9 Monaten (bis Rel.6.0: 12 Monate)
               AND MONTHS_BETWEEN (TO_DATE('&v_datum','YYYYMMDD'),
                                   NVL (c.commitment_reference_date, c.cntrct_start_date)) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))   -- Bindefrist 24 Monate: nach 23 Monaten
               AND MONTHS_BETWEEN (TO_DATE('&v_datum','YYYYMMDD'),
                                   NVL (c.commitment_reference_date, c.cntrct_start_date)) > 23  -- ab 1.9.2005 23 statt 18 Monate
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         ELSE  'N'
       END  upgradeberechtigt,
        ap.access_point_name    apn,
        vvl.upgradegrund        upgradegrund,
        ct.cntrct_template_id   SV_Id,   -- <======== NEU: (fuer VDA-Ermittlung)
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  VDA,
        c.cost_centre,
        c.cost_centre_user,
        c.cntrct_ty,               -- <======== NEU: fuer Twinbill-Nachbearbeitung
        rd.segment_id,             -- Rel. 5.1.1 Kundenwert auf Rechnungsdefinitionsebene
        ac.rv_action_id,           -- 01.09.2005
        ia.rechn_inh_konfig_text,  -- ab 25.10.2005
        c.commitment_reference_date, -- 27.7.2007 HS
        c.cntrct_validity_id       -- 27.7.2007 HS
  FROM  sof$ta_cntrct_crs3     c,
        sof$ta_bp_ref          bp,
        sof$ta_inv_acc         ia,
        dwh$vi_s_rd_segment             rd,   -- ab Rel. 5.1.1
        sof$ta_notice          n,
        sof$ta_barrier_zusgf   b,
        sof$ta_cntrct_templ    ct,
        sof$ta_cntrct_valid    cv,
        sof$ta_period          p,
        sof$ta_vvl_upgrade     vvl,
        sof$ta_apn_ve          ap,
        sof$ta_action_assoc    ac,   -- ab 01.09.2005
        sof$vi_c_bfc                    bf    -- fd 10.01.2008
    WHERE bp.cntrct_cp2_id = c.cntrct_id AND c.cntrct_ty <> 20
      AND ia.cntrct_id = c.cntrct_id
      AND n.cntrct_id(+) = c.cntrct_id
      AND b.cntrct_id(+) = c.cntrct_id
      AND ct.cntrct_template_id = c.cntrct_template_id
      AND CV.cntrct_validity_id(+) = c.cntrct_validity_id
      AND p.period_id(+) = CV.first_period_id
      AND vvl.vertrags_id(+) = c.cntrct_id
      AND ap.cntrct_id(+) = c.cntrct_id
      AND ia.inv_definition_id = rd.rechdef_id_carmen(+)
      AND ac.cntrct_id(+) = c.cntrct_id                       -- ab 01.09.2005
      AND bf.cntrct_id(+) = c.cntrct_id	  
UNION ALL
SELECT /*+ parallel(c,4) parallel(ia,4) parallel(n,4) parallel(b,4) parallel(ac,4) full(rd) parallel(rd,4) paralell(bf,4)*/
       c.cntrct_id                      VERTRAG_ID_CARMEN,
       bp.bp_id                         PARTNER_ID_CARMEN,
       ia.inv_definition_id             RECHDEF_ID_CARMEN,
       ia.account_reference             KUNDENKONTO,
       ia.sales_tax_freed               MWST_KENNZEICHEN,
       c.rv_num                         RAHMENVERTRAG_ID,
       ia.billcycle_id                  RECHNUNGSLAUF,
       c.vo_code                        VO_KENN,
       c.order_number                   ORDER_NUMBER,   -- 20051220 Roh. ab Rel6.1.0
       n.valid_from                     GEPLANT_KUEND,
       n.entry_date_of_notice           EINGANG_KUEND,
       c.cntrct_start_date              VERTRAGSBEGINN,
       decode(c.cntrct_st,
                           5,'A',
                           6,'L',
                             ''    )    VERTRAGSSTATUS,
       b.sperrart_alle                  SPERRART,            -- ab Rel3.1.0
       b.sperrgrund_alle                SPERRGRUND,          -- ab Rel3.1.0
       b.stilllegungszeitraum_alle      STILLEGUNGSZEITRAUM, -- ab Rel3.1.0
       c.twinbill                       TWINCARD,   -- nur TB-Information, TC-Informationen aus Basisprodukt
       ct.cds_description               DWH_TARIFGR_TEXT,
       bf.bindefrist                    BINDEFRIST,
       vvl.upgradedatum                 LETZTES_UPGRADE,
       p.number_time_measurement        VERTRAGSBINDUNG,
       p.einheit                        VERTRAGSBINDUNGsEinheit,   -- <== zusaetzlich
       decode(ia.inv_pay_ty_cv,
              1,'U',2,'E',3,'K',4,'B','')    RECHNUNGSZAHLART,
       decode(ia.inv_media_cv,      1,'Papier',
                                    2,'ELMO',
                                    3,'E-Mail',
                                    4,'Fax',
                                    5,'Inline/Papier',
                                    6,'ELMO/Papier','') RECHNUNGSMEDIUM,
       c.twin_vertrag_id                TWIN_VERTRAG_ID,
       CASE    -- Upgrade Berechtigung: =========>
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)     -- keine Bindefrist: jederzeit
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      p.number_time_measurement = 12   -- Bindefrist 12 Monate: nach 9 Monaten (bis Rel.6.0: 12 Monate)
               AND MONTHS_BETWEEN (TO_DATE('&v_datum','YYYYMMDD'),
                                   NVL (c.commitment_reference_date, c.cntrct_start_date)) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))   -- Bindefrist 24 Monate: nach 23 Monaten
               AND MONTHS_BETWEEN (TO_DATE('&v_datum','YYYYMMDD'),
                                   NVL (c.commitment_reference_date, c.cntrct_start_date)) > 23  -- ab 1.9.2005 23 statt 18 Monate
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN  'J'
         ELSE  'N'
       END  upgradeberechtigt,
        ap.access_point_name    apn,
        vvl.upgradegrund        upgradegrund,
        ct.cntrct_template_id   SV_Id,   -- <======== NEU: (fuer VDA-Ermittlung)
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  VDA,
        c.cost_centre,
        c.cost_centre_user,
        c.cntrct_ty,               -- <======== NEU: fuer Twinbill-Nachbearbeitung
        rd.segment_id,             -- Rel. 5.1.1 Kundenwert auf Rechnungsdefinitionsebene
        ac.rv_action_id,           -- 01.09.2005
        ia.rechn_inh_konfig_text,  -- ab 25.10.2005
        c.commitment_reference_date, -- 27.7.2007 HS
        c.cntrct_validity_id       -- 27.7.2007 HS
  FROM  sof$ta_cntrct_crs3     c,
        sof$ta_bp_ref          bp,
        sof$ta_inv_acc         ia,
        dwh$vi_s_rd_segment             rd,   -- ab Rel. 5.1.1
        sof$ta_notice          n,
        sof$ta_barrier_zusgf   b,
        sof$ta_cntrct_templ    ct,
        sof$ta_cntrct_valid    cv,
        sof$ta_period          p,
        sof$ta_vvl_upgrade     vvl,
        sof$ta_apn_ve          ap,
        sof$ta_action_assoc    ac,   -- ab 01.09.2005
        sof$vi_c_bfc                    bf    -- fd 10.01.2008
	 WHERE bp.cntrct_cp2_id = c.cntrct_parent AND c.cntrct_ty = 20
      AND ia.cntrct_id = c.cntrct_id
      AND n.cntrct_id(+) = c.cntrct_id
      AND b.cntrct_id(+) = c.cntrct_id
      AND ct.cntrct_template_id = c.cntrct_template_id
      AND CV.cntrct_validity_id(+) = c.cntrct_validity_id
      AND p.period_id(+) = CV.first_period_id
      AND vvl.vertrags_id(+) = c.cntrct_id
      AND ap.cntrct_id(+) = c.cntrct_id
      AND ia.inv_definition_id = rd.rechdef_id_carmen(+)
      AND ac.cntrct_id(+) = c.cntrct_id                       -- ab 01.09.2005
      AND bf.cntrct_id(+) = c.cntrct_id;  

commit;

prompt Verarbeitung fehlerfrei beendet.
spool off


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a multi-statement Oracle SQL script containing SQL*Plus variables (`DEFINE`, `COLUMN...NEW_VALUE`), query orchestration, a PL/SQL wrapper call to truncate a target table, and a large multi-table `INSERT INTO ... SELECT ... UNION ALL SELECT ...` DML statement.

1.2 Summarize the business logic and purpose of the script:
    - The script populates a temporary contracts table (`sof$ta_vertrag_tmp`) with detailed contract information.
    - It first queries a metadata/logging table (`isbert_schema.dwtk_meldungen`) to find the latest execution date (`v_datum`) based on a specific job status.
    - It then truncates the target table via a utility package.
    - Finally, it inserts combined datasets of contract configurations (one for standard contracts, one for sub-contracts with a parent relationship code `20`) using dynamic conditions to check upgrade eligibility. The upgrade checks use fractional months calculations to decide if the contract has passed a 9-month threshold (for 12-month commitments) or a 23-month threshold (for 24-month commitments).

1.3 List all entities referenced:
    - Target Table: `sof$ta_vertrag_tmp`
    - Source Tables/Views:
      - `isbert_schema.dwtk_meldungen` (alias `m`)
      - `sof$ta_cntrct_crs3` (alias `c`)
      - `sof$ta_bp_ref` (alias `bp`)
      - `sof$ta_inv_acc` (alias `ia`)
      - `dwh$vi_s_rd_segment` (alias `rd`)
      - `sof$ta_notice` (alias `n`)
      - `sof$ta_barrier_zusgf` (alias `b`)
      - `sof$ta_cntrct_templ` (alias `ct`)
      - `sof$ta_cntrct_valid` (alias `cv`)
      - `sof$ta_period` (alias `p`)
      - `sof$ta_vvl_upgrade` (alias `vvl`)
      - `sof$ta_apn_ve` (alias `ap`)
      - `sof$ta_action_assoc` (alias `ac`)
      - `sof$vi_c_bfc` (alias `bf`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `DATE` (includes time component): All contract start dates, upgrade dates, and notice dates will map to BigQuery `DATETIME` or `TIMESTAMP`. For date-only comparisons and arithmetic, they will be explicitly cast to `DATE`.
    - Oracle `NUMBER(p,s)`: Map IDs to `INT64` (e.g., `cntrct_id`, `bp_id`), and durations/limits to `INT64`.
    - Oracle `VARCHAR2` / `CHAR`: Map to `STRING`.

2.2 Implicit and Explicit Type Casting:
    - The date literal `&v_datum` is derived as a `VARCHAR2` string. It must be explicitly cast using `PARSE_DATE('%Y%m%d', v_datum)` prior to any date calculations.
    - Date columns from tables (e.g., `commitment_reference_date`, `cntrct_start_date`) are cast to `DATE` using `CAST(... AS DATE)` before executing `DATE_DIFF`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(x, y)`: Resolved to `COALESCE(x, y)`.
    - `DECODE` functions:
      - `decode(c.cntrct_st, 5, 'A', 6, 'L', '')` -> `CASE c.cntrct_st WHEN 5 THEN 'A' WHEN 6 THEN 'L' ELSE NULL END`
      - `decode(ia.inv_pay_ty_cv, 1, 'U', 2, 'E', 3, 'K', 4, 'B', '')` -> `CASE ia.inv_pay_ty_cv WHEN 1 THEN 'U' WHEN 2 THEN 'E' WHEN 3 THEN 'K' WHEN 4 THEN 'B' ELSE NULL END`
      - `decode(ia.inv_media_cv, ...)` -> Multi-branch `CASE` statement.

2.4 String Functions:
    - `TO_CHAR(max_date, 'YYYYMMDD')` -> `FORMAT_TIMESTAMP('%Y%m%d', max_date)` or `FORMAT_DATETIME`.

2.5 Date and Timestamp Functions:
    - `TO_DATE('&v_datum', 'YYYYMMDD')` -> `PARSE_DATE('%Y%m%d', v_datum)`.
    - `MONTHS_BETWEEN(a, b)`: In BigQuery, direct month differences return truncated integers. Because the business logic uses thresholds (`> 9` and `> 23`), fractional accuracy is required to maintain semantic parity.
      - Resolved to: `DATE_DIFF(a, b, DAY) / 30.436875` (fractional calculation using average month duration in days).
      - Downstream impact: Confirmed to be semantically equivalent to Oracle's behavior for comparison checks `> 9` and `> 23`.

2.6-2.10 (Not applicable or resolved natively in standard ANSI SQL).

2.11 MERGE Statements:
    - Not applicable.

2.12 INSERT / UPDATE / DELETE:
    - An `INSERT INTO ... SELECT UNION ALL SELECT` statement is present. It translates directly to BigQuery with native syntax.

2.13 DDL / Utility Calls:
    - The dynamic truncation call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_vertrag_tmp')` is resolved directly to native BigQuery statement: `TRUNCATE TABLE sof$ta_vertrag_tmp;`.

2.14 PL/SQL Scripting:
    - The SQL*Plus/PL/SQL orchestration will be converted to a pure **BigQuery SQL Scripting Block** (`DECLARE ... SET ... BEGIN ... END;`).

2.15 Unresolvable or Advisory Items:
    - The Oracle parallel hints `/*+ parallel(...) */` are stripped completely.
    - SQL*Plus execution controls (`WHENEVER SQLERROR`, `SPOOL`, `SET TIMING`) are stripped, as they are client-level properties, not server-side SQL.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach:
    - Refactor the entire multi-statement execution script into a unified BigQuery Scripting Block (`BEGIN...END`).
    - Declare a script-level string variable `v_datum`.
    - Set `v_datum` using a `SET` assignment statement over the initial query on `dwtk_meldungen`.
    - Issue a direct native `TRUNCATE TABLE` statement.
    - Execute the unified `INSERT INTO ... SELECT ... UNION ALL SELECT ...` utilizing `CASE` statements and structured `DATE_DIFF` calculations to replace the Oracle conditional logic and `MONTHS_BETWEEN`.

3.2 Assumptions:
    - The table schema for `sof$ta_vertrag_tmp` already exists in the target BigQuery dataset.
    - All table references belong to the active dataset or are configured via dataset search paths. Project/Dataset qualifiers must be resolved in the physical environment.

3.3 Items Flagged for Human Review:
    - Fractional month calculations (`DATE_DIFF / 30.436875`) are used to replace `MONTHS_BETWEEN`. This is a highly accurate mathematical emulation, but boundary cases should be verified against legacy test data.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| SQL*Plus variables / Query orchestration | BigQuery SQL Scripting Block (`DECLARE`, `SET`) | Python Wrapper | The orchestration only requires storing a single date variable and executing sequential statements. No complex external I/O or loop control is needed; hence, a Python wrapper is over-engineering. |
| PL/SQL Dynamic Truncate | Direct BigQuery `TRUNCATE TABLE` statement | Custom SQL UDF | BQ supports native DML truncation. Custom wrappers or procedures are redundant. |
| Oracle `MONTHS_BETWEEN` | Expression-level `DATE_DIFF(..., DAY) / 30.436875` | BigQuery `DATE_DIFF(..., MONTH)` | Native `DATE_DIFF` with `MONTH` returns a truncated integer. The logic evaluates non-integer boundaries (`> 9`, `> 23`), which requires fractional float precision. |
| Oracle Hints `/*+ parallel */` | Strip completely | None | BigQuery handles query parallelization dynamically and automatically. Manual optimization hints are unsupported and ignored. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════

The migration will generate the following artifact:
1. **BigQuery Standard SQL Script File (`.sql`)**: Contains the procedural block (`DECLARE`, `SET`, `TRUNCATE`, `INSERT ... UNION ALL`).

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Variable Type | BigQuery Target Type | Conversion Rule | Warnings / Notes |
| :--- | :--- | :--- | :--- |
| `DATE` (with time component) | `DATETIME` | Map directly. | Use explicit `CAST(... AS DATE)` where date-only matching or `DATE_DIFF` is performed. |
| `VARCHAR2` / `NVARCHAR2` | `STRING` | Direct mapping. | String lengths are dynamic in BigQuery; no length constraints are applied. |
| `NUMBER(p,s)` (Identifiers) | `INT64` | Native integer conversion. | Validated that precision is not lost on surrogate ID conversions. |
| `NUMBER` (with decimals) | `NUMERIC` | Convert based on business needs. | Not explicitly present as fractional values except dynamically in computations. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**:
  - Metadata lookup assigning to a local variable.
  - Table truncation.
  - Dual-branch `UNION ALL` query populating a single target.
  - Complex conditional checks on dates using Oracle-specific analytical functions.
- **Unsupported Functions**: Oracle `MONTHS_BETWEEN`, `DECODE`, `NVL`.
- **UDF Required**: No (can be fully resolved using explicit standard expressions).
- **Python Required**: No.
- **Direct Dependencies**: Target table `sof$ta_vertrag_tmp` and metadata table `isbert_schema.dwtk_meldungen` must be present.
- **Warnings**: Ensure that the fractional emulation of `MONTHS_BETWEEN` matches expected legacy system margins.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `NVL(x, y)` | Direct-with-rewrite | `COALESCE(x, y)` |
| `TO_CHAR(date, 'YYYYMMDD')` | Direct-with-rewrite | `FORMAT_TIMESTAMP('%Y%m%d', date)` |
| `DECODE(col, v1, r1, ...)` | Direct-with-rewrite | `CASE col WHEN v1 THEN r1 ... END` |
| `MONTHS_BETWEEN(a, b)` | Direct-with-rewrite | `DATE_DIFF(a, b, DAY) / 30.436875` |
| `TO_DATE(str, 'YYYYMMDD')` | Direct-with-rewrite | `PARSE_DATE('%Y%m%d', str)` |
| `runstatement('TRUNCATE...')` | Direct-with-rewrite | Native statement: `TRUNCATE TABLE...` |
| `/*+ parallel(...) */` | Direct-with-rewrite | Strip completely |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- ===================================================================
-- BigQuery SQL Scripting Block
-- File: d_ausd_v_ta_vertrag_tmp.sql
-- ===================================================================

BEGIN
  -- Declare variable to store the processing date
  DECLARE v_datum STRING;

  -- Step 1: Retrieve reporting date (replaces SQL*Plus COLUMN query)
  -- COALESCE converted from NVL
  -- FORMAT_TIMESTAMP converted from TO_CHAR
  SET v_datum = (
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Step 2: Truncate target table (replaces PL/SQL package call)
  TRUNCATE TABLE sof$ta_vertrag_tmp;

  -- Step 3: Insert combined dataset
  INSERT INTO sof$ta_vertrag_tmp (
    vertrag_id_carmen,
    partner_id_carmen,
    rechdef_id_carmen,
    kundenkonto,
    mwst_kennzeichen,
    rahmenvertrag_id,
    rechnungslauf,
    vo_kenn,
    order_number,
    geplant_kuend,
    eingang_kuend,
    vertragsbeginn,
    vertragsstatus,
    sperrart,
    sperrgrund,
    stillegungszeitraum,
    twincard,
    dwh_tarifgr_text,
    bindefrist,
    letztes_upgrade,
    vertragsbindung,
    vertragsbindungseinheit,
    rechnungszahlart,
    rechnungsmedium,
    twin_vertrag_id,
    upgradeberechtigt,
    apn,
    upgradegrund,
    SV_Id,
    VDA,
    cost_centre,
    cost_centre_user,
    cntrct_ty,
    segment_id,
    rv_action_id,
    rechn_inh_konfig_text,
    commitment_reference_date,
    cntrct_validity_id
  )
  -- Oracle Optimizer parallel hints stripped
  SELECT
    c.cntrct_id                      AS VERTRAG_ID_CARMEN,
    bp.bp_id                         AS PARTNER_ID_CARMEN,
    ia.inv_definition_id             AS RECHDEF_ID_CARMEN,
    ia.account_reference             AS KUNDENKONTO,
    ia.sales_tax_freed               AS MWST_KENNZEICHEN,
    c.rv_num                         AS RAHMENVERTRAG_ID,
    ia.billcycle_id                  AS RECHNUNGSLAUF,
    c.vo_code                        AS VO_KENN,
    c.order_number                   AS ORDER_NUMBER,
    n.valid_from                     AS GEPLANT_KUEND,
    n.entry_date_of_notice           AS EINGANG_KUEND,
    c.cntrct_start_date              AS VERTRAGSBEGINN,
    -- CASE WHEN converted from DECODE
    CASE c.cntrct_st
      WHEN 5 THEN 'A'
      WHEN 6 THEN 'L'
      ELSE NULL
    END                              AS VERTRAGSSTATUS,
    b.sperrart_alle                  AS SPERRART,
    b.sperrgrund_alle                AS SPERRGRUND,
    b.stilllegungszeitraum_alle      AS STILLEGUNGSZEITRAUM,
    c.twinbill                       AS TWINCARD,
    ct.cds_description               AS DWH_TARIFGR_TEXT,
    bf.bindefrist                    AS BINDEFRIST,
    vvl.upgradedatum                 AS LETZTES_UPGRADE,
    p.number_time_measurement        AS VERTRAGSBINDUNG,
    p.einheit                        AS VERTRAGSBINDUNGsEinheit,
    -- CASE WHEN converted from DECODE
    CASE ia.inv_pay_ty_cv
      WHEN 1 THEN 'U'
      WHEN 2 THEN 'E'
      WHEN 3 THEN 'K'
      WHEN 4 THEN 'B'
      ELSE NULL
    END                              AS RECHNUNGSZAHLART,
    -- CASE WHEN converted from DECODE
    CASE ia.inv_media_cv
      WHEN 1 THEN 'Papier'
      WHEN 2 THEN 'ELMO'
      WHEN 3 THEN 'E-Mail'
      WHEN 4 THEN 'Fax'
      WHEN 5 THEN 'Inline/Papier'
      WHEN 6 THEN 'ELMO/Papier'
      ELSE NULL
    END                              AS RECHNUNGSMEDIUM,
    c.twin_vertrag_id                AS TWIN_VERTRAG_ID,
    -- Upgrade authorization conditional checks
    CASE
      -- Condition 1: No fixed terms
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'

      -- Condition 2: 12 Month contract term. Evaluates difference > 9 months
      -- DATE_DIFF(..., DAY) / 30.436875 converted from MONTHS_BETWEEN
      -- PARSE_DATE converted from TO_DATE
      -- COALESCE converted from NVL
      WHEN p.number_time_measurement = 12
           AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 9
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'

      -- Condition 3: 24 Month contract term. Evaluates difference > 23 months
      -- DATE_DIFF(..., DAY) / 30.436875 converted from MONTHS_BETWEEN
      -- PARSE_DATE converted from TO_DATE
      -- COALESCE converted from NVL
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
           AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 23
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'
      ELSE 'N'
    END                              AS upgradeberechtigt,
    ap.access_point_name             AS apn,
    vvl.upgradegrund                 AS upgradegrund,
    ct.cntrct_template_id            AS SV_Id,
    CASE
       WHEN (
             ct.cntrct_template_id IN (5104, 5105, 5106)
             OR (ct.cntrct_template_id >= 5155 AND ct.cntrct_template_id <= 5161)
            )
       THEN c.contract_number
       ELSE NULL
    END                              AS VDA,
    c.cost_centre                    AS cost_centre,
    c.cost_centre_user               AS cost_centre_user,
    c.cntrct_ty                      AS cntrct_ty,
    rd.segment_id                    AS segment_id,
    ac.rv_action_id                  AS rv_action_id,
    ia.rechn_inh_konfig_text,
    c.commitment_reference_date,
    c.cntrct_validity_id
  FROM sof$ta_cntrct_crs3 c
  JOIN sof$ta_bp_ref bp 
    ON bp.cntrct_cp2_id = c.cntrct_id 
    AND c.cntrct_ty <> 20
  JOIN sof$ta_inv_acc ia 
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN sof$ta_notice n 
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN sof$ta_barrier_zusgf b 
    ON b.cntrct_id = c.cntrct_id
  JOIN sof$ta_cntrct_templ ct 
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN sof$ta_cntrct_valid cv 
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN sof$ta_period p 
    ON p.period_id = cv.first_period_id
  LEFT JOIN sof$ta_vvl_upgrade vvl 
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN sof$ta_apn_ve ap 
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN dwh$vi_s_rd_segment rd 
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN sof$ta_action_assoc ac 
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN sof$vi_c_bfc bf 
    ON bf.cntrct_id = c.cntrct_id

  UNION ALL

  -- Oracle Optimizer parallel hints stripped
  SELECT
    c.cntrct_id                      AS VERTRAG_ID_CARMEN,
    bp.bp_id                         AS PARTNER_ID_CARMEN,
    ia.inv_definition_id             AS RECHDEF_ID_CARMEN,
    ia.account_reference             AS KUNDENKONTO,
    ia.sales_tax_freed               AS MWST_KENNZEICHEN,
    c.rv_num                         AS RAHMENVERTRAG_ID,
    ia.billcycle_id                  AS RECHNUNGSLAUF,
    c.vo_code                        AS VO_KENN,
    c.order_number                   AS ORDER_NUMBER,
    n.valid_from                     AS GEPLANT_KUEND,
    n.entry_date_of_notice           AS EINGANG_KUEND,
    c.cntrct_start_date              AS VERTRAGSBEGINN,
    -- CASE WHEN converted from DECODE
    CASE c.cntrct_st
      WHEN 5 THEN 'A'
      WHEN 6 THEN 'L'
      ELSE NULL
    END                              AS VERTRAGSSTATUS,
    b.sperrart_alle                  AS SPERRART,
    b.sperrgrund_alle                AS SPERRGRUND,
    b.stilllegungszeitraum_alle      AS STILLEGUNGSZEITRAUM,
    c.twinbill                       AS TWINCARD,
    ct.cds_description               AS DWH_TARIFGR_TEXT,
    bf.bindefrist                    AS BINDEFRIST,
    vvl.upgradedatum                 AS LETZTES_UPGRADE,
    p.number_time_measurement        AS VERTRAGSBINDUNG,
    p.einheit                        AS VERTRAGSBINDUNGsEinheit,
    -- CASE WHEN converted from DECODE
    CASE ia.inv_pay_ty_cv
      WHEN 1 THEN 'U'
      WHEN 2 THEN 'E'
      WHEN 3 THEN 'K'
      WHEN 4 THEN 'B'
      ELSE NULL
    END                              AS RECHNUNGSZAHLART,
    -- CASE WHEN converted from DECODE
    CASE ia.inv_media_cv
      WHEN 1 THEN 'Papier'
      WHEN 2 THEN 'ELMO'
      WHEN 3 THEN 'E-Mail'
      WHEN 4 THEN 'Fax'
      WHEN 5 THEN 'Inline/Papier'
      WHEN 6 THEN 'ELMO/Papier'
      ELSE NULL
    END                              AS RECHNUNGSMEDIUM,
    c.twin_vertrag_id                AS TWIN_VERTRAG_ID,
    -- Upgrade authorization conditional checks
    CASE
      -- Condition 1: No fixed terms
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'

      -- Condition 2: 12 Month contract term. Evaluates difference > 9 months
      -- DATE_DIFF(..., DAY) / 30.436875 converted from MONTHS_BETWEEN
      -- PARSE_DATE converted from TO_DATE
      -- COALESCE converted from NVL
      WHEN p.number_time_measurement = 12
           AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 9
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'

      -- Condition 3: 24 Month contract term. Evaluates difference > 23 months
      -- DATE_DIFF(..., DAY) / 30.436875 converted from MONTHS_BETWEEN
      -- PARSE_DATE converted from TO_DATE
      -- COALESCE converted from NVL
      WHEN (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
           AND (DATE_DIFF(PARSE_DATE('%Y%m%d', v_datum), CAST(COALESCE(c.commitment_reference_date, c.cntrct_start_date) AS DATE), DAY) / 30.436875) > 23
           AND (
                b.sperrart_alle IS NULL
                OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
               )
      THEN 'J'
      ELSE 'N'
    END                              AS upgradeberechtigt,
    ap.access_point_name             AS apn,
    vvl.upgradegrund                 AS upgradegrund,
    ct.cntrct_template_id            AS SV_Id,
    CASE
       WHEN (
             ct.cntrct_template_id IN (5104, 5105, 5106)
             OR (ct.cntrct_template_id >= 5155 AND ct.cntrct_template_id <= 5161)
            )
       THEN c.contract_number
       ELSE NULL
    END                              AS VDA,
    c.cost_centre                    AS cost_centre,
    c.cost_centre_user               AS cost_centre_user,
    c.cntrct_ty                      AS cntrct_ty,
    rd.segment_id                    AS segment_id,
    ac.rv_action_id                  AS rv_action_id,
    ia.rechn_inh_konfig_text,
    c.commitment_reference_date,
    c.cntrct_validity_id
  FROM sof$ta_cntrct_crs3 c
  JOIN sof$ta_bp_ref bp 
    ON bp.cntrct_cp2_id = c.cntrct_parent 
    AND c.cntrct_ty = 20
  JOIN sof$ta_inv_acc ia 
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN sof$ta_notice n 
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN sof$ta_barrier_zusgf b 
    ON b.cntrct_id = c.cntrct_id
  JOIN sof$ta_cntrct_templ ct 
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN sof$ta_cntrct_valid cv 
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN sof$ta_period p 
    ON p.period_id = cv.first_period_id
  LEFT JOIN sof$ta_vvl_upgrade vvl 
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN sof$ta_apn_ve ap 
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN dwh$vi_s_rd_segment rd 
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN sof$ta_action_assoc ac 
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN sof$vi_c_bfc bf 
    ON bf.cntrct_id = c.cntrct_id;

END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Fractional Month Emulation**: The legacy logic uses `MONTHS_BETWEEN`. To guarantee floating-point consistency, this design emulates it using `DATE_DIFF(..., DAY) / 30.436875`. Ensure that target systems and reporting logic agree with small variations in daily decimals during boundary conditions.
2. **Variable Datatype Assumptions**: `v_datum` is parsed as `STRING` in `'YYYYMMDD'` format, mimicking the Oracle character retrieval. If table structures on `dwtk_meldungen` have timestamp logic changed, ensure parse safety is maintained.

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql` | Transformed into BigQuery SQL Scripting block (direct 1:1 model conversion). |

# Job Dependencies

*   **Downstream Jobs**:
    *   `DW.BERT_AUSD_V_TA_P_VERTRAG` — Not yet migrated. Under the target Cloud Composer orchestration, the execution sequence must be configured so that `DW.BERT_AUSD_V_TA_P_VERTRAG` only triggers after this script finishes successfully.
    *   `DW.BERT_AUSD_V_TA_VERTRAG_TMP` — Not yet migrated. Similarly, downstream dependency execution must be deferred until this job populates `sof$ta_vertrag_tmp`.

# Schedule & Variables

*   **Schedule**: This job is not directly triggered by any scheduler. It operates as an included/shared module within other scheduled pipelines. On BigQuery/Cloud Composer, it must remain a callable/importable unit (e.g., task inside a parent Airflow DAG) without its own standalone cron scheduler.
*   **Variables**:
    *   `v_datum`: Extracted dynamically at runtime from the metadata table `isbert_schema.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. In BigQuery, this is assigned to a script-level declared variable (`DECLARE v_datum STRING;`).
    *   `v_carmen` (`@pcrs1`): Legacy Oracle database link. In the target environment, the referenced Carmen tables are assumed to be loaded in the local BigQuery environment, making this DB link suffix redundant.

# Lineage

*   **Upstream Producers (Reads)**:
    *   `isbert_schema.dwtk_meldungen`
    *   `sof$ta_cntrct_crs3`
    *   `sof$ta_bp_ref`
    *   `sof$ta_inv_acc`
    *   `dwh$vi_s_rd_segment`
    *   `sof$ta_notice`
    *   `sof$ta_barrier_zusgf`
    *   `sof$ta_cntrct_templ`
    *   `sof$ta_cntrct_valid`
    *   `sof$ta_period`
    *   `sof$ta_vvl_upgrade`
    *   `sof$ta_apn_ve`
    *   `sof$ta_action_assoc`
    *   `sof$vi_c_bfc`
*   **Downstream Consumers (Writes)**:
    *   `sof$ta_vertrag_tmp` (target table populated via `TRUNCATE` and `INSERT`)

# External System Replacements

*   **Oracle Database Link (`@pcrs1`)**: The legacy system uses an Oracle DB link to query the Carmen source database. In BigQuery, direct cross-database links are not native. This is replaced under the assumption that Carmen tables have been ingested into BigQuery. If they remain external, this must be mapped using BigQuery federated queries or an Omni connection.

# Cross-File Dependencies

*   **Shared Target Table**: The table `sof$ta_vertrag_tmp` serves as a shared temporary storage interface that is read by subsequent downstream jobs (`DW.BERT_AUSD_V_TA_P_VERTRAG` and `DW.BERT_AUSD_V_TA_VERTRAG_TMP`).
*   **Utility PL/SQL Package**: The script depends on `isbert_schema.DWPA_UTIL_SKRIPT` for executing table truncations dynamically. In BigQuery, this dependency is retired and replaced with native standard DML (`TRUNCATE TABLE sof$ta_vertrag_tmp;`).

# Target File Plan

*   **Target File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql`
    *   **Language**: SQL (BigQuery Standard SQL)
    *   **Source File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql`

# Environment-Specific Values

*   **`GCP_PROJECT`** (GLOBAL): Google Cloud Project ID where the target BigQuery tables and script are located. Passed at runtime via Composer/Airflow query parameterization.
*   **`BQ_DATASET`** (GLOBAL): Target BigQuery dataset (replacing `isbert_schema` and other schema prefix namespaces). Sourced using query parameters.
*   **`CARMEN_CONNECTION_ID`** (JOB-SPECIFIC): BigQuery connection ID or dataset location replacing the `@pcrs1` legacy database link.

# Risks and Manual Steps

*   **Downstream Wiring**: The downstream jobs `DW.BERT_AUSD_V_TA_P_VERTRAG` and `DW.BERT_AUSD_V_TA_VERTRAG_TMP` are marked as "not yet migrated". The parent Airflow DAG cannot be fully finalized and verified until those jobs are available in the target environment.
*   **DB Link Replacement Validation**: Ensure the Carmen source data ingestion pipeline into BigQuery is fully validated, and schema/table structures match the queries.
*   **German Logging Preservation**: In compliance with the Output/Print Literal Rule, original logging comments and prompt statements (such as `"variablendefinitionen"`, `"tabelle von vorherigem lauf loeschen"`, `"Verarbeitung fehlerfrei beendet."`) are kept verbatim inside SQL comments or query log outputs.