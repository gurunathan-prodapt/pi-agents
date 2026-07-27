import os
import sys
import argparse
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# --- PARSE COMMAND LINE ARGUMENTS ---
parser = argparse.ArgumentParser()
parser.add_argument("--BHB_Quellverzeichnis", required=False, default="crs/work/")
parser.add_argument("--BHB_Zielverzeichnis", required=False, default="crs/store/")
parser.add_argument("--BHB_Dateimaske", required=False, default="CARMEN_B_*_pos.fix")
parser.add_argument("--BHB_Kopfdatensatzkennung", required=False, default="H")
parser.add_argument("--BHB_Nutzdatensatzkennung", required=False, default="P")
parser.add_argument("--BHB_Endedatensatzkennung", required=False, default="X")
parser.add_argument("--BHB_Eintragsnr", required=False, default="")
parser.add_argument("--BHB_Dateiname", required=False, default="")
args, unknown = parser.parse_known_args()

# --- GLOBAL ENVIRONMENT VALUES ---
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_LOCATION = os.environ.get("BQ_LOCATION")

# --- JOB-SPECIFIC PARAMETERS ---
JOB_CONFIG = {
    "BHB_Quellverzeichnis": args.BHB_Quellverzeichnis,
    "BHB_Zielverzeichnis": args.BHB_Zielverzeichnis,
    "BHB_Dateimaske": args.BHB_Dateimaske,
    "BHB_Kopfdatensatzkennung": args.BHB_Kopfdatensatzkennung,
    "BHB_Nutzdatensatzkennung": args.BHB_Nutzdatensatzkennung,
    "BHB_Endedatensatzkennung": args.BHB_Endedatensatzkennung,
    "BHB_Eintragsnr": args.BHB_Eintragsnr,
    "BHB_Dateiname": args.BHB_Dateiname
}

# --- INITIALIZE SPARK ---
spark = SparkSession.builder \
    .appName("DW.RPOS_CARM_IMPORT") \
    .getOrCreate()

# --- UTILITIES ---
def execute_delete_query(query):
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
    query_job = client.query(query)
    query_job.result()

def write_to_bq(df, table_name, mode="append"):
    gcs_bucket_clean = GCS_BUCKET if GCS_BUCKET.startswith("gs://") else f"gs://{GCS_BUCKET}"
    temporary_gcs_bucket = f"{gcs_bucket_clean}/tmp"
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", temporary_gcs_bucket) \
        .mode(mode) \
        .save()

def delete_records_rpos(table_name, df_keys):
    keys = df_keys.distinct().collect()
    if not keys:
        return
    
    conditions = []
    for row in keys:
        cond_standardvertrags_id = f"standardvertrags_id = {row.standardvertrags_id}" if row.standardvertrags_id is not None else "standardvertrags_id IS NULL"
        cond_vertrags_id = f"vertrags_id = {row.vertrags_id}" if row.vertrags_id is not None else "vertrags_id IS NULL"
        cond = (
            f"rechnung_id = '{row.rechnung_id}' "
            f"AND rechnung_datum = DATE('{row.rechnung_datum}') "
            f"AND {cond_standardvertrags_id} "
            f"AND {cond_vertrags_id}"
        )
        conditions.append(f"({cond})")
        
    chunk_size = 500
    for i in range(0, len(conditions), chunk_size):
        chunk = conditions[i:i+chunk_size]
        where_clause = " OR ".join(chunk)
        query = f"DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.{table_name}` WHERE {where_clause}"
        execute_delete_query(query)

def delete_records_temp(table_name, df_keys):
    keys = df_keys.distinct().collect()
    if not keys:
        return
    
    conditions = []
    for row in keys:
        cond_debitor = f"debitor_id = '{row.debitor_id}'" if row.debitor_id is not None else "debitor_id IS NULL"
        cond = (
            f"{cond_debitor} "
            f"AND rechnung_datum = DATE('{row.rechnung_datum}') "
            f"AND rechnung_id = '{row.rechnung_id}'"
        )
        conditions.append(f"({cond})")
        
    chunk_size = 500
    for i in range(0, len(conditions), chunk_size):
        chunk = conditions[i:i+chunk_size]
        where_clause = " OR ".join(chunk)
        query = f"DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.{table_name}` WHERE {where_clause}"
        execute_delete_query(query)

# --- 1. SOURCE INGESTION ---
df_ta_f_rpos_carm = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm") \
    .load()
df_ta_f_rpos_carm.createOrReplaceTempView("src_ta_f_rpos_carm")

df_ta_f_rpos_fact_carm = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_fact_carm") \
    .load()
df_ta_f_rpos_fact_carm.createOrReplaceTempView("src_ta_f_rpos_fact_carm")

df_ta_f_rpos_reselling_carm = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_reselling_carm") \
    .load()
df_ta_f_rpos_reselling_carm.createOrReplaceTempView("src_ta_f_rpos_reselling_carm")

df_ta_c_vertrag = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag") \
    .load()
df_ta_c_vertrag.createOrReplaceTempView("src_ta_c_vertrag")

# Read raw input file from GCS
quell_verzeichnis = JOB_CONFIG["BHB_Quellverzeichnis"]
dateimaske = JOB_CONFIG["BHB_Dateimaske"]
gcs_input_path = f"gs://{GCS_BUCKET}/{quell_verzeichnis}{dateimaske}"

try:
    df_raw_file = spark.read.format("text").load(gcs_input_path)
    df_raw_file.createOrReplaceTempView("vw_raw_file")
except Exception as e:
    print(f"WARNING: vw_raw_file empty or path not found: {e}")
    schema_of_raw_file = StructType([StructField("value", StringType(), True)])
    df_raw_file = spark.createDataFrame([], schema_of_raw_file)
    df_raw_file.createOrReplaceTempView("vw_raw_file")

# --- 2. CLEAN COMMA TO DOT ---
df_cleaned_payload = spark.sql("""
    SELECT 
        substring(value, 1, 3) AS kennzeichen,
        replace(substring(value, 4), ',', '.') AS datensatz_rest
    FROM vw_raw_file
""")
df_cleaned_payload.createOrReplaceTempView("vw_cleaned_payload")
df_cleaned_payload.cache()

# --- 3. SPLIT PAYLOAD FROM CONTROL RECORDS ---
nutzdaten_id = JOB_CONFIG["BHB_Nutzdatensatzkennung"]
df_split_data = spark.sql(f"""
    SELECT * 
    FROM vw_cleaned_payload
    WHERE kennzeichen = '{nutzdaten_id}'
""")
df_split_data.createOrReplaceTempView("vw_split_data")

# --- 4. MAP TO FLAT SCHEMA & TRIM ---
df_reformat_for_db = spark.sql("""
    SELECT
        CAST(to_date(substring(datensatz_rest, 1, 6), 'yyyyMM') AS STRING) AS monats_id,
        trim(substring(datensatz_rest, 7, 13)) AS debitor_id,
        trim(substring(datensatz_rest, 20, 14)) AS rechnung_id,
        to_date(substring(datensatz_rest, 34, 8), 'yyyyMMdd') AS rechnung_datum,
        CASE WHEN trim(substring(datensatz_rest, 42, 10)) != '#' THEN trim(substring(datensatz_rest, 42, 10)) ELSE NULL END AS standardvertrags_id,
        CASE WHEN trim(substring(datensatz_rest, 52, 10)) != '#' THEN trim(substring(datensatz_rest, 52, 10)) ELSE NULL END AS vertrags_id,
        trim(substring(datensatz_rest, 62, 9)) AS rech_leistung_id_carm,
        CAST(trim(substring(datensatz_rest, 71, 15)) AS DECIMAL(15,2)) AS rechpos_brutto_eur,
        CAST(trim(substring(datensatz_rest, 86, 15)) AS DECIMAL(15,2)) AS rechpos_netto_eur,
        CAST(trim(substring(datensatz_rest, 101, 15)) AS DECIMAL(15,2)) AS rechpos_mwst_eur,
        trim(substring(substring(datensatz_rest, 20, 14), 9, 5)) AS abs_grp,
        trim(substring(datensatz_rest, 116, 1)) AS pooling,
        CAST(trim(substring(datensatz_rest, 117, 10)) AS DECIMAL(18,0)) AS rechnungvertrag_id,
        trim(substring(datensatz_rest, 127, 10)) AS prob_vertrag_id,
        trim(substring(datensatz_rest, 137, 2)) AS prob_provider_kenn,
        CAST(trim(substring(datensatz_rest, 139, 10)) AS DECIMAL(18,0)) AS anz_leistungen,
        CAST(trim(substring(datensatz_rest, 149, 10)) AS DECIMAL(18,0)) AS anz_tickets,
        substring(datensatz_rest, 159, 1) AS rpos_geschaftsform_kenn,
        trim(substring(datensatz_rest, 160, 6)) AS vas_kenn,
        trim(substring(datensatz_rest, 166, 10)) AS verkauftes_basisprodukt_id
    FROM vw_split_data
""")
df_reformat_for_db.createOrReplaceTempView("vw_reformat_for_db")
df_reformat_for_db.cache()

# --- 5. ENFORCE STRICT VALIDATIONS ---
# Validate and raise exact literal error messages character-for-character from legacy graph
if df_reformat_for_db.filter(F.col("monats_id").isNull() | (F.trim(F.col("monats_id")) == "")).count() > 0:
    raise ValueError("Invalid Data in field monats_id")
if df_reformat_for_db.filter(F.col("debitor_id").isNull() | (F.trim(F.col("debitor_id")) == "")).count() > 0:
    raise ValueError("Invalid Data in field debitor_id")
if df_reformat_for_db.filter(F.col("rechnung_id").isNull() | (F.trim(F.col("rechnung_id")) == "")).count() > 0:
    raise ValueError("Invalid Data in field rechnung_id")
if df_reformat_for_db.filter(F.col("rechnung_datum").isNull()).count() > 0:
    raise ValueError("Invalid Data in field rechnung_datum")
if df_reformat_for_db.filter(F.col("standardvertrags_id").isNull() | (F.trim(F.col("standardvertrags_id")) == "")).count() > 0:
    raise ValueError("Invalid Data in field standardvertrags_id")
if df_reformat_for_db.filter(F.col("vertrags_id").isNull() | (F.trim(F.col("vertrags_id")) == "")).count() > 0:
    raise ValueError("Invalid Data in field vertrags_id")
if df_reformat_for_db.filter(F.col("rech_leistung_id_carm").isNull() | (F.trim(F.col("rech_leistung_id_carm")) == "")).count() > 0:
    raise ValueError("Invalid Data in field rech_leistung_id_carm")
if df_reformat_for_db.filter(F.col("rechpos_brutto_eur").isNull()).count() > 0:
    raise ValueError("Invalid Data in field rechpos_brutto_eur")
if df_reformat_for_db.filter(F.col("rechpos_netto_eur").isNull()).count() > 0:
    raise ValueError("Invalid Data in field rechpos_netto_eur")
if df_reformat_for_db.filter(F.col("rechpos_mwst_eur").isNull()).count() > 0:
    raise ValueError("Invalid Data in field rechpos_mwst_eur")

df_reformat_for_db.createOrReplaceTempView("vw_validated_records")

# --- 6. DB LOOKUP CONTRACT ENRICHMENT ---
df_contract_joined = spark.sql("""
    SELECT 
        v.*,
        c.rahmenvertrag_id,
        c.dwh_vertrag_id,
        c.dwh_gp_id,
        c.dwh_konto_id,
        c.dwh_tarifgr_id,
        c.vo_kenn,
        c.zv_id,
        c.gueltig_von,
        c.gueltig_bis
    FROM vw_validated_records v
    LEFT OUTER JOIN src_ta_c_vertrag c
      ON v.vertrags_id = c.vertrag_id_carmen
      AND c.gueltig_bis >= to_date('20050401', 'yyyyMMdd')
""")
df_contract_joined.createOrReplaceTempView("vw_contract_joined")

# --- 7. IMPUTE LOW VALUE MARKERS ---
df_imputed_sort_keys = spark.sql("""
    SELECT 
        t.*,
        coalesce(substring(concat('000000000000000', CAST(t.dwh_vertrag_id AS STRING)), -16, 16), '\\000') AS clean_dwh_vertrag_id,
        coalesce(CAST(t.gueltig_von AS STRING), '\\000') AS clean_gueltig_von
    FROM vw_contract_joined t
""")
df_imputed_sort_keys.createOrReplaceTempView("vw_imputed_sort_keys")

# --- 8. VERSIONS RANKING ---
df_ranked_contracts = spark.sql("""
    SELECT 
        *,
        rank() OVER (
            PARTITION BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id, rech_leistung_id_carm 
            ORDER BY clean_gueltig_von DESC, clean_dwh_vertrag_id DESC
        ) AS rankindex
    FROM vw_imputed_sort_keys
""")
df_ranked_contracts.createOrReplaceTempView("vw_ranked_contracts")

# --- 9. PRIMARY ACTIVE KEY FILTER ---
df_prime_ranks = spark.sql("""
    SELECT * 
    FROM vw_ranked_contracts 
    WHERE rankindex = 1
""")
df_prime_ranks.createOrReplaceTempView("vw_prime_ranks")

# --- 10. EVALUATE HISTORICAL VALID BOUNDS ---
df_proof_joins = spark.sql("""
    SELECT 
        p.*,
        CASE 
            WHEN (gueltig_von IS NULL OR last_day(to_date(monats_id, 'yyyyMM')) > gueltig_von)
             AND (gueltig_bis IS NULL OR last_day(to_date(monats_id, 'yyyyMM')) <= gueltig_bis)
            THEN 0 ELSE 1 
        END AS valid_flag
    FROM vw_prime_ranks p
""")
df_proof_joins.createOrReplaceTempView("vw_proof_joins")

# --- 11. RESTRUCTURE RELEASES ---
df_normalized_contracts = spark.sql("""
    SELECT 
        monats_id, debitor_id, rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id,
        rech_leistung_id_carm, rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur,
        abs_grp, pooling, rechnungvertrag_id, prob_vertrag_id, prob_provider_kenn,
        anz_leistungen, anz_tickets, rpos_geschaftsform_kenn, vas_kenn, verkauftes_basisprodukt_id,
        CASE WHEN valid_flag = 0 THEN rahmenvertrag_id ELSE NULL END AS rahmenvertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_vertrag_id ELSE NULL END AS dwh_vertrag_id,
        CASE WHEN valid_flag = 0 THEN dwh_gp_id ELSE NULL END AS dwh_gp_id,
        CASE WHEN valid_flag = 0 THEN dwh_konto_id ELSE NULL END AS dwh_konto_id,
        CASE WHEN valid_flag = 0 THEN dwh_tarifgr_id ELSE NULL END AS dwh_tarifgr_id,
        CASE WHEN valid_flag = 0 THEN vo_kenn ELSE NULL END AS vo_kenn,
        CASE WHEN valid_flag = 0 THEN zv_id ELSE NULL END AS zv_id,
        CASE WHEN valid_flag = 0 THEN gueltig_von ELSE NULL END AS gueltig_von
    FROM vw_proof_joins
""")
df_normalized_contracts.createOrReplaceTempView("vw_normalized_contracts")

# --- 12. COMMERCIAL CODES RE-MAPPING ---
df_decoded_positions = spark.sql("""
    SELECT 
        *,
        CASE 
            WHEN rpos_geschaftsform_kenn = 'F' AND vas_kenn = 'P30002' THEN 'G'
            ELSE rpos_geschaftsform_kenn 
        END AS clean_geschaftsform_kenn,
        current_timestamp() AS ladedatum
    FROM vw_normalized_contracts
""")
df_decoded_positions.createOrReplaceTempView("vw_decoded_positions")

# --- 13. ESTABLISH FINAL WORKING STREAM ---
df_mapped_inserts = spark.sql("""
    SELECT 
        *,
        rahmenvertrag_id AS rahmenvertrag
    FROM vw_decoded_positions
""")
df_mapped_inserts.createOrReplaceTempView("vw_mapped_inserts")
df_mapped_inserts.cache()

# --- 14. SEPARATE CHANNELS (Invoices, Credit Notes, Reselling, GPOS) ---
# Factoring stream (combining F and G)
df_factoring_fact = spark.sql("""
    SELECT * 
    FROM vw_mapped_inserts
    WHERE clean_geschaftsform_kenn IN ('F', 'G')
""")
df_factoring_fact.createOrReplaceTempView("vw_factoring_fact")

# Reselling stream
df_reselling_items = spark.sql("""
    SELECT * 
    FROM vw_mapped_inserts
    WHERE clean_geschaftsform_kenn = 'R'
""")
df_reselling_items.createOrReplaceTempView("vw_reselling_items")

# GPOS Aggregate standard invoices
# RETRY FIX: Added debitor_id and rech_leistung_id_carm to both select and group by, updated typ logic to use rech_leistung_id_carm directly
df_gpos_aggregation = spark.sql("""
    SELECT 
        vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id, debitor_id, rech_leistung_id_carm,
        sum(rechpos_brutto_eur) AS rechpos_brutto_eur,
        sum(rechpos_netto_eur) AS rechpos_netto_eur,
        sum(rechpos_mwst_eur) AS rechpos_mwst_eur,
        current_timestamp() AS ladedatum,
        CASE WHEN (rech_leistung_id_carm = 'RABATT' AND vertrags_id = 0) OR first(pooling) = 'P' THEN 'T' ELSE NULL END AS typ
    FROM vw_mapped_inserts
    GROUP BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id, debitor_id, rech_leistung_id_carm
""")
df_gpos_aggregation.createOrReplaceTempView("vw_gpos_aggregation")
df_gpos_aggregation.cache()

# Temporary Billing Positions filter (Typ = 'T')
df_temp_billing = spark.sql("""
    SELECT *, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS bearbeitung_datum
    FROM vw_gpos_aggregation
    WHERE typ = 'T'
""")
df_temp_billing.createOrReplaceTempView("vw_temp_billing")

# ABSGRP Upsert dynamic table updates
df_absgrp_upsert = spark.sql(f"""
    SELECT DISTINCT
        monats_id,
        abs_grp,
        '{JOB_CONFIG["BHB_Dateiname"]}' AS dateiname,
        'P' AS rechnungsteil,
        rechnung_datum,
        current_timestamp() AS ladedatum
    FROM vw_mapped_inserts
""")
df_absgrp_upsert.createOrReplaceTempView("vw_absgrp_upsert")

# --- 15. PARSE CONTROL FOOTER ---
endedaten_id = JOB_CONFIG["BHB_Endedatensatzkennung"]
df_split_metadata = spark.sql(f"""
    SELECT * 
    FROM vw_cleaned_payload
    WHERE kennzeichen = '{endedaten_id}'
""")

footer_rows = df_split_metadata.collect()
if footer_rows:
    footer_text = footer_rows[0].datensatz_rest
    parts = footer_text.split(";")
    bemerkung = parts[0].strip() if len(parts) > 0 else ""
    stichtag = parts[1].strip() if len(parts) > 1 else ""
    anzahl = parts[2].strip() if len(parts) > 2 else "0"
    inhalt = parts[3].strip() if len(parts) > 3 else ""
    erstellt_am = parts[4].strip() if len(parts) > 4 else ""
    if erstellt_am.endswith(";"):
        erstellt_am = erstellt_am[:-1]
else:
    bemerkung, stichtag, anzahl, inhalt, erstellt_am = "", "", "0", "", ""

# --- 16. TRANSACTIONAL PRE-DELETE & APPEND ---

# Target 1: dwh_ta_f_rpos_carm
df_mapped_inserts_proj = spark.sql("""
    SELECT 
        monats_id, debitor_id, rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id,
        rech_leistung_id_carm, rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur,
        abs_grp, pooling, rechnungvertrag_id, prob_vertrag_id, prob_provider_kenn,
        anz_leistungen, anz_tickets, rpos_geschaftsform_kenn, vas_kenn, verkauftes_basisprodukt_id,
        rahmenvertrag, dwh_vertrag_id, dwh_gp_id, dwh_konto_id, dwh_tarifgr_id, vo_kenn, zv_id, gueltig_von
    FROM vw_mapped_inserts
""")
df_mapped_inserts_proj.cache()
df_deletes_rpos = df_mapped_inserts_proj.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
delete_records_rpos("dwh_ta_f_rpos_carm", df_deletes_rpos)
write_to_bq(df_mapped_inserts_proj, "dwh_ta_f_rpos_carm")
df_mapped_inserts_proj.unpersist()

# Target 2: dwh_ta_f_rpos_fact_carm
df_factoring_invoices_proj = spark.sql("""
    SELECT 
        rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm
    FROM vw_factoring_fact
""")
df_factoring_invoices_proj.cache()
df_deletes_fact = df_factoring_invoices_proj.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
delete_records_rpos("dwh_ta_f_rpos_fact_carm", df_deletes_fact)
write_to_bq(df_factoring_invoices_proj, "dwh_ta_f_rpos_fact_carm")
df_factoring_invoices_proj.unpersist()

# Target 3: dwh_ta_f_rpos_reselling_carm
df_reselling_items_proj = spark.sql("""
    SELECT 
        rechnung_datum, rechnung_id, standardvertrags_id, vertrags_id, rech_leistung_id_carm
    FROM vw_reselling_items
""")
df_reselling_items_proj.cache()
df_deletes_resell = df_reselling_items_proj.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
delete_records_rpos("dwh_ta_f_rpos_reselling_carm", df_deletes_resell)
write_to_bq(df_reselling_items_proj, "dwh_ta_f_rpos_reselling_carm")
df_reselling_items_proj.unpersist()

# Target 4: dwh_ta_f_gpos_fact_carm
df_gpos_aggregation_proj = spark.sql("""
    SELECT 
        rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id,
        rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur, ladedatum, typ
    FROM vw_gpos_aggregation
""")
df_gpos_aggregation_proj.cache()
df_deletes_gpos = df_gpos_aggregation_proj.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
delete_records_rpos("dwh_ta_f_gpos_fact_carm", df_deletes_gpos)
write_to_bq(df_gpos_aggregation_proj, "dwh_ta_f_gpos_fact_carm")
df_gpos_aggregation_proj.unpersist()

# Target 5: dwh_ta_t_rpos_carm
# RETRY FIX: Selecting debitor_id correctly from vw_temp_billing without AnalysisException
df_temp_billing_proj = spark.sql("""
    SELECT 
        debitor_id, rechnung_datum, rechnung_id, vertrags_id, standardvertrags_id,
        rechpos_brutto_eur, rechpos_netto_eur, rechpos_mwst_eur, bearbeitung_datum, ladedatum
    FROM vw_temp_billing
""")
df_temp_billing_proj.cache()
df_deletes_temp = df_temp_billing_proj.select("debitor_id", "rechnung_datum", "rechnung_id").distinct()
delete_records_temp("dwh_ta_t_rpos_carm", df_deletes_temp)
# RETRY FIX: Write with the correct debitor_id column without dropping it, as it is a required column
write_to_bq(df_temp_billing_proj, "dwh_ta_t_rpos_carm")
df_temp_billing_proj.unpersist()

# --- 17. METADATA UPDATES ---
# Update Run log (ta_k_meldungen)
eintragsnr = JOB_CONFIG["BHB_Eintragsnr"]
dateiname = JOB_CONFIG["BHB_Dateiname"]

if footer_rows and eintragsnr:
    meldungen_query = f"""
    UPDATE `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_meldungen`
    SET anzahl_ds_eof = {int(anzahl) if anzahl.isdigit() else 0}
      , dateiname = '{dateiname}'
      , enderecord_text = '{inhalt}'
      , zusatzinfo = '{bemerkung}'
    WHERE entrynr = {int(eintragsnr) if eintragsnr.isdigit() else 0}
    """
    execute_delete_query(meldungen_query)

# Update or insert Billing status log (ta_k_rech_absgrp)
absgrp_rows = df_absgrp_upsert.collect()
if absgrp_rows:
    for row in absgrp_rows:
        merge_stmt = f"""
        MERGE `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_rech_absgrp` T
        USING (
            SELECT 
                '{row.monats_id}' AS monats_id,
                '{row.abs_grp}' AS abs_grp,
                '{row.dateiname}' AS dateiname,
                '{row.rechnungsteil}' AS rechnungsteil,
                DATE('{row.rechnung_datum}') AS rechnung_datum
        ) S
        ON T.monats_id = S.monats_id
           AND T.abs_grp = S.abs_grp
           AND T.dateiname = S.dateiname
           AND T.rechnungsteil = S.rechnungsteil
        WHEN MATCHED THEN
            UPDATE SET rechnung_datum = S.rechnung_datum, ladedatum = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (monats_id, abs_grp, dateiname, rechnungsteil, rechnung_datum, ladedatum)
            VALUES (S.monats_id, S.abs_grp, S.dateiname, S.rechnungsteil, S.rechnung_datum, CURRENT_TIMESTAMP())
        """
        execute_delete_query(merge_stmt)

# Clean up Spark cache resources
df_reformat_for_db.unpersist()
df_mapped_inserts.unpersist()
df_gpos_aggregation.unpersist()
df_cleaned_payload.unpersist()

print("Pipeline completed successfully.")