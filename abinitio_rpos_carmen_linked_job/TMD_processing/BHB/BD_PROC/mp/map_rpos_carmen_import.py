import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from google.cloud import bigquery

# Retrieve global environment variables
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
DW_DIR_IMP_SAP = os.environ.get("DW_DIR_IMP_SAP", f"gs://{GCS_BUCKET}/stage/imp_sap")

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--cfg_path", required=True, help="Path to the JSON configuration file")
args = parser.parse_known_args()[0]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("map_rpos_carmen_import") \
    .config("viewsEnabled", "true") \
    .getOrCreate()

# Load JSON configuration parameters natively via Spark
df_cfg = spark.read.option("multiLine", True).json(args.cfg_path)
config = df_cfg.first().asDict()

# Resolve Dynamic Quellverzeichnis and Dateimaske
bhb_quellverzeichnis = config.get("BHB_Quellverzeichnis", "$DW_DIR_IMP_SAP/crs/work/")
bhb_quellverzeichnis = bhb_quellverzeichnis.replace("$DW_DIR_IMP_SAP", DW_DIR_IMP_SAP)
bhb_dateimaske = config.get("BHB_Dateimaske", "CARMEN_B_*_pos.fix")

input_path = os.path.join(bhb_quellverzeichnis, bhb_dateimaske)

# Define output writers and readers
def write_to_bq(df, table_name, mode="append"):
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}") \
        .mode(mode) \
        .save()

def delete_existing_records(df_keys, target_table_name):
    # Write distinct keys to a temporary staging table to execute highly efficient BigQuery join delete
    temp_table_name = f"{target_table_name}_temp_keys_delete"
    df_keys.select("rechnung_datum", "rechnung_id", "standardvertrags_id", "vertrags_id").distinct() \
        .write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{temp_table_name}") \
        .mode("overwrite") \
        .save()
    
    # Execute the DELETE statement utilizing BigQuery Client
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.{target_table_name}` t
        WHERE EXISTS (
            SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.{temp_table_name}` s
            WHERE t.rechnung_datum = s.rechnung_datum
              AND t.rechnung_id = s.rechnung_id
              AND t.standardvertrags_id = s.standardvertrags_id
              AND t.vertrags_id = s.vertrags_id
        )
    """
    query_job = client.query(query)
    query_job.result() # Wait for job completion
    
    # Clean up the staging table
    client.delete_table(f"{GCP_PROJECT}.{BQ_DATASET}.{temp_table_name}", not_found_ok=True)

# Step 1: Read raw input text/CSV records
try:
    df_raw_file = spark.read.format("text").load(input_path)
    df_raw_file.createOrReplaceTempView("raw_file_view")
except Exception as e:
    print(f"WARNING: Inbound Billing CSV not found or empty - skipping execution: {e}")
    schema_raw_file = StructType([StructField("value", StringType(), True)])
    df_raw_file = spark.createDataFrame([], schema_raw_file)
    df_raw_file.createOrReplaceTempView("raw_file_view")

# Step 2: Extract Kennzeichen and Datensatz payloads
df_split_data = spark.sql("""
    SELECT 
        substring(value, 1, 5) AS kennzeichen,
        substring(value, 6) AS datensatz_rest,
        value AS raw_record
    FROM raw_file_view
""")
df_split_data.createOrReplaceTempView("split_data_view")
df_split_data.cache()

# Step 3: Parse and Clean payload records (Nutzdaten)
bhb_nutzdaten_kennung = config.get("BHB_Nutzdatensatzkennung", "P")
df_pay_raw = spark.sql(f"""
    SELECT 
        replace(datensatz_rest, ',', '.') AS datensatz_clean
    FROM split_data_view
    WHERE trim(kennzeichen) = '{bhb_nutzdaten_kennung}'
""")
df_pay_raw.createOrReplaceTempView("pay_raw_view")

df_parsed_payload = spark.sql("""
    SELECT 
        split(datensatz_clean, ';')[0] AS monats_id,
        split(datensatz_clean, ';')[1] AS rechnung_id,
        split(datensatz_clean, ';')[2] AS rechnung_datum,
        split(datensatz_clean, ';')[3] AS standardvertrags_id,
        split(datensatz_clean, ';')[4] AS vertrags_id,
        CAST(split(datensatz_clean, ';')[5] AS DECIMAL(18,2)) AS rechpos_brutto_eur,
        CAST(split(datensatz_clean, ';')[6] AS DECIMAL(18,2)) AS rechpos_netto_eur,
        CAST(split(datensatz_clean, ';')[7] AS DECIMAL(18,2)) AS rechpos_mwst_eur,
        split(datensatz_clean, ';')[8] AS rpos_geschaftsform_kenn,
        split(datensatz_clean, ';')[9] AS rech_leistung_id_carm,
        split(datensatz_clean, ';')[10] AS typ
    FROM pay_raw_view
""")
df_parsed_payload.createOrReplaceTempView("parsed_payload_view")

# Step 4: Validate Critical payload structures according to exact legacy validation requirements
df_validated_payload = spark.sql("""
    SELECT 
        CASE WHEN monats_id IS NULL OR trim(monats_id) = '' THEN raise_error('Invalid data format in monats_id') ELSE monats_id END AS monats_id,
        CASE WHEN rechnung_id IS NULL OR trim(rechnung_id) = '' THEN raise_error('Invalid data format in rechnung_id') ELSE rechnung_id END AS rechnung_id,
        CASE WHEN rechnung_datum IS NULL OR trim(rechnung_datum) = '' THEN raise_error('Invalid data format in rechnung_datum') ELSE rechnung_datum END AS rechnung_datum,
        CASE WHEN standardvertrags_id IS NULL OR trim(standardvertrags_id) = '' THEN raise_error('Invalid data format in standardvertrags_id') ELSE standardvertrags_id END AS standardvertrags_id,
        CASE WHEN vertrags_id IS NULL OR trim(vertrags_id) = '' THEN raise_error('Invalid data format in vertrags_id') ELSE vertrags_id END AS vertrags_id,
        CASE WHEN rechpos_brutto_eur IS NULL THEN raise_error('Invalid data format in rechpos_brutto_eur') ELSE rechpos_brutto_eur END AS rechpos_brutto_eur,
        CASE WHEN rechpos_netto_eur IS NULL THEN raise_error('Invalid data format in rechpos_netto_eur') ELSE rechpos_netto_eur END AS rechpos_netto_eur,
        CASE WHEN rechpos_mwst_eur IS NULL THEN raise_error('Invalid data format in rechpos_mwst_eur') ELSE rechpos_mwst_eur END AS rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ
    FROM parsed_payload_view
""")
df_validated_payload.createOrReplaceTempView("validated_payload_view")

# Step 5: Read historical contract dimension
df_vertrag_source = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag") \
    .load()
df_vertrag_source.createOrReplaceTempView("dwh_ta_c_vertrag_source")

df_vertrag = spark.sql("""
    SELECT 
        rahmenvertrag_id,
        vertrag_id_carmen,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von, 
        gueltig_bis
    FROM dwh_ta_c_vertrag_source
    WHERE gueltig_bis >= to_date('20050401', 'yyyyMMdd')
""")
df_vertrag.createOrReplaceTempView("dwh_ta_c_vertrag")

# Step 6: Join payload with contracts on active date intervals
df_joined_contracts = spark.sql("""
    SELECT 
        p.*,
        v.rahmenvertrag_id,
        v.dwh_vertrag_id,
        v.dwh_gp_id,
        v.dwh_konto_id,
        v.dwh_tarifgr_id,
        v.vo_kenn,
        v.zv_id,
        v.gueltig_von,
        v.gueltig_bis,
        last_day(to_date(concat(p.monats_id, '01'), 'yyyyMMdd')) AS month_last_day
    FROM validated_payload_view p
    LEFT JOIN dwh_ta_c_vertrag v ON p.vertrags_id = v.vertrag_id_carmen
""")
df_joined_contracts.createOrReplaceTempView("joined_contracts_view")

# Validate date constraints on the join (Proof Join reformat)
df_proof_join = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN rahmenvertrag_id ELSE NULL END AS rahmenvertrag_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_vertrag_id ELSE NULL END AS dwh_vertrag_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_gp_id ELSE NULL END AS dwh_gp_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_konto_id ELSE NULL END AS dwh_konto_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN dwh_tarifgr_id ELSE NULL END AS dwh_tarifgr_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN vo_kenn ELSE NULL END AS vo_kenn,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN zv_id ELSE NULL END AS zv_id,
        CASE WHEN (gueltig_von IS NULL OR month_last_day > gueltig_von) 
                  AND (gueltig_bis IS NULL OR month_last_day <= gueltig_bis)
             THEN gueltig_von ELSE NULL END AS gueltig_von
    FROM joined_contracts_view
""")
df_proof_join.createOrReplaceTempView("proof_join_view")

# Step 7: Apply deduplication & ranking filter on rankindex == 1
df_ranked_join = spark.sql("""
    SELECT *,
        row_number() OVER (
            PARTITION BY vertrags_id, rechnung_id, rechnung_datum, standardvertrags_id
            ORDER BY gueltig_von DESC, dwh_vertrag_id DESC
        ) AS rankindex
    FROM proof_join_view
""")
df_ranked_join.createOrReplaceTempView("ranked_join_view")

df_rank_filtered = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von
    FROM ranked_join_view
    WHERE rankindex = 1
""")
df_rank_filtered.createOrReplaceTempView("rank_filtered_view")

# Step 8: Perform aggregation rollup
df_rolled_up = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ,
        SUM(rechpos_brutto_eur) AS rechpos_brutto_eur,
        SUM(rechpos_netto_eur) AS rechpos_netto_eur,
        SUM(rechpos_mwst_eur) AS rechpos_mwst_eur
    FROM rank_filtered_view
    GROUP BY 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rpos_geschaftsform_kenn,
        rech_leistung_id_carm,
        typ
""")
df_rolled_up.createOrReplaceTempView("rolled_up_view")
df_rolled_up.cache()

# Step 9: Delete existing dynamic records before running inserts (Maintenance step)
delete_existing_records(df_rolled_up, "dwh_ta_f_rpos_carm")

# Step 10: Route payload records to distinct outputs based on Business Rules

# 10a. Target: Factoring Gutschriften (G) -> DWH$TA_F_GPOS_FACT_CARM
df_gutschriften = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'G'
""")
write_to_bq(df_gutschriften, "dwh_ta_f_gpos_fact_carm")

# 10b. Target: Factoring Rechnungen (F) -> DWH$TA_F_RPOS_FACT_CARM
df_factoring_rechnungen = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'F'
""")
write_to_bq(df_factoring_rechnungen, "dwh_ta_f_rpos_fact_carm")

# 10c. Target: Reselling (R) -> DWH$TA_F_RPOS_RESELLING_CARM
df_reselling = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        substring(rech_leistung_id_carm, 1, 9) AS rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE rpos_geschaftsform_kenn = 'R'
""")
write_to_bq(df_reselling, "dwh_ta_f_rpos_reselling_carm")

# 10d. Target: Temporary Positions (T) -> DWH$TA_T_RPOS_CARM
df_temporary_data = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rahmenvertrag_id,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur,
        CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS bearbeitung_datum
    FROM rolled_up_view
    WHERE typ = 'T'
""")
write_to_bq(df_temporary_data, "dwh_ta_t_rpos_carm")

# 10e. Target: Base Fact Positions (Non-T) -> DWH$TA_F_RPOS_CARM
df_fact_data = spark.sql("""
    SELECT 
        monats_id,
        rechnung_id,
        rechnung_datum,
        standardvertrags_id,
        vertrags_id,
        rech_leistung_id_carm,
        rahmenvertrag_id AS rahmenvertrag,
        dwh_vertrag_id,
        dwh_gp_id,
        dwh_konto_id,
        dwh_tarifgr_id,
        vo_kenn,
        zv_id,
        gueltig_von,
        rechpos_brutto_eur,
        rechpos_netto_eur,
        rechpos_mwst_eur
    FROM rolled_up_view
    WHERE typ != 'T'
""")
write_to_bq(df_fact_data, "dwh_ta_f_rpos_carm")

df_rolled_up.unpersist()

# Step 11: Parse Metadata End Record (Footer) and write to BQ Staging tables for post-merging
bhb_endedaten_kennung = config.get("BHB_Endedatensatzkennung", "X")
df_metadata_raw = spark.sql(f"""
    SELECT 
        datensatz_rest
    FROM split_data_view
    WHERE trim(kennzeichen) = '{bhb_endedaten_kennung}'
""")
df_metadata_raw.createOrReplaceTempView("metadata_raw_view")

df_split_data.unpersist()

df_metadata_parsed = spark.sql("""
    SELECT 
        split(datensatz_rest, ';')[0] AS bemerkung,
        split(datensatz_rest, ';')[1] AS stichtag,
        CAST(split(datensatz_rest, ';')[2] AS INT) AS anzahl,
        split(datensatz_rest, ';')[3] AS inhalt,
        split(datensatz_rest, ';')[4] AS erstellt_am,
        1001 AS eintragsnr 
    FROM metadata_raw_view
""")
df_metadata_parsed.createOrReplaceTempView("metadata_parsed_view")

df_ende_record = spark.sql("""
    SELECT 
        bemerkung,
        stichtag,
        anzahl,
        inhalt,
        CASE WHEN instr(erstellt_am, ';') = 0 THEN erstellt_am 
             ELSE substring(erstellt_am, 1, length(erstellt_am) - 1) 
        END AS erstellt_am,
        eintragsnr
    FROM metadata_parsed_view
""")
df_ende_record.createOrReplaceTempView("ende_record_view")
df_ende_record.cache()

# Write staging data for dwh_ta_k_meldungen to execute MERGE in Airflow
write_to_bq(df_ende_record, "dwh_ta_k_meldungen_stage", mode="overwrite")

df_rech_absgrp = spark.sql("""
    SELECT 
        date_format(add_months(to_date(substring(stichtag, 1, 6), 'yyyyMM'), -1), 'yyyyMM') AS monats_id,
        substring(bemerkung, 10, 5) AS abs_grp,
        bemerkung AS dateiname,
        to_date(stichtag, 'yyyyMMdd') AS rechnung_datum,
        'P' AS rechnungsteil,
        current_timestamp() AS ladedatum
    FROM ende_record_view
""")
df_rech_absgrp.createOrReplaceTempView("rech_absgrp_view")

# Write staging data for dwh_ta_k_rech_absgrp to execute MERGE in Airflow
write_to_bq(df_rech_absgrp, "dwh_ta_k_rech_absgrp_stage", mode="overwrite")

df_ende_record.unpersist()
spark.stop()