import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

# Setup environment variables using strictly runtime retrieval
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
TEMP_GCS_BUCKET = os.environ.get("TEMP_GCS_BUCKET")

# Job Specific variables derived from configuration mappings
BHB_Quellverzeichnis = os.environ.get("BHB_Quellverzeichnis", f"gs://{GCS_BUCKET}/crs/work/")
BHB_Zielverzeichnis = os.environ.get("BHB_Zielverzeichnis", f"gs://{GCS_BUCKET}/crs/store/")
BHB_Dateimaske = os.environ.get("BHB_Dateimaske", "CARMEN_B_*_pos.fix")
BHB_Kopfdatensatzkennung = os.environ.get("BHB_Kopfdatensatzkennung", "H")
BHB_Nutzdatensatzkennung = os.environ.get("BHB_Nutzdatensatzkennung", "P")
BHB_Endedatensatzkennung = os.environ.get("BHB_Endedatensatzkennung", "X")
BHB_Eintragsnr = os.environ.get("BHB_Eintragsnr")

spark = SparkSession.builder.appName("map_rpos_carmen_import").getOrCreate()

def write_to_bq(df, table_name, mode="overwrite"):
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .mode(mode) \
        .save()

def is_blank_or_null(col):
    return (col.isNull()) | (F.trim(col) == "")

# Read Master Contract Ledger (dwh_ta_c_vertrag)
df_c_vertrag_src = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag") \
    .load()

# 1. Read input file as raw text lines
df_lines = spark.read.text(f"{BHB_Quellverzeichnis}/{BHB_Dateimaske}")

# 2. Extract kennzeichen and datensatz_rest
df_split_lines = df_lines.select(
    F.substring_index(F.col("value"), ";", 1).alias("kennzeichen"),
    F.substring(F.col("value"), F.length(F.substring_index(F.col("value"), ";", 1)) + 2, 999999).alias("datensatz_rest")
)

# Replace European commas with decimal points in datensatz_rest
df_split_lines = df_split_lines.withColumn(
    "datensatz_rest", 
    F.regexp_replace(F.col("datensatz_rest"), ",", ".")
)

# Split payload (P) and trailer (X) records
df_payload_lines = df_split_lines.filter(F.col("kennzeichen") == BHB_Nutzdatensatzkennung)
df_trailer_lines = df_split_lines.filter(F.col("kennzeichen") == BHB_Endedatensatzkennung)

# Parse payload fields by splitting datensatz_rest
df_payload_fields = df_payload_lines.select(
    F.col("kennzeichen"),
    F.split(F.col("datensatz_rest"), ";").alias("fields")
)

df_payload_parsed = df_payload_fields.select(
    F.col("kennzeichen"),
    F.col("fields").getItem(0).alias("monats_id"),
    F.col("fields").getItem(1).alias("debitor_id"),
    F.col("fields").getItem(2).alias("rechnung_id"),
    F.col("fields").getItem(3).alias("rechnung_datum"),
    F.col("fields").getItem(4).alias("standardvertrags_id"),
    F.col("fields").getItem(5).alias("vertrags_id"),
    F.col("fields").getItem(6).alias("rech_leistung_id_carm"),
    F.col("fields").getItem(7).alias("rechpos_brutto_eur"),
    F.col("fields").getItem(8).alias("rechpos_netto_eur"),
    F.col("fields").getItem(9).alias("rechpos_mwst_eur"),
    F.col("fields").getItem(10).alias("pooling"),
    F.col("fields").getItem(11).alias("rechnungvertrag_id"),
    F.col("fields").getItem(12).alias("prob_vertrag_id"),
    F.col("fields").getItem(13).alias("prob_provider_kenn"),
    F.col("fields").getItem(14).alias("anz_leistungen"),
    F.col("fields").getItem(15).alias("anz_tickets"),
    F.col("fields").getItem(16).alias("rpos_geschaftsform_kenn"),
    F.col("fields").getItem(17).alias("vas_kenn"),
    F.col("fields").getItem(18).alias("kennung5") # verkauftes_basisprodukt_id
)

# Apply cleansing and trimming
df_payload_cleaned = df_payload_parsed \
    .withColumn("monats_id_clean", F.trim(F.col("monats_id"))) \
    .withColumn("debitor_id_clean", F.trim(F.col("debitor_id"))) \
    .withColumn("rechnung_id_clean", F.trim(F.col("rechnung_id"))) \
    .withColumn("rechnung_datum_clean", F.trim(F.col("rechnung_datum"))) \
    .withColumn("standardvertrags_id_clean", F.trim(F.col("standardvertrags_id"))) \
    .withColumn("vertrags_id_clean", F.trim(F.col("vertrags_id"))) \
    .withColumn("rech_leistung_id_carm_clean", F.trim(F.col("rech_leistung_id_carm"))) \
    .withColumn("rechpos_brutto_eur_clean", F.trim(F.col("rechpos_brutto_eur"))) \
    .withColumn("rechpos_netto_eur_clean", F.trim(F.col("rechpos_netto_eur"))) \
    .withColumn("rechpos_mwst_eur_clean", F.trim(F.col("rechpos_mwst_eur")))

# Perform validations copying legacy GDE logic precisely
df_payload_validated = df_payload_cleaned.select(
    # monats_id validation
    F.when(is_blank_or_null(F.col("monats_id")), F.raise_error(F.lit("Invalid Data in field monats_id")))
     .when(~F.col("monats_id_clean").rlike(r"^\d{6}$"), F.raise_error(F.lit("Invalid data format in monats_id")))
     .otherwise(F.col("monats_id_clean")).alias("monats_id"),
     
    # debitor_id validation
    F.when(is_blank_or_null(F.col("debitor_id")), F.raise_error(F.lit("Invalid Data in field debitor_id")))
     .otherwise(F.col("debitor_id_clean")).alias("debitor_id"),
     
    # rechnung_id validation
    F.when(is_blank_or_null(F.col("rechnung_id")), F.raise_error(F.lit("Invalid Data in field rechnung_id")))
     .otherwise(F.col("rechnung_id_clean")).alias("rechnung_id"),
     
    # rechnung_datum validation
    F.when(is_blank_or_null(F.col("rechnung_datum")), F.raise_error(F.lit("Invalid Data in field rechnung_datum")))
     .when(~F.col("rechnung_datum_clean").rlike(r"^\d{8}$"), F.raise_error(F.lit("Invalid data format in rechnung_datum")))
     .otherwise(F.to_date(F.col("rechnung_datum_clean"), "yyyyMMdd")).alias("rechnung_datum"),
     
    # standardvertrags_id validation
    F.when(is_blank_or_null(F.col("standardvertrags_id")), F.raise_error(F.lit("Invalid Data in field standardvertrags_id")))
     .when((F.col("standardvertrags_id_clean") != "#") & ~F.col("standardvertrags_id_clean").rlike(r"^-?\d+(\.\d+)?$"), F.raise_error(F.lit("Invalid data format in standardvertrags_id")))
     .otherwise(F.when(F.col("standardvertrags_id_clean") == "#", F.lit(None)).otherwise(F.col("standardvertrags_id_clean").cast("decimal(18,2)"))).alias("standardvertrags_id"),
     
    # vertrags_id validation
    F.when(is_blank_or_null(F.col("vertrags_id")), F.raise_error(F.lit("Invalid Data in field vertrags_id")))
     .when((F.col("vertrags_id_clean") != "#") & ~F.col("vertrags_id_clean").rlike(r"^-?\d+(\.\d+)?$"), F.raise_error(F.lit("Invalid data format in vertrags_id")))
     .otherwise(F.when(F.col("vertrags_id_clean") == "#", F.lit(None)).otherwise(F.col("vertrags_id_clean").cast("decimal(18,2)"))).alias("vertrags_id"),
     
    # rech_leistung_id_carm validation
    F.when(is_blank_or_null(F.col("rech_leistung_id_carm")), F.raise_error(F.lit("Invalid Data in field rech_leistung_id_carm")))
     .otherwise(F.col("rech_leistung_id_carm_clean")).alias("rech_leistung_id_carm"),
     
    # rechpos_brutto_eur validation
    F.when(is_blank_or_null(F.col("rechpos_brutto_eur")), F.raise_error(F.lit("Invalid Data in field rechpos_brutto_eur")))
     .when(~F.col("rechpos_brutto_eur_clean").rlike(r"^-?\d+(\.\d+)?$"), F.raise_error(F.lit("Invalid data format in rechpos_brutto_eur")))
     .otherwise(F.col("rechpos_brutto_eur_clean").cast("decimal(18,2)")).alias("rechpos_brutto_eur"),
     
    # rechpos_netto_eur validation
    F.when(is_blank_or_null(F.col("rechpos_netto_eur")), F.raise_error(F.lit("Invalid Data in field rechpos_netto_eur")))
     .when(~F.col("rechpos_netto_eur_clean").rlike(r"^-?\d+(\.\d+)?$"), F.raise_error(F.lit("Invalid data format in rechpos_netto_eur")))
     .otherwise(F.col("rechpos_netto_eur_clean").cast("decimal(18,2)")).alias("rechpos_netto_eur"),
     
    # rechpos_mwst_eur validation
    F.when(is_blank_or_null(F.col("rechpos_mwst_eur")), F.raise_error(F.lit("Invalid Data in field rechpos_mwst_eur")))
     .when(~F.col("rechpos_mwst_eur_clean").rlike(r"^-?\d+(\.\d+)?$"), F.raise_error(F.lit("Invalid data format in rechpos_mwst_eur")))
     .otherwise(F.col("rechpos_mwst_eur_clean").cast("decimal(18,2)")).alias("rechpos_mwst_eur"),
     
    # Inherent values & attributes
    F.col("pooling"),
    F.col("rechnungvertrag_id").cast("decimal(18,2)").alias("rechnungvertrag_id"),
    F.col("prob_vertrag_id"),
    F.col("prob_provider_kenn"),
    F.col("anz_leistungen").cast("decimal(18,2)").alias("anz_leistungen"),
    F.col("anz_tickets").cast("decimal(18,2)").alias("anz_tickets"),
    F.col("rpos_geschaftsform_kenn"),
    F.col("vas_kenn"),
    F.col("kennung5").cast("decimal(18,2)").alias("verkauftes_basisprodukt_id"),
    F.lit("#").alias("kontier_grp_id"),
    F.col("typ")
)

# Resolve abs_grp from rechnung_id
df_payload_validated = df_payload_validated.withColumn(
    "abs_grp",
    F.when(~is_blank_or_null(F.substring(F.col("rechnung_id"), 9, 5)), F.trim(F.substring(F.col("rechnung_id"), 9, 5)))
     .otherwise(F.lit("#"))
)

# Filter and group the Master Contract Ledger
df_c_vertrag_filtered = df_c_vertrag_src.filter(F.col("gueltig_bis") >= F.to_date(F.lit("20050401"), "yyyyMMdd"))

window_spec = Window.partitionBy("vertrag_id_carmen").orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())

df_contract_dedup = df_c_vertrag_filtered \
    .withColumn("rankindex", F.row_number().over(window_spec)) \
    .filter(F.col("rankindex") == 1) \
    .drop("rankindex")

# Left join payload with master ledger
df_joined_payload = df_payload_validated.join(
    df_contract_dedup,
    on=df_payload_validated.vertrags_id == df_contract_dedup.vertrag_id_carmen,
    how="left"
)

# Proof Join validity checks
month_last_day = F.last_day(F.to_date(F.col("monats_id"), "yyyyMM"))
valid_flag_expr = F.when(
    (F.col("gueltig_von").isNull() | (month_last_day > F.col("gueltig_von"))) &
    (F.col("gueltig_bis").isNull() | (month_last_day <= F.col("gueltig_bis"))),
    0
).otherwise(1)

df_proofed_payload = df_joined_payload.select(
    df_payload_validated["*"],
    F.when(valid_flag_expr == 0, df_contract_dedup["rahmenvertrag_id"]).otherwise(F.lit(None)).alias("rahmenvertrag_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["dwh_vertrag_id"]).otherwise(F.lit(None)).alias("dwh_vertrag_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["dwh_gp_id"]).otherwise(F.lit(None)).alias("dwh_gp_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["dwh_konto_id"]).otherwise(F.lit(None)).alias("dwh_konto_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["dwh_tarifgr_id"]).otherwise(F.lit(None)).alias("dwh_tarifgr_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["vo_kenn"]).otherwise(F.lit(None)).alias("vo_kenn_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["zv_id"]).otherwise(F.lit(None)).alias("zv_id_checked"),
    F.when(valid_flag_expr == 0, df_contract_dedup["gueltig_von"]).otherwise(F.lit(None)).alias("gueltig_von_checked")
)

df_proofed_payload.cache()

# -----------------------------------------------------
# Define streams mapped to target reload definitions
# -----------------------------------------------------

# Helper for left anti join deletions before inserting to destinations
def save_with_paired_reload(df_new, table_name):
    full_table_path = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    try:
        df_target_exist = spark.read.format("bigquery").option("table", full_table_path).load()
        
        # Delete matches by left anti join
        df_keys_to_delete = df_new.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm").distinct()
        df_cleared = df_target_exist.join(
            df_keys_to_delete,
            on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
            how="leftanti"
        )
        df_final = df_cleared.unionByName(df_new, allowMissingColumns=True)
    except Exception as e:
        print(f"Target table {table_name} does not exist. Writing data directly. Details: {e}")
        df_final = df_new
        
    write_to_bq(df_final, table_name, mode="overwrite")

# Target 1: dwh_ta_f_rpos_carm Reload Stream
df_general_carm = df_proofed_payload.select(
    F.col("monats_id"), F.col("debitor_id"), F.col("kontier_grp_id"), F.col("rechnung_id"), F.col("rechnung_datum"),
    F.col("standardvertrags_id"), F.col("vertrags_id"), F.col("rech_leistung_id_carm"), F.col("rechpos_brutto_eur"),
    F.col("rechpos_netto_eur"), F.col("rechpos_mwst_eur"), F.col("abs_grp"), F.col("pooling"), F.col("rechnungvertrag_id"),
    F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"), F.col("anz_tickets"),
    F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("verkauftes_basisprodukt_id"),
    F.col("rahmenvertrag_id_checked").alias("rahmenvertrag")
)
save_with_paired_reload(df_general_carm, "dwh_ta_f_rpos_carm")

# Target 2: dwh_ta_f_rpos_fact_carm Reload Stream (rpos_geschaftsform_kenn == 'F')
df_factoring_f = df_proofed_payload.filter(F.col("rpos_geschaftsform_kenn") == "F").select(
    F.col("monats_id"), F.col("debitor_id"), F.col("kontier_grp_id"), F.col("rechnung_id"), F.col("rechnung_datum"),
    F.col("standardvertrags_id"), F.col("vertrags_id"), F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
    F.col("rechpos_brutto_eur"), F.col("rechpos_netto_eur"), F.col("rechpos_mwst_eur"), F.col("abs_grp"), F.col("pooling"),
    F.col("rechnungvertrag_id"), F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"),
    F.col("anz_tickets"), F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("verkauftes_basisprodukt_id"),
    F.col("rahmenvertrag_id_checked").alias("rahmenvertrag")
)
save_with_paired_reload(df_factoring_f, "dwh_ta_f_rpos_fact_carm")

# Target 3: dwh_ta_f_gpos_fact_carm Reload Stream (rpos_geschaftsform_kenn == 'G')
df_factoring_g = df_proofed_payload.filter(F.col("rpos_geschaftsform_kenn") == "G").select(
    F.col("monats_id"), F.col("debitor_id"), F.col("kontier_grp_id"), F.col("rechnung_id"), F.col("rechnung_datum"),
    F.col("standardvertrags_id"), F.col("vertrags_id"), F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
    F.col("rechpos_brutto_eur"), F.col("rechpos_netto_eur"), F.col("rechpos_mwst_eur"), F.col("abs_grp"), F.col("pooling"),
    F.col("rechnungvertrag_id"), F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"),
    F.col("anz_tickets"), F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("verkauftes_basisprodukt_id"),
    F.col("rahmenvertrag_id_checked").alias("rahmenvertrag")
)
save_with_paired_reload(df_factoring_g, "dwh_ta_f_gpos_fact_carm")

# Target 4: dwh_ta_f_rpos_reselling_carm Reload Stream (rpos_geschaftsform_kenn == 'R')
df_reselling = df_proofed_payload.filter(F.col("rpos_geschaftsform_kenn") == "R").select(
    F.col("monats_id"), F.col("debitor_id"), F.col("kontier_grp_id"), F.col("rechnung_id"), F.col("rechnung_datum"),
    F.col("standardvertrags_id"), F.col("vertrags_id"), F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm"),
    F.col("rechpos_brutto_eur"), F.col("rechpos_netto_eur"), F.col("rechpos_mwst_eur"), F.col("abs_grp"), F.col("pooling"),
    F.col("rechnungvertrag_id"), F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"),
    F.col("anz_tickets"), F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("verkauftes_basisprodukt_id"),
    F.col("rahmenvertrag_id_checked").alias("rahmenvertrag")
)
save_with_paired_reload(df_reselling, "dwh_ta_f_rpos_reselling_carm")

# Target 5: dwh_ta_t_rpos_carm Reload Stream (typ == 'T')
df_temp_data = df_proofed_payload.filter(F.col("typ") == "T").select(
    F.col("monats_id"), F.col("debitor_id"), F.col("kontier_grp_id"), F.col("rechnung_id"), F.col("rechnung_datum"),
    F.col("standardvertrags_id"), F.col("vertrags_id"), F.col("rech_leistung_id_carm"), F.col("rechpos_brutto_eur"),
    F.col("rechpos_netto_eur"), F.col("rechpos_mwst_eur"), F.col("abs_grp"), F.col("pooling"), F.col("rechnungvertrag_id"),
    F.col("prob_vertrag_id"), F.col("prob_provider_kenn"), F.col("anz_leistungen"), F.col("anz_tickets"),
    F.col("rpos_geschaftsform_kenn"), F.col("vas_kenn"), F.col("verkauftes_basisprodukt_id"),
    F.to_timestamp(F.lit("19000101000000"), "yyyyMMddHHmmss").alias("bearbeitung_datum")
)
save_with_paired_reload(df_temp_data, "dwh_ta_t_rpos_carm")

df_proofed_payload.unpersist()

# -----------------------------------------------------
# Process Operational Run Logging / Enderecord Metrics
# -----------------------------------------------------

df_trailer_fields = df_trailer_lines.select(
    F.split(F.col("datensatz_rest"), ";").alias("fields")
)

df_trailer_parsed = df_trailer_fields.select(
    F.col("fields").getItem(0).alias("bemerkung"),
    F.col("fields").getItem(1).alias("stichtag"),
    F.col("fields").getItem(2).alias("anzahl"),
    F.col("fields").getItem(3).alias("inhalt"),
    F.col("fields").getItem(4).alias("erstellt_am")
)

df_trailer_processed = df_trailer_parsed.select(
    F.col("bemerkung").alias("dateiname"),
    F.col("bemerkung").alias("zusatzinfo"),
    F.col("stichtag"),
    F.col("anzahl").cast("decimal(18,2)").alias("anzahl_ds_eof"),
    F.col("inhalt").alias("enderecord_text"),
    F.when(F.instr(F.col("erstellt_am"), ";") == 0, F.col("erstellt_am"))
     .otherwise(F.substring(F.col("erstellt_am"), 1, F.length(F.col("erstellt_am")) - 1))
     .alias("erstellt_am")
)

trailer_row = df_trailer_processed.first()
if trailer_row:
    dateiname = trailer_row["dateiname"]
    zusatzinfo = trailer_row["zusatzinfo"]
    stichtag = trailer_row["stichtag"]
    anzahl_ds_eof = trailer_row["anzahl_ds_eof"]
    enderecord_text = trailer_row["enderecord_text"]
    
    # Update Logging Table dwh_ta_k_meldungen via Read-Modify-Write
    if BHB_Eintragsnr:
        try:
            df_meldungen = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_meldungen").load()
            df_meldungen_updated = df_meldungen.withColumn(
                "anzahl_ds_eof",
                F.when(F.col("entrynr") == BHB_Eintragsnr, F.lit(anzahl_ds_eof)).otherwise(F.col("anzahl_ds_eof"))
            ).withColumn(
                "dateiname",
                F.when(F.col("entrynr") == BHB_Eintragsnr, F.lit(dateiname)).otherwise(F.col("dateiname"))
            ).withColumn(
                "enderecord_text",
                F.when(F.col("entrynr") == BHB_Eintragsnr, F.lit(enderecord_text)).otherwise(F.col("enderecord_text"))
            ).withColumn(
                "zusatzinfo",
                F.when(F.col("entrynr") == BHB_Eintragsnr, F.lit(zusatzinfo)).otherwise(F.col("zusatzinfo"))
            )
            write_to_bq(df_meldungen_updated, "dwh_ta_k_meldungen", mode="overwrite")
        except Exception as e:
            print(f"Could not update table dwh_ta_k_meldungen: {e}")

    # Process & Upsert table dwh_ta_k_rech_absgrp
    df_absgrp_processed = df_trailer_processed.select(
        F.date_format(
            F.add_months(F.to_date(F.substring(F.col("stichtag"), 1, 6), "yyyyMM"), -1),
            "yyyyMM"
        ).alias("monats_id"),
        F.substring(F.col("dateiname"), 10, 5).alias("abs_grp"),
        F.col("dateiname"),
        F.to_date(F.col("stichtag"), "yyyyMMdd").alias("rechnung_datum"),
        F.lit("P").alias("rechnungsteil"),
        F.current_timestamp().alias("ladedatum")
    )

    try:
        df_absgrp_target = spark.read.format("bigquery").option("table", f"{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_k_rech_absgrp").load()
        join_keys = ["monats_id", "abs_grp", "dateiname", "rechnungsteil"]
        df_absgrp_cleared = df_absgrp_target.join(df_absgrp_processed, on=join_keys, how="leftanti")
        df_absgrp_final = df_absgrp_cleared.unionByName(df_absgrp_processed, allowMissingColumns=True)
        write_to_bq(df_absgrp_final, "dwh_ta_k_rech_absgrp", mode="overwrite")
    except Exception as e:
        print(f"Target table dwh_ta_k_rech_absgrp does not exist. Saving directly. Details: {e}")
        write_to_bq(df_absgrp_processed, "dwh_ta_k_rech_absgrp", mode="overwrite")