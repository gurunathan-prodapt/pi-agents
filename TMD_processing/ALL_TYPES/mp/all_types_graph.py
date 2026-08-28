import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# -----------------------------------------------------------------------------
# ENVIRONMENT CONFIGURATION
# -----------------------------------------------------------------------------
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

if not GCP_PROJECT or not BQ_DATASET:
    raise ValueError("GCP_PROJECT and BQ_DATASET environment variables must be set.")

# Resolve directories
ccr_ai_dat_file_dir = os.environ.get("CCR_AI_DAT_FILE_DIR")
if not ccr_ai_dat_file_dir:
    if GCS_BUCKET:
        ccr_ai_dat_file_dir = f"gs://{GCS_BUCKET}/CCR_AI_DAT_FILE_DIR"
    else:
        ccr_ai_dat_file_dir = "/tmp"

tcn_ds_serial_lookup = os.environ.get("TCN_DS_SERIAL_LOOKUP")
if not tcn_ds_serial_lookup:
    if GCS_BUCKET:
        tcn_ds_serial_lookup = f"gs://{GCS_BUCKET}/TCN_DS_SERIAL_LOOKUP"
    else:
        tcn_ds_serial_lookup = "/tmp"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("agg_distribute_measures_ccos") \
    .getOrCreate()

# -----------------------------------------------------------------------------
# STEP 1: Read lookup source tables from BigQuery
# -----------------------------------------------------------------------------
df_ta_f_teamsichtbarkeit = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ccr_ta_f_teamsichtbarkeit") \
    .load()
df_ta_f_teamsichtbarkeit.createOrReplaceTempView("ta_f_teamsichtbarkeit")

df_ta_s_sdm_team = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ccr_ta_s_sdm_team") \
    .load()
df_ta_s_sdm_team.createOrReplaceTempView("ta_s_sdm_team")

df_ta_s_sdm_abteilung = spark.read.format("bigquery") \
    .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ccr_ta_s_sdm_abteilung") \
    .load()
df_ta_s_sdm_abteilung.createOrReplaceTempView("ta_s_sdm_abteilung")

# -----------------------------------------------------------------------------
# STEP 2: Generate Team Virtuality Lookup View
# -----------------------------------------------------------------------------
df_teamvirt_ccos = spark.sql("""
    SELECT 
      vir.stichtag, 
      tea.sdm_team_id 
    FROM 
      ta_f_teamsichtbarkeit vir,
      ta_s_sdm_team tea,
      ta_s_sdm_abteilung abt
    WHERE vir.team_sichtbarkeitstyp_id = 10
      AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1)
      AND tea.sdm_team_id = vir.sdm_team_id
      AND abt.sdm_abteilung_id = tea.sdm_abteilung_id
    ORDER BY vir.stichtag, tea.sdm_team_id
""")
df_teamvirt_ccos.createOrReplaceTempView("lkp_teamvirt_ccos")

# Cache lookup since it is used by multiple downstream join branches
df_teamvirt_ccos.cache()
df_teamvirt_ccos.count()

# Write the lookup to file target lkp_team_virt_ccos.dat
df_teamvirt_ccos.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{tcn_ds_serial_lookup}/lkp_team_virt_ccos.dat")

# -----------------------------------------------------------------------------
# STEP 3: Read Main Input Stream (Flat-file source x_tos_measures.dat)
# -----------------------------------------------------------------------------
schema_x_tos_measures = StructType([
    StructField("stichtag", StringType(), True),
    StructField("kkm_kampagne_id", StringType(), True),
    StructField("tos_offer_folder_id", StringType(), True),
    StructField("tos_offer_id", StringType(), True),
    StructField("tos_rank", StringType(), True),
    StructField("vo_kenn", StringType(), True),
    StructField("vt_segment_id", StringType(), True),
    StructField("kd_segment_id", StringType(), True),
    StructField("sdm_team_id", StringType(), True),
    StructField("kkm_medium_id", StringType(), True),
    StructField("kkm_richtung_id", StringType(), True),
    StructField("tos_mea_group_name", StringType(), True),
    StructField("mea_1", StringType(), True),
    StructField("mea_2", StringType(), True),
    StructField("tcn_product_id", StringType(), True),
    StructField("vt_bic_segment_endntz_id", StringType(), True),
    StructField("vt_bic_segment_entsch_id", StringType(), True),
    StructField("one_segment_id", StringType(), True)
])

try:
    df_x_tos_measures = spark.read \
        .option("delimiter", ";") \
        .option("header", "false") \
        .schema(schema_x_tos_measures) \
        .csv(f"{ccr_ai_dat_file_dir}/x_tos_measures.dat")
except Exception as e:
    print(f"WARNING: x_tos_measures.dat not found or empty — using empty schema: {e}")
    df_x_tos_measures = spark.createDataFrame([], schema_x_tos_measures)

# Cache measures since it is queried multiple times downstream
df_x_tos_measures.cache()
df_x_tos_measures.count()

# Prepare lookup DataFrame for left join
df_lkp = df_teamvirt_ccos.select(
    F.col("stichtag").alias("lkp_stichtag"),
    F.col("sdm_team_id").alias("lkp_sdm_team_id")
)

# Join and resolve sdm_team_id
df_joined = df_x_tos_measures.join(
    df_lkp,
    (df_x_tos_measures["stichtag"] == df_lkp["lkp_stichtag"]) & 
    (df_x_tos_measures["sdm_team_id"] == df_lkp["lkp_sdm_team_id"]),
    how="left"
)

df_resolved = df_joined.withColumn(
    "resolved_sdm_team_id",
    F.when(F.col("sdm_team_id").isNotNull(),
           F.coalesce(F.col("lkp_sdm_team_id"), F.lit(""))
    ).otherwise(F.lit(None))
).drop("lkp_stichtag", "lkp_sdm_team_id")

df_resolved = df_resolved.drop("sdm_team_id").withColumnRenamed("resolved_sdm_team_id", "sdm_team_id")

# -----------------------------------------------------------------------------
# STEP 4: Process Cancellations Standard and Weekly Streams
# -----------------------------------------------------------------------------
df_cancellations_standard = df_resolved \
    .filter(F.col("tos_mea_group_name") == "CANCELLATIONS") \
    .withColumn("anzahl_stornos", F.col("mea_1"))

df_cancellations_standard.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_cancellations.dat")

df_cancellations_wk = df_x_tos_measures \
    .filter(
        (F.col("tos_mea_group_name") == "CANCELLATIONS") &
        (F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date()))
    ) \
    .withColumn("anzahl_stornos", F.col("mea_1"))

df_cancellations_wk.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_cancellations_wk.dat")

# -----------------------------------------------------------------------------
# STEP 5: Process Products Standard and Weekly Streams
# -----------------------------------------------------------------------------
df_products_standard = df_resolved \
    .filter(F.col("tos_mea_group_name") == "PRODUCTS") \
    .withColumn("anzahl_produkte", F.col("mea_1")) \
    .withColumn("tcn_offer_product_id", F.concat(F.trim(F.col("tos_offer_id")), F.lit("~"), F.trim(F.col("tcn_product_id"))))

df_products_standard.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_products.dat")

df_products_wk = df_x_tos_measures \
    .filter(
        (F.col("tos_mea_group_name") == "PRODUCTS") &
        (F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date()))
    ) \
    .withColumn("anzahl_produkte", F.col("mea_1")) \
    .withColumn("tcn_offer_product_id", F.concat(F.trim(F.col("tos_offer_id")), F.lit("~"), F.trim(F.col("tcn_product_id"))))

df_products_wk.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_products_wk.dat")

# -----------------------------------------------------------------------------
# STEP 6: Process Quotes & Contracts Standard and Weekly Streams
# -----------------------------------------------------------------------------
df_quotes_contracts_standard = df_resolved \
    .filter((F.col("tos_mea_group_name") == "CONTRACTS") | (F.col("tos_mea_group_name") == "QUOTES")) \
    .withColumn("anzahl_angebote", F.when(F.col("tos_mea_group_name") == "QUOTES", F.col("mea_1")).otherwise(F.lit(None))) \
    .withColumn("subventionen", F.when(F.col("tos_mea_group_name") == "QUOTES", F.regexp_replace(F.col("mea_2"), "\\.", ",")).otherwise(F.lit(None))) \
    .withColumn("anzahl_vertraege", F.when(F.col("tos_mea_group_name") == "CONTRACTS", F.col("mea_1")).otherwise(F.lit(None)))

df_quotes_contracts_standard.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_quotes_contracts.dat")

df_quotes_contracts_wk = df_x_tos_measures \
    .filter(
        ((F.col("tos_mea_group_name") == "CONTRACTS") | (F.col("tos_mea_group_name") == "QUOTES")) &
        (F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date()))
    ) \
    .withColumn("anzahl_angebote", F.when(F.col("tos_mea_group_name") == "QUOTES", F.col("mea_1")).otherwise(F.lit(None))) \
    .withColumn("subventionen", F.when(F.col("tos_mea_group_name") == "QUOTES", F.regexp_replace(F.col("mea_2"), "\\.", ",")).otherwise(F.lit(None))) \
    .withColumn("anzahl_vertraege", F.when(F.col("tos_mea_group_name") == "CONTRACTS", F.col("mea_1")).otherwise(F.lit(None)))

df_quotes_contracts_wk.coalesce(1).write.mode("overwrite") \
    .option("delimiter", ";") \
    .option("header", "false") \
    .csv(f"{ccr_ai_dat_file_dir}/tos_quotes_contracts_wk.dat")

# Cleanup Cached DataFrames
df_teamvirt_ccos.unpersist()
df_x_tos_measures.unpersist()