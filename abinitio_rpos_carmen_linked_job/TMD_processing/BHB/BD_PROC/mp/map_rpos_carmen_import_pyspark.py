#!/usr/bin/env python3
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, trim, to_date, substring, regexp_replace, lit, row_number, last_day, current_timestamp
from pyspark.sql.window import Window
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def subtract_one_month(dt):
    val = dt.year * 12 + dt.month - 2
    new_year = val // 12
    new_month = val % 12 + 1
    return datetime(new_year, new_month, 1)

def main():
    logging.info("Starting map_rpos_carmen_import PySpark Pipeline...")

    # Load environments
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    BQ_DATASET = os.environ.get("BQ_DATASET")

    BHB_Quellverzeichnis = os.environ.get("BHB_Quellverzeichnis")
    BHB_Dateiname = os.environ.get("BHB_Dateiname")
    BHB_Eintragsnr = os.environ.get("BHB_Eintragsnr")

    BHB_Nutzdatensatzkennung = os.environ.get("BHB_Nutzdatensatzkennung", "P")
    BHB_Endedatensatzkennung = os.environ.get("BHB_Endedatensatzkennung", "X")

    if not all([GCP_PROJECT, GCS_BUCKET, BQ_DATASET, BHB_Quellverzeichnis, BHB_Dateiname, BHB_Eintragsnr]):
        logging.error("Missing required environment configuration variables for GCS and BigQuery target mapping.")
        sys.exit(1)

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DW_RPOS_CARM_IMPORT_pyspark") \
        .getOrCreate()

    # BigQuery Client for staging & trailer updates
    bq_client = bigquery.Client(project=GCP_PROJECT)

    # Construct GCS source uri
    quell_dir = BHB_Quellverzeichnis.strip("/")
    input_path = f"gs://{GCS_BUCKET}/{quell_dir}/{BHB_Dateiname}"
    logging.info(f"Loading input file from path: {input_path}")

    # Read raw text dataframe
    raw_df = spark.read.text(input_path)

    # Filter billing lines
    parsed_raw_df = raw_df.filter(col("value").startswith(lit(BHB_Nutzdatensatzkennung)))

    # Parse EOF trailer record on the driver
    trailer_rows = raw_df.filter(col("value").startswith(lit(BHB_Endedatensatzkennung))).collect()
    trailer_record = trailer_rows[0][0] if trailer_rows else None

    # Load active Contract Dimension lookup from BigQuery
    logging.info("Loading active Contract Dimension lookup from BigQuery...")
    contracts_df = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_c_vertrag") \
        .load() \
        .filter(col("gueltig_bis") >= lit("2005-04-01")) \
        .withColumn("vertrag_id_carmen", col("vertrag_id_carmen").cast("long"))

    # Parse billing records into columns
    split_col = split(col("value"), ";")
    parsed_df = parsed_raw_df.select(
        split_col.getItem(1).alias("monats_id"),
        split_col.getItem(2).alias("debitor_id"),
        split_col.getItem(3).alias("rechnung_id"),
        split_col.getItem(4).alias("rechnung_datum_str"),
        split_col.getItem(5).alias("standardvertrags_id_str"),
        split_col.getItem(6).alias("vertrags_id_str"),
        split_col.getItem(7).alias("rech_leistung_id_carm"),
        split_col.getItem(8).alias("rechpos_brutto_eur_str"),
        split_col.getItem(9).alias("rechpos_netto_eur_str"),
        split_col.getItem(10).alias("rechpos_mwst_eur_str"),
        split_col.getItem(11).alias("pooling"),
        split_col.getItem(12).alias("rechnungvertrag_id_str"),
        split_col.getItem(13).alias("anz_leistungen_str"),
        split_col.getItem(14).alias("anz_tickets_str"),
        split_col.getItem(15).alias("rpos_geschaftsform_kenn"),
        split_col.getItem(16).alias("vas_kenn")
    )

    # Convert types mapping standard schema specifications
    parsed_df = parsed_df \
        .withColumn("rechnung_datum", to_date(col("rechnung_datum_str"), "yyyyMMdd")) \
        .withColumn("standardvertrags_id", when(col("standardvertrags_id_str") == "#", lit(0)).otherwise(col("standardvertrags_id_str").cast("long"))) \
        .withColumn("vertrags_id", when(col("vertrags_id_str") == "#", lit(0)).otherwise(col("vertrags_id_str").cast("long"))) \
        .withColumn("rechpos_brutto_eur", regexp_replace(col("rechpos_brutto_eur_str"), ",", ".").cast("double")) \
        .withColumn("rechpos_netto_eur", regexp_replace(col("rechpos_netto_eur_str"), ",", ".").cast("double")) \
        .withColumn("rechpos_mwst_eur", regexp_replace(col("rechpos_mwst_eur_str"), ",", ".").cast("double")) \
        .withColumn("abs_grp", substring(col("rechnung_id"), 9, 5)) \
        .withColumn("rechnungvertrag_id", col("rechnungvertrag_id_str").cast("long")) \
        .withColumn("anz_leistungen", col("anz_leistungen_str").cast("long")) \
        .withColumn("anz_tickets", col("anz_tickets_str").cast("long"))

    # Join with Contract dimension lookup
    merged_df = parsed_df.join(contracts_df, parsed_df.vertrags_id == contracts_df.vertrag_id_carmen, "left")

    # Temporal boundary check
    merged_df = merged_df.withColumn("monats_date", to_date(col("monats_id"), "yyyyMM"))
    merged_df = merged_df.withColumn("month_last_day", last_day(col("monats_date")))

    valid_cond = (
        (col("gueltig_von").isNull() | (col("month_last_day") > col("gueltig_von"))) &
        (col("gueltig_bis").isNull() | (col("month_last_day") <= col("gueltig_bis")))
    )

    # Nullify temporal data on validation check failure
    merged_df = merged_df.withColumn("rahmenvertrag_id", when(valid_cond, col("rahmenvertrag_id")).otherwise(lit("#"))) \
                         .withColumn("dwh_vertrag_id", when(valid_cond, col("dwh_vertrag_id")).otherwise(lit(0))) \
                         .withColumn("dwh_gp_id", when(valid_cond, col("dwh_gp_id")).otherwise(lit(0))) \
                         .withColumn("dwh_konto_id", when(valid_cond, col("dwh_konto_id")).otherwise(lit(0))) \
                         .withColumn("dwh_tarifgr_id", when(valid_cond, col("dwh_tarifgr_id")).otherwise(lit(0))) \
                         .withColumn("vo_kenn", when(valid_cond, col("vo_kenn")).otherwise(lit("#"))) \
                         .withColumn("gueltig_von", when(valid_cond, col("gueltig_von")).otherwise(lit(None)))

    # Sort and rank within partition group to deduplicate historical records
    window_spec = Window.partitionBy(
        "vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm"
    ).orderBy(
        col("gueltig_von").desc_nulls_last(),
        col("dwh_vertrag_id").desc_nulls_last()
    )

    dedup_df = merged_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")

    # Decode rpos_geschaftsform_kenn fields
    dedup_df = dedup_df.withColumn(
        "rpos_geschaftsform_kenn",
        when((col("rpos_geschaftsform_kenn") == "F") & (col("vas_kenn") == "P30002"), lit("G")).otherwise(col("rpos_geschaftsform_kenn"))
    )

    # Append ingestion metadata timestamp
    final_ingest_df = dedup_df.withColumn("ladedatum", current_timestamp()).cache()

    if final_ingest_df.count() > 0:
        # Segment data streams
        factoring_rechnungen = final_ingest_df.filter(col("rpos_geschaftsform_kenn") == "F")
        factoring_gutschriften = final_ingest_df.filter(col("rpos_geschaftsform_kenn") == "G")
        reselling_records = final_ingest_df.filter(col("rpos_geschaftsform_kenn") == "R")

        final_ingest_df = final_ingest_df.withColumn(
            "typ",
            when(((col("rech_leistung_id_carm") == "RABATT") & (col("vertrags_id") == 0)) | (col("pooling") == "P"), lit("T")).otherwise(lit("F"))
        )
        temp_records = final_ingest_df.filter(col("typ") == "T")
        fact_general_records = final_ingest_df.filter(col("typ") == "F")

        # Execute Targeted Deletes safely using BigQuery staging tables to bypass DML limit limits
        logging.info("Executing BigQuery overlap purging (DELETE execution)...")

        # Distinct keys write mapping
        delete_keys_fact_df = final_ingest_df.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
        delete_keys_fact_df.write.format("bigquery") \
            .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact") \
            .option("writeMethod", "indirect") \
            .mode("overwrite") \
            .save()

        delete_keys_temp_df = final_ingest_df.select("debitor_id", "rechnung_datum", "rechnung_id").distinct()
        delete_keys_temp_df.write.format("bigquery") \
            .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_temp") \
            .option("writeMethod", "indirect") \
            .mode("overwrite") \
            .save()

        # Perform dynamic BigQuery SQL DML deletes
        delete_queries = [
            f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_carm` t
            WHERE EXISTS (
              SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact` s
              WHERE t.rechnung_id = s.rechnung_id
                AND t.rechnung_datum = s.rechnung_datum
                AND t.standardvertrags_id = s.standardvertrags_id
                AND t.vertrags_id = s.vertrags_id
            )
            """,
            f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.ta_f_gpos_fact_carm` t
            WHERE EXISTS (
              SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact` s
              WHERE t.rechnung_id = s.rechnung_id
                AND t.rechnung_datum = s.rechnung_datum
                AND t.standardvertrags_id = s.standardvertrags_id
                AND t.vertrags_id = s.vertrags_id
            )
            """,
            f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_fact_carm` t
            WHERE EXISTS (
              SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact` s
              WHERE t.rechnung_id = s.rechnung_id
                AND t.rechnung_datum = s.rechnung_datum
                AND t.standardvertrags_id = s.standardvertrags_id
                AND t.vertrags_id = s.vertrags_id
            )
            """,
            f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_reselling_carm` t
            WHERE EXISTS (
              SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact` s
              WHERE t.rechnung_id = s.rechnung_id
                AND t.rechnung_datum = s.rechnung_datum
                AND t.standardvertrags_id = s.standardvertrags_id
                AND t.vertrags_id = s.vertrags_id
            )
            """,
            f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.ta_t_rpos_carm` t
            WHERE EXISTS (
              SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_temp` s
              WHERE t.debitor_id = s.debitor_id
                AND t.rechnung_datum = s.rechnung_datum
                AND t.rechnung_id = s.rechnung_id
            )
            """
        ]

        for q in delete_queries:
            bq_client.query(q).result()

        # Ingestion step to destination tables using schema layout
        logging.info("Executing BigQuery final append operations...")
        cols_fact = [
            col("monats_id").cast("int"),
            col("debitor_id"),
            col("rechnung_id"),
            col("rechnung_datum"),
            col("standardvertrags_id").cast("long"),
            col("vertrags_id").cast("long"),
            col("rech_leistung_id_carm"),
            col("rechpos_brutto_eur").cast("double"),
            col("rechpos_netto_eur").cast("double"),
            col("rechpos_mwst_eur").cast("double"),
            col("abs_grp"),
            col("rahmenvertrag_id").alias("rahmenvertrag"),
            col("dwh_vertrag_id").cast("long"),
            col("dwh_gp_id").cast("long"),
            col("dwh_konto_id").cast("long"),
            col("vo_kenn"),
            col("ladedatum")
        ]

        cols_temp = [
            col("monats_id").cast("int"),
            col("debitor_id"),
            col("rechnung_id"),
            col("rechnung_datum"),
            col("standardvertrags_id").cast("long"),
            col("vertrags_id").cast("long"),
            col("rech_leistung_id_carm"),
            col("rechpos_brutto_eur").cast("double"),
            col("rechpos_netto_eur").cast("double"),
            col("rechpos_mwst_eur").cast("double"),
            col("abs_grp"),
            col("pooling"),
            col("rechnungvertrag_id").cast("long"),
            col("dwh_vertrag_id").cast("long"),
            col("dwh_gp_id").cast("long"),
            col("dwh_konto_id").cast("long"),
            col("vo_kenn"),
            col("bearbeitung_datum"),
            col("ladedatum")
        ]

        # Ingest Factoring Rechnungen -> ta_f_rpos_fact_carm
        if factoring_rechnungen.count() > 0:
            factoring_rechnungen.select(cols_fact).write.format("bigquery") \
                .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_fact_carm") \
                .option("writeMethod", "indirect") \
                .mode("append") \
                .save()

        # Ingest Factoring Gutschriften -> ta_f_gpos_fact_carm
        if factoring_gutschriften.count() > 0:
            factoring_gutschriften.select(cols_fact).write.format("bigquery") \
                .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_f_gpos_fact_carm") \
                .option("writeMethod", "indirect") \
                .mode("append") \
                .save()

        # Ingest Reselling records -> ta_f_rpos_reselling_carm
        if reselling_records.count() > 0:
            reselling_records.select(cols_fact).write.format("bigquery") \
                .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_reselling_carm") \
                .option("writeMethod", "indirect") \
                .mode("append") \
                .save()

        # Ingest general facts -> ta_f_rpos_carm
        if fact_general_records.count() > 0:
            fact_general_records.select(cols_fact).write.format("bigquery") \
                .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_f_rpos_carm") \
                .option("writeMethod", "indirect") \
                .mode("append") \
                .save()

        # Ingest Temporary Debit -> ta_t_rpos_carm
        if temp_records.count() > 0:
            temp_records = temp_records.withColumn("bearbeitung_datum", to_date(lit("1900-01-01"), "yyyy-MM-dd"))
            temp_records.select(cols_temp).write.format("bigquery") \
                .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.ta_t_rpos_carm") \
                .option("writeMethod", "indirect") \
                .mode("append") \
                .save()

        # Clean up staging tables
        bq_client.delete_table(f"{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_fact", not_found_ok=True)
        bq_client.delete_table(f"{GCP_PROJECT}.{BQ_DATASET}.stg_delete_keys_temp", not_found_ok=True)

    else:
        logging.info("No records found mapping the specified billing indicator.")

    # Step 13: Parse Trailer (EOF record) & Log audit details to monitoring systems
    if trailer_record:
        logging.info("Parsing EOF trailer and updating control tables...")
        t_fields = trailer_record.split(';')
        t_bemerkung = t_fields[1].strip() if len(t_fields) > 1 else ''
        t_stichtag = t_fields[2].strip() if len(t_fields) > 2 else ''
        t_anzahl = int(t_fields[3].strip()) if len(t_fields) > 3 and t_fields[3].strip() else 0
        t_inhalt = t_fields[4].strip() if len(t_fields) > 4 else ''
        
        stichtag_prefix = t_stichtag[0:6]  # YYYYMM
        stichtag_dt = datetime.strptime(stichtag_prefix, '%Y%m')
        
        # Subtract one month
        monats_id_calc = int(subtract_one_month(stichtag_dt).strftime('%Y%m'))
        abs_grp_calc = t_bemerkung[9:14] if len(t_bemerkung) >= 14 else '#'
        rechnungsteil_calc = "P"
        
        # Update or insert into ta_k_rech_absgrp
        update_q = f"""
            UPDATE `{GCP_PROJECT}.{BQ_DATASET}.ta_k_rech_absgrp`
            SET rechnung_datum = @rechnung_datum, ladedatum = @ladedatum
            WHERE monats_id = @monats_id AND abs_grp = @abs_grp AND dateiname = @dateiname AND rechnungsteil = @rechnungsteil
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("rechnung_datum", "DATE", datetime.strptime(t_stichtag, '%Y%m%d').date()),
                bigquery.ScalarQueryParameter("ladedatum", "TIMESTAMP", datetime.now()),
                bigquery.ScalarQueryParameter("monats_id", "INT64", monats_id_calc),
                bigquery.ScalarQueryParameter("abs_grp", "STRING", abs_grp_calc),
                bigquery.ScalarQueryParameter("dateiname", "STRING", t_bemerkung),
                bigquery.ScalarQueryParameter("rechnungsteil", "STRING", rechnungsteil_calc)
            ]
        )
        result = bq_client.query(update_q, job_config=job_config).result()
        
        if result.num_dml_affected_rows == 0:
            insert_q = f"""
                INSERT INTO `{GCP_PROJECT}.{BQ_DATASET}.ta_k_rech_absgrp` (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
                VALUES (@monats_id, @abs_grp, @dateiname, @rechnung_datum, @rechnungsteil, @ladedatum)
            """
            bq_client.query(insert_q, job_config=job_config).result()

        # Update operational statistics ta_k_meldungen
        update_meldungen = f"""
            UPDATE `{GCP_PROJECT}.{BQ_DATASET}.ta_k_meldungen`
            SET anzahl_ds_eof = @anzahl, dateiname = @dateiname, enderecord_text = @enderecord_text, zusatzinfo = @zusatzinfo
            WHERE entrynr = @entrynr
        """
        job_config_m = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("anzahl", "INT64", t_anzahl),
                bigquery.ScalarQueryParameter("dateiname", "STRING", BHB_Dateiname),
                bigquery.ScalarQueryParameter("enderecord_text", "STRING", t_inhalt),
                bigquery.ScalarQueryParameter("zusatzinfo", "STRING", t_bemerkung),
                bigquery.ScalarQueryParameter("entrynr", "INT64", int(BHB_Eintragsnr))
            ]
        )
        bq_client.query(update_meldungen, job_config=job_config_m).result()

    spark.stop()
    logging.info("PySpark Import Pipeline completed successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()