#!/usr/bin/env python3
import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType, TimestampType
from pyspark.sql.window import Window
from google.cloud import bigquery

def main():
    parser = argparse.ArgumentParser(description="PySpark map_rpos_carmen_import ETL conversion pipeline")
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--entry-nr', required=True)
    parser.add_argument('--file-mask', required=True)
    parser.add_argument('--src-dir', required=True)
    parser.add_argument('--dst-dir', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # 1. READ INPUT FILES
    raw_path = f"gs://{args.bucket}/{args.src_dir}*"
    raw_df = spark.read.text(raw_path)

    # Filter and parse Payload records (kennzeichen == 'P')
    payload_raw = raw_df.filter(F.split(F.col("value"), ";").getItem(0) == "P")
    parsed_payload = payload_raw.select(
        F.split(F.col("value"), ";").getItem(0).alias("kennzeichen"),
        F.split(F.col("value"), ";").getItem(1).alias("monats_id"),
        F.split(F.col("value"), ";").getItem(2).alias("debitor_id"),
        F.split(F.col("value"), ";").getItem(3).alias("rechnung_id"),
        F.split(F.col("value"), ";").getItem(4).alias("rechnung_datum"),
        F.split(F.col("value"), ";").getItem(5).alias("standardvertrags_id"),
        F.split(F.col("value"), ";").getItem(6).alias("vertrags_id"),
        F.split(F.col("value"), ";").getItem(7).alias("rech_leistung_id_carm"),
        F.split(F.col("value"), ";").getItem(8).cast(DecimalType(15,4)).alias("rechpos_brutto_eur"),
        F.split(F.col("value"), ";").getItem(9).cast(DecimalType(15,4)).alias("rechpos_netto_eur"),
        F.split(F.col("value"), ";").getItem(10).cast(DecimalType(15,4)).alias("rechpos_mwst_eur"),
        F.split(F.col("value"), ";").getItem(11).alias("abs_grp"),
        F.split(F.col("value"), ";").getItem(12).alias("pooling"),
        F.split(F.col("value"), ";").getItem(13).cast(DecimalType(15,0)).alias("rechnungvertrag_id"),
        F.split(F.col("value"), ";").getItem(14).alias("prob_vertrag_id"),
        F.split(F.col("value"), ";").getItem(15).alias("prob_provider_kenn"),
        F.split(F.col("value"), ";").getItem(16).cast(DecimalType(15,0)).alias("anz_leistungen"),
        F.split(F.col("value"), ";").getItem(17).cast(DecimalType(15,0)).alias("anz_tickets"),
        F.split(F.col("value"), ";").getItem(18).alias("rpos_geschaftsform_kenn"),
        F.split(F.col("value"), ";").getItem(19).alias("vas_kenn"),
        F.split(F.col("value"), ";").getItem(20).cast(DecimalType(15,0)).alias("verkauftes_basisprodukt_id")
    ).cache()

    # Filter and parse Footer records (kennzeichen == 'X')
    footer_raw = raw_df.filter(F.split(F.col("value"), ";").getItem(0) == "X")
    footer_df = footer_raw.select(
        F.split(F.col("value"), ";").getItem(0).alias("kennzeichen"),
        F.split(F.col("value"), ";").getItem(1).alias("bemerkung"),
        F.split(F.col("value"), ";").getItem(2).alias("stichtag"),
        F.split(F.col("value"), ";").getItem(3).alias("anzahl"),
        F.split(F.col("value"), ";").getItem(4).alias("inhalt"),
        F.split(F.col("value"), ";").getItem(5).alias("erstellt_am")
    )

    # 2. VALIDATION & FORMATTING
    cleaned_nutzdaten = parsed_payload.withColumn(
        "monats_id_parsed", F.to_date(F.trim(F.col("monats_id")), "yyyyMM")
    ).withColumn(
        "rechnung_datum", F.to_date(F.trim(F.col("rechnung_datum")), "yyyyMMdd")
    ).withColumn(
        "debitor_id", F.trim(F.col("debitor_id"))
    ).withColumn(
        "rechnung_id", F.trim(F.col("rechnung_id"))
    ).withColumn(
        "standardvertrags_id", F.when(F.trim(F.col("standardvertrags_id")) != "#", F.trim(F.col("standardvertrags_id"))).otherwise("0").cast(DecimalType(15,0))
    ).withColumn(
        "vertrags_id", F.when(F.trim(F.col("vertrags_id")) != "#", F.trim(F.col("vertrags_id"))).otherwise("0").cast(DecimalType(15,0))
    )

    # Throw exception for invalid formats if present (force_error replacement)
    if cleaned_nutzdaten.filter(F.col("monats_id_parsed").isNull()).count() > 0:
        raise ValueError("Invalid data format in monats_id")
    if cleaned_nutzdaten.filter(F.col("rechnung_datum").isNull()).count() > 0:
        raise ValueError("Invalid data format in rechnung_datum")

    # 3. RABATT AGGREGATION
    rabatt_df = cleaned_nutzdaten.filter(F.col("rech_leistung_id_carm") == "RABATT")
    non_rabatt_df = cleaned_nutzdaten.filter(F.col("rech_leistung_id_carm") != "RABATT")

    aggregated_rabatt = rabatt_df.groupBy("rechnung_datum", "rechnung_id", "standardvertrags_id", "vertrags_id", "debitor_id").agg(
        F.sum("rechpos_brutto_eur").alias("rechpos_brutto_eur"),
        F.sum("rechpos_netto_eur").alias("rechpos_netto_eur"),
        F.sum("rechpos_mwst_eur").alias("rechpos_mwst_eur"),
        F.first("monats_id").alias("monats_id"),
        F.first("rech_leistung_id_carm").alias("rech_leistung_id_carm"),
        F.first("abs_grp").alias("abs_grp"),
        F.first("pooling").alias("pooling"),
        F.first("rechnungvertrag_id").alias("rechnungvertrag_id"),
        F.first("prob_vertrag_id").alias("prob_vertrag_id"),
        F.first("prob_provider_kenn").alias("prob_provider_kenn"),
        F.sum("anz_leistungen").alias("anz_leistungen"),
        F.sum("anz_tickets").alias("anz_tickets"),
        F.first("rpos_geschaftsform_kenn").alias("rpos_geschaftsform_kenn"),
        F.first("vas_kenn").alias("vas_kenn"),
        F.first("verkauftes_basisprodukt_id").alias("verkauftes_basisprodukt_id")
    )

    consolidated_df = non_rabatt_df.unionByName(aggregated_rabatt)

    # 4. CONTRACT ENRICHMENT (Join with dwh$ta_c_vertrag)
    vertrag_table = f"{args.project}.{args.dataset}.dwh_ta_c_vertrag"
    vertrag_df = spark.read.format("bigquery").option("table", vertrag_table).load() \
        .filter(F.col("gueltig_bis") >= F.to_date(F.lit("2005-04-01"), "yyyy-MM-dd"))

    # Rename contract columns to avoid conflict
    vertrag_df = vertrag_df.select(
        F.col("rahmenvertrag_id"),
        F.col("vertrag_id_carmen").alias("join_vertrag_id_carmen"),
        F.col("dwh_vertrag_id"),
        F.col("dwh_gp_id"),
        F.col("dwh_konto_id"),
        F.col("dwh_tarifgr_id"),
        F.col("vo_kenn"),
        F.col("zv_id"),
        F.col("gueltig_von"),
        F.col("gueltig_bis")
    )

    enriched_df = consolidated_df.join(
        vertrag_df,
        consolidated_df.vertrags_id == vertrag_df.join_vertrag_id_carmen,
        "left"
    )

    # Proof validity dates (valid_flag equivalent logic)
    enriched_df = enriched_df.withColumn(
        "month_last_day", F.last_day(F.to_date(F.col("monats_id"), "yyyyMM"))
    )

    valid_cond = (
        (F.col("gueltig_von").isNull() | (F.col("month_last_day") > F.col("gueltig_von"))) &
        (F.col("gueltig_bis").isNull() | (F.col("month_last_day") <= F.col("gueltig_bis")))
    )

    enriched_df = enriched_df.withColumn(
        "rahmenvertrag_id", F.when(valid_cond, F.col("rahmenvertrag_id")).otherwise("#")
    ).withColumn(
        "dwh_vertrag_id", F.when(valid_cond, F.col("dwh_vertrag_id")).otherwise(0)
    ).withColumn(
        "dwh_gp_id", F.when(valid_cond, F.col("dwh_gp_id")).otherwise(0)
    ).withColumn(
        "dwh_konto_id", F.when(valid_cond, F.col("dwh_konto_id")).otherwise(0)
    ).withColumn(
        "dwh_tarifgr_id", F.when(valid_cond, F.col("dwh_tarifgr_id")).otherwise(0)
    ).withColumn(
        "vo_kenn", F.when(valid_cond, F.col("vo_kenn")).otherwise("#")
    ).withColumn(
        "zv_id", F.when(valid_cond, F.col("zv_id")).otherwise("0")
    )

    # Generate rankindex
    window_spec = Window.partitionBy("rechnung_id", "rechnung_datum").orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())
    enriched_df = enriched_df.withColumn("rankindex", F.row_number().over(window_spec))

    # Cast monats_id to decimal matching the target DDL definitions
    enriched_df = enriched_df.withColumn("monats_id", F.col("monats_id").cast(DecimalType(15,0)))

    # 5. IDEMPOTENT DELETE STRATEGY
    # Extract distinct keys representing the batch being processed to perform optimized deletion
    delete_keys_df = enriched_df.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id").distinct()
    
    target_f_rpos = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_carm"
    target_f_gpos = f"{args.project}.{args.dataset}.dwh_ta_f_gpos_fact_carm"
    target_f_rpos_fact = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_fact_carm"
    target_f_rpos_resell = f"{args.project}.{args.dataset}.dwh_ta_f_rpos_reselling_carm"
    target_t_rpos = f"{args.project}.{args.dataset}.dwh_ta_t_rpos_carm"

    client = bigquery.Client(project=args.project)
    
    if delete_keys_df.count() > 0:
        temp_delete_table = f"{args.project}.{args.dataset}.temp_delete_keys_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        delete_keys_df.write.format("bigquery").option("table", temp_delete_table).mode("overwrite").save()

        for target_table in [target_f_rpos, target_f_gpos, target_f_rpos_fact, target_f_rpos_resell]:
            delete_query = f"""
                DELETE FROM `{target_table}` t
                WHERE EXISTS (
                    SELECT 1 FROM `{temp_delete_table}` s
                    WHERE t.rechnung_id = s.rechnung_id
                      AND t.rechnung_datum = s.rechnung_datum
                      AND t.standardvertrags_id = s.standardvertrags_id
                      AND t.vertrags_id = s.vertrags_id
                )
            """
            client.query(delete_query).result()
            
        client.delete_table(temp_delete_table, not_found_ok=True)

    # 6. ROUTING AND TARGET WRITING
    # Processing without Sonstige Positionen (Filter out "S")
    no_s_df = enriched_df.filter(F.col("rpos_geschaftsform_kenn") != "S")
    no_s_df = no_s_df.withColumn(
        "rpos_geschaftsform_kenn",
        F.when((F.col("rpos_geschaftsform_kenn") == "F") & (F.col("vas_kenn") == "P30002"), "G")
         .otherwise(F.col("rpos_geschaftsform_kenn"))
    ).withColumn(
        "ladedatum", F.current_timestamp()
    )

    # Route based on decoded rpos_geschaftsform_kenn
    factoring_rechnungen = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "F")
    factoring_gutschriften = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "G")
    reselling = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "R")

    # Prepare target columns
    fact_cols = [
        "monats_id", "debitor_id", "kontier_grp_id", "rechnung_id", "rechnung_datum",
        "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm", "rechpos_brutto_eur",
        "rechpos_netto_eur", "rechpos_mwst_eur", "abs_grp", "prob_vertrag_id", "prob_provider_kenn",
        "anz_leistungen", "anz_tickets", "rpos_geschaftsform_kenn", "vas_kenn",
        "rahmenvertrag_id", "dwh_vertrag_id", "dwh_gp_id", "dwh_konto_id", "dwh_tarifgr_id",
        "vo_kenn", "ladedatum"
    ]

    if factoring_rechnungen.count() > 0:
        factoring_rechnungen.select(*fact_cols).write.format("bigquery").option("table", target_f_rpos_fact).mode("append").save()
    if factoring_gutschriften.count() > 0:
        factoring_gutschriften.select(*fact_cols).write.format("bigquery").option("table", target_f_gpos).mode("append").save()
    if reselling.count() > 0:
        reselling.select(*fact_cols).write.format("bigquery").option("table", target_f_rpos_resell).mode("append").save()

    # Processing with Sonstige Positionen
    with_s_df = enriched_df.filter(
        ((F.col("rech_leistung_id_carm") == "RABATT") & (F.col("vertrags_id") == 0)) |
        (F.col("pooling") == "P")
    ).withColumn(
        "ladedatum", F.current_timestamp()
    ).withColumn(
        "typ", F.lit("T")
    )

    if with_s_df.count() > 0:
        # Idempotent delete for temporary records
        delete_keys_t = with_s_df.select("debitor_id", "rechnung_datum", "rechnung_id").distinct()
        temp_delete_table_t = f"{args.project}.{args.dataset}.temp_delete_keys_t_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        delete_keys_t.write.format("bigquery").option("table", temp_delete_table_t).mode("overwrite").save()
        
        delete_query_t = f"""
            DELETE FROM `{target_t_rpos}` t
            WHERE EXISTS (
                SELECT 1 FROM `{temp_delete_table_t}` s
                WHERE t.debitor_id = s.debitor_id
                  AND t.rechnung_datum = s.rechnung_datum
                  AND t.rechnung_id = s.rechnung_id
            )
        """
        client.query(delete_query_t).result()
        client.delete_table(temp_delete_table_t, not_found_ok=True)

        temp_cols = fact_cols + ["pooling", "rechnungvertrag_id", "verkauftes_basisprodukt_id", "zv_id", "typ"]
        with_s_df.select(*temp_cols).write.format("bigquery").option("table", target_t_rpos).mode("append").save()

    # 7. METADATA FOOTER PROCESSING (Update meldungen and audit tables)
    footer_record = footer_df.first()
    if footer_record:
        stichtag = footer_record["stichtag"]
        anzahl = int(footer_record["anzahl"]) if footer_record["anzahl"] else 0
        inhalt = footer_record["inhalt"]
        bemerkung = footer_record["bemerkung"]
        
        absgrp_table = f"{args.project}.{args.dataset}.dwh_ta_k_rech_absgrp"
        meldungen_table = f"{args.project}.{args.dataset}.dwh_ta_k_meldungen"

        # Process monats_id and abs_grp for auditing updates
        monats_id = stichtag[:6]
        abs_grp = bemerkung[9:14] if len(bemerkung) >= 14 else ""
        dateiname = args.file_mask
        
        # Check if ABSGRP row exists
        check_query = f"""
            SELECT 1 FROM `{absgrp_table}`
            WHERE monats_id = '{monats_id}'
              AND abs_grp = '{abs_grp}'
              AND dateiname = '{dateiname}'
              AND rechnungsteil = 'P'
        """
        exists = client.query(check_query).result().total_rows > 0
        
        if exists:
            update_query = f"""
                UPDATE `{absgrp_table}`
                SET rechnung_datum = DATE('{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:8]}'),
                    ladedatum = CURRENT_TIMESTAMP()
                WHERE monats_id = '{monats_id}'
                  AND abs_grp = '{abs_grp}'
                  AND dateiname = '{dateiname}'
                  AND rechnungsteil = 'P'
            """
            client.query(update_query).result()
        else:
            insert_query = f"""
                INSERT INTO `{absgrp_table}` (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
                VALUES ('{monats_id}', '{abs_grp}', '{dateiname}', DATE('{stichtag[:4]}-{stichtag[4:6]}-{stichtag[6:8]}'), 'P', CURRENT_TIMESTAMP())
            """
            client.query(insert_query).result()

        # Update Meldungen Table
        meldungen_query = f"""
            UPDATE `{meldungen_table}`
            SET anzahl_ds_eof = {anzahl},
                dateiname = '{dateiname}',
                enderecord_text = '{inhalt}',
                zusatzinfo = '{bemerkung}'
            WHERE entrynr = {args.entry_nr}
        """
        client.query(meldungen_query).result()

    print("PySpark ETL complete.")

if __name__ == "__main__":
    main()