#!/usr/bin/env python3
"""
Migrated from: DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh
Purpose: Taeglicher Export der Rechnungsdaten (RECHNUNG) aus DWH_KERN in das Reporting-Austauschverzeichnis (GCS)
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage


def usage(prog_name: str) -> None:
    print(f"   Programm: {prog_name}")
    print("   Zweck: Taeglicher Export der Rechnungsdaten (RECHNUNG) aus DWH_KERN")
    print("          in das Reporting-Austauschverzeichnis")
    print("   Parameter:")
    print("       -s     Stichtag (Format: 'YYYYMMDD')")


def f_alis_msgerr(level: str, text: str) -> None:
    print(f"[{level}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {text}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", dest="stichtag")
    parser.add_argument("-h", action="store_true")
    args, _ = parser.parse_known_args()

    prog_name = f"Ausfuehrung Script {sys.argv[0]}"

    if args.h:
        usage(prog_name)
        return 0

    # Environment variables
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    bq_dataset = os.environ.get("BQ_DATASET")
    home = os.environ.get("HOME", "")

    if not gcp_project:
        print("ERROR: Environment variable 'GCP_PROJECT' must be set.", file=sys.stderr)
        return 1
    if not gcs_bucket:
        print("ERROR: Environment variable 'GCS_BUCKET' must be set.", file=sys.stderr)
        return 1

    l_stichtag = args.stichtag
    if not l_stichtag:
        yesterday = datetime.now() - timedelta(days=1)
        l_stichtag = yesterday.strftime("%Y%m%d")

    print(f"Starte Export Rechnungsdaten fuer Stichtag {l_stichtag}")

    # Determine paths for SQL script
    dw_dir_root = os.environ.get("DW_DIR_ROOT", os.path.join(home, "aktuell", "dw_source", "isdwh"))
    sql_file_name = "d_exp_rechnung_taeglich.sql"

    # Check multiple locations for the SQL file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        os.path.join(dw_dir_root, "exporter", "rechnung", "sql", sql_file_name),
        os.path.join(script_dir, "..", "sql", sql_file_name),
        os.path.join(script_dir, "sql", sql_file_name),
        os.path.join(os.getcwd(), sql_file_name)
    ]

    sql_file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            sql_file_path = path
            break

    # Determine if we should use the inline query or read the SQL file
    use_inline = True
    sql_template = ""
    if sql_file_path:
        try:
            with open(sql_file_path, "r", encoding="utf-8") as f:
                content = f.read()
                # If it looks like raw SQL*Plus / Oracle, we don't execute it directly
                lower_content = content.lower()
                if not any(x in lower_content for x in ["whenever", "set ", "exit", "define", "to_date", "to_char"]):
                    sql_template = content
                    use_inline = False
        except Exception:
            pass

    if use_inline:
        # Safe inline clean BigQuery SQL query to avoid incompatible SQL*Plus / Oracle syntax
        sql_query = f"""
        SELECT RECHNUNGSNUMMER, VERTRAG, KUNDE, TARIF, ABRECHNUNGSZEITRAUM, RECHNUNGSBETRAG, WAEHRUNG, RECHNUNGSDATUM
        FROM `{gcp_project}.{bq_dataset or 'dwh_kern'}.T_RECHNUNG`
        WHERE RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @stichtag)
        ORDER BY RECHNUNGSNUMMER
        """
    else:
        # Use clean migrated SQL file
        sql_query = sql_template.replace("&1.", l_stichtag).replace("&1", l_stichtag).replace(":1", l_stichtag)

    local_temp_file = os.path.join("/tmp", f"rechnung_export_{l_stichtag}.dat")

    try:
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=gcp_project)

        # Execute the query
        if use_inline:
            query_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("stichtag", "STRING", l_stichtag)
                ]
            )
            query_job = bq_client.query(sql_query, job_config=query_config)
        else:
            query_job = bq_client.query(sql_query)
            
        results = query_job.result()

        l_anzahl = 0
        with open(local_temp_file, "w", encoding="utf-8") as out_f:
            for row in results:
                # Format row as pipe-separated values
                row_str = "|".join("" if val is None else str(val) for val in row)
                out_f.write(row_str + "\n")
                l_anzahl += 1

        # Upload file to GCS
        storage_client = storage.Client(project=gcp_project)
        bucket = storage_client.bucket(gcs_bucket)

        gcs_blob_path = f"rechnung/ausgang/rechnung_export_{l_stichtag}.dat"
        blob = bucket.blob(gcs_blob_path)

        blob.upload_from_filename(local_temp_file)

    except Exception as e:
        print(f"ERROR: Export process failed: {e}", file=sys.stderr)
        return 1
    finally:
        # Clean up local temporary file
        if os.path.exists(local_temp_file):
            try:
                os.remove(local_temp_file)
            except Exception:
                pass

    print(f"Anzahl exportierter Rechnungssaetze: {l_anzahl}")

    if l_anzahl == 0:
        f_alis_msgerr("W", f"Keine Rechnungsdaten fuer Stichtag {l_stichtag} exportiert")

    print("Export Rechnungsdaten ohne erkennbare Fehler beendet")
    return 0


if __name__ == "__main__":
    sys.exit(main())