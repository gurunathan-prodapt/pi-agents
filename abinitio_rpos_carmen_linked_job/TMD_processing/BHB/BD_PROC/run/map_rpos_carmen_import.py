#! /usr/bin/env python3
"""
Legacy Ab Initio GDE Compiled KornShell Wrapper Migration
Target File: map_rpos_carmen_import.py
Target Platform: Google BigQuery + Google Cloud Storage
"""

import os
import sys
import argparse
import logging
import datetime
import tempfile
import shutil
import atexit
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage

# Setup detailed execution logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("map_rpos_carmen_import")

# GLOBAL (environment-wide) - sourced at runtime following ENV VARIABLE POLICY
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# JOB-SPECIFIC - Sourced from local job environment or trigger inputs
BHB_Dateiname = os.environ.get("BHB_Dateiname")
BHB_Nutzdatensatzkennung = os.environ.get("BHB_Nutzdatensatzkennung", "P")
BHB_Endedatensatzkennung = os.environ.get("BHB_Endedatensatzkennung", "X")
BHB_Eintragsnr = os.environ.get("BHB_Eintragsnr")

MANDATORY_ENV_VARS = {
    "GCP_PROJECT": GCP_PROJECT,
    "BQ_DATASET": BQ_DATASET,
    "GCS_BUCKET": GCS_BUCKET,
    "BHB_Dateiname": BHB_Dateiname,
    "BHB_Nutzdatensatzkennung": BHB_Nutzdatensatzkennung,
    "BHB_Endedatensatzkennung": BHB_Endedatensatzkennung,
    "BHB_Eintragsnr": BHB_Eintragsnr
}

def validate_and_load_parameters():
    """
    Step 1 & 2: Core Parameter Evaluation and Environment Verification.
    Reads configuration environment variables; fails loudly if any mandatory variable is missing.
    """
    logger.info("Evaluating and validating execution environment parameters...")
    
    for var_name, var_val in MANDATORY_ENV_VARS.items():
        if not var_val:
            raise SystemExit(f"CRITICAL ERROR: Mandatory environment variable {var_name} must be set.")
            
    return MANDATORY_ENV_VARS

def read_file_content(path):
    """
    Reads file content either from local system or Google Cloud Storage (GCS).
    """
    if path.startswith("gs://"):
        logger.info(f"Reading remote file from GCS: {path}")
        bucket_name = path.split("/")[2]
        blob_name = "/".join(path.split("/")[3:])
        storage_client = storage.Client(project=GCP_PROJECT)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding="latin-1")
    else:
        logger.info(f"Reading local file path: {path}")
        with open(path, "r", encoding="latin-1") as f:
            return f.read()

def parse_and_filter_file(params):
    """
    Step 7: Reads the incoming retail flat file from BHB_Dateiname.
    Splits rows into Nutzdaten stream and trailer statistics.
    """
    raw_content = read_file_content(params["BHB_Dateiname"])
    nutz_id = params["BHB_Nutzdatensatzkennung"]
    end_id = params["BHB_Endedatensatzkennung"]
    
    nutzdaten_raw = []
    trailer_raw = []
    
    for line in raw_content.splitlines():
        stripped = line.rstrip("\r\n")
        if not stripped:
            continue
        kennzeichen = stripped[0]
        datensatz_rest = stripped[1:]
        
        if kennzeichen == nutz_id:
            nutzdaten_raw.append(datensatz_rest)
        elif kennzeichen == end_id:
            trailer_raw.append(datensatz_rest)
            
    logger.info(f"Loaded {len(nutzdaten_raw)} Nutzdaten records and {len(trailer_raw)} EOF statistics records.")
    return nutzdaten_raw, trailer_raw

def clean_and_validate_nutzdaten(raw_lines):
    """
    Step 8: Enforces data validation rules, cleans formats and maps fields.
    Mimics Reformat_for_DB-20.xfr & replace_by_-18.xfr.
    """
    logger.info("Executing normalization and schema compliance mapping on transactions...")
    records = []
    
    for idx, line in enumerate(raw_lines):
        cleaned_line = line.replace(",", ".")
        fields = [f.strip() for f in cleaned_line.split(";")]
        
        while len(fields) < 20:
            fields.append("")
            
        try: 
            monats_id = fields[0]
            if not monats_id:
                raise ValueError("monats_id cannot be blank")
                
            debitor_id = fields[1]
            if not debitor_id:
                raise ValueError("debitor_id cannot be blank")
                
            rechnung_id = fields[2]
            if not rechnung_id:
                raise ValueError("rechnung_id cannot be blank")
                
            rechnung_datum_str = fields[3]
            if not rechnung_datum_str:
                raise ValueError("rechnung_datum cannot be blank")
            rechnung_datum = datetime.datetime.strptime(rechnung_datum_str, "Y%m%d" if "%" in "Y%m%d" else "%Y%m%d").date()
            
            standardvertrags_id_str = fields[4]
            standardvertrags_id = int(standardvertrags_id_str) if standardvertrags_id_str not in ["", "#"] else 0
            
            vertrags_id_str = fields[5]
            vertrags_id = int(vertrags_id_str) if vertrags_id_str not in ["", "#"] else 0
            
            rech_leistung_id_carm = fields[6]
            if not rech_leistung_id_carm:
                raise ValueError("rech_leistung_id_carm cannot be blank")
                
            rechpos_brutto_eur = float(fields[7]) if fields[7] else 0.0
            rechpos_netto_eur = float(fields[8]) if fields[8] else 0.0
            rechpos_mwst_eur = float(fields[9]) if fields[9] else 0.0
            
            abs_grp = rechnung_id[8:13] if len(rechnung_id) >= 13 else "#"
            pooling = fields[10] if fields[10] else "#"
            
            rechnungvertrag_id = int(fields[11]) if fields[11] and fields[11] != "#" else 0
            prob_vertrag_id = fields[12] if fields[12] else "#"
            prob_provider_kenn = fields[13] if fields[13] else "#"
            
            anz_leistungen = int(fields[14]) if fields[14] else 0
            anz_tickets = int(fields[15]) if fields[15] else 0
            
            rpos_geschaftsform_kenn = fields[16] if fields[16] else "#"
            vas_kenn = fields[17] if fields[17] else "#"
            
            verkauftes_basisprodukt_id = int(fields[18]) if fields[18] and fields[18] != "#" else 0
            
            records.append({
                "monats_id": monats_id,
                "debitor_id": debitor_id,
                "rechnung_id": rechnung_id,
                "rechnung_datum": rechnung_datum,
                "standardvertrags_id": standardvertrags_id,
                "vertrags_id": vertrags_id,
                "rech_leistung_id_carm": rech_leistung_id_carm,
                "rechpos_brutto_eur": rechpos_brutto_eur,
                "rechpos_netto_eur": rechpos_netto_eur,
                "rechpos_mwst_eur": rechpos_mwst_eur,
                "abs_grp": abs_grp,
                "pooling": pooling,
                "rechnungvertrag_id": rechnungvertrag_id,
                "prob_vertrag_id": prob_vertrag_id,
                "prob_provider_kenn": prob_provider_kenn,
                "anz_leistungen": anz_leistungen,
                "anz_tickets": anz_tickets,
                "rpos_geschaftsform_kenn": rpos_geschaftsform_kenn,
                "vas_kenn": vas_kenn,
                "verkauftes_basisprodukt_id": verkauftes_basisprodukt_id
            })
        except Exception as e:
            logger.error(f"Validation failure at record index {idx}: {line}")
            logger.error(f"Details: {e}")
            raise ValueError(f"CRITICAL ERROR: Transaction validation schema exception: {e}")
            
    df = pd.DataFrame(records)
    logger.info(f"Validation complete. Loaded {len(df)} transactions into clean frame.")
    return df

def fetch_active_contracts(client):
    """
    Step 9: Fetches the reference database records from BigQuery table DWH_TA_C_VERTRAG.
    """
    logger.info("Executing active contract lookup query against BigQuery registries...")
    query = f"""SELECT 
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
    FROM 
        `{GCP_PROJECT}.{BQ_DATASET}.DWH_TA_C_VERTRAG`
    WHERE 
        gueltig_bis >= '2005-04-01'"""
    
    try:
        df_contracts = client.query(query).to_dataframe()
        df_contracts.columns = [c.lower() for c in df_contracts.columns]
        logger.info(f"Retrieved {len(df_contracts)} reference contract items.")
        return df_contracts
    except Exception as e:
        logger.error("CRITICAL ERROR: Failed to execute reference contract registry lookup query.")
        logger.error(f"Details: {e}")
        raise e

def join_and_validate_temporal_boundaries(df_tx, df_contracts):
    """
    Step 9 (cont): Left-joins billing records with temporal contract frames and calculates ranking indices.
    """
    logger.info("Evaluating active temporal validation boundaries and calculating rankings...")
    
    df_tx["vertrags_id_float"] = df_tx["vertrags_id"].astype(float)
    df_contracts["vertrag_id_carmen_float"] = df_contracts["vertrag_id_carmen"].astype(float)
    
    merged = pd.merge(
        df_tx,
        df_contracts,
        left_on="vertrags_id_float",
        right_on="vertrag_id_carmen_float",
        how="left"
    )
    
    def calculate_month_end(monats_str):
        try:
            year = int(monats_str[:4])
            month = int(monats_str[4:6])
            if month == 12:
                return datetime.date(year, 12, 31)
            else:
                return datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
        except Exception:
            return datetime.date(1900, 1, 1)
            
    merged["month_last_day"] = merged["monats_id"].apply(calculate_month_end)
    
    merged["gueltig_von_dt"] = pd.to_datetime(merged["gueltig_von"]).dt.date
    merged["gueltig_bis_dt"] = pd.to_datetime(merged["gueltig_bis"]).dt.date
    
    valid_mask = (
        (merged["gueltig_von_dt"].isna() | (merged["month_last_day"] > merged["gueltig_von_dt"]))
        & (merged["gueltig_bis_dt"].isna() | (merged["month_last_day"] <= merged["gueltig_bis_dt"]))
    )
    
    merged["valid_flag"] = np.where(valid_mask, 0, 1)
    df_valid = merged[merged["valid_flag"] == 0].copy()
    
    fill_cols = {
        "rahmenvertrag_id": "#",
        "dwh_vertrag_id": 0,
        "dwh_gp_id": 0,
        "dwh_konto_id": 0,
        "dwh_tarifgr_id": 0,
        "vo_kenn": "#",
        "zv_id": "0"
    }
    for col, val in fill_cols.items():
        if col in df_valid.columns:
            df_valid[col] = df_valid[col].fillna(val)
            
    df_valid.sort_values(
        by=["vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm", "debitor_id", "gueltig_von_dt", "dwh_vertrag_id"],
        ascending=[True, True, True, True, True, True, False, False],
        inplace=True
    )
    
    pk_cols = ["vertrags_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm", "debitor_id"]
    df_ranked = df_valid.groupby(pk_cols).first().reset_index()
    
    logger.info(f"Ranking complete. Kept {len(df_ranked)} primary validated transaction nodes.")
    return df_ranked

def aggregate_and_rollup(df):
    """
    Step 10: Aggregates metrics and groups records mimicking Rollup operations.
    """
    logger.info("Executing rollup aggregations over primary transaction metrics...")
    
    group_cols = [
        "monats_id", "debitor_id", "rechnung_id", "rechnung_datum",
        "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm",
        "abs_grp", "pooling", "rechnungvertrag_id", "prob_vertrag_id",
        "prob_provider_kenn", "rpos_geschaftsform_kenn", "vas_kenn",
        "verkauftes_basisprodukt_id", "rahmenvertrag_id", "dwh_vertrag_id",
        "dwh_gp_id", "dwh_konto_id", "dwh_tarifgr_id", "vo_kenn", "zv_id"
    ]
    
    df_rolled = df.groupby(group_cols, as_index=False).agg({
        "rechpos_brutto_eur": "sum",
        "rechpos_netto_eur": "sum",
        "rechpos_mwst_eur": "sum",
        "anz_leistungen": "sum",
        "anz_tickets": "sum"
    })
    
    df_rolled["rpos_geschaftsform_kenn"] = np.where(
        (df_rolled["rpos_geschaftsform_kenn"] == "F") & (df_rolled["vas_kenn"] == "P30002"),
        "G",
        df_rolled["rpos_geschaftsform_kenn"]
    )
    
    typ_mask = (
        ((df_rolled["rech_leistung_id_carm"] == "RABATT") & (df_rolled["vertrags_id"] == 0))
        | (df_rolled["pooling"] == "P")
    )
    df_rolled["typ"] = np.where(typ_mask, "T", "F")
    
    logger.info(f"Aggregations complete. Created {len(df_rolled)} grouped metrics.")
    return df_rolled

def execute_idempotent_deletions(client, df_keys):
    """
    Step 11: Idempotency clearance scripts.
    Executes optimized BigQuery standard SQL deletes using STRUCT UNNEST.
    """
    logger.info("Executing idempotent clear procedures on BigQuery target tables...")
    if df_keys.empty:
        logger.warning("No records to evaluate for deletion. Skipping clearance.")
        return
        
    standard_keys = df_keys[["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"]].drop_duplicates().values.tolist()
    temp_keys = df_keys[["debitor_id", "rechnung_datum", "rechnung_id"]].drop_duplicates().values.tolist()
    
    # Batch deletion for core tables
    if standard_keys:
        struct_elements = []
        for r_id, r_date, s_id, v_id in standard_keys:
            r_date_str = r_date.strftime("%Y-%m-%d") if isinstance(r_date, (datetime.date, datetime.datetime)) else str(r_date)
            struct_elements.append(f"STRUCT('{r_id}' AS rechnung_id, DATE('{r_date_str}') AS rechnung_datum, {int(s_id)} AS standardvertrags_id, {int(v_id)} AS vertrags_id)")
        
        structs_joined = ", ".join(struct_elements)
        
        tables = [
            "DWH_TA_F_RPOS_CARM",
            "DWH_TA_F_GPOS_FACT_CARM",
            "DWH_TA_F_RPOS_FACT_CARM",
            "DWH_TA_F_RPOS_RESELLING_CARM"
        ]
        
        for tbl in tables:
            logger.info(f"Clearing idempotent keys on {tbl}...")
            query = f"""DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.{tbl}` t
            WHERE EXISTS (
                SELECT 1 FROM UNNEST([
                    {structs_joined}
                ]) k
                WHERE t.rechnung_id = k.rechnung_id
                  AND t.rechnung_datum = k.rechnung_datum
                  AND t.standardvertrags_id = k.standardvertrags_id
                  AND t.vertrags_id = k.vertrags_id
            )"""
            client.query(query).result()
            
    # Batch deletion for temporary structures table
    if temp_keys:
        struct_elements_temp = []
        for d_id, r_date, r_id in temp_keys:
            r_date_str = r_date.strftime("%Y-%m-%d") if isinstance(r_date, (datetime.date, datetime.datetime)) else str(r_date)
            struct_elements_temp.append(f"STRUCT('{d_id}' AS debitor_id, DATE('{r_date_str}') AS rechnung_datum, '{r_id}' AS rechnung_id)")
            
        structs_joined_temp = ", ".join(struct_elements_temp)
        
        logger.info("Clearing idempotent keys on DWH_TA_T_RPOS_CARM...")
        query_temp = f"""DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.DWH_TA_T_RPOS_CARM` t
        WHERE EXISTS (
            SELECT 1 FROM UNNEST([
                {structs_joined_temp}
            ]) k
            WHERE t.debitor_id = k.debitor_id
              AND t.rechnung_datum = k.rechnung_datum
              AND t.rechnung_id = k.rechnung_id
        )"""
        client.query(query_temp).result()
        
    logger.info("Idempotency clearance processes successfully completed.")

def load_df_to_bq(client, df, table_name):
    """
    Executes BigQuery bulk load from Pandas Dataframe.
    """
    if df.empty:
        logger.info(f"No records to load into {table_name}.")
        return
        
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    
    logger.info(f"Loading {len(df)} records into {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Successfully loaded {len(df)} records into {table_ref}.")

def bulk_insert_to_targets(client, df):
    """
    Step 12: Route and load processed blocks to target database tables using BigQuery Client.
    """
    logger.info("Routing records and executing bulk insert transactions...")
    
    # Routing conditions mappings
    df_fact = df[df["typ"] != "T"].copy()
    df_temp = df[df["typ"] == "T"].copy()
    
    # 1. Standard Fact Table
    load_df_to_bq(client, df_fact, "DWH_TA_F_RPOS_CARM")
    
    # 2. Factoring Bills
    df_bills = df_fact[df_fact["rpos_geschaftsform_kenn"] == "F"].copy()
    load_df_to_bq(client, df_bills, "DWH_TA_F_RPOS_FACT_CARM")
    
    # 3. Factoring Credits
    df_credits = df_fact[df_fact["rpos_geschaftsform_kenn"] == "G"].copy()
    load_df_to_bq(client, df_credits, "DWH_TA_F_GPOS_FACT_CARM")
    
    # 4. Reselling
    df_reselling = df_fact[df_fact["rpos_geschaftsform_kenn"] == "R"].copy()
    load_df_to_bq(client, df_reselling, "DWH_TA_F_RPOS_RESELLING_CARM")
    
    # 5. Temporary Data Target Table mapping fields appropriately
    if not df_temp.empty:
        df_temp_subset = df_temp[["debitor_id", "rechnung_id", "rechnung_datum", "standardvertrags_id", "rech_leistung_id_carm", "vertrags_id"]].copy()
        df_temp_subset["bearbeitung_datum"] = datetime.datetime.now()
        load_df_to_bq(client, df_temp_subset, "DWH_TA_T_RPOS_CARM")

def audit_and_log_updates(client, raw_trailers, params):
    """
    Step 13: Verifies stats from Enderecord and updates audits in BigQuery tables.
    """
    logger.info("Processing trailer statistics audit updates...")
    if not raw_trailers:
        logger.warning("No Enderecord found in current run source file. Skipping log updates.")
        return
        
    trailer = raw_trailers[0]
    fields = [f.strip() for f in trailer.split(";")]
    
    while len(fields) < 6:
        fields.append("")
        
    bemerkung = fields[0]
    stichtag_str = fields[1]
    anzahl = int(fields[2]) if fields[2].isdigit() else 0
    inhalt = fields[3]
    
    try:
        stichtag_dt = datetime.datetime.strptime(stichtag_str, "Y%m%d" if "%" in "Y%m%d" else "%Y%m%d").date()
        first_of_month = stichtag_dt.replace(day=1)
        prev_month_dt = first_of_month - datetime.timedelta(days=1)
        monats_id = int(prev_month_dt.strftime("%Y%m"))
    except Exception:
        monats_id = 0
        stichtag_dt = datetime.date.today()
        
    abs_grp = bemerkung[9:14] if len(bemerkung) >= 14 else ""
    dateiname = bemerkung
    rechnungsteil = "P"
    ladedatum = datetime.datetime.now()
    stichtag_formatted = stichtag_dt.strftime("%Y-%m-%d")
    
    # Step 13A: BigQuery Merge into DWH_TA_K_RECH_ABSGRP
    query_absgrp = f"""
    MERGE `{GCP_PROJECT}.{BQ_DATASET}.DWH_TA_K_RECH_ABSGRP` t
    USING (
        SELECT 
            @monats_id AS monats_id, 
            @abs_grp AS abs_grp, 
            @dateiname AS dateiname, 
            DATE(@rechnung_datum) AS rechnung_datum, 
            @rechnungsteil AS rechnungsteil, 
            TIMESTAMP(@ladedatum) AS ladedatum
    ) s
    ON t.monats_id = s.monats_id 
       AND t.abs_grp = s.abs_grp 
       AND t.dateiname = s.dateiname 
       AND t.rechnungsteil = s.rechnungsteil
    WHEN MATCHED THEN
      UPDATE SET rechnung_datum = s.rechnung_datum, ladedatum = s.ladedatum
    WHEN NOT MATCHED THEN
      INSERT (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
      VALUES (s.monats_id, s.abs_grp, s.dateiname, s.rechnung_datum, s.rechnungsteil, s.ladedatum)
    """
    job_config_absgrp = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("monats_id", "INT64", int(monats_id)),
            bigquery.ScalarQueryParameter("abs_grp", "STRING", abs_grp),
            bigquery.ScalarQueryParameter("dateiname", "STRING", dateiname),
            bigquery.ScalarQueryParameter("rechnung_datum", "STRING", stichtag_formatted),
            bigquery.ScalarQueryParameter("rechnungsteil", "STRING", rechnungsteil),
            bigquery.ScalarQueryParameter("ladedatum", "TIMESTAMP", ladedatum)
        ]
    )
    logger.info("Executing DWH_TA_K_RECH_ABSGRP audit synchronization...")
    client.query(query_absgrp, job_config=job_config_absgrp).result()
    
    # Step 13B: Update dwh$ta_k_meldungen stats
    query_meldungen = f"""
    UPDATE `{GCP_PROJECT}.{BQ_DATASET}.DWH_TA_K_MELDUNGEN`
    SET anzahl_ds_eof = @anzahl,
        dateiname = @dateiname,
        enderecord_text = @inhalt,
        zusatzinfo = @bemerkung
    WHERE entrynr = @eintragsnr
    """
    entry_nr = int(params["BHB_Eintragsnr"])
    job_config_meldungen = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("anzahl", "INT64", int(anzahl)),
            bigquery.ScalarQueryParameter("dateiname", "STRING", params["BHB_Dateiname"]),
            bigquery.ScalarQueryParameter("inhalt", "STRING", inhalt),
            bigquery.ScalarQueryParameter("bemerkung", "STRING", bemerkung),
            bigquery.ScalarQueryParameter("eintragsnr", "INT64", entry_nr)
        ]
    )
    logger.info("Executing dwh$ta_k_meldungen audit logging synchronization...")
    client.query(query_meldungen, job_config=job_config_meldungen).result()
    logger.info("Audit synchronization successfully concluded.")

def main():
    """
    Main job coordinator orchestrating sequential execution of migrated pipeline components.
    """
    parser = argparse.ArgumentParser(description="Carmen RPOS Billing transactional ETL pipeline")
    parser.add_argument("-reposit-tracking", action="store_true", help="Repository tracking parameter support")
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    
    if "-help" in args.args or "--help" in args.args:
        sys.exit(1)
        
    params = validate_and_load_parameters()
    
    # Establish client connections natively on BigQuery
    client = bigquery.Client(project=GCP_PROJECT)
    
    try:
        # Stream raw content and separate elements
        raw_nutzdaten, raw_trailers = parse_and_filter_file(params)
        
        if not raw_nutzdaten:
            logger.warning("Empty transaction data set. Skipping executions.")
            return 0
            
        # Standard schema validation
        df_validated = clean_and_validate_nutzdaten(raw_nutzdaten)
        
        # Fetch BigQuery contract history
        df_contracts = fetch_active_contracts(client)
        
        # Merge frames and validate temporal boundaries
        df_ranked = join_and_validate_temporal_boundaries(df_validated, df_contracts)
        
        # Group rollups
        df_rolled = aggregate_and_rollup(df_ranked)
        
        # Clear unique table transaction key bounds
        execute_idempotent_deletions(client, df_rolled)
        
        # Bulk load processed blocks
        bulk_insert_to_targets(client, df_rolled)
        
        # EOF stats validation audits
        audit_and_log_updates(client, raw_trailers, params)
        
        logger.info("SUCCESS: map_rpos_carmen_import run complete.")
        return 0
        
    except Exception as e:
        logger.critical(f"PIPELINE CRASHED: Runtime termination exception: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())