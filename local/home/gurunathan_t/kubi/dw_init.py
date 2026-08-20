#!/usr/bin/env python3
import os
import sys
import argparse

def init_env():
    # Step 2: Extract base GCS_BUCKET or HOME directory environment variable
    gcs_bucket = os.environ.get("GCS_BUCKET")
    if gcs_bucket:
        base_path = gcs_bucket.rstrip("/")
    else:
        home = os.environ.get("HOME")
        if not home:
            home = os.path.expanduser("~")
        base_path = home.rstrip("/")

    # Step 3: Establish and assign DWH pathing variables
    os.environ["DW_DIR_ROOT"] = f"{base_path}/aktuell"
    os.environ["DW_DIR_PROT"] = f"{base_path}/daten/logfiles"
    os.environ["DW_DIR_CUBES"] = f"{base_path}/daten/cubes"

    os.environ["DW_DIR_IMP_D1"] = f"{base_path}/daten/d1"
    os.environ["DW_DIR_IMP_BWA"] = f"{base_path}/daten/dpps/bwa"
    os.environ["DW_DIR_IMP_XTRA"] = f"{base_path}/daten/xtra"
    os.environ["DW_DIR_IMP_CTEL"] = f"{base_path}/daten/ctel"
    os.environ["DW_DIR_IMP_VO"] = f"{base_path}/daten/vo"
    os.environ["DW_DIR_IMP_RV"] = f"{base_path}/daten/rv"
    os.environ["DW_DIR_IMP_IF"] = f"{base_path}/daten/ees"
    os.environ["DW_DIR_IMP_NNV"] = f"{base_path}/daten/nnv"
    os.environ["DW_DIR_IMP_SIGMA"] = f"{base_path}/daten/gd/sigma"
    os.environ["DW_DIR_EXP_SIGMA"] = f"{base_path}/daten/gd/sigma/export"
    os.environ["DW_DIR_IMP_TRF"] = f"{base_path}/daten/trf"
    os.environ["DW_DIR_IMP_AUF"] = f"{base_path}/daten/sd/auf"
    os.environ["DW_DIR_IMP_GUT"] = f"{base_path}/daten/sd/gut"
    os.environ["DW_DIR_IMP_KDG"] = f"{base_path}/daten/sd/kdg"
    os.environ["DW_DIR_IMP_MP_KDG"] = f"{base_path}/daten/mp/kdg"
    os.environ["DW_DIR_IMP_MP_TS"] = f"{base_path}/daten/mp/ts"
    os.environ["DW_DIR_IMP_MP_ZM"] = f"{base_path}/daten/mp/zm"
    os.environ["DW_DIR_IMP_TS"] = f"{base_path}/daten/sd/ts"
    os.environ["DW_DIR_IMP_ZM"] = f"{base_path}/daten/sd/zm"
    os.environ["DW_DIR_EXP"] = f"{base_path}/daten/exporter"
    os.environ["DW_DIR_IMP_BPM"] = f"{base_path}/daten/bm"
    os.environ["DW_DIR_IMP_ZTS"] = f"{base_path}/daten/zts"
    os.environ["DW_DIR_IMP_VRS"] = f"{base_path}/daten/vrs"

    os.environ["DW_DIR_IMP_BRUNET"] = f"{base_path}/daten/brunet"
    os.environ["DW_DIR_IMP_DWH"] = f"{base_path}/daten/dwh"
    os.environ["DW_DIR_IMP_PLATO"] = f"{base_path}/daten/dwh/plato"
    os.environ["DW_DIR_IMP_CARMEN"] = f"{base_path}/daten/carmen"
    os.environ["DW_DIR_IMP_SAP"] = f"{base_path}/daten/sap"
    os.environ["DW_DIR_IMP_SR_RV"] = f"{base_path}/daten/sap/sr_rv_dpps"

    # Step 4: Map legacy variable mismatch (assigned vs. exported)
    # Legacy mismatch in variable assignment and export: DW_DIR_IMP_SAP_L_GUTGR was assigned but DW_DIR_IMP_SAP_L was exported. Both have been mapped to ensure stability.
    os.environ["DW_DIR_IMP_SAP_L_GUTGR"] = f"{base_path}/daten/sap/sap_l_gutgr"
    os.environ["DW_DIR_IMP_SAP_L"] = f"{base_path}/daten/sap/sap_l_gutgr"

    os.environ["DW_DIR_IMP_L_MAHNSTYP_IST"] = f"{base_path}/daten/sap/mahn"
    os.environ["DW_DIR_IMP_L_MAHNV_FI"] = f"{base_path}/daten/sap/mahn"
    os.environ["DW_DIR_IMP_L_MAHNV_IST"] = f"{base_path}/daten/sap/mahn"
    os.environ["DW_DIR_IMP_L_GUTGR"] = f"{base_path}/daten/sd/l_gutschr"
    os.environ["DW_DIR_IMP_L_LEIST"] = f"{base_path}/daten/sd/l_leist"
    os.environ["DW_DIR_IMP_L_PROD"] = f"{base_path}/daten/sd/l_prod"
    os.environ["DW_DIR_IMP_LKODE"] = f"{base_path}/daten/sd/lkode"

    os.environ["DW_DIR_IMP_SUBSE"] = f"{base_path}/daten/subse"

    os.environ["DW_DIR_SMS_PRG"] = f"{base_path}/aktuell/allgemein/is/util"
    os.environ["DW_DIR_SMS_ADR"] = f"{base_path}/daten/sms/adressen"
    os.environ["DW_DIR_SMS_TMP"] = f"{base_path}/daten/sms/tmp"

    os.environ["DW_DIR_IMP_DPPS"] = f"{base_path}/daten/dpps"
    os.environ["DW_DIR_IMP_PLANF2"] = f"{base_path}/daten/planf2"

    os.environ["DW_HOST_CUSTOMER"] = os.environ.get("DW_HOST_CUSTOMER", "dxcst3.bn.detemobil.de")

    # Step 5: Conditionally resolve ORACLE_HOME path
    oracle_home = os.environ.get("ORACLE_HOME")
    if not oracle_home:
        if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
            oracle_home = "/appl/local/oracle/12.2.0.1.0"
        elif os.path.isdir("/appl/local/oracle/11.2.0"):
            oracle_home = "/appl/local/oracle/11.2.0"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
        
        if oracle_home:
            os.environ["ORACLE_HOME"] = oracle_home

    # Step 6: Trigger sourced environment modules
    # Sourced environment files .dw_global and .dw_lokal are retired/not supplied.

    # Step 7: Resolve runtime dynamic database administrative output directory
    oracle_sid = os.environ.get("ORACLE_SID", "")
    os.environ["DW_DIR_UTL_FILE"] = f"/appl/local/oracle/admin/{oracle_sid}/utl_file"

def get_bigquery_parameters(monats_id, eintrags_nr=None):
    """
    RETRY FIX: Helper function to address BigQuery parameter mismatch issues.
    This explicitly maps legacy orchestration parameters (like MONATSID) to 
    BigQuery-compatible parameter names (@param_monats_id, @param_eintrags_nr)
    and ensures SQL queries are loaded directly as strings rather than GCS URIs.
    """
    params = {
        "param_monats_id": monats_id
    }
    if eintrags_nr is not None:
        params["param_eintrags_nr"] = eintrags_nr
    return params

def get_sql_query(sql_path_or_uri):
    """
    RETRY FIX: Helper function to load SQL query content as a string.
    This addresses the issue where BigQueryInsertJobOperator does not support
    gcs:// URIs directly in the query field and expects the actual SQL string.
    """
    if sql_path_or_uri.startswith("gs://"):
        try:
            from google.cloud import storage
            bucket_name = sql_path_or_uri.split("/")[2]
            blob_name = "/".join(sql_path_or_uri.split("/")[3:])
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.download_as_text()
        except Exception as e:
            print(f"Error reading from GCS: {e}", file=sys.stderr)
            raise
    else:
        with open(sql_path_or_uri, "r", encoding="utf-8") as f:
            return f.read()

def main():
    parser = argparse.ArgumentParser(description="Initialize environment variables for the DWH platform.")
    parser.parse_known_args()

    init_env()
    return 0

if __name__ == "__main__":
    sys.exit(main())