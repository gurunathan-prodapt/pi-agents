#!/usr/bin/env python3
# Module: f_alis_msgerr.py
# Re-usable utility library for BigQuery environment-based logging and execution tracking.

import os
import sys
import datetime
import argparse
from google.cloud import bigquery

# Helper: Retrieve BigQuery Client
def get_bq_client():
    return bigquery.Client()

# Helper: Translate Oracle Datetime Format to BigQuery format string
def translate_oracle_format(fmt: str) -> str:
    # Basic translation for common patterns. Extend as needed.
    mapping = {
        'YYYYMMDD_HH24MI': '%Y%m%d_%H%M',
        'YYYYMMDD': '%Y%m%d',
        'HH24:MI:SS': '%H:%M:%S',
        'DD.MM.YYYY HH24:MI:SS': '%d.%m.%Y %H:%M:%S'
    }
    return mapping.get(fmt, fmt)

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_error_code=None):
    """
    Error handling routine called when a shell trap catches a failure.
    Sets the entry to Aborted and logs an unexpected error code 10.
    """
    if last_error_code is None:
        last_error_code = 1  # Default fallback error code
    
    k_unerw_fehler = 10
    
    # Report standard unexpected failure
    dwmsg_melde_fehler(eintrags_nr, 'F', k_unerw_fehler, f"ErrorCode ist: {last_error_code}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    """Sets status of the job execution metadata entry to Success (Ok)."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    query = f"CALL `{project_id}.{dataset_id}.BERT_MELDUNG__SetzeStatusOk`({int(eintrags_nr)})"
    client.query(query).result()

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """Sets status of the job execution metadata entry to Aborted (Abbruch)."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    query = f"CALL `{project_id}.{dataset_id}.BERT_MELDUNG__SetzeStatusAbbruch`({int(eintrags_nr)})"
    client.query(query).result()

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    """
    Obtains a unique job entry ID from the sequence or ID generation logic.
    Returns the generated integer.
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)

    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    
    query = f"SELECT `{project_id}.{dataset_id}.generate_next_eintrags_nr`()"
    query_job = client.query(query)
    results = query_job.result()
    
    eintrags_nr = None
    for row in results:
        eintrags_nr = str(row[0]).strip()
        break
    
    if not eintrags_nr:
        raise RuntimeError("Could not retrieve a unique entry number from BigQuery.")
    
    return eintrags_nr

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    """Creates a tracking entry in the metadata logging structure."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    query = f"""
        CALL `{project_id}.{dataset_id}.BERT_MELDUNG__Erzeuge_Eintrag`(
            @eintrags_nr, @job_kennung, @programmname, @log_datei
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programmname", "STRING", programmname),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1=None, zusatz2=None):
    """Logs an error entry against a tracking job ID."""
    # Guard check
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    query = f"""
        CALL `{project_id}.{dataset_id}.BERT_MELDUNG__Fehler`(
            @typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr)),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1 if zusatz1 is not None else ""),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2 if zusatz2 is not None else "")
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    """
    Assembles a standardized diagnostic log filename and returns it.
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{now_str}_{eintrags_nr}.log"
    return filename

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """Saves business reporting date (Stichtag) context in metadata record."""
    # Guard checks
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    
    bq_fmt = translate_oracle_format(stichtag_fmt)
    
    query = f"""
        CALL `{project_id}.{dataset_id}.BERT_MELDUNG__SetzeZusatzInfos`(
            @eintrags_nr, 
            PARSE_DATE(@bq_fmt, @stichtag),
            NULL
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("bq_fmt", "STRING", bq_fmt),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag)
        ]
    )
    client.query(query, job_config=job_config).result()

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """Appends timestamps and profiling remarks to metadata execution record."""
    # Guard checks
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    if not project_id:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not dataset_id:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    client = get_bq_client()
    bq_fmt = translate_oracle_format(date_format)
    
    # Calculate formatted datetime in python, then execute call
    now_formatted = datetime.datetime.now().strftime(bq_fmt)
    timing_str = f"{info_text} {now_formatted} "
    
    query = f"""
        CALL `{project_id}.{dataset_id}.BERT_MELDUNG__SetzeZusatzInfos`(
            @eintrags_nr, 
            NULL,
            @timing_str
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("timing_str", "STRING", timing_str)
        ]
    )
    client.query(query, job_config=job_config).result()

def main():
    parser = argparse.ArgumentParser(description="f_alis_msgerr Python utility wrapper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # dwmsg_fehlerbehandlung
    p_fb = subparsers.add_parser("dwmsg_fehlerbehandlung")
    p_fb.add_argument("eintrags_nr")
    p_fb.add_argument("--last-error-code", type=int, default=1)
    
    # dwmsg_setze_status_ok
    p_ok = subparsers.add_parser("dwmsg_setze_status_ok")
    p_ok.add_argument("eintrags_nr")
    
    # dwmsg_setze_status_abbruch
    p_ab = subparsers.add_parser("dwmsg_setze_status_abbruch")
    p_ab.add_argument("eintrags_nr")
    
    # dwmsg_ermittle_nr
    p_en = subparsers.add_parser("dwmsg_ermittle_nr")
    p_en.add_argument("var_name", nargs="?", default=None)
    
    # dwmsg_erzeuge_eintrag
    p_ee = subparsers.add_parser("dwmsg_erzeuge_eintrag")
    p_ee.add_argument("eintrags_nr")
    p_ee.add_argument("job_kennung")
    p_ee.add_argument("programmname")
    p_ee.add_argument("log_datei")
    
    # dwmsg_melde_fehler
    p_mf = subparsers.add_parser("dwmsg_melde_fehler")
    p_mf.add_argument("eintrags_nr")
    p_mf.add_argument("typ")
    p_mf.add_argument("fehler_nr")
    p_mf.add_argument("zusatz1", nargs="?", default=None)
    p_mf.add_argument("zusatz2", nargs="?", default=None)
    
    # dwmsg_logdateiname
    p_ld = subparsers.add_parser("dwmsg_logdateiname")
    p_ld.add_argument("job_kennung")
    p_ld.add_argument("eintrags_nr")
    
    # dwmsg_setze_stichtag_info
    p_st = subparsers.add_parser("dwmsg_setze_stichtag_info")
    p_st.add_argument("eintrags_nr")
    p_st.add_argument("stichtag")
    p_st.add_argument("stichtag_fmt")
    
    # dwmsg_append_timing_infos
    p_ti = subparsers.add_parser("dwmsg_append_timing_infos")
    p_ti.add_argument("eintrags_nr")
    p_ti.add_argument("info_text")
    p_ti.add_argument("date_format")
    
    args = parser.parse_args()
    
    try:
        if args.command == "dwmsg_fehlerbehandlung":
            dwmsg_fehlerbehandlung(args.eintrags_nr, args.last_error_code)
        elif args.command == "dwmsg_setze_status_ok":
            dwmsg_setze_status_ok(args.eintrags_nr)
        elif args.command == "dwmsg_setze_status_abbruch":
            dwmsg_setze_status_abbruch(args.eintrags_nr)
        elif args.command == "dwmsg_ermittle_nr":
            nr = dwmsg_ermittle_nr(args.var_name)
            print(nr)
        elif args.command == "dwmsg_erzeuge_eintrag":
            dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
        elif args.command == "dwmsg_melde_fehler":
            dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
        elif args.command == "dwmsg_logdateiname":
            print(dwmsg_logdateiname(args.job_kennung, args.eintrags_nr))
        elif args.command == "dwmsg_setze_stichtag_info":
            dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
        elif args.command == "dwmsg_append_timing_infos":
            dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())