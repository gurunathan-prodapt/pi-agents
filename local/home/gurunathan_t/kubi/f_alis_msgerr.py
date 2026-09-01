#!/usr/bin/env python3
import os
import sys
import datetime
import logging
import argparse
from google.cloud import bigquery

# Configure standard logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Initialize the GCP BigQuery client connection (using implicit/env credentials)
bq_client = bigquery.Client()

def get_procedure_path(proc_name: str) -> str:
    """
    Helper to construct the fully qualified BigQuery procedure/function path
    using environment variables.
    """
    project = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET")
    if project and dataset:
        return f"`{project}.{dataset}.{proc_name}`"
    elif dataset:
        return f"`{dataset}.{proc_name}`"
    else:
        return f"`{proc_name}`"

# Step 2: Define unexpected error trap helper
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr: str, last_exit_code: int):
    """
    Simulates shell 'trap ... ERR' execution.
    """
    # Step 2.1: Define internal unexpected exception code
    k_unerw_fehler = 10
    
    # Step 2.2: Log internal error code to BigQuery
    dwmsg_melde_fehler(
        dwmsg_eintrags_nr, 
        "F", 
        k_unerw_fehler, 
        f"ErrorCode ist: {last_exit_code}"
    )
    
    # Step 2.3: Set aborted tracking status in BigQuery
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 3: Define OK status tracker function
def dwmsg_setze_status_ok(dwmsg_eintrags_nr: str):
    """
    Updates the log entry state to OK.
    """
    # Step 3.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 3.2: Perform BigQuery procedure execution
    proc = get_procedure_path("BERT_MELDUNG_SetzeStatusOk")
    query = f"CALL {proc}(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery SetzeStatusOk: {e}", file=sys.stderr)
        raise


# Step 4: Define ABORT status tracker function
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr: str):
    """
    Updates the log entry state to Aborted.
    """
    # Step 4.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 4.2: Perform BigQuery procedure execution
    proc = get_procedure_path("BERT_MELDUNG_SetzeStatusAbbruch")
    query = f"CALL {proc}(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery SetzeStatusAbbruch: {e}", file=sys.stderr)
        raise


# Step 5: Define unique ID retriever function
def dwmsg_ermittle_nr() -> str:
    """
    Queries and returns a unique tracker run sequence number.
    """
    # Step 5.1: Perform BigQuery sequence fetch
    proc = get_procedure_path("generate_tracking_nr")
    query = f"SELECT {proc}() AS eintrags_nr"
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        row = next(iter(results))
        return str(row["eintrags_nr"]).strip()
    except Exception as e:
        print(f"Error querying BigQuery sequence: {e}", file=sys.stderr)
        raise


# Step 6: Define log entry constructor function
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr: str, job_kennung: str, programm_name: str, log_datei: str):
    """
    Inserts a newly generated job execution log entry.
    """
    # Step 6.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 6.2: Execute initialization call in BigQuery
    proc = get_procedure_path("BERT_MELDUNG_Erzeuge_Eintrag")
    query = f"CALL {proc}(@eintrags_nr, @job_kennung, @programm_name, @log_datei)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery ErzeugeEintrag: {e}", file=sys.stderr)
        raise


# Step 7: Define exception reporter function
def dwmsg_melde_fehler(dwmsg_eintrags_nr: str, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
    """
    Records an error occurrence.
    """
    # Step 7.1: Audit validation check
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Step 7.2: Perform logging execution call in BigQuery
    proc = get_procedure_path("BERT_MELDUNG_Fehler")
    query = f"CALL {proc}(@typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", fehler_nr),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery MeldeFehler: {e}", file=sys.stderr)
        raise


# Step 8: Define log directory file mapping function
def dwmsg_logdateiname(job_kennung: str, dwmsg_eintrags_nr: str) -> str:
    """
    Constructs a standardized execution log string path.
    """
    # Step 8.1: Query system runtime date
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    # Step 8.2: Build and return full file string path
    gcs_bucket = os.environ.get("GCS_BUCKET")
    if gcs_bucket:
        filename = f"gs://{gcs_bucket}/{job_kennung}_{now_str}_{dwmsg_eintrags_nr}.log"
    else:
        dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
        filename = f"{dw_dir_prot}/{job_kennung}_{now_str}_{dwmsg_eintrags_nr}.log"
    return filename


# Step 9: Define target business date logging function
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr: str, stichtag: str, stichtag_fmt: str):
    """
    Appends execution stichtag info using parsed datetime strings.
    """
    # Step 9.1: Audit validation checks
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Step 9.2: Call BigQuery target stored procedure
    proc = get_procedure_path("BERT_MELDUNG_SetzeZusatzInfos")
    query = f"CALL {proc}(@eintrags_nr, PARSE_DATE(@stichtag_fmt, @stichtag))"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("stichtag", "STRING", stichtag),
            bigquery.ScalarQueryParameter("stichtag_fmt", "STRING", stichtag_fmt)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery SetzeStichtagInfo: {e}", file=sys.stderr)
        raise


# Step 10: Define telemetry execution timers function
def dwmsg_append_timing_infos(dwmsg_eintrags_nr: str, info_text: str, date_format: str):
    """
    Saves timing/performance logs to tracking table.
    """
    # Step 10.1: Audit validation checks
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Step 10.2: Execute query mapping to post concatenated telemetry string values
    proc = get_procedure_path("BERT_MELDUNG_SetzeZusatzInfos")
    query = f"CALL {proc}(@eintrags_nr, NULL, CONCAT(@info_text, ' ', FORMAT_DATETIME(@date_format, CURRENT_DATETIME()), ' '))"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("info_text", "STRING", info_text),
            bigquery.ScalarQueryParameter("date_format", "STRING", date_format)
        ]
    )
    try:
        bq_client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"Error executing BigQuery AppendTimingInfos: {e}", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(description="Python wrapper for f_alis_msgerr utility functions")
    subparsers = parser.add_subparsers(dest="command", help="Function to execute")

    # Subparser for fehlerbehandlung
    parser_fb = subparsers.add_parser("fehlerbehandlung", help="Call DWMSG_Fehlerbehandlung")
    parser_fb.add_argument("eintrags_nr", help="Logging track entry ID")
    parser_fb.add_argument("--exit-code", type=int, default=1, help="Last exit code")

    # Subparser for setze-status-ok
    parser_ok = subparsers.add_parser("setze-status-ok", help="Call DWMSG_SetzeStatusOK")
    parser_ok.add_argument("eintrags_nr", help="Logging track entry ID")

    # Subparser for setze-status-abbruch
    parser_abbruch = subparsers.add_parser("setze-status-abbruch", help="Call DWMSG_SetzeStatusAbbruch")
    parser_abbruch.add_argument("eintrags_nr", help="Logging track entry ID")

    # Subparser for ermittle-nr
    subparsers.add_parser("ermittle-nr", help="Call DWMSG_ErmittleNr and print result")

    # Subparser for erzeuge-eintrag
    parser_ee = subparsers.add_parser("erzeuge-eintrag", help="Call DWMSG_ErzeugeEintrag")
    parser_ee.add_argument("eintrags_nr")
    parser_ee.add_argument("job_kennung")
    parser_ee.add_argument("programm_name")
    parser_ee.add_argument("log_datei")

    # Subparser for melde-fehler
    parser_mf = subparsers.add_parser("melde-fehler", help="Call DWMSG_MeldeFehler")
    parser_mf.add_argument("eintrags_nr")
    parser_mf.add_argument("typ")
    parser_mf.add_argument("fehler_nr", type=int)
    parser_mf.add_argument("zusatz1", nargs="?", default="")
    parser_mf.add_argument("zusatz2", nargs="?", default="")

    # Subparser for logdateiname
    parser_ld = subparsers.add_parser("logdateiname", help="Call DWMSG_Logdateiname")
    parser_ld.add_argument("job_kennung")
    parser_ld.add_argument("eintrags_nr")

    # Subparser for setze-stichtag-info
    parser_st = subparsers.add_parser("setze-stichtag-info", help="Call DWMSG_SetzeStichtagInfo")
    parser_st.add_argument("eintrags_nr")
    parser_st.add_argument("stichtag")
    parser_st.add_argument("stichtag_fmt")

    # Subparser for append-timing-infos
    parser_at = subparsers.add_parser("append-timing-infos", help="Call DWMSG_AppendTimingInfos")
    parser_at.add_argument("eintrags_nr")
    parser_at.add_argument("info_text")
    parser_at.add_argument("date_format")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.exit_code)
    elif args.command == "setze-status-ok":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.command == "setze-status-abbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.command == "ermittle-nr":
        nr = dwmsg_ermittle_nr()
        print(nr)
    elif args.command == "erzeuge-eintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programm_name, args.log_datei)
    elif args.command == "melde-fehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "logdateiname":
        fn = dwmsg_logdateiname(args.job_kennung, args.eintrags_nr)
        print(fn)
    elif args.command == "setze-stichtag-info":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "append-timing-infos":
        dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)

    return 0


if __name__ == "__main__":
    sys.exit(main())