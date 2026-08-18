#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime
from google.cloud import bigquery

# Initialize BigQuery client
# Sourced from environment variables
BQ_PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_METADATA_DATASET", "metadata_dataset")

if BQ_PROJECT:
    client = bigquery.Client(project=BQ_PROJECT)
else:
    client = bigquery.Client()

def _execute_procedure(proc_name: str, params: list):
    """Helper utility to invoke BigQuery procedures representing the BERT_MELDUNG package."""
    dataset_prefix = f"`{BQ_PROJECT}.{BQ_DATASET}`" if BQ_PROJECT else f"`{BQ_DATASET}`"
    param_placeholders = ", ".join(["?" for _ in params])
    query = f"CALL {dataset_prefix}.{proc_name}({param_placeholders})"
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING" if isinstance(p, str) else "INT64", p) 
            for p in params
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for procedure to complete
    except Exception as e:
        print(f"Database error executing procedure {proc_name}: {e}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, last_error_code=10):
    """
    Handles errors caught by traps in the calling script.
    Registers a fatal entry and sets job status to Aborted.
    """
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", last_error_code, f"ErrorCode ist: {last_error_code}")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """Sets execution record state to successful."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    _execute_procedure("BERT_MELDUNG_SetzeStatusOk", [dwmsg_eintrags_nr])

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """Sets execution record state to aborted."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    _execute_procedure("BERT_MELDUNG_SetzeStatusAbbruch", [dwmsg_eintrags_nr])

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr() -> str:
    """
    Retrieves a unique tracking sequence ID from the metadata DB.
    Replaces temporary file logic and returns the value directly.
    """
    dataset_prefix = f"`{BQ_PROJECT}.{BQ_DATASET}`" if BQ_PROJECT else f"`{BQ_DATASET}`"
    query = f"SELECT {dataset_prefix}.GetUniqueEintragsNr()"
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        if results:
            return str(results[0][0]).strip()
        raise ValueError("No sequence number returned from database query")
    except Exception as e:
        print(f"Error fetching unique sequence number: {e}", file=sys.stderr)
        raise

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei):
    """Creates a new run entry in tracking tables."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    _execute_procedure("BERT_MELDUNG_Erzeuge_Eintrag", [dwmsg_eintrags_nr, job_kennung, programmname, log_datei])

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """Logs an application or environment warning/error message."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNummer")

    _execute_procedure("BERT_MELDUNG_Fehler", [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr) -> str:
    """Generates standard tracking protocol file path."""
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{dwmsg_eintrags_nr}.log"
    return filename

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, stichtag, stichtag_fmt):
    """Logs processing date and its date format to the run execution log."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNr")
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        raise ValueError("Missing Stichtag")
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        raise ValueError("Missing Stichtag Format")

    _execute_procedure("BERT_MELDUNG_SetzeZusatzInfos_Stichtag", [dwmsg_eintrags_nr, stichtag, stichtag_fmt])

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, info_text, date_format):
    """Appends duration or timestamp info to supplementary run details."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        raise ValueError("Missing EintragsNr")
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        raise ValueError("Missing Date Format")

    _execute_procedure("BERT_MELDUNG_SetzeZusatzInfos_Timing", [dwmsg_eintrags_nr, info_text, date_format])

def main():
    parser = argparse.ArgumentParser(description="Unified logging and status tracking utility.")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # fehlerbehandlung
    parser_fb = subparsers.add_parser("dwmsg_fehlerbehandlung")
    parser_fb.add_argument("dwmsg_eintrags_nr")
    parser_fb.add_argument("--last_error_code", type=int, default=10)

    # setze_status_ok
    parser_ok = subparsers.add_parser("dwmsg_setze_status_ok")
    parser_ok.add_argument("dwmsg_eintrags_nr")

    # setze_status_abbruch
    parser_ab = subparsers.add_parser("dwmsg_setze_status_abbruch")
    parser_ab.add_argument("dwmsg_eintrags_nr")

    # ermittle_nr
    parser_nr = subparsers.add_parser("dwmsg_ermittle_nr")

    # erzeuge_eintrag
    parser_er = subparsers.add_parser("dwmsg_erzeuge_eintrag")
    parser_er.add_argument("dwmsg_eintrags_nr")
    parser_er.add_argument("job_kennung")
    parser_er.add_argument("programmname")
    parser_er.add_argument("log_datei")

    # melde_fehler
    parser_mf = subparsers.add_parser("dwmsg_melde_fehler")
    parser_mf.add_argument("dwmsg_eintrags_nr")
    parser_mf.add_argument("typ")
    parser_mf.add_argument("fehler_nr", type=int)
    parser_mf.add_argument("--zusatz1", default="")
    parser_mf.add_argument("--zusatz2", default="")

    # logdateiname
    parser_ld = subparsers.add_parser("dwmsg_logdateiname")
    parser_ld.add_argument("job_kennung")
    parser_ld.add_argument("dwmsg_eintrags_nr")

    # setze_stichtag_info
    parser_st = subparsers.add_parser("dwmsg_setze_stichtag_info")
    parser_st.add_argument("dwmsg_eintrags_nr")
    parser_st.add_argument("stichtag")
    parser_st.add_argument("stichtag_fmt")

    # append_timing_infos
    parser_ti = subparsers.add_parser("dwmsg_append_timing_infos")
    parser_ti.add_argument("dwmsg_eintrags_nr")
    parser_ti.add_argument("info_text")
    parser_ti.add_argument("date_format")

    args = parser.parse_args()

    if args.command == "dwmsg_fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.dwmsg_eintrags_nr, args.last_error_code)
    elif args.command == "dwmsg_setze_status_ok":
        dwmsg_setze_status_ok(args.dwmsg_eintrags_nr)
    elif args.command == "dwmsg_setze_status_abbruch":
        dwmsg_setze_status_abbruch(args.dwmsg_eintrags_nr)
    elif args.command == "dwmsg_ermittle_nr":
        nr = dwmsg_ermittle_nr()
        print(nr)
    elif args.command == "dwmsg_erzeuge_eintrag":
        dwmsg_erzeuge_eintrag(args.dwmsg_eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
    elif args.command == "dwmsg_melde_fehler":
        dwmsg_melde_fehler(args.dwmsg_eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "dwmsg_logdateiname":
        name = dwmsg_logdateiname(args.job_kennung, args.dwmsg_eintrags_nr)
        print(name)
    elif args.command == "dwmsg_setze_stichtag_info":
        dwmsg_setze_stichtag_info(args.dwmsg_eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "dwmsg_append_timing_infos":
        dwmsg_append_timing_infos(args.dwmsg_eintrags_nr, args.info_text, args.date_format)
    else:
        parser.print_help()
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())