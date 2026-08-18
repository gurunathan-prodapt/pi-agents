#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
from google.cloud import bigquery

# REVIEW-STRUCT: legacy Oracle status-logging package BERT_MELDUNG replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying

# Read required environment variables and fail loudly if missing
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
if not PROJECT_ID:
    raise SystemExit("GCP_PROJECT or PROJECT_ID must be set by the calling Airflow task")

DATASET_ID = os.environ.get("BQ_DATASET") or os.environ.get("DATASET_ID")
if not DATASET_ID:
    raise SystemExit("BQ_DATASET or DATASET_ID must be set by the calling Airflow task")

DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
if not DW_DIR_ROOT:
    raise SystemExit("DW_DIR_ROOT must be set by the calling Airflow task")

DW_DIR_PROT = os.environ.get("DW_DIR_PROT")
if not DW_DIR_PROT:
    raise SystemExit("DW_DIR_PROT must be set by the calling Airflow task")


def get_bq_client():
    """
    Initializes and returns a BigQuery client.
    """
    return bigquery.Client(project=PROJECT_ID)


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, error_code=None):
    """
    Funktion, die bei Auftreten eines Fehlers aufgerufen wird (falls so konfiguriert).
    Sie regelt das Eintragen in der Meldungstabelle und ggf. das Anstoßen weiterer Aktionen.
    """
    # sichern des FehlerCodes
    fehler_nr = error_code if error_code is not None else 1
    k_unerw_fehler = 10
    
    # Melde Fehler in der Meldungstabelle.
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 2: DWMSG_SetzeStatusOk
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich beendet.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET status = 'OK', end_timestamp = CURRENT_TIMESTAMP()
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to update status to OK for entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET status = 'ABORTED', end_timestamp = CURRENT_TIMESTAMP()
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to update status to ABORTED for entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    """
    Funktion ermittelt eine eineindeutige Nr.
    """
    if var_name == "":
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = "SELECT GENERATE_UUID() as new_id"
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return str(row.new_id)
    except Exception as e:
        print(f"ERROR: Failed to generate UUID sequence: {e}", file=sys.stderr)
        sys.exit(1)


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Funktion erzeugt einen Eintrag in der Meldungstabelle.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG` 
        (entry_id, job_kennung, programm_name, log_datei, status, start_timestamp)
        VALUES (@entry_id, @job_kennung, @programm_name, @log_datei, 'RUNNING', CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr)),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to create log entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1=None, zusatz2=None):
    """
    Funktion meldet einen Fehler.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    client = get_bq_client()
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG_ERRORS` 
        (entry_id, type, error_no, detail_1, detail_2, log_timestamp)
        VALUES (@entry_id, @typ, @fehler_nr, @zusatz1, @zusatz2, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr)),
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr)),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2)
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to log error for entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(var_name_or_job_kennung, job_kennung_or_eintrags_nr, dwmsg_eintrags_nr=None):
    """
    Funktion baut einen Logdateinamen auf.
    Supports both:
      dwmsg_logdateiname(job_kennung, eintrags_nr) -> returns path
      dwmsg_logdateiname(var_name, job_kennung, eintrags_nr) -> returns path
    """
    if dwmsg_eintrags_nr is None:
        # Called with 2 arguments
        job_kennung = var_name_or_job_kennung
        eintrags_nr = job_kennung_or_eintrags_nr
    else:
        # Called with 3 arguments (legacy/compatibility mode)
        job_kennung = job_kennung_or_eintrags_nr
        eintrags_nr = dwmsg_eintrags_nr
        
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{DW_DIR_PROT}/{job_kennung}_{timestamp}_{eintrags_nr}.log"
    return filename


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dw_stichtag, dw_stichtag_fmt):
    """
    Funktion setzt weitere Infofelder des Eintrages mit Nummer EintragsNr.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not dw_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not dw_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Translate Oracle format masks to BigQuery/Python-style formats
    bq_format = dw_stichtag_fmt.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET stichtag = PARSE_DATE(@format, @stichtag)
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("format", "STRING", bq_format),
            bigquery.ScalarQueryParameter("stichtag", "STRING", dw_stichtag),
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to update stichtag info for entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format):
    """
    Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Translate date format
    bq_format = dwmsg_date_format.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")\
                                 .replace("HH24", "%H").replace("MI", "%M").replace("SS", "%S")
    
    current_time_str = datetime.datetime.now().strftime(bq_format)
    append_text = f"{dwmsg_info_text} {current_time_str} "
    
    client = get_bq_client()
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.BERT_MELDUNG`
        SET timing_info = CONCAT(COALESCE(timing_info, ''), @append_text)
        WHERE entry_id = @entry_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("append_text", "STRING", append_text),
            bigquery.ScalarQueryParameter("entry_id", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: Failed to append timing info for entry {dwmsg_eintrags_nr}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Python replacement for legacy KSH f_alis_msgerr library")
    subparsers = parser.add_subparsers(dest="command", help="Function execution commands")
    
    # Subcommand: SetzeStatusOk
    sp_ok = subparsers.add_parser("SetzeStatusOk")
    sp_ok.add_argument("entry_id", help="Database entry tracking ID")
    
    # Subcommand: SetzeStatusAbbruch
    sp_abbruch = subparsers.add_parser("SetzeStatusAbbruch")
    sp_abbruch.add_argument("entry_id", help="Database entry tracking ID")
    
    # Subcommand: ErmittleNr
    subparsers.add_parser("ErmittleNr")
    
    # Subcommand: ErzeugeEintrag
    sp_erzeuge = subparsers.add_parser("ErzeugeEintrag")
    sp_erzeuge.add_argument("entry_id", help="Database entry tracking ID")
    sp_erzeuge.add_argument("job_kennung", help="Job Identifier")
    sp_erzeuge.add_argument("programm_name", help="Program/Script name")
    sp_erzeuge.add_argument("log_datei", help="Log file path")
    
    # Subcommand: MeldeFehler
    sp_fehler = subparsers.add_parser("MeldeFehler")
    sp_fehler.add_argument("entry_id", help="Database entry tracking ID")
    sp_fehler.add_argument("typ", choices=["F", "E", "W"], help="Error type")
    sp_fehler.add_argument("fehler_nr", type=int, help="Error number")
    sp_fehler.add_argument("zusatz1", nargs="?", default=None, help="Optional text 1")
    sp_fehler.add_argument("zusatz2", nargs="?", default=None, help="Optional text 2")
    
    # Subcommand: Logdateiname
    sp_log = subparsers.add_parser("Logdateiname")
    sp_log.add_argument("job_kennung", help="Job Identifier")
    sp_log.add_argument("entry_id", help="Database entry tracking ID")
    
    # Subcommand: SetzeStichtagInfo
    sp_stichtag = subparsers.add_parser("SetzeStichtagInfo")
    sp_stichtag.add_argument("entry_id", help="Database entry tracking ID")
    sp_stichtag.add_argument("stichtag", help="Key date value")
    sp_stichtag.add_argument("stichtag_fmt", help="Date format mask")
    
    # Subcommand: AppendTimingInfos
    sp_timing = subparsers.add_parser("AppendTimingInfos")
    sp_timing.add_argument("entry_id", help="Database entry tracking ID")
    sp_timing.add_argument("info_text", help="Progress details text")
    sp_timing.add_argument("date_format", help="Timing date format mask")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0
        
    if args.command == "SetzeStatusOk":
        dwmsg_setze_status_ok(args.entry_id)
    elif args.command == "SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.entry_id)
    elif args.command == "ErmittleNr":
        new_id = dwmsg_ermittle_nr()
        print(new_id)
    elif args.command == "ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.entry_id, args.job_kennung, args.programm_name, args.log_datei)
    elif args.command == "MeldeFehler":
        dwmsg_melde_fehler(args.entry_id, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "Logdateiname":
        print(dwmsg_logdateiname(args.job_kennung, args.entry_id))
    elif args.command == "SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.entry_id, args.stichtag, args.stichtag_fmt)
    elif args.command == "AppendTimingInfos":
        dwmsg_append_timing_infos(args.entry_id, args.info_text, args.date_format)
        
    return 0


if __name__ == "__main__":
    sys.exit(main())