#!/usr/bin/env python3
import os
import sys
import datetime
import time
import argparse
from google.cloud import bigquery

# Retrieve global environment variables
GCP_PROJECT = os.environ.get("GCP_PROJECT")
if not GCP_PROJECT:
    raise SystemExit("GCP_PROJECT must be set by the calling environment")

BQ_DATASET = os.environ.get("BQ_DATASET")
if not BQ_DATASET:
    raise SystemExit("BQ_DATASET must be set by the calling environment")

DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("GCS_LOGS_BUCKET") or os.environ.get("DW_DIR_PROT")

def _get_bq_client():
    """Initializes and returns a BigQuery Client."""
    return bigquery.Client(project=GCP_PROJECT)

def _get_table_ref():
    """Constructs the fully qualified audit table path."""
    return f"{GCP_PROJECT}.{BQ_DATASET}.bert_meldung"

def oracle_to_python_fmt(oracle_fmt):
    """Converts standard Oracle date format strings to Python strftime formats."""
    mapping = {
        'YYYY': '%Y',
        'YYYYMMDD': '%Y%m%d',
        'YY': '%y',
        'MM': '%m',
        'DD': '%d',
        'HH24': '%H',
        'HH': '%I',
        'MI': '%M',
        'SS': '%S',
        'YYYY-MM-DD': '%Y-%m-%d'
    }
    py_fmt = oracle_fmt
    for ora in sorted(mapping.keys(), key=len, reverse=True):
        py_fmt = py_fmt.replace(ora, mapping[ora])
        py_fmt = py_fmt.replace(ora.lower(), mapping[ora])
    return py_fmt

def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, last_exit_code=1):
    """
    DWMSG_Fehlerbehandlung <EintragsNr>
    Funktion, die bei Auftreten eines Fehlers aufgerufen wird.
    Sie regelt das Eintragen in der Meldungstabelle und den Abbruch.
    """
    k_unerw_fehler = 10
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {last_exit_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """
    DWMSG_SetzeStatusOk <EintragsNr>
    Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich beendet.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    table = _get_table_ref()
    query = f"""
    UPDATE `{table}`
    SET status = 'OK', end_zeit = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = @eintrags_nr
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """
    DWMSG_SetzeStatusAbbruch <EintragsNr>
    Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    table = _get_table_ref()
    query = f"""
    UPDATE `{table}`
    SET status = 'ABBRUCH', end_zeit = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = @eintrags_nr
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_ermittle_nr(var_name=None):
    """
    DWMSG_ErmittleNr <VarName>
    Ermittelt eine eineindeutige Eintragsnummer.
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
    return str(time.time_ns())

def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei):
    """
    DWMSG_ErzeugeEintrag <EintragsNr> <JobKennung> <Programmname> <Logdatei>
    Funktion erzeugt einen Eintrag in der Meldungstabelle.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)

    table = _get_table_ref()
    query = f"""
    INSERT INTO `{table}` (eintrags_nr, job_kennung, programmname, log_datei, start_zeit, status)
    VALUES (@eintrags_nr, @job_kennung, @programmname, @log_datei, CURRENT_TIMESTAMP(), 'LAUFEND')
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr)),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programmname", "STRING", programmname),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei)
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    DWMSG_MeldeFehler <EintragsNr> <Typ> <FehlerNr> [[<Zusatz1>] <Zusatz2>]
    Funktion meldet einen Fehler in der Meldungstabelle.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    table = _get_table_ref()
    query = f"""
    UPDATE `{table}`
    SET fehler_typ = @typ, fehler_nr = @fehler_nr, zusatz1 = @zusatz1, zusatz2 = @zusatz2
    WHERE eintrags_nr = @eintrags_nr
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr)),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr) if fehler_nr else None),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2)
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr):
    """
    DWMSG_Logdateiname <JobKennung> <EintragsNr>
    Baut aus den Angaben einen LogDateinamen auf.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    base_dir = DW_DIR_PROT if DW_DIR_PROT else "."
    filename = f"{job_kennung}_{timestamp}_{dwmsg_eintrags_nr}.log"
    if base_dir.startswith("gs://"):
        return f"{base_dir.rstrip('/')}/{filename}"
    return os.path.join(base_dir, filename)

def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt):
    """
    DWMSG_SetzeStichtagInfo <EintragsNr> <Stichtag> <StichtagFormat>
    Funktion setzt das Stichtag-Infofeld des Eintrages.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)

    py_fmt = oracle_to_python_fmt(dwmsg_stichtag_fmt)
    try:
        parsed_date = datetime.datetime.strptime(dwmsg_stichtag, py_fmt).date()
    except ValueError as e:
        print(f"Fehler beim Parsen des Stichtags: {e}", file=sys.stderr)
        sys.exit(2)

    table = _get_table_ref()
    query = f"""
    UPDATE `{table}`
    SET stichtag = @stichtag
    WHERE eintrags_nr = @eintrags_nr
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("stichtag", "DATE", parsed_date),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_infotext, dwmsg_dateformat):
    """
    DWMSG_AppendTimingInfos <EintragsNr> <InfoText> <DateFormat>
    Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_dateformat:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    py_fmt = oracle_to_python_fmt(dwmsg_dateformat)
    now_str = datetime.datetime.now().strftime(py_fmt)
    append_val = f"{dwmsg_infotext} {now_str} "

    table = _get_table_ref()
    query = f"""
    UPDATE `{table}`
    SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), @append_val)
    WHERE eintrags_nr = @eintrags_nr
    """
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("append_val", "STRING", append_val),
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", str(dwmsg_eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def main():
    parser = argparse.ArgumentParser(description="Python utility module for Session Log & Telemetry")
    subparsers = parser.add_subparsers(dest="command", help="Centralized functions")

    p_err = subparsers.add_parser("DWMSG_Fehlerbehandlung")
    p_err.add_argument("eintrags_nr")
    p_err.add_argument("last_exit_code", nargs="?", default="1")

    p_ok = subparsers.add_parser("DWMSG_SetzeStatusOK")
    p_ok.add_argument("eintrags_nr")

    p_abb = subparsers.add_parser("DWMSG_SetzeStatusAbbruch")
    p_abb.add_argument("eintrags_nr")

    p_erm = subparsers.add_parser("DWMSG_ErmittleNr")
    p_erm.add_argument("var_name", nargs="?", default=None)

    p_erz = subparsers.add_parser("DWMSG_ErzeugeEintrag")
    p_erz.add_argument("eintrags_nr")
    p_erz.add_argument("job_kennung")
    p_erz.add_argument("programmname")
    p_erz.add_argument("log_datei")

    p_mel = subparsers.add_parser("DWMSG_MeldeFehler")
    p_mel.add_argument("eintrags_nr")
    p_mel.add_argument("typ")
    p_mel.add_argument("fehler_nr")
    p_mel.add_argument("zusatz1", nargs="?", default="")
    p_mel.add_argument("zusatz2", nargs="?", default="")

    p_log = subparsers.add_parser("DWMSG_Logdateiname")
    p_log.add_argument("job_kennung")
    p_log.add_argument("eintrags_nr")

    p_stich = subparsers.add_parser("DWMSG_SetzeStichtagInfo")
    p_stich.add_argument("eintrags_nr")
    p_stich.add_argument("stichtag")
    p_stich.add_argument("stichtag_fmt")

    p_time = subparsers.add_parser("DWMSG_AppendTimingInfos")
    p_time.add_argument("eintrags_nr")
    p_time.add_argument("info_text")
    p_time.add_argument("date_format")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == "DWMSG_Fehlerbehandlung":
            dwmsg_fehlerbehandlung(args.eintrags_nr, int(args.last_exit_code))
        elif args.command == "DWMSG_SetzeStatusOK":
            dwmsg_setze_status_ok(args.eintrags_nr)
        elif args.command == "DWMSG_SetzeStatusAbbruch":
            dwmsg_setze_status_abbruch(args.eintrags_nr)
        elif args.command == "DWMSG_ErmittleNr":
            nr = dwmsg_ermittle_nr(args.var_name)
            print(nr)
        elif args.command == "DWMSG_ErzeugeEintrag":
            dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
        elif args.command == "DWMSG_MeldeFehler":
            dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
        elif args.command == "DWMSG_Logdateiname":
            path = dwmsg_logdateiname(args.job_kennung, args.eintrags_nr)
            print(path)
        elif args.command == "DWMSG_SetzeStichtagInfo":
            dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
        elif args.command == "DWMSG_AppendTimingInfos":
            dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)
    except Exception as e:
        print(f"Error executing {args.command}: {e}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())