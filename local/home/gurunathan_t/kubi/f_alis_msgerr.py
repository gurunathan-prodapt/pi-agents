#!/usr/bin/env python3
import os
import sys
import datetime
from google.cloud import bigquery

# Ensure required environment variables are set, failing loudly if missing
bq_dataset = os.environ.get("BQ_DATASET") or os.environ.get("DW_ORAUSER")
if not bq_dataset:
    raise SystemExit("BQ_DATASET or DW_ORAUSER must be set by the calling Airflow task")

dw_dir_root = os.environ.get("DW_DIR_ROOT", "/default/path/root")

dw_dir_prot = os.environ.get("DW_DIR_PROT") or os.environ.get("GCS_BUCKET")
if not dw_dir_prot:
    raise SystemExit("DW_DIR_PROT or GCS_BUCKET must be set by the calling Airflow task")


# Step 1: DWMSG_Fehlerbehandlung
def DWMSG_Fehlerbehandlung(dwmsg_eintrags_nr: str, fehler_nr: int = 1):
    """
    Fehlerbehandlung wird NUR im Rahmenskript durchgeführt.
    Handles unexpected script failures caught by signal/error traps.
    """
    k_unerw_fehler = 10
    DWMSG_MeldeFehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    DWMSG_SetzeStatusAbbruch(dwmsg_eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def DWMSG_SetzeStatusOK(dwmsg_eintrags_nr: str):
    """Sets the log entry status to successful (OK)."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client()
    query = f"""
    UPDATE `{bq_dataset}.bert_meldung`
    SET status = 'OK'
    WHERE eintrags_nr = @eintrags_nr
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_SetzeStatusOK failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 3: DWMSG_SetzeStatusAbbruch
def DWMSG_SetzeStatusAbbruch(dwmsg_eintrags_nr: str):
    """Sets the log entry status to aborted/cancelled."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client()
    query = f"""
    UPDATE `{bq_dataset}.bert_meldung`
    SET status = 'ABBRUCH'
    WHERE eintrags_nr = @eintrags_nr
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr)
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_SetzeStatusAbbruch failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 4: DWMSG_ErmittleNr
def DWMSG_ErmittleNr(var_name: str) -> str:
    """Generates and returns a unique log tracker sequence number."""
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client()
    query = f"""
    SELECT GENERATE_UUID() as new_id
    """
    try:
        result = client.query(query).result()
        for row in result:
            new_id = str(row.new_id).replace(" ", "")
            return new_id
    except Exception as e:
        print(f"ERROR: DWMSG_ErmittleNr failed: {e}", file=sys.stderr)
        sys.exit(1)
    raise RuntimeError("Failed to generate unique logging ID from BigQuery.")


# Step 5: DWMSG_ErzeugeEintrag
def DWMSG_ErzeugeEintrag(dwmsg_eintrags_nr: str, job_kennung: str, programm_name: str, log_datei: str):
    """Registers the initial log execution entry."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client()
    query = f"""
    INSERT INTO `{bq_dataset}.bert_meldung` (eintrags_nr, job_kennung, programm_name, log_datei, status)
    VALUES (@eintrags_nr, @job_kennung, @programm_name, @log_datei, 'RUNNING')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programm_name", "STRING", programm_name),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_ErzeugeEintrag failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 6: DWMSG_MeldeFehler
def DWMSG_MeldeFehler(dwmsg_eintrags_nr: str, typ: str, fehler_nr: int, zusatz1: str = "", zusatz2: str = ""):
    """Dispatches warning, system, or application error reporting."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    client = bigquery.Client()
    query = f"""
    INSERT INTO `{bq_dataset}.bert_fehler` (eintrags_nr, typ, fehler_nr, zusatz1, zusatz2)
    VALUES (@eintrags_nr, @typ, @fehler_nr, @zusatz1, @zusatz2)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", fehler_nr),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_MeldeFehler failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 7: DWMSG_Logdateiname
def DWMSG_Logdateiname(var_name: str, job_kennung: str, dwmsg_eintrags_nr: str) -> str:
    """Assembles standard runtime target log path."""
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateipfad = os.path.join(dw_dir_prot, f"{job_kennung}_{now_str}_{dwmsg_eintrags_nr}.log")
    return dateipfad


# Step 8: DWMSG_SetzeStichtagInfo
def DWMSG_SetzeStichtagInfo(dwmsg_eintrags_nr: str, dwmsg_stichtag: str, dwmsg_stichtag_fmt: str):
    """Sets processing timestamp metadata for the logging record."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)

    client = bigquery.Client()
    query = f"""
    UPDATE `{bq_dataset}.bert_meldung`
    SET stichtag = PARSE_DATE(@stichtag_fmt, @stichtag)
    WHERE eintrags_nr = @eintrags_nr
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("stichtag", "STRING", dwmsg_stichtag),
            bigquery.ScalarQueryParameter("stichtag_fmt", "STRING", dwmsg_stichtag_fmt),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_SetzeStichtagInfo failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 9: DWMSG_AppendTimingInfos
def DWMSG_AppendTimingInfos(dwmsg_eintrags_nr: str, dwmsg_info_text: str, dwmsg_date_format: str):
    """Appends workflow metrics timing string to logging records."""
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    client = bigquery.Client()
    query = f"""
    UPDATE `{bq_dataset}.bert_meldung`
    SET zusatzinfos = CONCAT(COALESCE(zusatzinfos, ''), @info_text, ' ', FORMAT_TIMESTAMP(@date_format, CURRENT_TIMESTAMP()), ' ')
    WHERE eintrags_nr = @eintrags_nr
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "STRING", dwmsg_eintrags_nr),
            bigquery.ScalarQueryParameter("info_text", "STRING", dwmsg_info_text),
            bigquery.ScalarQueryParameter("date_format", "STRING", dwmsg_date_format),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        print(f"ERROR: DWMSG_AppendTimingInfos failed: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Python 3 utility library replacing f_alis_msgerr.ksh")
    subparsers = parser.add_subparsers(dest="action", help="Action to execute")

    p_fehler = subparsers.add_parser("Fehlerbehandlung")
    p_fehler.add_argument("eintrags_nr")
    p_fehler.add_argument("fehler_nr", type=int, nargs="?", default=1)

    p_ok = subparsers.add_parser("SetzeStatusOK")
    p_ok.add_argument("eintrags_nr")

    p_abb = subparsers.add_parser("SetzeStatusAbbruch")
    p_abb.add_argument("eintrags_nr")

    p_erm = subparsers.add_parser("ErmittleNr")
    p_erm.add_argument("var_name")

    p_erz = subparsers.add_parser("ErzeugeEintrag")
    p_erz.add_argument("eintrags_nr")
    p_erz.add_argument("job_kennung")
    p_erz.add_argument("programm_name")
    p_erz.add_argument("log_datei")

    p_mf = subparsers.add_parser("MeldeFehler")
    p_mf.add_argument("eintrags_nr")
    p_mf.add_argument("typ")
    p_mf.add_argument("fehler_nr", type=int)
    p_mf.add_argument("zusatz1", nargs="?", default="")
    p_mf.add_argument("zusatz2", nargs="?", default="")

    p_log = subparsers.add_parser("Logdateiname")
    p_log.add_argument("var_name")
    p_log.add_argument("job_kennung")
    p_log.add_argument("eintrags_nr")

    p_st = subparsers.add_parser("SetzeStichtagInfo")
    p_st.add_argument("eintrags_nr")
    p_st.add_argument("stichtag")
    p_st.add_argument("stichtag_fmt")

    p_ti = subparsers.add_parser("AppendTimingInfos")
    p_ti.add_argument("eintrags_nr")
    p_ti.add_argument("info_text")
    p_ti.add_argument("date_format")

    args = parser.parse_args()

    if args.action == "Fehlerbehandlung":
        DWMSG_Fehlerbehandlung(args.eintrags_nr, args.fehler_nr)
    elif args.action == "SetzeStatusOK":
        DWMSG_SetzeStatusOK(args.eintrags_nr)
    elif args.action == "SetzeStatusAbbruch":
        DWMSG_SetzeStatusAbbruch(args.eintrags_nr)
    elif args.action == "ErmittleNr":
        new_id = DWMSG_ErmittleNr(args.var_name)
        print(new_id)
    elif args.action == "ErzeugeEintrag":
        DWMSG_ErzeugeEintrag(args.eintrags_nr, args.job_kennung, args.programm_name, args.log_datei)
    elif args.action == "MeldeFehler":
        DWMSG_MeldeFehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.action == "Logdateiname":
        log_name = DWMSG_Logdateiname(args.var_name, args.job_kennung, args.eintrags_nr)
        print(log_name)
    elif args.action == "SetzeStichtagInfo":
        DWMSG_SetzeStichtagInfo(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.action == "AppendTimingInfos":
        DWMSG_AppendTimingInfos(args.eintrags_nr, args.info_text, args.date_format)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())