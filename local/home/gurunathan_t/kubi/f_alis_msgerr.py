#!/usr/bin/env python3
import os
import sys
import datetime
from google.cloud import bigquery

# Global environment-wide configurations
dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")

def get_bq_client():
    project = os.environ.get("GCP_PROJECT")
    if not project:
        return bigquery.Client()
    return bigquery.Client(project=project)

def get_bq_dataset():
    return os.environ.get("BQ_DATASET", "dwpa_meldung")

def map_oracle_to_python_fmt(oracle_fmt):
    fmt = oracle_fmt.upper()
    fmt = fmt.replace("YYYY", "%Y")
    fmt = fmt.replace("YY", "%y")
    fmt = fmt.replace("MM", "%m")
    fmt = fmt.replace("DD", "%d")
    fmt = fmt.replace("HH24", "%H")
    fmt = fmt.replace("HH", "%I")
    fmt = fmt.replace("MI", "%M")
    fmt = fmt.replace("SS", "%S")
    return fmt

def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code=1):
    fehler_nr = last_exit_code
    k_unerw_fehler = 10

    dwmsg_melde_fehler(eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")

    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

def dwmsg_setze_status_ok(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben")
        sys.exit(1)

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_setze_status_ok`(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_setze_status_abbruch(eintrags_nr):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben")
        sys.exit(1)

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_setze_status_abbruch`(@eintrags_nr)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr))
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_ermittle_nr(var_name=None):
    if var_name is None and len(sys.argv) > 2:
        var_name = sys.argv[2]
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben")
        sys.exit(1)

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"SELECT {project_prefix}`{dataset}.dwpa_meldung_next_val`()"
    query_job = client.query(query)
    results = list(query_job.result())
    if results:
        eintrags_nr = str(results[0][0])
    else:
        raise RuntimeError("Failed to retrieve next sequence number from BigQuery")

    return eintrags_nr

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben")
        sys.exit(1)

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_erzeuge_eintrag`(@eintrags_nr, @job_kennung, @programmname, @log_datei)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung),
            bigquery.ScalarQueryParameter("programmname", "STRING", programmname),
            bigquery.ScalarQueryParameter("log_datei", "STRING", log_datei),
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben")
        sys.exit(1)

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_fehler`(@typ, @eintrags_nr, @fehler_nr, @zusatz1, @zusatz2)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("typ", "STRING", typ),
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("fehler_nr", "INT64", int(fehler_nr)),
            bigquery.ScalarQueryParameter("zusatz1", "STRING", zusatz1),
            bigquery.ScalarQueryParameter("zusatz2", "STRING", zusatz2),
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_logdateiname(var_name, job_kennung, eintrags_nr):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{eintrags_nr}.log"
    return dateiname

def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)

    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!")
        sys.exit(1)

    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!")
        sys.exit(2)

    py_fmt = map_oracle_to_python_fmt(stichtag_fmt)
    parsed_date = datetime.datetime.strptime(stichtag, py_fmt).date()

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_setze_stichtag_info`(@eintrags_nr, @stichtag_date)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("stichtag_date", "DATE", parsed_date),
        ]
    )
    client.query(query, job_config=job_config).result()

def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)

    if not date_format:
        print("Argh!, Formatangabe erforderlich!")
        sys.exit(2)

    py_fmt = map_oracle_to_python_fmt(date_format)
    formatted_time = datetime.datetime.now().strftime(py_fmt)
    full_info_text = f"{info_text} {formatted_time} "

    client = get_bq_client()
    dataset = get_bq_dataset()
    project = os.environ.get("GCP_PROJECT")
    project_prefix = f"`{project}`." if project else ""

    query = f"CALL {project_prefix}`{dataset}.dwpa_meldung_append_timing_infos`(@eintrags_nr, @info_text)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("eintrags_nr", "INT64", int(eintrags_nr)),
            bigquery.ScalarQueryParameter("info_text", "STRING", full_info_text),
        ]
    )
    client.query(query, job_config=job_config).result()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Runnable utility library for central error management (f_alis_msgerr)")
    parser.add_argument("function", help="The KSH function to call")
    parser.add_argument("args", nargs="*", help="Arguments for the function")

    parsed_args = parser.parse_args()
    func = parsed_args.function.upper()
    args = parsed_args.args

    if func == "DWMSG_FEHLERBEHANDLUNG":
        if len(args) < 1:
            print("Usage: DWMSG_Fehlerbehandlung <EintragsNr> [last_exit_code]", file=sys.stderr)
            return 1
        last_code = int(args[1]) if len(args) > 1 else 1
        dwmsg_fehlerbehandlung(args[0], last_code)
    elif func == "DWMSG_SETZESTATUSOK":
        if len(args) < 1:
            print("Usage: DWMSG_SetzeStatusOK <EintragsNr>", file=sys.stderr)
            return 1
        dwmsg_setze_status_ok(args[0])
    elif func == "DWMSG_SETZESTATUSABBRUCH":
        if len(args) < 1:
            print("Usage: DWMSG_SetzeStatusAbbruch <EintragsNr>", file=sys.stderr)
            return 1
        dwmsg_setze_status_abbruch(args[0])
    elif func == "DWMSG_ERMITTLENR":
        if len(args) < 1:
            print("Usage: DWMSG_ErmittleNr <VarName>", file=sys.stderr)
            return 1
        val = dwmsg_ermittle_nr(args[0])
        print(val)
    elif func == "DWMSG_ERZEUGEEINTRAG":
        if len(args) < 4:
            print("Usage: DWMSG_ErzeugeEintrag <EintragsNr> <JobKennung> <Programmname> <LogDatei>", file=sys.stderr)
            return 1
        dwmsg_erzeuge_eintrag(args[0], args[1], args[2], args[3])
    elif func == "DWMSG_MELDEFEHLER":
        if len(args) < 3:
            print("Usage: DWMSG_MeldeFehler <EintragsNr> <Typ> <FehlerNr> [Zusatz1] [Zusatz2]", file=sys.stderr)
            return 1
        z1 = args[3] if len(args) > 3 else ""
        z2 = args[4] if len(args) > 4 else ""
        dwmsg_melde_fehler(args[0], args[1], args[2], z1, z2)
    elif func == "DWMSG_LOGDATEINAME":
        if len(args) < 3:
            print("Usage: DWMSG_Logdateiname <VarName> <JobKennung> <EintragsNr>", file=sys.stderr)
            return 1
        val = dwmsg_logdateiname(args[0], args[1], args[2])
        print(val)
    elif func == "DWMSG_SETZESTICHTAGINFO":
        if len(args) < 3:
            print("Usage: DWMSG_SetzeStichtagInfo <EintragsNr> <Stichtag> <StichtagFmt>", file=sys.stderr)
            return 1
        dwmsg_setze_stichtag_info(args[0], args[1], args[2])
    elif func == "DWMSG_APPENDTIMINGINFOS":
        if len(args) < 3:
            print("Usage: DWMSG_AppendTimingInfos <EintragsNr> <InfoText> <DateFormat>", file=sys.stderr)
            return 1
        dwmsg_append_timing_infos(args[0], args[1], args[2])
    else:
        print(f"Unknown function: {parsed_args.function}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())