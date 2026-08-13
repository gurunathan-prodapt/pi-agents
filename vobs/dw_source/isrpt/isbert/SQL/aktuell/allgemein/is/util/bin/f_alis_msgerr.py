#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess
import argparse

# Global environment variables based on environment-specific classification policy
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
DW_ORAUSER = os.environ.get("DW_ORAUSER")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
DW_DIR_PROT = os.environ.get("DW_DIR_PROT")

# Fail loudly if critical variables are missing upon function execution
def verify_env_vars(*vars_to_check):
    for var in vars_to_check:
        if not os.environ.get(var):
            raise SystemExit(f"{var} must be set by the calling environment")


def dwmsg_fehlerbehandlung(eintrags_nr, fehler_nr=1):
    # sichern des FehlerCodes:
    # DWMSG_Fehlerbehandlung <EintragsNr>
    unerw_fehler = 10
    dwmsg_melde_fehler(eintrags_nr, "F", unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)


def dwmsg_setze_status_ok(eintrags_nr):
    verify_env_vars("DW_ORAUSER", "DW_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")

    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    cmd = [
        "sqlplus",
        "-s",
        dw_orauser,
        f"@{sql_script}",
        "BERT_MELDUNG.SetzeStatusOk",
        str(eintrags_nr)
    ]
    try:
        subprocess.run(cmd, input=b"", check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: SetzeStatusOk failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


def dwmsg_setze_status_abbruch(eintrags_nr):
    verify_env_vars("DW_ORAUSER", "DW_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")

    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    cmd = [
        "sqlplus",
        dw_orauser,
        f"@{sql_script}",
        "BERT_MELDUNG.SetzeStatusAbbruch",
        str(eintrags_nr)
    ]
    try:
        subprocess.run(cmd, input=b"", check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: SetzeStatusAbbruch failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


def dwmsg_ermittle_nr(var_name=None):
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben")
        sys.exit(1)

    verify_env_vars("DW_ORAUSER", "DW_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")

    temp_file = f"/tmp/ErmittleNr_{os.getpid()}.lst"
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = [
        "sqlplus",
        "-s",
        dw_orauser,
        f"@{sql_script}",
        temp_file
    ]
    try:
        subprocess.run(cmd, input=b"", check=True)
        with open(temp_file, "r") as f:
            eintrags_nr = f.read().replace(" ", "").strip()
        return eintrags_nr
    except subprocess.CalledProcessError as e:
        print(f"ERROR: ErmittleNr failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass


def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    verify_env_vars("DW_ORAUSER", "DW_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")

    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p4.sql")
    cmd = [
        "sqlplus",
        "-s",
        dw_orauser,
        f"@{sql_script}",
        "BERT_MELDUNG.Erzeuge_Eintrag",
        str(eintrags_nr),
        str(job_kennung),
        str(programmname),
        str(log_datei)
    ]
    try:
        subprocess.run(cmd, input=b"", check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: ErzeugeEintrag failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    verify_env_vars("DW_ORAUSER", "DW_DIR_ROOT")
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")

    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben")
        sys.exit(1)

    if not zusatz1:
        num_parm = 3
    elif not zusatz2:
        num_parm = 4
    else:
        num_parm = 5

    sql_script = os.path.join(dw_dir_root, f"allgemein/is/util/sql/d_alis_spaufruf_p{num_parm}.sql")

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = [
        "sqlplus",
        "-s",
        dw_orauser,
        f"@{sql_script}",
        "BERT_MELDUNG.Fehler",
        str(typ),
        str(eintrags_nr),
        str(fehler_nr),
        f"'{zusatz1}'",
        f"'{zusatz2}'"
    ]
    try:
        subprocess.run(cmd, input=b"", check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: MeldeFehler failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


def dwmsg_logdateiname(var_name, job_kennung, eintrags_nr):
    verify_env_vars("DW_DIR_PROT")
    dw_dir_prot = os.environ.get("DW_DIR_PROT")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = os.path.join(dw_dir_prot, f"{job_kennung}_{timestamp}_{eintrags_nr}.log")
    return dateiname


def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    verify_env_vars("DW_ORAUSER")
    dw_orauser = os.environ.get("DW_ORAUSER")

    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)

    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!")
        sys.exit(1)

    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!")
        sys.exit(2)

    # REVIEW: target database platform not confirmed — defaulted to Oracle (python-oracledb) since it preserves the original SQL with no rewrite; confirm before deploying
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    import oracledb
    try:
        user, password, dsn = "", "", ""
        if "@" in dw_orauser:
            left, dsn = dw_orauser.split("@", 1)
        else:
            left = dw_orauser
        
        if "/" in left:
            user, password = left.split("/", 1)
        else:
            user = left

        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        with conn.cursor() as cur:
            sql_text = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                COMMIT;
            END;
            """
            cur.execute(sql_text, {
                "eintrags_nr": int(eintrags_nr),
                "stichtag": stichtag,
                "stichtag_fmt": stichtag_fmt
            })
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: SetzeStichtagInfo failed: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    verify_env_vars("DW_ORAUSER")
    dw_orauser = os.environ.get("DW_ORAUSER")

    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)

    if not date_format:
        print("Argh!, Formatangabe erforderlich!")
        sys.exit(2)

    # REVIEW: target database platform not confirmed — defaulted to Oracle (python-oracledb) since it preserves the original SQL with no rewrite; confirm before deploying
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    import oracledb
    try:
        user, password, dsn = "", "", ""
        if "@" in dw_orauser:
            left, dsn = dw_orauser.split("@", 1)
        else:
            left = dw_orauser
        
        if "/" in left:
            user, password = left.split("/", 1)
        else:
            user = left

        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        with conn.cursor() as cur:
            sql_text = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, null, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                COMMIT;
            END;
            """
            cur.execute(sql_text, {
                "eintrags_nr": int(eintrags_nr),
                "info_text": info_text,
                "date_format": date_format
            })
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: AppendTimingInfos failed: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Unified Error Management and Logging (f_alis_msgerr.ksh equivalent)")
    subparsers = parser.add_subparsers(dest="command", help="The function to invoke")

    p_feh = subparsers.add_parser("DWMSG_Fehlerbehandlung")
    p_feh.add_argument("eintrags_nr")
    p_feh.add_argument("fehler_nr", type=int, nargs="?", default=1)

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
    p_log.add_argument("var_name")
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
        sys.exit(1)

    if args.command == "DWMSG_Fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.fehler_nr)
    elif args.command == "DWMSG_SetzeStatusOK":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.command == "DWMSG_SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.command == "DWMSG_ErmittleNr":
        res = dwmsg_ermittle_nr(args.var_name)
        print(res)
    elif args.command == "DWMSG_ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
    elif args.command == "DWMSG_MeldeFehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "DWMSG_Logdateiname":
        res = dwmsg_logdateiname(args.var_name, args.job_kennung, args.eintrags_nr)
        print(res)
    elif args.command == "DWMSG_SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "DWMSG_AppendTimingInfos":
        dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)


if __name__ == "__main__":
    sys.exit(main())