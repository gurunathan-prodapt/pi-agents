#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess
import argparse

try:
    import oracledb
except ImportError:
    oracledb = None

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(entry_id, last_error_code=None):
    if last_error_code is None:
        last_error_code = 1
    dwmsg_eintrags_nr = entry_id
    k_unerw_fehler = 10

    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {last_error_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(entry_id):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{script_path}", "BERT_MELDUNG.SetzeStatusOk", entry_id],
            input="",
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(entry_id):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p1.sql")
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["sqlplus", dw_orauser, f"@{script_path}", "BERT_MELDUNG.SetzeStatusAbbruch", entry_id],
            input="",
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

    pid = os.getpid()
    temp_file = f"/tmp/ErmittleNr_{pid}.lst"

    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{script_path}", temp_file],
            input="",
            text=True,
            check=True
        )
        with open(temp_file, "r") as f:
            content = f.read()
        
        dwmsg_eintrags_nr = content.replace(" ", "").strip()
        return dwmsg_eintrags_nr
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(entry_id, job_kennung, programm_name, log_datei):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

    script_path = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_alis_spaufruf_p4.sql")
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            [
                "sqlplus", "-s", dw_orauser, f"@{script_path}",
                "BERT_MELDUNG.Erzeuge_Eintrag", entry_id, job_kennung,
                programm_name, log_datei
            ],
            input="",
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(entry_id, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not entry_id:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

    if not zusatz1:
        num_parm = 3
    elif not zusatz2:
        num_parm = 4
    else:
        num_parm = 5

    script_name = f"d_alis_spaufruf_p{num_parm}.sql"
    dateipfad = os.path.join(dw_dir_root, "allgemein/is/util/sql", script_name)

    arg_zusatz1 = f"'{zusatz1}'"
    arg_zusatz2 = f"'{zusatz2}'"

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            [
                "sqlplus", "-s", dw_orauser, f"@{dateipfad}", "BERT_MELDUNG.Fehler",
                typ, entry_id, str(fehler_nr), arg_zusatz1, arg_zusatz2
            ],
            input="",
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(var_name, job_kennung, entry_id):
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    dw_dir_prot = os.environ.get("DW_DIR_PROT")
    if not dw_dir_prot:
        raise SystemExit("DW_DIR_PROT must be set by the calling environment")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = os.path.join(dw_dir_prot, f"{job_kennung}_{timestamp}_{entry_id}.log")
    return dateiname


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(entry_id, stichtag, stichtag_fmt):
    if not entry_id:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # REVIEW-STRUCT: target database platform defaulted to Oracle (python-oracledb) since it preserves original SQL with no rewrite
    if oracledb is not None:
        try:
            user_part, dsn_part = dw_orauser.split('@', 1)
            user, password = user_part.split('/', 1)
            dsn = dsn_part
            
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            with conn.cursor() as cur:
                plsql_text = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:entry_id, to_date(:stichtag, :format_mask));
                    COMMIT;
                END;
                """
                cur.execute(plsql_text, {"entry_id": int(entry_id), "stichtag": stichtag, "format_mask": stichtag_fmt})
            return
        except Exception as e:
            print(f"Warning: python-oracledb execution failed: {e}. Falling back to sqlplus.", file=sys.stderr)

    # Fallback to sqlplus launcher
    plsql_payload = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({entry_id}, to_date('{stichtag}', '{stichtag_fmt}'));
    commit;
    """
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser],
            input=plsql_payload,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(entry_id, info_text, date_format):
    if not entry_id:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # REVIEW-STRUCT: target database platform defaulted to Oracle (python-oracledb) since it preserves original SQL with no rewrite
    if oracledb is not None:
        try:
            user_part, dsn_part = dw_orauser.split('@', 1)
            user, password = user_part.split('/', 1)
            dsn = dsn_part
            
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            with conn.cursor() as cur:
                plsql_text = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:entry_id, NULL, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                    COMMIT;
                END;
                """
                cur.execute(plsql_text, {"entry_id": int(entry_id), "info_text": info_text, "date_format": date_format})
            return
        except Exception as e:
            print(f"Warning: python-oracledb execution failed: {e}. Falling back to sqlplus.", file=sys.stderr)

    # Fallback to sqlplus launcher
    plsql_payload = f"""
    EXEC BERT_MELDUNG.SetzeZusatzInfos({entry_id},null,'{info_text}'||' '||to_char(SYSDATE,'{date_format}')||' ');
    commit;
    """
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser],
            input=plsql_payload,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


def main():
    parser = argparse.ArgumentParser(description="Python CLI wrapper for f_alis_msgerr.ksh library functions.")
    subparsers = parser.add_subparsers(dest="command", help="Function to execute")

    p_fehler = subparsers.add_parser("Fehlerbehandlung")
    p_fehler.add_argument("entry_id")
    p_fehler.add_argument("last_error_code", type=int, nargs="?", default=1)

    p_ok = subparsers.add_parser("SetzeStatusOK")
    p_ok.add_argument("entry_id")

    p_abbruch = subparsers.add_parser("SetzeStatusAbbruch")
    p_abbruch.add_argument("entry_id")

    p_ermittle = subparsers.add_parser("ErmittleNr")
    p_ermittle.add_argument("var_name", nargs="?", default=None)

    p_erzeuge = subparsers.add_parser("ErzeugeEintrag")
    p_erzeuge.add_argument("entry_id")
    p_erzeuge.add_argument("job_kennung")
    p_erzeuge.add_argument("programm_name")
    p_erzeuge.add_argument("log_datei")

    p_melde = subparsers.add_parser("MeldeFehler")
    p_melde.add_argument("entry_id")
    p_melde.add_argument("typ")
    p_melde.add_argument("fehler_nr", type=int)
    p_melde.add_argument("zusatz1", nargs="?", default="")
    p_melde.add_argument("zusatz2", nargs="?", default="")

    p_logname = subparsers.add_parser("Logdateiname")
    p_logname.add_argument("var_name")
    p_logname.add_argument("job_kennung")
    p_logname.add_argument("entry_id")

    p_stichtag = subparsers.add_parser("SetzeStichtagInfo")
    p_stichtag.add_argument("entry_id")
    p_stichtag.add_argument("stichtag")
    p_stichtag.add_argument("stichtag_fmt")

    p_timing = subparsers.add_parser("AppendTimingInfos")
    p_timing.add_argument("entry_id")
    p_timing.add_argument("info_text")
    p_timing.add_argument("date_format")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "Fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.entry_id, args.last_error_code)
    elif args.command == "SetzeStatusOK":
        dwmsg_setze_status_ok(args.entry_id)
    elif args.command == "SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.entry_id)
    elif args.command == "ErmittleNr":
        nr = dwmsg_ermittle_nr(args.var_name)
        print(nr)
    elif args.command == "ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.entry_id, args.job_kennung, args.programm_name, args.log_datei)
    elif args.command == "MeldeFehler":
        dwmsg_melde_fehler(args.entry_id, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "Logdateiname":
        path = dwmsg_logdateiname(args.var_name, args.job_kennung, args.entry_id)
        print(path)
    elif args.command == "SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.entry_id, args.stichtag, args.stichtag_fmt)
    elif args.command == "AppendTimingInfos":
        dwmsg_append_timing_infos(args.entry_id, args.info_text, args.date_format)


if __name__ == "__main__":
    sys.exit(main())