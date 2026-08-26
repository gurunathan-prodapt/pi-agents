#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
import oracledb

# REVIEW: target database platform not confirmed — defaulted to Oracle (python-oracledb) since it preserves the original SQL with no rewrite; confirm before deploying

def _get_db_connection():
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        print("ERROR: DW_ORAUSER must be set in the environment", file=sys.stderr)
        raise SystemExit("DW_ORAUSER must be set by the calling task")
    try:
        # Standard Oracle connect string pattern is username/password@dsn
        if "@" in dw_orauser:
            user_pass, dsn = dw_orauser.split("@", 1)
            if "/" in user_pass:
                user, password = user_pass.split("/", 1)
                return oracledb.connect(user=user, password=password, dsn=dsn)
        return oracledb.connect(dsn=dw_orauser)
    except Exception as e:
        print(f"ERROR: Failed to establish database connection: {e}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, exit_code=1):
    k_unerw_fehler = 10
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {exit_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied - executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [dwmsg_eintrags_nr])
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_setze_status_ok failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: d_alis_spaufruf_p1.sql body not supplied - executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [dwmsg_eintrags_nr])
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_setze_status_abbruch failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: d_al_is_ermittlenr.sql body not supplied - executing sequence query directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT BERT_MELDUNG_SEQ.NEXTVAL FROM DUAL")
                row = cur.fetchone()
                if row:
                    val = str(row[0]).strip()
                    # In Python, we return the value. If run as CLI, main() will print it.
                    return val
                else:
                    raise RuntimeError("Failed to fetch sequence value")
    except Exception as e:
        print(f"ERROR: dwmsg_ermittle_nr failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, log_datei):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: d_alis_spaufruf_p4.sql body not supplied - executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "BEGIN BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4); END;",
                    [dwmsg_eintrags_nr, job_kennung, programmname, log_datei]
                )
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_erzeuge_eintrag failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben")
        sys.exit(1)

    # REVIEW-STRUCT: d_alis_spaufruf_p*.sql body not supplied - executing procedure directly
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5); END;",
                    [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2]
                )
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_melde_fehler failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr, var_name=None):
    # NOTE: var_name parameter accepted for interface compatibility; returns the filename in Python
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "")
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{dw_dir_prot}/{job_kennung}_{current_time}_{dwmsg_eintrags_nr}.log"
    return filename

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)
    if not dwmsg_stichtag:
        print("Argh!, keinen Stichtag angegeben!")
        sys.exit(1)
    if not dwmsg_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!")
        sys.exit(2)

    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                # EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr, to_date('$DWMSG_Stichtag', '$DWMSG_StichtagFmt'));
                plsql = "BEGIN BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3)); END;"
                cur.execute(plsql, [dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt])
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_setze_stichtag_info failed: {e}", file=sys.stderr)
        sys.exit(1)

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format):
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben")
        sys.exit(1)
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!")
        sys.exit(2)

    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cur:
                # EXEC BERT_MELDUNG.SetzeZusatzInfos($DWMSG_EintragsNr,null,'$DWMSG_InfoText'||' '||to_char(SYSDATE,'$DWMSG_DateFormat')||' ');
                plsql = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:1, NULL, :2 || ' ' || to_char(SYSDATE, :3) || ' ');
                END;
                """
                cur.execute(plsql, [dwmsg_eintrags_nr, dwmsg_info_text, dwmsg_date_format])
                conn.commit()
    except Exception as e:
        print(f"ERROR: dwmsg_append_timing_infos failed: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Python equivalent of f_alis_msgerr.ksh library")
    parser.add_argument("--action", choices=[
        "Fehlerbehandlung", "SetzeStatusOK", "SetzeStatusAbbruch",
        "ErmittleNr", "ErzeugeEintrag", "MeldeFehler", "Logdateiname",
        "SetzeStichtagInfo", "AppendTimingInfos"
    ], help="The function to execute")
    parser.add_argument("--eintrags-nr", help="DWMSG_EintragsNr")
    parser.add_argument("--exit-code", type=int, default=1, help="Exit code for Fehlerbehandlung")
    parser.add_argument("--job-kennung", help="Jobkennung")
    parser.add_argument("--programmname", help="Programmname")
    parser.add_argument("--log-datei", help="Logdatei path")
    parser.add_argument("--typ", help="Error type (F/E/W)")
    parser.add_argument("--fehler-nr", help="FehlerNr")
    parser.add_argument("--zusatz1", default="", help="Zusatz1")
    parser.add_argument("--zusatz2", default="", help="Zusatz2")
    parser.add_argument("--stichtag", help="Stichtag date string")
    parser.add_argument("--stichtag-fmt", help="Stichtag format string")
    parser.add_argument("--info-text", help="Timing info text")
    parser.add_argument("--date-format", help="Date format for timing info")
    parser.add_argument("--var-name", help="VarName for compatibility")

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 0

    if args.action == "Fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.exit_code)
    elif args.action == "SetzeStatusOK":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.action == "SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.action == "ErmittleNr":
        val = dwmsg_ermittle_nr(args.var_name)
        print(val)
    elif args.action == "ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
    elif args.action == "MeldeFehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.action == "Logdateiname":
        val = dwmsg_logdateiname(args.job_kennung, args.eintrags_nr, args.var_name)
        print(val)
    elif args.action == "SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.action == "AppendTimingInfos":
        dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)

    return 0

if __name__ == "__main__":
    sys.exit(main())