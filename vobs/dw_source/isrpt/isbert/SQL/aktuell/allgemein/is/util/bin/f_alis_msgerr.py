#!/usr/bin/env python3
import os
import sys
import datetime
import re
import argparse
import oracledb

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
def get_db_connection():
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        print("ERROR: DW_ORAUSER environment variable is missing", file=sys.stderr)
        sys.exit(1)
    
    # Expected format: user/password@dsn
    match = re.match(r"([^/]+)/([^@]+)@(.+)", dw_orauser)
    if not match:
        print("ERROR: DW_ORAUSER must be in format 'user/password@dsn'", file=sys.stderr)
        sys.exit(1)
        
    db_user, db_password, db_dsn = match.groups()
    try:
        conn = oracledb.connect(
            user=db_user,
            password=db_password,
            dsn=db_dsn
        )
        return conn
    except oracledb.DatabaseError as e: 
        print(f"Database connection error: {e}", file=sys.stderr)
        sys.exit(1)

def dwmsg_fehlerbehandlung(eintrags_nr, error_code=1):
    """
    Step 1: DWMSG_Fehlerbehandlung
    """
    # kUnerwFehler = 10
    dwmsg_melde_fehler(eintrags_nr, "F", 10, f"ErrorCode ist: {error_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)

def dwmsg_setze_status_ok(eintrags_nr):
    """
    Step 2: DWMSG_SetzeStatusOK
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [eintrags_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusOK: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_setze_status_abbruch(eintrags_nr):
    """
    Step 3: DWMSG_SetzeStatusAbbruch
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [eintrags_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusAbbruch: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_ermittle_nr(var_name=None):
    """
    Step 4: DWMSG_ErmittleNr
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT bert_sequence.NEXTVAL FROM dual")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to retrieve tracking number from sequence.")
            val = str(row[0])
            return val
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_ErmittleNr: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Step 5: DWMSG_ErzeugeEintrag
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4); END;", 
                        [eintrags_nr, job_kennung, programm_name, log_datei])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_ErzeugeEintrag: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Step 6: DWMSG_MeldeFehler
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5); END;",
                        [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_MeldeFehler: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_logdateiname(var_name, job_kennung, eintrags_nr):
    """
    Step 7: DWMSG_Logdateiname
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT")
    if not dw_dir_prot:
        print("ERROR: DW_DIR_PROT environment variable is missing", file=sys.stderr)
        sys.exit(1)
        
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = os.path.join(dw_dir_prot, f"{job_kennung}_{timestamp}_{eintrags_nr}.log")
    return dateiname

def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """
    Step 8: DWMSG_SetzeStichtagInfo
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            plsql = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, to_date(:stichtag, :stichtag_fmt));
                COMMIT;
            END;
            """
            cur.execute(plsql, eintrags_nr=eintrags_nr, stichtag=stichtag, stichtag_fmt=stichtag_fmt)
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStichtagInfo: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """
    Step 9: DWMSG_AppendTimingInfos
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            plsql = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:eintrags_nr, NULL, :info_text || ' ' || to_char(SYSDATE, :date_format) || ' ');
                COMMIT;
            END;
            """
            cur.execute(plsql, eintrags_nr=eintrags_nr, info_text=info_text, date_format=date_format)
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_AppendTimingInfos: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Python translation of f_alis_msgerr.ksh")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-commands")
    
    # fehlerbehandlung
    p_err = subparsers.add_parser("fehlerbehandlung")
    p_err.add_argument("eintrags_nr", type=int)
    p_err.add_argument("error_code", type=int, nargs="?", default=1)
    
    # setze-status-ok
    p_ok = subparsers.add_parser("setze-status-ok")
    p_ok.add_argument("eintrags_nr", type=int)
    
    # setze-status-abbruch
    p_abb = subparsers.add_parser("setze-status-abbruch")
    p_abb.add_argument("eintrags_nr", type=int)
    
    # ermittle-nr
    p_erm = subparsers.add_parser("ermittle-nr")
    p_erm.add_argument("var_name", nargs="?", default=None)
    
    # erzeuge-eintrag
    p_erz = subparsers.add_parser("erzeuge-eintrag")
    p_erz.add_argument("eintrags_nr", type=int)
    p_erz.add_argument("job_kennung")
    p_erz.add_argument("programm_name")
    p_erz.add_argument("log_datei")
    
    # melde-fehler
    p_mld = subparsers.add_parser("melde-fehler")
    p_mld.add_argument("eintrags_nr", type=int)
    p_mld.add_argument("typ")
    p_mld.add_argument("fehler_nr", type=int)
    p_mld.add_argument("zusatz1", nargs="?", default="")
    p_mld.add_argument("zusatz2", nargs="?", default="")
    
    # logdateiname
    p_log = subparsers.add_parser("logdateiname")
    p_log.add_argument("var_name")
    p_log.add_argument("job_kennung")
    p_log.add_argument("eintrags_nr", type=int)
    
    # setze-stichtag-info
    p_st = subparsers.add_parser("setze-stichtag-info")
    p_st.add_argument("eintrags_nr", type=int)
    p_st.add_argument("stichtag")
    p_st.add_argument("stichtag_fmt")
    
    # append-timing-infos
    p_tm = subparsers.add_parser("append-timing-infos")
    p_tm.add_argument("eintrags_nr", type=int)
    p_tm.add_argument("info_text")
    p_tm.add_argument("date_format")
    
    args = parser.parse_args()
    
    if args.command == "fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.error_code)
    elif args.command == "setze-status-ok":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.command == "setze-status-abbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.command == "ermittle-nr":
        val = dwmsg_ermittle_nr(args.var_name)
        print(val)
    elif args.command == "erzeuge-eintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programm_name, args.log_datei)
    elif args.command == "melde-fehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "logdateiname":
        val = dwmsg_logdateiname(args.var_name, args.job_kennung, args.eintrags_nr)
        print(val)
    elif args.command == "setze-stichtag-info":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "append-timing-infos":
        dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)
        
    return 0

if __name__ == "__main__":
    sys.exit(main())