#!/usr/bin/env python3
import os
import sys
from datetime import datetime
import oracledb

# REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
def _get_db_connection():
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")
        
    try:
        # Extract credentials and host/DSN info from legacy format "user/password@dsn"
        if "@" in dw_orauser:
            creds, dsn = dw_orauser.split("@", 1)
        else:
            creds = dw_orauser
            dsn = None
            
        if "/" in creds:
            user, password = creds.split("/", 1)
        else:
            user = creds
            password = ""
            
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as e:
        print(f"Failed to connect to database using DW_ORAUSER: {e}", file=sys.stderr)
        sys.exit(1)


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code=1):
    """
    Funktion, die bei Auftreten eines Fehlers aufgerufen wird (falls so konfiguriert).
    Sie regelt das Eintragen in der Meldungstabelle und ggf. das Anstoßen weiterer Aktionen.
    """
    k_unerw_fehler = 10
    dwmsg_melde_fehler(eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {last_exit_code}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOk
def dwmsg_setze_status_ok(eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich beendet.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL script 'd_alis_spaufruf_p1.sql' not supplied. Implementing direct native PL/SQL call.
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [eintrags_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusOK: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL script 'd_alis_spaufruf_p1.sql' not supplied. Implementing direct native PL/SQL call.
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [eintrags_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusAbbruch: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    """
    Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine eine Nr.
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
        
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            # REVIEW-STRUCT: SQL script 'd_al_is_ermittlenr.sql' not supplied. 
            # Using a fallback sequence query to retrieve unique ID. Update as required.
            cur.execute("SELECT SEQ_BERT_MELDUNG.NEXTVAL FROM DUAL")
            row = cur.fetchone()
            eintrags_nr = str(row[0]).strip() if row else "1"
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_ErmittleNr: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()
        
    # NOTE: var_name parameter accepted for interface compatibility; returns the retrieved ID value
    return eintrags_nr


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    """
    Funktion erzeugt durch Aufruf einer entsprechenden PL/SQL Routine einen Eintrag in der Meldungstabelle.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL script 'd_alis_spaufruf_p4.sql' not supplied. Implementing direct native PL/SQL call.
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "BEGIN BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4); END;",
                [eintrags_nr, job_kennung, programmname, log_datei]
            )
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_ErzeugeEintrag: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL Routine.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # REVIEW-STRUCT: SQL scripts d_alis_spaufruf_p[3-5].sql not supplied. Implementing direct native PL/SQL call.
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            if not zusatz1:
                cur.execute(
                    "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3); END;",
                    [typ, eintrags_nr, fehler_nr]
                )
            elif not zusatz2:
                cur.execute(
                    "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4); END;",
                    [typ, eintrags_nr, fehler_nr, zusatz1]
                )
            else:
                cur.execute(
                    "BEGIN BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5); END;",
                    [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2]
                )
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_MeldeFehler: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(var_name, job_kennung, eintrags_nr):
    """
    Funktion baut aus den Angaben JobKennung und Eintragsnummer einen LogDateinamen auf.
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT")
    if not dw_dir_prot:
        raise SystemExit("DW_DIR_PROT must be set by the calling context")
        
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    dateiname = f"{dw_dir_prot}/{job_kennung}_{timestamp}_{eintrags_nr}.log"
    
    # NOTE: var_name accepted for interface compatibility; returns the constructed path
    return dateiname


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """
    Funktion setzt weitere Infofelder des Eintrages mit Nummer EintragsNr.
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
        
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            # Reproducing exactly the PL/SQL execution logic using proper bind syntax
            sql_text = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3));
                COMMIT;
            END;
            """
            cur.execute(sql_text, [eintrags_nr, stichtag, stichtag_fmt])
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStichtagInfo: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """
    Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            # Reproducing exactly the PL/SQL execution logic using proper bind syntax
            sql_text = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:1, null, :2 || ' ' || to_char(SYSDATE, :3) || ' ');
                COMMIT;
            END;
            """
            cur.execute(sql_text, [eintrags_nr, info_text, date_format])
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_AppendTimingInfos: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Operational metadata logging library for DWH pipelines.")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")
    
    # Sub-command: fehlerbehandlung
    p_err = subparsers.add_parser("fehlerbehandlung", help="Handle run failure")
    p_err.add_argument("eintrags_nr", help="Entry ID")
    p_err.add_argument("--exit-code", type=int, default=1, help="Last exit code")
    
    # Sub-command: ok
    p_ok = subparsers.add_parser("setze_ok", help="Set entry status to OK")
    p_ok.add_argument("eintrags_nr", help="Entry ID")
    
    # Sub-command: abbruch
    p_abb = subparsers.add_parser("setze_abbruch", help="Set entry status to Abbruch")
    p_abb.add_argument("eintrags_nr", help="Entry ID")
    
    # Sub-command: ermittle
    p_erm = subparsers.add_parser("ermittle_nr", help="Generate entry ID")
    p_erm.add_argument("var_name", help="Variable name (interface compatibility)")
    
    # Sub-command: erzeuge
    p_erz = subparsers.add_parser("erzeuge_eintrag", help="Create logging entry")
    p_erz.add_argument("eintrags_nr", help="Entry ID")
    p_erz.add_argument("job_kennung", help="Job identifier")
    p_erz.add_argument("programmname", help="Program name")
    p_erz.add_argument("log_datei", help="Log file path")
    
    # Sub-command: melde
    p_mel = subparsers.add_parser("melde_fehler", help="Log error details")
    p_mel.add_argument("eintrags_nr", help="Entry ID")
    p_mel.add_argument("typ", help="Type (F/E/W)")
    p_mel.add_argument("fehler_nr", help="Error code")
    p_mel.add_argument("zusatz1", nargs="?", default="", help="Optional info 1")
    p_mel.add_argument("zusatz2", nargs="?", default="", help="Optional info 2")
    
    # Sub-command: logdatei
    p_log = subparsers.add_parser("logdateiname", help="Generate standardized log file name")
    p_log.add_argument("var_name", help="Variable name (interface compatibility)")
    p_log.add_argument("job_kennung", help="Job identifier")
    p_log.add_argument("eintrags_nr", help="Entry ID")
    
    # Sub-command: stichtag
    p_sti = subparsers.add_parser("setze_stichtag", help="Set reference date details")
    p_sti.add_argument("eintrags_nr", help="Entry ID")
    p_sti.add_argument("stichtag", help="Date string")
    p_sti.add_argument("stichtag_fmt", help="Date format")
    
    # Sub-command: timing
    p_tim = subparsers.add_parser("append_timing", help="Log pipeline execution timings")
    p_tim.add_argument("eintrags_nr", help="Entry ID")
    p_tim.add_argument("info_text", help="Timing details context")
    p_tim.add_argument("date_format", help="Timing date format")
    
    args = parser.parse_args()
    
    if args.command == "fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.exit_code)
    elif args.command == "setze_ok":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.command == "setze_abbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.command == "ermittle_nr":
        val = dwmsg_ermittle_nr(args.var_name)
        print(val)
    elif args.command == "erzeuge_eintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
    elif args.command == "melde_fehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "logdateiname":
        val = dwmsg_logdateiname(args.var_name, args.job_kennung, args.eintrags_nr)
        print(val)
    elif args.command == "setze_stichtag":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "append_timing":
        dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())