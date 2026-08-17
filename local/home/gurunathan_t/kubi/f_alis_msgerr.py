#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
import oracledb

# Global/Environment configuration
DW_ORAUSER = os.environ.get("DW_ORAUSER")
if not DW_ORAUSER:
    raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")

DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
if not DW_DIR_ROOT:
    raise SystemExit("DW_DIR_ROOT must be set by the calling Airflow task")

DW_DIR_PROT = os.environ.get("DW_DIR_PROT")
if not DW_DIR_PROT:
    raise SystemExit("DW_DIR_PROT must be set by the calling Airflow task")


# Helper function to get a DB connection
def get_db_connection():
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    # REVIEW: Target database connection established from legacy DW_ORAUSER.
    # Ensure correct credentials and TNS setup in target environment.
    try:
        # Standard Oracle user/password@dsn resolution
        if "@" in DW_ORAUSER:
            user_pass, dsn = DW_ORAUSER.split("@", 1)
        else:
            user_pass = DW_ORAUSER
            dsn = None
        
        if "/" in user_pass:
            user, password = user_pass.split("/", 1)
        else:
            user = user_pass
            password = ""
            
        if dsn:
            return oracledb.connect(user=user, password=password, dsn=dsn)
        else:
            return oracledb.connect(user=user, password=password)
    except Exception as e:
        print(f"Error establishing database connection: {e}", file=sys.stderr)
        raise


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, last_exit_code):
    # Capture error status and report to tracking system
    k_unerw_fehler = 10
    msg = f"ErrorCode ist: {last_exit_code}"
    
    print("Ich bin im Fehlerhandler, fehler der DB melden...", file=sys.stderr)
    dwmsg_melde_fehler(eintrags_nr, "F", k_unerw_fehler, msg)
    
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus", file=sys.stderr)
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Execute database status update
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Replaces call to d_alis_spaufruf_p1.sql with direct procedure invocation
                cursor.callproc("BERT_MELDUNG.SetzeStatusOk", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_setze_status_ok: {e}", file=sys.stderr)
        sys.exit(1)


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Execute database status update
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_setze_status_abbruch: {e}", file=sys.stderr)
        sys.exit(1)


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr():
    # REVIEW: out-parameter validation "Argh!, keinen Variablennamen bei ErmittleNr angegeben" guarded a parameter this refactor removed — confirm no equivalent guard is needed for the return-based version.
    
    # Query database to get unique generated registration ID
    # Replaces execution of d_al_is_ermittlenr.sql writing to temporary files
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Assuming sequencing query logic from d_al_is_ermittlenr.sql
                # In Python, we bypass the need for '/tmp/ErmittleNr_$$' file generation.
                cursor.execute("SELECT BERT_MELDUNG_SEQ.NEXTVAL FROM DUAL")  # REVIEW: Verify actual generation logic in d_al_is_ermittlenr.sql
                row = cursor.fetchone()
                if row:
                    return str(row[0]).strip()
                else:
                    raise RuntimeError("Failed to fetch next tracking sequence number")
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_ermittle_nr: {e}", file=sys.stderr)
        sys.exit(1)


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programmname, log_datei):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Register the job entry details
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [eintrags_nr, job_kennung, programmname, log_datei])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_erzeuge_eintrag: {e}", file=sys.stderr)
        sys.exit(1)


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    # Guard check for missing parameter
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    # Log incident with optional details parameters
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Calls procedure BERT_MELDUNG.Fehler
                # Resolves dynamic signature cleanly in Python
                cursor.callproc("BERT_MELDUNG.Fehler", [typ, eintrags_nr, fehler_nr, zusatz1, zusatz2])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_melde_fehler: {e}", file=sys.stderr)
        sys.exit(1)


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    # Builds standard protocol log path and returns it directly
    # Replacing original shell 'eval' architecture with return
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    log_name = f"{job_kennung}_{current_time}_{eintrags_nr}.log"
    return os.path.join(DW_DIR_PROT, log_name)


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    # Guard check for missing operational inputs
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    # Update execution metadata using direct PL/SQL bindings
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Map parameters safely to avoid SQL injection
                # Oracle to_date handling is replaced by converting python datetime or passing strings
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:e_nr, TO_DATE(:stich, :fmt));
                END;
                """
                cursor.execute(plsql_block, e_nr=eintrags_nr, stich=stichtag, fmt=stichtag_fmt)
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_setze_stichtag_info: {e}", file=sys.stderr)
        sys.exit(1)


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    # Guard check for missing operational inputs
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    # Log progress status and timing metrics to Oracle DB
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Construct statement with native current database timestamp formatted via parameters
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:e_nr, NULL, :info || ' ' || TO_CHAR(SYSDATE, :fmt) || ' ');
                END;
                """
                cursor.execute(plsql_block, e_nr=eintrags_nr, info=info_text, fmt=date_format)
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in dwmsg_append_timing_infos: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Python replacement for f_alis_msgerr library functions")
    subparsers = parser.add_subparsers(dest="command", help="Function to execute")
    
    # dwmsg_fehlerbehandlung
    p1 = subparsers.add_parser("dwmsg_fehlerbehandlung")
    p1.add_argument("eintrags_nr")
    p1.add_argument("last_exit_code", type=int)
    
    # dwmsg_setze_status_ok
    p2 = subparsers.add_parser("dwmsg_setze_status_ok")
    p2.add_argument("eintrags_nr")
    
    # dwmsg_setze_status_abbruch
    p3 = subparsers.add_parser("dwmsg_setze_status_abbruch")
    p3.add_argument("eintrags_nr")
    
    # dwmsg_ermittle_nr
    subparsers.add_parser("dwmsg_ermittle_nr")
    
    # dwmsg_erzeuge_eintrag
    p5 = subparsers.add_parser("dwmsg_erzeuge_eintrag")
    p5.add_argument("eintrags_nr")
    p5.add_argument("job_kennung")
    p5.add_argument("programmname")
    p5.add_argument("log_datei")
    
    # dwmsg_melde_fehler
    p6 = subparsers.add_parser("dwmsg_melde_fehler")
    p6.add_argument("eintrags_nr")
    p6.add_argument("typ")
    p6.add_argument("fehler_nr")
    p6.add_argument("zusatz1", nargs="?", default="")
    p6.add_argument("zusatz2", nargs="?", default="")
    
    # dwmsg_logdateiname
    p7 = subparsers.add_parser("dwmsg_logdateiname")
    p7.add_argument("job_kennung")
    p7.add_argument("eintrags_nr")
    
    # dwmsg_setze_stichtag_info
    p8 = subparsers.add_parser("dwmsg_setze_stichtag_info")
    p8.add_argument("eintrags_nr")
    p8.add_argument("stichtag")
    p8.add_argument("stichtag_fmt")
    
    # dwmsg_append_timing_infos
    p9 = subparsers.add_parser("dwmsg_append_timing_infos")
    p9.add_argument("eintrags_nr")
    p9.add_argument("info_text")
    p9.add_argument("date_format")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
        
    try: 
        if args.command == "dwmsg_fehlerbehandlung":
            dwmsg_fehlerbehandlung(args.eintrags_nr, args.last_exit_code)
        elif args.command == "dwmsg_setze_status_ok":
            dwmsg_setze_status_ok(args.eintrags_nr)
        elif args.command == "dwmsg_setze_status_abbruch":
            dwmsg_setze_status_abbruch(args.eintrags_nr)
        elif args.command == "dwmsg_ermittle_nr":
            nr = dwmsg_ermittle_nr()
            print(nr)
        elif args.command == "dwmsg_erzeuge_eintrag":
            dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
        elif args.command == "dwmsg_melde_fehler":
            dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
        elif args.command == "dwmsg_logdateiname":
            path = dwmsg_logdateiname(args.job_kennung, args.eintrags_nr)
            print(path)
        elif args.command == "dwmsg_setze_stichtag_info":
            dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
        elif args.command == "dwmsg_append_timing_infos":
            dwmsg_append_timing_infos(args.eintrags_nr, args.info_text, args.date_format)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())