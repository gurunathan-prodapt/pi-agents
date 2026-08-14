#!/usr/bin/env python3
import os
import sys
import argparse
import datetime
import subprocess
import oracledb

def get_db_connection():
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")
    
    # Parse dw_orauser which is typically user/password@dsn or user/password
    try:
        if "@" in dw_orauser:
            user_pass, dsn = dw_orauser.split("@", 1)
        else:
            user_pass = dw_orauser
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
        print(f"Error connecting to database with DW_ORAUSER: {e}", file=sys.stderr)
        raise

# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, fehler_nr=1):
    """
    Funktion, die bei Auftreten eines Fehlers aufgerufen wird.
    Sie regelt das Eintragen in der Meldungstabelle und setzt den Abbruchstatus.
    """
    k_unerw_fehler = 10
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", k_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)

# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf erfolgreich beendet.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.callproc("BERT_MELDUNG.SetzeStatusOk", [val_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusOK: {e}", file=sys.stderr)
        sys.exit(1)

# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """
    Funktion setzt den Eintrag mit Nummer EintragsNr auf abgebrochen.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.callproc("BERT_MELDUNG.SetzeStatusAbbruch", [val_nr])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStatusAbbruch: {e}", file=sys.stderr)
        sys.exit(1)

# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name):
    """
    Funktion ermittelt durch Aufruf einer entsprechenden PL/SQl Routine eine Nr.
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")
    dw_dir_root = os.environ.get("DW_DIR_ROOT")
    if not dw_dir_root:
        raise SystemExit("DW_DIR_ROOT must be set by the calling Airflow task")
        
    temp_file = f"/tmp/ErmittleNr_{os.getpid()}.lst"
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{dw_dir_root}/allgemein/is/util/sql/d_al_is_ermittlenr.sql", temp_file],
            input=b"",
            check=True
        )
        if os.path.exists(temp_file):
            with open(temp_file, "r") as f:
                dwmsg_eintrags_nr = f.read().replace(" ", "").strip()
        else:
            dwmsg_eintrags_nr = ""
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed in DWMSG_ErmittleNr with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
    # Assign to environment and globals for reference
    os.environ[var_name] = dwmsg_eintrags_nr
    globals()[var_name] = dwmsg_eintrags_nr
    return dwmsg_eintrags_nr

# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Funktion erzeugt einen Eintrag in der Meldungstabelle.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.callproc("BERT_MELDUNG.Erzeuge_Eintrag", [val_nr, job_kennung, programm_name, log_datei])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_ErzeugeEintrag: {e}", file=sys.stderr)
        sys.exit(1)

# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Funktion meldet einen Fehler durch Aufruf einer entsprechenden PL/SQL Routine.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        val_fnr = int(fehler_nr) if str(fehler_nr).isdigit() else fehler_nr
        
        args = [typ, val_nr, val_fnr]
        if zusatz1:
            args.append(zusatz1)
            if zusatz2:
                args.append(zusatz2)
        elif zusatz2:
            args.append("")
            args.append(zusatz2)
            
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.callproc("BERT_MELDUNG.Fehler", args)
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_MeldeFehler: {e}", file=sys.stderr)
        sys.exit(1)

# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(var_name, job_kennung, dwmsg_eintrags_nr):
    """
    Funktion baut aus den Angaben JobKennung und Eintragsnummer einen LogDateinamen auf.
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT")
    if not dw_dir_prot:
        raise SystemExit("DW_DIR_PROT must be set by the calling Airflow task")
        
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = f"{dw_dir_prot}/{job_kennung}_{current_time}_{dwmsg_eintrags_nr}.log"
    
    os.environ[var_name] = dateiname
    globals()[var_name] = dateiname
    return dateiname

# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, stichtag, stichtag_fmt):
    """
    Funktion setzt weitere Infofelder des Eintrages mit Nummer EintragsNr.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
        
    if not stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        conn = get_db_connection()
        with conn.cursor() as cur:
            plsql_block = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3));
            END;
            """
            cur.execute(plsql_block, [val_nr, stichtag, stichtag_fmt])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_SetzeStichtagInfo: {e}", file=sys.stderr)
        sys.exit(1)

# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(dwmsg_eintrags_nr, info_text, date_format):
    """
    Funktion fuegt Timinginfos in die Spalte ZUSATZINFOS hinzu.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    try:
        val_nr = int(dwmsg_eintrags_nr) if str(dwmsg_eintrags_nr).isdigit() else dwmsg_eintrags_nr
        conn = get_db_connection()
        with conn.cursor() as cur:
            plsql_block = """
            BEGIN
                BERT_MELDUNG.SetzeZusatzInfos(:1, null, :2 || ' ' || to_char(SYSDATE, :3) || ' ');
            END;
            """
            cur.execute(plsql_block, [val_nr, info_text, date_format])
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in DWMSG_AppendTimingInfos: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Python translation of f_alis_msgerr.ksh library functions")
    subparsers = parser.add_subparsers(dest="function", help="Function to execute")
    
    # DWMSG_Fehlerbehandlung
    p_feh = subparsers.add_parser("DWMSG_Fehlerbehandlung")
    p_feh.add_argument("dwmsg_eintrags_nr")
    p_feh.add_argument("fehler_nr", type=int, nargs="?", default=1)
    
    # DWMSG_SetzeStatusOK
    p_ok = subparsers.add_parser("DWMSG_SetzeStatusOK")
    p_ok.add_argument("dwmsg_eintrags_nr")
    
    # DWMSG_SetzeStatusAbbruch
    p_abb = subparsers.add_parser("DWMSG_SetzeStatusAbbruch")
    p_abb.add_argument("dwmsg_eintrags_nr")
    
    # DWMSG_ErmittleNr
    p_erm = subparsers.add_parser("DWMSG_ErmittleNr")
    p_erm.add_argument("var_name")
    
    # DWMSG_ErzeugeEintrag
    p_erz = subparsers.add_parser("DWMSG_ErzeugeEintrag")
    p_erz.add_argument("dwmsg_eintrags_nr")
    p_erz.add_argument("job_kennung")
    p_erz.add_argument("programm_name")
    p_erz.add_argument("log_datei")
    
    # DWMSG_MeldeFehler
    p_mel = subparsers.add_parser("DWMSG_MeldeFehler")
    p_mel.add_argument("dwmsg_eintrags_nr")
    p_mel.add_argument("typ")
    p_mel.add_argument("fehler_nr")
    p_mel.add_argument("zusatz1", nargs="?", default="")
    p_mel.add_argument("zusatz2", nargs="?", default="")
    
    # DWMSG_Logdateiname
    p_log = subparsers.add_parser("DWMSG_Logdateiname")
    p_log.add_argument("var_name")
    p_log.add_argument("job_kennung")
    p_log.add_argument("dwmsg_eintrags_nr")
    
    # DWMSG_SetzeStichtagInfo
    p_sti = subparsers.add_parser("DWMSG_SetzeStichtagInfo")
    p_sti.add_argument("dwmsg_eintrags_nr")
    p_sti.add_argument("stichtag")
    p_sti.add_argument("stichtag_fmt")
    
    # DWMSG_AppendTimingInfos
    p_tim = subparsers.add_parser("DWMSG_AppendTimingInfos")
    p_tim.add_argument("dwmsg_eintrags_nr")
    p_tim.add_argument("info_text")
    p_tim.add_argument("date_format")
    
    args = parser.parse_args()
    
    if not args.function:
        parser.print_help()
        return 0
        
    if args.function == "DWMSG_Fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.dwmsg_eintrags_nr, args.fehler_nr)
    elif args.function == "DWMSG_SetzeStatusOK":
        dwmsg_setze_status_ok(args.dwmsg_eintrags_nr)
    elif args.function == "DWMSG_SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.dwmsg_eintrags_nr)
    elif args.function == "DWMSG_ErmittleNr":
        res = dwmsg_ermittle_nr(args.var_name)
        print(res)
    elif args.function == "DWMSG_ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.dwmsg_eintrags_nr, args.job_kennung, args.programm_name, args.log_datei)
    elif args.function == "DWMSG_MeldeFehler":
        dwmsg_melde_fehler(args.dwmsg_eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.function == "DWMSG_Logdateiname":
        res = dwmsg_logdateiname(args.var_name, args.job_kennung, args.dwmsg_eintrags_nr)
        print(res)
    elif args.function == "DWMSG_SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.dwmsg_eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.function == "DWMSG_AppendTimingInfos":
        dwmsg_append_timing_infos(args.dwmsg_eintrags_nr, args.info_text, args.date_format)
        
    return 0

if __name__ == "__main__":
    sys.exit(main())