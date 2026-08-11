#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
import subprocess
import re
import oracledb

# REVIEW: target database platform not confirmed — defaulted to Oracle (python-oracledb) since it preserves the original SQL with no rewrite; confirm before deploying

def get_db_connection():
    """
    Establishes a database connection using the DW_ORAUSER environment variable.
    Fails loudly if DW_ORAUSER is missing.
    """
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling task")
    
    # Parse standard connection strings like user/password@dsn
    match = re.match(r"^([^/]+)/([^@]+)(?:@(.+))?$", dw_orauser)
    if match:
        user = match.group(1)
        password = match.group(2)
        dsn = match.group(3) or ""
        try:
            return oracledb.connect(user=user, password=password, dsn=dsn)
        except Exception as e:
            raise SystemExit(f"Failed to connect to Oracle using parsed DW_ORAUSER: {e}")
    else:
        try:
            return oracledb.connect(dsn=dw_orauser)
        except Exception as e:
            raise SystemExit(f"Failed to connect to Oracle using DW_ORAUSER connection string: {e}")


# Step 1: DWMSG_Fehlerbehandlung
def dwmsg_fehlerbehandlung(eintrags_nr, fehler_nr=1):
    """
    Error handling routine called upon execution failure.
    Logs unexpected fatal failure and marks the status as aborted.
    """
    const_unerw_fehler = 10
    dwmsg_melde_fehler(eintrags_nr, "F", const_unerw_fehler, f"ErrorCode ist: {fehler_nr}")
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(eintrags_nr)


# Step 2: DWMSG_SetzeStatusOK
def dwmsg_setze_status_ok(eintrags_nr):
    """
    Updates the execution log status to successfully completed (OK).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL call to SetzeStatusOk
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: SetzeStatusOK failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 3: DWMSG_SetzeStatusAbbruch
def dwmsg_setze_status_abbruch(eintrags_nr):
    """
    Updates the execution log status to aborted (Abbruch).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL call to SetzeStatusAbbruch
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: SetzeStatusAbbruch failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 4: DWMSG_ErmittleNr
def dwmsg_ermittle_nr(var_name=None):
    """
    Acquires a unique execution sequence ID from the database using the external SQL script.
    """
    if not var_name:
        print("Argh!, keinen Variablennamen bei ErmittleNr angegeben", file=sys.stderr)
        sys.exit(1)
        
    dw_orauser = os.environ.get("DW_ORAUSER")
    dw_dir_root = os.environ.get("DW_DIR_ROOT", "/tmp")
    
    pid = os.getpid()
    temp_file = f"/tmp/ErmittleNr_{pid}.lst"
    sql_script = os.path.join(dw_dir_root, "allgemein/is/util/sql/d_al_is_ermittlenr.sql")
    
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["sqlplus", "-s", dw_orauser, f"@{sql_script}", temp_file],
            input="",
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: sqlplus failed in DWMSG_ErmittleNr with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
        
    try:
        with open(temp_file, "r") as f:
            dwmsg_eintrags_nr = f.read().replace(" ", "").strip()
    except IOError as e:
        print(f"ERROR: Failed to read temp file {temp_file}: {e}", file=sys.stderr)
        sys.exit(1)
        
    try:
        os.remove(temp_file)
    except OSError:
        pass
        
    return dwmsg_eintrags_nr


# Step 5: DWMSG_ErzeugeEintrag
def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, programm_name, log_datei):
    """
    Creates a new logging entry record in the database tracking ledger.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL call to Erzeuge_Eintrag
                cur.execute(
                    "BEGIN BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4); END;",
                    [eintrags_nr, job_kennung, programm_name, log_datei]
                )
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: ErzeugeEintrag failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 6: DWMSG_MeldeFehler
def dwmsg_melde_fehler(eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Registers an error status or warning trace in the database tracking ledger.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL call to Fehler
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
        print(f"ERROR: MeldeFehler failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 7: DWMSG_Logdateiname
def dwmsg_logdateiname(job_kennung, eintrags_nr):
    """
    Compiles and constructs the dynamic path for process logs.
    """
    dw_dir_prot = os.environ.get("DW_DIR_PROT", "/tmp")
    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{job_kennung}_{now_str}_{eintrags_nr}.log"
    return os.path.join(dw_dir_prot, filename)


# Step 8: DWMSG_SetzeStichtagInfo
def dwmsg_setze_stichtag_info(eintrags_nr, stichtag, stichtag_fmt):
    """
    Saves reporting business reference dates inside the tracking table.
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
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL block with bind variables
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(:e_nr, TO_DATE(:s_tag, :s_fmt));
                    COMMIT;
                END;
                """
                cur.execute(plsql_block, {
                    "e_nr": eintrags_nr,
                    "s_tag": stichtag,
                    "s_fmt": stichtag_fmt
                })
    except oracledb.DatabaseError as e:
        print(f"ERROR: SetzeStichtagInfo failed: {e}", file=sys.stderr)
        sys.exit(1)


# Step 9: DWMSG_AppendTimingInfos
def dwmsg_append_timing_infos(eintrags_nr, info_text, date_format):
    """
    Appends formatted metrics checkpoints to execution record auxiliary fields.
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
        
    if not date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)
        
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REIMPLEMENTATION: Directly executing Oracle PL/SQL block.
                # Replaced SYSDATE with CURRENT_TIMESTAMP per instructions.
                plsql_block = """
                BEGIN
                    BERT_MELDUNG.SetzeZusatzInfos(
                        :e_nr,
                        NULL,
                        :i_txt || ' ' || TO_CHAR(CURRENT_TIMESTAMP, :d_fmt) || ' '
                    );
                    COMMIT;
                END;
                """
                cur.execute(plsql_block, {
                    "e_nr": eintrags_nr,
                    "i_txt": info_text if info_text is not None else "",
                    "d_fmt": date_format
                })
    except oracledb.DatabaseError as e:
        print(f"ERROR: AppendTimingInfos failed: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Python CLI wrapper for f_alis_msgerr.ksh routines.")
    parser.add_argument("function", help="Name of the KSH library function to run")
    parser.add_argument("args", nargs="*", help="Function positional arguments")
    
    parsed_args = parser.parse_args()
    func_name = parsed_args.function.lower().replace("_", "")
    func_args = parsed_args.args
    
    # Map KSH function names to python functions
    if func_name == "dwmsgfehlerbehandlung":
        if len(func_args) < 1:
            print("ERROR: dwmsg_fehlerbehandlung requires: eintrags_nr [fehler_nr]", file=sys.stderr)
            sys.exit(1)
        e_nr = func_args[0]
        err_code = int(func_args[1]) if len(func_args) > 1 else 1
        dwmsg_fehlerbehandlung(e_nr, err_code)
        
    elif func_name == "dwmsgsetzestatusok":
        if len(func_args) < 1:
            print("ERROR: dwmsg_setze_status_ok requires: eintrags_nr", file=sys.stderr)
            sys.exit(1)
        dwmsg_setze_status_ok(func_args[0])
        
    elif func_name == "dwmsgsetzestatusabbruch":
        if len(func_args) < 1:
            print("ERROR: dwmsg_setze_status_abbruch requires: eintrags_nr", file=sys.stderr)
            sys.exit(1)
        dwmsg_setze_status_abbruch(func_args[0])
        
    elif func_name == "dwmsgermittlenr":
        if len(func_args) < 1:
            print("ERROR: dwmsg_ermittle_nr requires: var_name", file=sys.stderr)
            sys.exit(1)
        val = dwmsg_ermittle_nr(func_args[0])
        print(val)
        
    elif func_name == "dwmsgerzeugeeintrag":
        if len(func_args) < 4:
            print("ERROR: dwmsg_erzeuge_eintrag requires: eintrags_nr job_kennung programm_name log_datei", file=sys.stderr)
            sys.exit(1)
        dwmsg_erzeuge_eintrag(func_args[0], func_args[1], func_args[2], func_args[3])
        
    elif func_name == "dwmsgmeldefehler":
        if len(func_args) < 3:
            print("ERROR: dwmsg_melde_fehler requires: eintrags_nr typ fehler_nr [zusatz1] [zusatz2]", file=sys.stderr)
            sys.exit(1)
        z1 = func_args[3] if len(func_args) > 3 else ""
        z2 = func_args[4] if len(func_args) > 4 else ""
        dwmsg_melde_fehler(func_args[0], func_args[1], func_args[2], z1, z2)
        
    elif func_name == "dwmsglogdateiname":
        if len(func_args) < 2:
            print("ERROR: dwmsg_logdateiname requires: job_kennung eintrags_nr", file=sys.stderr)
            sys.exit(1)
        val = dwmsg_logdateiname(func_args[0], func_args[1])
        print(val)
        
    elif func_name == "dwmsgsetzezstichtaginfo" or func_name == "dwmsgsetzestichtaginfo":
        if len(func_args) < 3:
            print("ERROR: dwmsg_setze_stichtag_info requires: eintrags_nr stichtag stichtag_fmt", file=sys.stderr)
            sys.exit(1)
        dwmsg_setze_stichtag_info(func_args[0], func_args[1], func_args[2])
        
    elif func_name == "dwmsgappendtiminginfos":
        if len(func_args) < 3:
            print("ERROR: dwmsg_append_timing_infos requires: eintrags_nr info_text date_format", file=sys.stderr)
            sys.exit(1)
        dwmsg_append_timing_infos(func_args[0], func_args[1], func_args[2])
        
    else:
        print(f"ERROR: Unknown function '{parsed_args.function}'", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())