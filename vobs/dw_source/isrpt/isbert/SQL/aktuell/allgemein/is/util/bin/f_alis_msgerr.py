#!/usr/bin/env python3
import os
import sys
import datetime
import re
import argparse
import oracledb

# Validate environment variables and fail loudly if missing
dw_orauser = os.environ.get("DW_ORAUSER")
if not dw_orauser:
    raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")

dw_dir_root = os.environ.get("DW_DIR_ROOT")
if not dw_dir_root:
    raise SystemExit("DW_DIR_ROOT must be set by the calling Airflow task")

dw_dir_prot = os.environ.get("DW_DIR_PROT")
if not dw_dir_prot:
    raise SystemExit("DW_DIR_PROT must be set by the calling Airflow task")


def get_db_connection():
    """
    Establishes and returns a connection to the Oracle database using DW_ORAUSER.
    Formats supported: user/password@dsn or user/password.
    """
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    match = re.match(r"([^/]+)/([^@]+)(?:@(.+))?", dw_orauser)
    if match:
        user = match.group(1)
        password = match.group(2)
        dsn = match.group(3) or ""
        return oracledb.connect(user=user, password=password, dsn=dsn)
    else:
        return oracledb.connect(dsn=dw_orauser)


def dwmsg_fehlerbehandlung(dwmsg_eintrags_nr, last_exit_code=1):
    """
    Error handling routine called when an error occurs in the calling process.
    Logs the error to the database and sets the execution status to aborted.
    """
    fehler_nr = last_exit_code
    unerw_fehler = 10

    # Melde Fehler in der Meldungstabelle
    dwmsg_melde_fehler(dwmsg_eintrags_nr, "F", unerw_fehler, f"ErrorCode ist: {fehler_nr}")

    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    dwmsg_setze_status_abbruch(dwmsg_eintrags_nr)


def dwmsg_setze_status_ok(dwmsg_eintrags_nr):
    """
    Sets the status of the log entry with the given ID to successful (OK).
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p1.sql not supplied — Converting to native PL/SQL call.
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [dwmsg_eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to set status OK: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_setze_status_abbruch(dwmsg_eintrags_nr):
    """
    Sets the status of the log entry with the given ID to aborted (Abbruch).
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p1.sql not supplied — Converting to native PL/SQL call.
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [dwmsg_eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to set status Abbruch: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_ermittle_nr():
    """
    Retrieves a unique execution/sequence ID from the database.
    """
    # REVIEW: d_al_is_ermittlenr.sql not supplied — assumed BERT_MELDUNG.ErmittleNr exists to return sequence number. Confirm correct schema object.
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                out_val = cur.var(oracledb.NUMBER)
                cur.execute("""
                    BEGIN
                        :1 := BERT_MELDUNG.ErmittleNr;
                    END;
                """, [out_val])
                return str(int(out_val.getvalue())).strip()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to retrieve entry sequence number: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_erzeuge_eintrag(dwmsg_eintrags_nr, job_kennung, programmname, logdatei):
    """
    Creates a new entry in the execution tracking and log table.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # REVIEW-STRUCT: SQL wrapper script d_alis_spaufruf_p4.sql not supplied — Converting to native PL/SQL call.
                cur.execute("""
                    BEGIN
                        BERT_MELDUNG.Erzeuge_Eintrag(:1, :2, :3, :4);
                    END;
                """, [dwmsg_eintrags_nr, job_kennung, programmname, logdatei])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to create log entry: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_melde_fehler(dwmsg_eintrags_nr, typ, fehler_nr, zusatz1="", zusatz2=""):
    """
    Logs an error/warning message in the execution tracking table.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    BEGIN
                        BERT_MELDUNG.Fehler(:1, :2, :3, :4, :5);
                    END;
                """, [typ, dwmsg_eintrags_nr, fehler_nr, zusatz1, zusatz2])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to report error: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_logdateiname(job_kennung, dwmsg_eintrags_nr):
    """
    Generates a log file path based on job identifier, timestamp, and sequence number.
    """
    date_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    dateiname = f"{dw_dir_prot}/{job_kennung}_{date_str}_{dwmsg_eintrags_nr}.log"
    return dateiname


def dwmsg_setze_stichtag_info(dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt):
    """
    Appends execution key-date information to the log entry record.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag:
        print("Argh!, keinen Stichtag angegeben!", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_stichtag_fmt:
        print("Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!", file=sys.stderr)
        sys.exit(2)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Executing exact embedded SQL statement
                cur.execute("""
                    BEGIN
                        BERT_MELDUNG.SetzeZusatzInfos(:1, to_date(:2, :3));
                    END;
                """, [dwmsg_eintrags_nr, dwmsg_stichtag, dwmsg_stichtag_fmt])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to set key-date info: {e}", file=sys.stderr)
        sys.exit(1)


def dwmsg_append_timing_infos(dwmsg_eintrags_nr, dwmsg_infotext, dwmsg_date_format):
    """
    Appends timing checkpoints or stage descriptions directly to the log entry record.
    """
    if not dwmsg_eintrags_nr:
        print("Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben", file=sys.stderr)
        sys.exit(1)
    if not dwmsg_date_format:
        print("Argh!, Formatangabe erforderlich!", file=sys.stderr)
        sys.exit(2)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Executing exact embedded SQL statement using local bind parameters
                cur.execute("""
                    BEGIN
                        BERT_MELDUNG.SetzeZusatzInfos(:1, null, :2||' '||to_char(SYSDATE,:3)||' ');
                    END;
                """, [dwmsg_eintrags_nr, dwmsg_infotext, dwmsg_date_format])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"ERROR: Failed to append timing info: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Python port of f_alis_msgerr.ksh utility library")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Subparser for Fehlerbehandlung
    parser_feh = subparsers.add_parser("Fehlerbehandlung")
    parser_feh.add_argument("eintrags_nr")
    parser_feh.add_argument("--exit_code", type=int, default=1)

    # Subparser for SetzeStatusOK
    parser_ok = subparsers.add_parser("SetzeStatusOK")
    parser_ok.add_argument("eintrags_nr")

    # Subparser for SetzeStatusAbbruch
    parser_abr = subparsers.add_parser("SetzeStatusAbbruch")
    parser_abr.add_argument("eintrags_nr")

    # Subparser for ErmittleNr
    subparsers.add_parser("ErmittleNr")

    # Subparser for ErzeugeEintrag
    parser_erz = subparsers.add_parser("ErzeugeEintrag")
    parser_erz.add_argument("eintrags_nr")
    parser_erz.add_argument("job_kennung")
    parser_erz.add_argument("programmname")
    parser_erz.add_argument("log_datei")

    # Subparser for MeldeFehler
    parser_mel = subparsers.add_parser("MeldeFehler")
    parser_mel.add_argument("eintrags_nr")
    parser_mel.add_argument("typ")
    parser_mel.add_argument("fehler_nr")
    parser_mel.add_argument("zusatz1", nargs="?", default="")
    parser_mel.add_argument("zusatz2", nargs="?", default="")

    # Subparser for Logdateiname
    parser_log = subparsers.add_parser("Logdateiname")
    parser_log.add_argument("job_kennung")
    parser_log.add_argument("eintrags_nr")

    # Subparser for SetzeStichtagInfo
    parser_sti = subparsers.add_parser("SetzeStichtagInfo")
    parser_sti.add_argument("eintrags_nr")
    parser_sti.add_argument("stichtag")
    parser_sti.add_argument("stichtag_fmt")

    # Subparser for AppendTimingInfos
    parser_tim = subparsers.add_parser("AppendTimingInfos")
    parser_tim.add_argument("eintrags_nr")
    parser_tim.add_argument("infotext")
    parser_tim.add_argument("date_format")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "Fehlerbehandlung":
        dwmsg_fehlerbehandlung(args.eintrags_nr, args.exit_code)
    elif args.command == "SetzeStatusOK":
        dwmsg_setze_status_ok(args.eintrags_nr)
    elif args.command == "SetzeStatusAbbruch":
        dwmsg_setze_status_abbruch(args.eintrags_nr)
    elif args.command == "ErmittleNr":
        nr = dwmsg_ermittle_nr()
        print(nr)
    elif args.command == "ErzeugeEintrag":
        dwmsg_erzeuge_eintrag(args.eintrags_nr, args.job_kennung, args.programmname, args.log_datei)
    elif args.command == "MeldeFehler":
        dwmsg_melde_fehler(args.eintrags_nr, args.typ, args.fehler_nr, args.zusatz1, args.zusatz2)
    elif args.command == "Logdateiname":
        path = dwmsg_logdateiname(args.job_kennung, args.eintrags_nr)
        print(path)
    elif args.command == "SetzeStichtagInfo":
        dwmsg_setze_stichtag_info(args.eintrags_nr, args.stichtag, args.stichtag_fmt)
    elif args.command == "AppendTimingInfos":
        dwmsg_append_timing_infos(args.eintrags_nr, args.infotext, args.date_format)

    return 0


if __name__ == "__main__":
    sys.exit(main())