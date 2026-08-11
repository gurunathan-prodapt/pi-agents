#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime
import oracledb

# Ensure mandatory environment variables are set
dw_orauser = os.environ.get("DW_ORAUSER")
if not dw_orauser:
    raise SystemExit("DW_ORAUSER must be set by the calling task / environment")

dw_dir_root = os.environ.get("DW_DIR_ROOT")
if not dw_dir_root:
    raise SystemExit("DW_DIR_ROOT must be set by the calling task / environment")

dw_dir_prot = os.environ.get("DW_DIR_PROT")
if not dw_dir_prot:
    raise SystemExit("DW_DIR_PROT must be set by the calling task / environment")


def fehlerbehandlung(eintrags_nr: int, error_code: int):
    """
    Step 1: DW_Fehlerbehandlung
    Handles errors trapped during execution. Logs the error status and aborts execution tracker.
    """
    k_unerw_fehler = 10
    melde_fehler(
        eintrags_nr=eintrags_nr,
        typ="F",
        fehler_nr=k_unerw_fehler,
        zusatz1=f"ErrorCode ist: {error_code}",
        zusatz2=None
    )
    print("Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus")
    setze_status_abbruch(eintrags_nr)


def setze_status_ok(eintrags_nr: int):
    """
    Step 2: DWMSG_SetzeStatusOK
    Sets the tracking entry status to successful (OK).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben", file=sys.stderr)
        sys.exit(1)

    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    try:
        with oracledb.connect(dsn=dw_orauser) as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusOk(:1); END;", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Database error in SetzeStatusOK: {e}", file=sys.stderr)
        sys.exit(1)


def setze_status_abbruch(eintrags_nr: int):
    """
    Step 3: DWMSG_SetzeStatusAbbruch
    Sets the tracking entry status to aborted (Abbruch).
    """
    if not eintrags_nr:
        print("Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben", file=sys.stderr)
        sys.exit(1)

    try:
        with oracledb.connect(dsn=dw_orauser) as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN BERT_MELDUNG.SetzeStatusAbbruch(:1); END;", [eintrags_nr])
                conn.commit()
    except oracledb.DatabaseError as e:
        print(f