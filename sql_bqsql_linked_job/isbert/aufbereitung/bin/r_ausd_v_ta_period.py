#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import datetime

# Step 1: Environment initialization and imports
# Sourced scheduler-set variables
DWH_JOB_KENNUNG = os.environ.get("DWH_JOB_KENNUNG", "AUSD_V_TA_PERIOD")

# Fail loudly if required environment variables are missing
BERT_DIR_ROOT = os.environ.get("BERT_DIR_ROOT")
if not BERT_DIR_ROOT:
    raise SystemExit("BERT_DIR_ROOT must be set by the calling environment")


def main():
    # Step 2: Define Program Information
    PROG_NAME = "Vertragsdatenabgleich"
    PROG_VERSION = "V1.0.0"

    # Step 3: Parse command line parameters using argparse
    parser = argparse.ArgumentParser(
        description=f"Programm: {PROG_NAME}\nVersion:  {PROG_VERSION}\n\nRahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.",
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=True
    )
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    parser.add_argument("-s", required=False, help="Unused parameter -s (maintained for compatibility)")
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    parser.add_argument("-l", required=False, help="Unused parameter -l (maintained for compatibility)")

    try:
        args = parser.parse_args()
    except Exception as err:
        # Legacy error behavior: exit with 192 for general parsing/unknown option issues
        print(f"Error parsing arguments: {err}", file=sys.stderr)
        sys.exit(192)

    # Step 4: Define core variables
    Name_Kernskript = os.path.join(BERT_DIR_ROOT, "aufbereitung/bin/k_ausd_v_ta_period.py")
    JobKennung = "BERT_V_TA_PERIOD"
    v_sysdate = datetime.datetime.now().strftime("%d%m%Y")

    # Step 5: Initialize BERT Logging and Job metadata
    # We establish representations matching legacy wrapper execution.
    DW_EintragsNr = "MOCK_ENTRY_01"  # In practice, this would be resolved via the legacy framework
    LogDatei = os.environ.get("LogDatei", f"/tmp/{JobKennung}_{DW_EintragsNr}.log")

    # Setup standard print headers
    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{DW_EintragsNr}'")
    print(f" JobKennung: '{JobKennung}'")
    print(f" Logdatei  : '{LogDatei}'")
    print(" ---------------------------------------------")

    # Step 6: Define process traps and execute core script
    try:
        # Ensure log directory exists
        log_dir = os.path.dirname(LogDatei)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        with open(LogDatei, "a") as log_file:
            # Write the initial registration and header info into the log
            log_file.write(f"Job registered: {JobKennung} (Entry ID: {DW_EintragsNr})\n")
            log_file.write(" ----------------- Job -----------------------\n")
            log_file.write(f" Job-Nr    : '{DW_EintragsNr}'\n")
            log_file.write(f" JobKennung: '{JobKennung}'\n")
            log_file.write(f" Logdatei  : '{LogDatei}'\n")
            log_file.write(" ---------------------------------------------\n")
            log_file.flush()

            # Execute the core Python script synchronously using the current python executable
            subprocess.run(
                [sys.executable, Name_Kernskript, "-j", JobKennung, "-f", str(DW_EintragsNr)],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=True
            )

    except subprocess.CalledProcessError as err:
        # Emulates the ERR trap
        err_msg = "AppError: Abbruch"
        print(err_msg, file=sys.stderr)
        try:
            with open(LogDatei, "a") as log_file:
                log_file.write(f"[ERROR] Subprocess failed with exit code {err.returncode}\n")
                log_file.write(f"{err_msg}\n")
        except Exception:
            pass
        sys.exit(err.returncode)

    except KeyboardInterrupt:
        # Emulates the INT trap
        err_msg = "OSError: Abbruch"
        print(err_msg, file=sys.stderr)
        try:
            with open(LogDatei, "a") as log_file:
                log_file.write(f"{err_msg}\n")
        except Exception:
            pass
        sys.exit(1)

    # Step 7: Successful termination
    success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
    print(success_msg)
    try:
        with open(LogDatei, "a") as log_file:
            log_file.write(success_msg + "\n")
    except Exception:
        pass

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())