# This file replaces the legacy KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

# Horizon Python script for BigQuery environment

from datetime import datetime, timedelta

# -----------------------------
# Variables declared at beginning
# -----------------------------
prog_name = "Bereitstellung Basisprodukte BERT"
prog_version = "V2.0.0"

p_stichtag = None
p_wiederanlaufwert = None
v_sysdate = None
v_ladedatum = None

dw_eintragsnr = 0
job_kennung = "AUSD_BP_TA_BPR_EVN"
log_datei = None
name_kernskript = "k_ausd_bp_ta_bpr_evn"

err_nr = 0
err_arg = ""

# -----------------------------
# Helper functions
# -----------------------------
def usage():
    print(f"Programm: {prog_name}")
    print(f"Version:  {prog_version}")
    print("Aufruf:   Parameter")
    print("Parameter:")
    print("    -h     zeigt diese Seite an")
    print("    -s     Stichtag DDMMYYYY")
    print("    -l     Wiederanlaufwert")
    print("           wird dieser Wert gesetzt, so werden nur Vertraege zu")
    print("           DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle")
    print("           geschrieben (die Eintraege bzgl. Werten >= diesem")
    print("           Wert werden geloescht)")
    print("")
    print("Beschreibung:")
    print("    Dieser Job erzeugt einen Stichtags-Abzug der Vertrags-Cache")
    print("    im DWH und stellt sie Forderungsscoring zur Verfuegung.")
    print("    Zu beachten ist hierbei, dass eine bereits bereitgestellte")
    print("    Tabelle dann geloescht wird, wenn keine aktive Vertragscache")
    print("    existiert, die noch nicht abgeholt worden ist.")
    print("    Eine solche Abholung muss vom FOS-Loader entsprechend markiert")
    print("    worden sein.")
    print("    Es werden jeweils Records selektiert, fuer die")
    print("           Gueltig_von <= Stichtag < Gueltig_bis AND")
    print("           LADEDATUM   < Stichtag")
    print("    gilt.")
    print("    Falls der Stichtag nicht gesetzt wird, dann wird das")
    print("    MINIMUM aus aktuellem Systemdatum und maximalem Ladedatum")
    print("    (Quelltabelle)")
    print("    herangezogen.")


def get_system_date_ddmmyyyy():
    return datetime.utcnow().strftime("%d%m%Y")


def validate_required_parameter(param_name, param_value):
    if param_value is None or str(param_value).strip() == "":
        raise ValueError(f"Notwendiger Parameter fehlt: {param_name}")


def parse_args(argv):
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", action="store_true")
    parser.add_argument("-s", dest="stichtag")
    parser.add_argument("-l", dest="wiederanlaufwert")
    args, unknown = parser.parse_known_args(argv)

    if unknown:
        raise ValueError(f"Unbekannter Parameter: {unknown[0]}")

    return args


def run_kernel_script(job_kennung_value, stichtag_value, eintragsnr_value, wiederanlaufwert_value):
    # Placeholder for the downstream kernel script execution in BigQuery-oriented orchestration.
    # In BigQuery environments, this would typically be replaced by a stored procedure call,
    # scheduled query, or orchestration step.
    print(
        f"Executing {name_kernskript} with "
        f"-j {job_kennung_value} -s {stichtag_value} -f {eintragsnr_value} -l {wiederanlaufwert_value}"
    )


# -----------------------------
# Main logic
# -----------------------------
def main():
    global p_stichtag, p_wiederanlaufwert, v_sysdate, dw_eintragsnr, log_datei

    import sys

    try:
        args = parse_args(sys.argv[1:])

        if args.h:
            usage()
            return 0

        p_stichtag = args.stichtag
        p_wiederanlaufwert = args.wiederanlaufwert

        if p_wiederanlaufwert is None or str(p_wiederanlaufwert).strip() == "":
            p_wiederanlaufwert = 0

        v_sysdate = get_system_date_ddmmyyyy()

        if p_stichtag is None or str(p_stichtag).strip() == "":
            p_stichtag = v_sysdate

        validate_required_parameter("Stichtag", p_stichtag)

        dw_eintragsnr = 1
        log_datei = f"{job_kennung}_{dw_eintragsnr}.log"

        print(" ----------------- Job -----------------------")
        print(f" Job-Nr    : '{dw_eintragsnr}'")
        print(f" JobKennung: '{job_kennung}'")
        print(f" Logdatei  : '{log_datei}'")
        print(f" Stichtag  : '{p_stichtag}'")
        print(" ---------------------------------------------")

        run_kernel_script(job_kennung, p_stichtag, dw_eintragsnr, p_wiederanlaufwert)

        print("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
        return 0

    except Exception as exc:
        print(f"AppError: Abbruch - {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())