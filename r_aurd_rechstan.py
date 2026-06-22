# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

import argparse
import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def usage():
    """
    Prints the program description and usage.
    """
    logger.info(f"Programm: Erzeugung eines Abzugs der Rechnungsdaten")
    logger.info(f"Version:  V2.0.5")
    logger.info(f"Aufruf:   python {sys.argv[0]} [Parameter]")
    logger.info(f"Parameter:")
    logger.info(f"\t-h     zeigt diese Seite an")
    logger.info(f"\t-s     Stichtag DDMMYYYY")
    logger.info(f"\t-l     Wiederanlaufwert")
    logger.info(f"\n    Beschreibung:")
    logger.info(f"        Dieser Job erzeugt einen Stichtags-Abzug der Vertrags-Cache")
    logger.info(f"        im DWH und stellt sie Forderungsscoring zur Verfuegung.")
    logger.info(f"        Zu beachten ist hierbei, dass eine bereits bereitgestellte")
    logger.info(f"        Tabelle dann geloescht wird, wenn keine aktive Vertragscache")
    logger.info(f"        existiert, die noch nicht abgeholt worden ist.")
    logger.info(f"        Eine solche Abholung muss vom FOS-Loader entsprechend markiert")
    logger.info(f"        worden sein.")
    logger.info(f"        Es werden jeweils Records selektiert, fuer die")
    logger.info(f"               Gueltig_von <= Stichtag < Gueltig_bis AND")
    logger.info(f"               LADEDATUM   < Stichtag")
    logger.info(f"        gilt.")
    logger.info(f"        Falls der Stichtag nicht gesetzt wird, dann wird das")
    logger.info(f"        aktuelle Systemdatum herangezogen.")


def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(add_help=False) # We'll handle help manually
    parser.add_argument('-h', action='store_true', help='Show this help message and exit')
    parser.add_argument('-s', dest='stichtag', type=str,
                        help='Reference date in DDMMYYYY format')
    parser.add_argument('-l', dest='wiederanlaufwert', type=int, default=0,
                        help='Restart value')

    args = parser.parse_args()

    if args.h:
        usage()
        sys.exit(0)

    return args

def validate_date(date_str, format_str="%d%m%Y"):
    """
    Validates if a string is a valid date in the given format.
    """
    try:
        datetime.datetime.strptime(date_str, format_str)
        return True
    except ValueError:
        return False

def run_core_job(job_kennung: str, stichtag: str, wiederanlaufwert: int):
    """
    Placeholder for the actual data processing logic.
    This function will eventually trigger BigQuery SQL or stored procedures.
    """
    logger.info(f"Executing core job '{job_kennung}' with parameters:")
    logger.info(f"  Stichtag: {stichtag}")
    logger.info(f"  Wiederanlaufwert: {wiederanlaufwert}")
    logger.info("  *** Placeholder: Actual BigQuery ETL logic will be integrated here ***")
    # In a real scenario, this would call a BigQuery operator or client
    # For example:
    # from google.cloud import bigquery
    # client = bigquery.Client()
    # query = f"CALL your_bigquery_stored_procedure('{stichtag}', {wiederanlaufwert})"
    # client.query(query).result()
    pass # Placeholder for actual implementation

def main():
    args = parse_arguments()

    # Default Wiederanlaufwert if not provided (already handled by argparse default)
    p_wiederanlaufwert = args.wiederanlaufwert

    # Determine Stichtag
    if args.stichtag:
        if not validate_date(args.stichtag):
            logger.error(f"ERROR: Invalid Stichtag format: {args.stichtag}. Expected DDMMYYYY.")
            usage()
            sys.exit(193) # Mimic legacy error code for invalid argument
        p_stichtag = args.stichtag
    else:
        # Default to current system date in DDMMYYYY format
        p_stichtag = datetime.datetime.now().strftime("%d%m%Y")
        logger.info(f"Stichtag not provided, defaulting to current system date: {p_stichtag}")

    # JobKennung (from legacy script)
    job_kennung = "BERT_RKOPF_STAN" # This would ideally come from environment or config

    logger.info("----------------- Job -----------------------")
    logger.info(f" JobKennung: '{job_kennung}'")
    logger.info(f" Stichtag  : '{p_stichtag}'")
    logger.info(f" Wiederanlaufwert: '{p_wiederanlaufwert}'")
    logger.info("---------------------------------------------")

    try:
        run_core_job(job_kennung, p_stichtag, p_wiederanlaufwert)
        logger.info("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1) # Generic error code

if __name__ == "__main__":
    main()