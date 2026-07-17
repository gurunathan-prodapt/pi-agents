"""
Runner module implementing logging, validation, and verification routines
for the daily invoice export job. Preserves all legacy UC4 and KSH German
log output exactly as requested in the design specifications.
"""

import logging
from typing import Any, Dict
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def log_header(**context: Dict[str, Any]) -> None:
    """
    Logs UC4 and Shell script initialization headers verbatim.
    
    Args:
        **context: Airflow task execution context containing 'templates_dict'.
    """
    templates_dict = context.get('templates_dict') or {}
    stichtag = templates_dict.get('stichtag')
    
    if not stichtag:
        raise ValueError("Missing required template parameter: 'stichtag'")

    # PRESERVED VERBATIM UC4 LOGGING
    print(f"Rechnungsexport fuer Stichtag {stichtag}")
    # PRESERVED VERBATIM KSH LOGGING
    print(f"Starte Export Rechnungsdaten fuer {stichtag}...")


def validate_and_log_results(**context: Dict[str, Any]) -> None:
    """
    Downloads the exported CSV from Google Cloud Storage, counts the rows,
    and prints success or failure logs verbatim in German.
    
    Args:
        **context: Airflow task execution context containing 'templates_dict'.
    """
    templates_dict = context.get('templates_dict') or {}
    stichtag = templates_dict.get('stichtag')
    bucket = templates_dict.get('gcs_bucket')
    
    if not stichtag or not bucket:
        raise ValueError(
            f"Missing required context parameters. stichtag: {stichtag}, bucket: {bucket}"
        )

    object_name = f"rechnung_export/daily/rechnung_export_{stichtag}.csv"
    gcs_hook = GCSHook()
    
    try:
        logging.info(f"Downloading {object_name} from bucket {bucket} for validation...")
        file_bytes = gcs_hook.download(bucket_name=bucket, object_name=object_name)
        file_content = file_bytes.decode('utf-8')
        
        # Split lines and filter out empty lines
        lines = [line for line in file_content.splitlines() if line.strip()]
        row_count = len(lines)
        
        # Deduct 1 from row count if there is a header row in the CSV
        if row_count > 0:
            # Assumes standard BigQuery export containing a header row
            data_row_count = row_count - 1
        else:
            data_row_count = 0

        if data_row_count == 0:
            # PRESERVED VERBATIM KSH LOGGING
            print("Keine Rechnungsdaten gefunden.")
        else:
            # PRESERVED VERBATIM KSH LOGGING
            print(f"Anzahl exportierter Rechnungsdatensaetze: {data_row_count}")
            print("Export Rechnungsdaten erfolgreich beendet.")
            
    except Exception as e:
        logging.error(f"Error validating exported file {object_name}: {str(e)}")
        raise