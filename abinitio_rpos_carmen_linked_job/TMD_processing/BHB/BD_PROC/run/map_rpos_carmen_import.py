#!/usr/bin/env python3
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_param(param_name, value):
    if not value:
        # VERBATIM PRINT: German/legacy standard system output
        print(f"Error evaluating: 'parameter {param_name} of map_rpos_carmen_import', interpretation 'shell'")
        sys.exit(1)

def main():
    logging.info("Starting wrapper validation for map_rpos_carmen_import")

    # Sourcing environments
    DB_TNS_NAME_DWH = os.environ.get("DB_TNS_NAME_DWH")
    DB_USER_DWH = os.environ.get("DB_USER_DWH")
    DB_PASSWD_DWH = os.environ.get("DB_PASSWD_DWH")
    BHB_Quellverzeichnis = os.environ.get("BHB_Quellverzeichnis")
    BHB_Dateiname = os.environ.get("BHB_Dateiname")
    BHB_Eintragsnr = os.environ.get("BHB_Eintragsnr")

    # Validate and assert parameters using legacy standard system format on failure
    validate_param("DB_TNS_NAME_DWH", DB_TNS_NAME_DWH)
    validate_param("DB_USER_DWH", DB_USER_DWH)
    validate_param("DB_PASSWD_DWH", DB_PASSWD_DWH)
    validate_param("BHB_Quellverzeichnis", BHB_Quellverzeichnis)
    validate_param("BHB_Dateiname", BHB_Dateiname)
    validate_param("BHB_Eintragsnr", BHB_Eintragsnr)

    # Validate Google Cloud platform variables
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    BQ_DATASET = os.environ.get("BQ_DATASET")

    validate_param("GCP_PROJECT", GCP_PROJECT)
    validate_param("GCS_BUCKET", GCS_BUCKET)
    validate_param("BQ_DATASET", BQ_DATASET)

    logging.info("Wrapper environment and parameter validation completed successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()