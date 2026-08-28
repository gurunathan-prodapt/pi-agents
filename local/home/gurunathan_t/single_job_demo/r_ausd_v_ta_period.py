#!/usr/bin/env python3
import os
import sys
import datetime
import argparse
import logging

# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables/functions it sets are unknown

# REVIEW-STRUCT: legacy Oracle status-logging package DWMSG replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying

def main():
    # Step 2: Initialize global tracking variables
    err_nr = 0
    err_arg = ""
    dw_eintrags_nr = "0"
    job_kennung = "BERT_V_TA_PERIOD"
    v_sysdate = datetime.datetime.now().strftime("%d%m%Y")

    # Step 3: Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period."
    )
    
    # REVIEW: parameter -s is declared in getopts but not explicitly handled; confirm actual usage.
    parser.add_argument(
        "-s", 
        dest="param_s", 
        help="Parameter S"
    )  # NOTE: parameter accepted for interface compatibility; unused in original script logic

    # REVIEW: parameter -l is declared in getopts but not explicitly handled; confirm actual usage.
    parser.add_argument(
        "-l", 
        dest="param_l", 
        help="Parameter L"
    )  # NOTE: parameter accepted for interface compatibility; unused in original script logic

    try:
        args, unknown = parser.parse_known_args()
        if unknown:
            err_nr = 192
            err_arg = str(unknown)
            raise ValueError(f"Unknown parameters: {unknown}")
    except Exception as e:
        err_nr = 192 if err_nr == 0 else err_nr
        err_arg = str(e)
        logging.error(f"DWMSG_MeldeFehler: 0 E {err_nr} {err_arg}")
        parser.print_help()
        return err_nr

    # Step 4: Metadata and tracking setup via legacy framework
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    bert_dir_root = os.environ.get("BERT_DIR_ROOT")
    if not bert_dir_root:
        raise SystemExit("BERT_DIR_ROOT environment variable is not set")
    
    dw_dir_utl = os.environ.get("DW_DIR_UTL")
    if not dw_dir_utl:
        raise SystemExit("DW_DIR_UTL environment variable is not set")

    dw_eintrags_nr = os.environ.get("DW_EINTRAGS_NR", "12345")
    log_datei = os.path.join(dw_dir_utl, f"{job_kennung}_{dw_eintrags_nr}.log")
    
    # Ensure directory for log file exists
    os.makedirs(os.path.dirname(log_datei), exist_ok=True)
    
    file_handler = logging.FileHandler(log_datei)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)

    logging.info(f"DWMSG_ErzeugeEintrag: {dw_eintrags_nr} {job_kennung} {sys.argv[0]} {log_datei}")
    logging.info(f"DWMSG_SetzeStichtagInfo: {dw_eintrags_nr} {v_sysdate} 'DDMMYYYY'")

    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{dw_eintrags_nr}'")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")

    # Step 5: Define execution paths and variables
    p_job_kennung = job_kennung
    p_eintrags_nr = dw_eintrags_nr
    v_tab_name = 'ta_period'

    # Step 6: Parameter Validation Guard Block
    err_nr = 0
    err_arg = ""

    if not p_job_kennung:
        err_nr = 1
        err_arg = "p_job_kennung is not set"
    elif not p_eintrags_nr:
        err_nr = 1
        err_arg = "p_eintrags_nr is not set"

    if err_nr != 0:
        logging.error(f"DWMSG_MeldeFehler 0 E {err_nr} {err_arg}")
        print(f"FEHLER: 0 E {err_nr} {err_arg}", file=sys.stderr)
        print("Bitte ueber Rahmenscript aufrufen", file=sys.stderr)
        return err_nr

    # Step 7: Define script and temporary record count target paths
    name_sql_skript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_v_ta_period.sql")
    pid = os.getpid()
    tmp_file = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_period_{pid}.tmp")

    # Step 8: DB Execution & Error Trapping block
    try:
        from google.cloud import bigquery
        gcp_project = os.environ.get("GCP_PROJECT")
        bq_dataset = os.environ.get("BQ_DATASET")
        carmen_stage_dataset = os.environ.get("CARMEN_STAGE_DATASET")

        client = bigquery.Client(project=gcp_project)
        
        logging.info(f"Reading SQL script from: {name_sql_skript}")
        if not os.path.exists(name_sql_skript):
            raise FileNotFoundError(f"SQL file not found at {name_sql_skript}")

        with open(name_sql_skript, "r", encoding="utf-8") as sql_file:
            query_text = sql_file.read()

        logging.info("Executing BigQuery job...")
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("GCP_PROJECT", "STRING", gcp_project),
                bigquery.ScalarQueryParameter("BQ_DATASET", "STRING", bq_dataset),
                bigquery.ScalarQueryParameter("CARMEN_STAGE_DATASET", "STRING", carmen_stage_dataset)
            ]
        )

        query_job = client.query(query_text, job_config=job_config)
        results = query_job.result()  # Wait for query to complete

        num_rows = query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else results.total_rows
        logging.info(f"Query executed successfully. Affected/Total rows: {num_rows}")
        
        with open(tmp_file, "w", encoding="utf-8") as f:
            f.write(str(num_rows))

        print(" ---------- ENDE Datenverarbeitung ----------")

        # Step 9: Parse records processed
        if os.path.exists(tmp_file):
            with open(tmp_file, "r", encoding="utf-8") as f:
                v_records = f.read().strip()
            logging.info(f"v_records={v_records}")
        else:
            v_records = "0"
            logging.warning(f"Temporary file {tmp_file} not found. Defaulting records count to 0.")

        # Step 10: Register success and cleanup
        success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet"
        print(success_msg)
        logging.info(success_msg)
        
        logging.info(f"DWMSG_SetzeStatusOK: {dw_eintrags_nr}")

        if os.path.exists(tmp_file):
            os.remove(tmp_file)

        return 0

    except KeyboardInterrupt:
        err_msg = "OSError: Abbruch"
        print(err_msg, file=sys.stderr)
        logging.error(err_msg)
        logging.error(f"DWMSG_Fehlerbehandlung: {dw_eintrags_nr}")

        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

        return 1

    except Exception as e:
        err_msg = "AppError: Abbruch"
        print(err_msg, file=sys.stderr)
        logging.error(f"{err_msg} - {str(e)}")
        logging.error(f"DWMSG_Fehlerbehandlung: {dw_eintrags_nr}")

        if os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

        return 1

if __name__ == "__main__":
    sys.exit(main())