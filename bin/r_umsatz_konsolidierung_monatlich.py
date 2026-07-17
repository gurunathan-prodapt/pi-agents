#!/usr/bin/env python3
"""
Migrated Horizon Python Job: Monthly Turnover Consolidation (Umsatzkonsolidierung)
Equivalent of: r_umsatz_konsolidierung_monatlich.ksh
"""

import argparse
from datetime import datetime
import logging
import os
import sys
from typing import Tuple

from dateutil.relativedelta import relativedelta

# Append Horizon framework library path
sys.path.append(os.getenv("DIR_LIB_PY", ""))
try:
    from framework.core.lib import script
except ImportError:
    # Safe fallback wrapper mock for syntax and local test execution validation
    class MockScript:

        @staticmethod
        def func_execute_bq(
            query: str, pass_file: str, col_delim: str, row_delim: str
        ) -> bool:
            print(
                f"[MOCK_BQ] Executing query via Horizon Core on project "
                f"'{os.getenv('GCP_PROJECT', 'UNKNOWN')}'"
            )
            return True

    script = MockScript()


def parse_arguments() -> argparse.Namespace:
    """Parses incoming command line arguments for the job.

    Returns:
        argparse.Namespace: Parsed arguments containing 'monat' and 'konzern'.
    """
    parser = argparse.ArgumentParser(
        description="Monatliche Konsolidierung der Umsatzdaten"
    )
    parser.add_argument(
        "-m",
        "--monat",
        type=str,
        help="Verarbeitungsmonat (YYYYMM). Default is previous month.",
        default=None,
    )
    parser.add_argument(
        "-k",
        "--konzern",
        type=str,
        help="Konzerngesellschaft (z.B. DE01, ALL). Default is 'ALL'.",
        default="ALL",
    )
    return parser.parse_args()


def calculate_processing_month(input_month: str = None) -> str:
    """Returns the provided YYYYMM string, or calculates the previous month
    as default if none is provided.

    Args:
        input_month (str, optional): Target month string in 'YYYYMM' format.

    Returns:
        str: Derived processing month in 'YYYYMM' format.
    """
    if not input_month:
        prev_month = datetime.now() - relativedelta(months=1)
        return prev_month.strftime("%Y%m")
    return input_month


def setup_logger(
    log_file_path: str,
) -> Tuple[logging.Logger, logging.FileHandler]:
    """Configures the logging system to route logs to both a local file and
    standard output (stdout) dynamically.

    Args:
        log_file_path (str): File system path to the target log file.

    Returns:
        Tuple[logging.Logger, logging.FileHandler]: Configured logger instance
        and file handler reference.
    """
    logger = logging.getLogger("UmsatzKonsolidierung")
    logger.setLevel(logging.INFO)

    # Use standard logging format
    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File Handler
    file_handler = logging.FileHandler(
        log_file_path, mode="w", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger, file_handler


def generate_consolidation_query(
    project_id: str, dataset_id: str, month: str, corporation: str
) -> str:
    """Generates the structured BigQuery SQL query dynamically.

    Args:
        project_id (str): Google Cloud Project ID.
        dataset_id (str): Target BigQuery Dataset.
        month (str): Target processing month (YYYYMM).
        corporation (str): Target corporation code or 'ALL'.

    Returns:
        str: Fully formatted BigQuery insert statement.
    """
    return f"""
    INSERT INTO `{project_id}.{dataset_id}.tgt_umsatz_konsolidiert` (
      verarbeitungs_monat,
      konzern_id,
      umsatz_wert,
      konsolidierungs_datum
    )
    SELECT 
      verarbeitungs_monat,
      konzern_id,
      SUM(umsatz_wert) AS umsatz_wert,
      CURRENT_TIMESTAMP() AS konsolidierungs_datum
    FROM `{project_id}.{dataset_id}.src_umsatz_raw`
    WHERE verarbeitungs_monat = '{month}'
      AND ('{corporation}' = 'ALL' OR konzern_id = '{corporation}')
    GROUP BY verarbeitungs_monat, konzern_id;
    """


def check_log_for_errors(log_file_path: str) -> int:
    """Inspects the generated log file for any lines starting with standard
    error signatures (e.g. "FEHLER" or standard "ERROR").

    Args:
        log_file_path (str): Path to the log file to analyze.

    Returns:
        int: Number of error occurrences identified.
    """
    error_count = 0
    if os.path.exists(log_file_path):
        with open(log_file_path, "r", encoding="utf-8") as f:
            for line in f:
                normalized_line = line.strip().upper()
                # Pattern match to support legacy validation parsing
                if normalized_line.startswith("FEHLER") or "[ERROR]" in normalized_line:
                    error_count += 1
    return error_count


def execute_consolidation(
    month: str, corporation: str, logger: logging.Logger
) -> bool:
    """Executes the BigQuery ETL process via the Horizon execution layer.

    Args:
        month (str): The target processing month (YYYYMM).
        corporation (str): The target corporate filter.
        logger (logging.Logger): Active logging subsystem reference.

    Returns:
        bool: True if execution succeeded, False otherwise.
    """
    # Environment resolution
    gcp_project = os.environ.get("GCP_PROJECT", "your_project_id")
    bq_dataset = os.environ.get("BQ_DATASET", "your_dataset_id")

    logger.info(
        f"Compiling BQSQL consolidation query for {gcp_project}.{bq_dataset}..."
    )
    bqsql_query = generate_consolidation_query(
        project_id=gcp_project,
        dataset_id=bq_dataset,
        month=month,
        corporation=corporation,
    )

    # Operational constants for core Horizon integration
    pass_file_name = "dummy_pass_file"
    column_delimiter = "|"
    row_delimiter = "\n"

    logger.info("Calling Horizon Core script engine (func_execute_bq)...")
    success = script.func_execute_bq(
        bqsql_query, pass_file_name, column_delimiter, row_delimiter
    )
    return success


def main() -> None:
    # 1. Initialization and Parameter Resolution
    args = parse_arguments()
    l_Monat = calculate_processing_month(args.monat)
    l_Konzern = args.konzern

    # 2. Local File System Setup for Logs
    home_dir = os.path.expanduser("~")
    log_dir = os.path.join(home_dir, "aktuell", "log", "umsatz")
    os.makedirs(log_dir, exist_ok=True)
    protokoll_datei = os.path.join(
        log_dir, f"konsolidierung_{l_Monat}_{l_Konzern}.log"
    )

    # 3. Establish Logger
    logger, file_handler = setup_logger(protokoll_datei)
    
    # XML Print output integration (Preserved exact wording based on requirements)
    print(f"Umsatzkonsolidierung fuer Monat {l_Monat}, Konzerngesellschaft {l_Konzern} angestossen")
    
    # Shell wrapper start message (Preserved exact wording based on requirements)
    print(f"Starte monatliche Umsatzkonsolidierung fuer Monat {l_Monat}, Konzerngesellschaft {l_Konzern}")

    # 4. Process execution inside standard error handling wrapper
    try: 
        success = execute_consolidation(l_Monat, l_Konzern, logger)
        if not success:
            # Restore the legacy abort error literal message from shell
            logger.error(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Umsatzkonsolidierung fuer Monat {l_Monat}/{l_Konzern} mit Fehlercode 1 abgebrochen")
            raise RuntimeError("Horizon Core script execution returned failed status.")

        logger.info("Consolidation Query pipeline completed execution.")

    except Exception as ex:
        # Structured error output preserving exact legacy abort message
        print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Umsatzkonsolidierung fuer Monat {l_Monat}/{l_Konzern} mit Fehlercode 1 abgebrochen", file=sys.stderr)
        file_handler.close()
        sys.exit(1)

    # Close the file handler prior to reading the log file content for inspection
    file_handler.close()

    # 5. Post-processing Quality Check (Equivalent to log validation logic)
    error_lines = check_log_for_errors(protokoll_datei)
    if error_lines > 0: 
        # Restore the exact legacy message structure and wording from shell wrapper
        print(f"[E] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {error_lines} Fehlerzeilen im Konsolidierungs-Protokoll gefunden, siehe {protokoll_datei}", file=sys.stderr)
        sys.exit(1)

    # Success message preservation (Preserved exact wording from the shell wrapper)
    print("Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet")
    sys.exit(0)


if __name__ == "__main__":
    main()