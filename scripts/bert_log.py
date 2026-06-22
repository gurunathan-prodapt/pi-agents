# Legacy Source: UNRESOLVED:BERT_LOG.KSH
# Job: DW.BERT_ABLAUFSTEUERUNG
# Re-implementation of BERT_LOG.KSH for logging within the Cloud environment.
# This script is a placeholder; its exact functionality needs to be derived
# from the original KSH script. For now, it logs a message.

import argparse
import logging

# Configure logging to output to stdout, which Cloud Logging will capture
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def bert_logger(log_message: str, log_level: str = 'INFO'):
    """Logs a message with a specified level."""
    if log_level.upper() == 'INFO':
        logging.info(log_message)
    elif log_level.upper() == 'WARNING':
        logging.warning(log_message)
    elif log_level.upper() == 'ERROR':
        logging.error(log_message)
    elif log_level.upper() == 'DEBUG':
        logging.debug(log_message)
    else:
        logging.info(f"Unknown log level '{log_level}'. Logging as INFO: {log_message}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="A simple logging script for Bert processes."
    )
    parser.add_argument("--message", required=True, help="The message to log.")
    parser.add_argument("--level", default="INFO", help="The logging level (INFO, WARNING, ERROR, DEBUG).")

    args = parser.parse_args()

    bert_logger(args.message, args.level)