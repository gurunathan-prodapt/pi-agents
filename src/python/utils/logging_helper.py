"""
Legacy Include: DW.BERT_LESE_LOG
Job: BERT_P_ADRESSEN
Description: Structured cloud logging helper to write runtime logs and audit entries to GCP.
"""

import json
import logging
from typing import Any, Dict, Optional
from google.cloud import logging as gcp_logging

def get_cloud_logger(logger_name: str = "BERT_P_ADRESSEN") -> logging.Logger:
    """
    Initializes a Python logger and attaches Google Cloud Logging handlers.
    """
    try:
        client = gcp_logging.Client()
        client.setup_logging()
    except Exception:
        # Fallback to local default log config if GCP credentials are not found
        pass
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    return logger

def log_job_event(
    logger: logging.Logger, 
    job_name: str, 
    event_type: str, 
    status: str, 
    extra_info: Optional[Dict[str, Any]] = None
) -> None:
    """
    Emits structured, machine-readable JSON logs for tracking job steps.
    """
    log_payload = {
        "job_name": job_name,
        "event_type": event_type,
        "status": status,
    }
    if extra_info:
        log_payload.update(extra_info)
        
    log_string = json.dumps(log_payload)
    if status == "FAILED":
        logger.error(log_string)
    else:
        logger.info(log_string)