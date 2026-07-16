"""Module containing temporal parameter resolution functions."""

from datetime import datetime, timedelta
from typing import Any, Dict, Optional


def resolve_stichtag(
    dag_run_conf: Optional[Dict[str, Any]] = None
) -> str:
    """Calculates extraction target execution date parameter ('stichtag').

    Checks manual DAG trigger configurations, falling back to T-1 when empty.
    """
    if dag_run_conf and "stichtag" in dag_run_conf:
        return str(dag_run_conf["stichtag"])
    
    # Default to T-1 execution logic when triggered on a schedule
    yesterday = datetime.utcnow() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")