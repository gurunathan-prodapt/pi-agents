"""Module to handle system logging while preserving legacy German formats."""

import logging

logger = logging.getLogger(__name__)


def log_initialization(stichtag: str) -> None:
    """Exact original XML log output."""
    msg = f"Rechnungsexport fuer Stichtag {stichtag} angestossen"
    print(msg)
    logger.info(msg)


def log_start_export(stichtag: str) -> None:
    """Exact original KSH log output."""
    msg = f"Starte Export Rechnungsdaten fuer Stichtag {stichtag}"
    print(msg)
    logger.info(msg)


def log_row_count(row_count: int) -> None:
    """Exact original KSH metrics log output."""
    msg = f"Anzahl exportierter Rechnungssaetze: {row_count}"
    print(msg)
    logger.info(msg)


def log_warning_no_data(stichtag: str) -> None:
    """Exact original KSH Warning log output."""
    msg = f"[W] Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert"
    print(msg)
    logger.warning(msg)


def log_clean_completion() -> None:
    """Exact original KSH completion log output."""
    msg = "Export Rechnungsdaten ohne erkennbare Fehler beendet"
    print(msg)
    logger.info(msg)