"""
Module for resolving system-specific and global environmental path hierarchies.
Replaces the legacy UC4 JOBI: DW.HOLE_PFAD_VTRG.
"""

from typing import Dict
from airflow.models import Variable


def get_vtrg_paths() -> Dict[str, str]:
    """Resolves and retrieves environment home directory paths.

    Attempts to pull configurations from Airflow Variables. Fallbacks to default
    legacy system directory structures if the configuration keys are absent.

    Returns:
        Dict[str, str]: A dictionary containing resolved paths for:
            - 'dwh_home': Primary Data Warehouse operation path.
            - 'home': Base execution home path.
            - 'pms_home': Portfolio Management System home path.
    """
    return Variable.get(
        "dw_variablen_paths",
        default_var={
            "dwh_home": "/opt/dwh",
            "home": "/home/dwarf",
            "pms_home": "/opt/pms",
        },
        deserialize_json=True,
    )