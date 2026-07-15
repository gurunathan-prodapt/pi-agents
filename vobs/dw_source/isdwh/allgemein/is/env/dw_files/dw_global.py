#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module Name: dw_global
Description: Dynamic validation engine and global interface mapping.
"""

import os
import sys
from dw_config_helper import GCPConfigHelper

sys.path.append(os.getenv('DIR_LIB_PY', ''))

def setup_global_environment():
    """
    Initializes global path mappings, database session settings,
    and legacy host parameters rewritten for GCP Cloud architecture.
    """
    required_env_vars = [
        "DW_DIR_ROOT",
        "DW_DIR_PROT",
        "DW_DIR_CUBES",
        "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA",
        "DW_DIR_IMP_CTEL",
        "DW_DIR_IMP_VO",
        "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF",
        "DW_DIR_IMP_NNV",
        "ORACLE_HOME"
    ]
    GCPConfigHelper.validate_environment(required_env_vars)

    # Database Session/NLS Settings (Backwards compatibility for remaining Oracle tasks)
    os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['NLS_DATE_FORMAT'] = 'DD.MM.YY'
    os.environ['NLS_DATE_LANGUAGE'] = 'GERMAN_GERMANY.WE8ISO8859P1'

    # Project and Host Environment Metadata Parameters
    os.environ['ETL_Host'] = "dxcsa4.bn.detemobil.de"
    os.environ['ETL_Projekt'] = "BHB"
    os.environ['LANG'] = 'de'

    # Clean legacy Cognos settings
    os.environ['PYA_USR'] = ''

if __name__ == '__main__':
    setup_global_environment()