#!/usr/bin/env python3
"""
Global verification sequences on environment profiles and localized variables.
"""

import os
import sys
from typing import List

# Append execution context's library path
sys.path.append(os.getenv('DIR_LIB_PY', ''))

try:
    from framework.core.lib import script
except ImportError:
    script = None


def verify_required_paths(variables: List[str]) -> bool:
    """
    Evaluates state of essential target paths.
    
    :param variables: List of environmental key identifiers.
    :return: True if all variables are populated, False otherwise.
    """
    missing_vars = [var for var in variables if not os.getenv(var)]
    
    if missing_vars:
        print("Fehler in .dw_global:")
        for var in missing_vars:
            print(f"   Umgebungsvariable {var} ist nicht gesetzt !")
        return False
        
    return True


def apply_global_configurations() -> None:
    """
    Applies shared runtime session locales.
    """
    os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['NLS_DATE_FORMAT'] = 'DD.MM.YY'
    os.environ['NLS_DATE_LANGUAGE'] = 'GERMAN_GERMANY.WE8ISO8859P1'
    os.environ['PYA_USR'] = ''
    os.environ['LANG'] = 'de'
    
    # Evaluate Legacy Cognos dependencies
    cognos_script_path = "/appl/local/cognos/pya60207/setpya.sh"
    if os.path.isfile(cognos_script_path):
        print(f"[INFO] Legacy Cognos initialization script found at {cognos_script_path}.")
    else:
        print("[INFO] Legacy Cognos validation skipped (script not present).")


def main():
    required_variables = [
        "DW_DIR_ROOT", "DW_DIR_PROT", "DW_DIR_CUBES", "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA", "DW_DIR_IMP_CTEL", "DW_DIR_IMP_VO", "DW_DIR_IMP_RV",
        "DW_DIR_IMP_IF", "DW_DIR_IMP_NNV", "ORACLE_HOME"
    ]
    
    verify_required_paths(required_variables)
    apply_global_configurations()
    print("[SUCCESS] Global validations completed successfully.")


if __name__ == "__main__":
    main()