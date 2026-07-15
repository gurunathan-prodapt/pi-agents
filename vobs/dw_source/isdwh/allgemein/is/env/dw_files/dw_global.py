#!/usr/bin/env python3
"""
Migration of .dw_global Initialization Script to Horizon Python.
Purpose: Validate runtime paths, set up localizations, and source Cognos/Oracle configurations.
"""

import os
import sys
import subprocess

# Ensure Horizon Core library is accessible
sys.path.append(os.getenv('DIR_LIB_PY', ''))

def validate_environment():
    """
    Validates required paths and environment parameters.
    Replicates the shell error-checking routine.
    """
    required_variables = [
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
    
    missing_variables = []
    
    for var in required_variables:
        if not os.getenv(var):
            missing_variables.append(var)
            
    if missing_variables:
        print("Fehler in .dw_global:")
        for varname in missing_variables:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")


def source_shell_script(script_path):
    """
    Helper function to execute an external shell script, capture the exported 
    variables, and apply them directly to Python's os.environ.
    """
    if os.path.exists(script_path) and os.path.isfile(script_path):
        try:
            # Run the script and output the updated environment variables
            command = f"exec /bin/ksh -c '. {script_path} && env'"
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, text=True)
            stdout, _ = proc.communicate()
            
            if proc.returncode == 0:
                for line in stdout.splitlines():
                    # Parse exported key=value pairs
                    if '=' in line:
                        key, _, value = line.partition('=')
                        os.environ[key] = value
            else:
                print(f"Warning: Execution of {script_path} failed with return code {proc.returncode}")
        except Exception as e:
            print(f"Warning: Could not source {script_path}. Error: {str(e)}")


def set_global_parameters():
    """
    Applies Oracle NLS, Cognos settings, and other localization variables.
    """
    # SQL-Net 2 Connections Configuration
    os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
    os.environ["NLS_DATE_FORMAT"] = "DD.MM.YY"
    os.environ["NLS_DATE_LANGUAGE"] = "GERMAN_GERMANY.WE8ISO8859P1"

    # Cognos PowerPlay configuration
    os.environ["PYA_USR"] = ""
    
    # Conditional sourcing of the Cognos configuration script
    # SOURCE: NOT FOUND — SETPYA.SH — No candidate found in scan.
    cognos_setpya_path = "/appl/local/cognos/pya60207/setpya.sh"
    source_shell_script(cognos_setpya_path)
    
    # Final localization overrides
    os.environ["LANG"] = "de"


def main():
    # 1. Run validation of critical paths
    validate_environment()
    
    # 2. Set Oracle/Cognos/Lang parameters
    set_global_parameters()
    
    # 3. Print success acknowledgement
    print("Horizon global variables initialized successfully.")


if __name__ == "__main__":
    main()