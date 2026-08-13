#!/usr/bin/env python3
import os
import sys
import pathlib

def main():
    # Step 1: Initialize error tracker
    fehler = []

    # Step 2: Validate required environment variables
    required_vars = [
        "DW_DIR_ROOT",
        "DW_DIR_PROT",
        "DW_DIR_CUBES",
        "DW_DIR_IMP_D1",
        "DW_DIR_IMP_XTRA",
        "DW_DIR_IMP_CTEL",
        "ORACLE_HOME"
    ]

    for varname in required_vars:
        if not os.environ.get(varname):
            fehler.append(varname)

    # Step 3: Handle validation errors
    if fehler:
        print("Fehler in .dw_global:", file=sys.stderr)
        for varname in fehler:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !", file=sys.stderr)
        print("Breche ab ..", file=sys.stderr)
        raise EnvironmentError(f"Required environment variables are not set: {', '.join(fehler)}")

    # Step 4: Prepend/Append dependent paths
    oracle_home = os.environ["ORACLE_HOME"]

    # Update LD_LIBRARY_PATH
    ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
    if ld_library_path:
        os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib:{ld_library_path}"
    else:
        os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib:"

    # Update PATH
    path = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{path}:{oracle_home}/bin:"

    # Step 5: Export database session locale variables
    os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
    os.environ["NLS_DATE_FORMAT"] = "DD-MON-YY"
    os.environ["NLS_DATE_LANGUAGE"] = "AMERICAN"

    # Step 6: Conditionally source Cognos setup script
    cognos_script_path = pathlib.Path("/appl/local/cognos/cognos5.2/pya52b17/setpya.sh")
    if cognos_script_path.is_file():
        # REVIEW-STRUCT: environment file [/appl/local/cognos/cognos5.2/pya52b17/setpya.sh] not supplied — variables it sets are unknown; do not guess their names or values
        # Since this script runs in Python, modifications to os.environ from an external shell script 
        # cannot be dynamically applied without executing it in a subshell and parsing exports.
        pass

if __name__ == "__main__":
    try:
        main()
    except EnvironmentError as e:
        sys.exit(1)