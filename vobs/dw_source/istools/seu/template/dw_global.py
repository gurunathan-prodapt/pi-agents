#!/usr/bin/env python3
import os
import sys

def main():
    # Step 1: Initialize list of validation errors
    fehler = []

    # Step 2: Validate DW_DIR_ROOT
    if not os.environ.get("DW_DIR_ROOT"):
        fehler.append("DW_DIR_ROOT")

    # Step 3: Validate DW_DIR_PROT
    if not os.environ.get("DW_DIR_PROT"):
        fehler.append("DW_DIR_PROT")

    # Step 4: Validate DW_DIR_CUBES
    if not os.environ.get("DW_DIR_CUBES"):
        fehler.append("DW_DIR_CUBES")

    # Step 5: Validate DW_DIR_IMP_D1
    if not os.environ.get("DW_DIR_IMP_D1"):
        fehler.append("DW_DIR_IMP_D1")

    # Step 6: Validate DW_DIR_IMP_XTRA
    if not os.environ.get("DW_DIR_IMP_XTRA"):
        fehler.append("DW_DIR_IMP_XTRA")

    # Step 7: Validate DW_DIR_IMP_CTEL
    if not os.environ.get("DW_DIR_IMP_CTEL"):
        fehler.append("DW_DIR_IMP_CTEL")

    # Step 8: Validate ORACLE_HOME
    if not os.environ.get("ORACLE_HOME"):
        fehler.append("ORACLE_HOME")

    # Step 9: Report validation errors
    if fehler:
        print("Fehler in .dw_global:")
        for varname in fehler:
            print(f"   Umgebungsvariable {varname} ist nicht gesetzt !")
        print("Breche ab ..")
        # REVIEW: The original script printed "Breche ab .." but did not exit, continuing with invalid state.
        # Following the design document, we raise an EnvironmentError to strictly prevent downstream execution.
        raise EnvironmentError(f"Missing required environment variables: {', '.join(fehler)}")

    # Step 10: Prepend Oracle library path to LD_LIBRARY_PATH
    oracle_home = os.environ.get("ORACLE_HOME", "")
    current_ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
    if oracle_home:
        if current_ld_library_path:
            os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib:{current_ld_library_path}"
        else:
            os.environ["LD_LIBRARY_PATH"] = f"{oracle_home}/lib"

    # Step 11: Append Oracle binary path to PATH
    current_path = os.environ.get("PATH", "")
    if oracle_home:
        os.environ["PATH"] = f"{current_path}:{oracle_home}/bin:"

    # Step 12: Export Database localization and session NLS environment variables
    os.environ["NLS_LANG"] = "GERMAN_GERMANY.WE8ISO8859P1"
    os.environ["NLS_DATE_FORMAT"] = "DD-MON-YY"
    os.environ["NLS_DATE_LANGUAGE"] = "AMERICAN"

    # Step 13: Conditionally execute Cognos environment script
    # REVIEW-STRUCT: environment file /appl/local/cognos/cognos5.2/pya52b17/setpya.sh not supplied — variables it sets are unknown
    cognos_script = "/appl/local/cognos/cognos5.2/pya52b17/setpya.sh"
    if os.path.exists(cognos_script) and os.path.isfile(cognos_script):
        # Sourcing a shell script directly inside Python is not natively possible.
        # Since the script was not supplied, we cannot emulate its environment modifications.
        pass

    return 0

if __name__ == "__main__":
    sys.exit(main())