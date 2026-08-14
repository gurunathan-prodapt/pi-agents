#!/usr/bin/env python3
import os
import sys

def main():
    # Step 1: Define and export DW_DIR_ROOT
    home_dir = os.environ.get("HOME")
    if not home_dir:
        home_dir = os.path.expanduser("~")

    dw_dir_root = os.path.join(home_dir, "aktuell")
    os.environ["DW_DIR_ROOT"] = dw_dir_root

    # Step 2: Define and export log and cube directories
    os.environ["DW_DIR_PROT"] = os.path.join(home_dir, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = os.path.join(home_dir, "daten/cubes")

    # Step 3: Define and export importer interface directories
    os.environ["DW_DIR_IMP_D1"] = os.path.join(home_dir, "daten/d1")
    os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home_dir, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home_dir, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = os.path.join(home_dir, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = os.path.join(home_dir, "daten/rv")
    os.environ["DW_DIR_IMP_TRF"] = os.path.join(home_dir, "daten/trf")
    os.environ["DW_DIR_IMP_TS"] = os.path.join(home_dir, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = os.path.join(home_dir, "daten/sd/zm")
    os.environ["DW_DIR_IMP_AUF"] = os.path.join(home_dir, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = os.path.join(home_dir, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = os.path.join(home_dir, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home_dir, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home_dir, "daten/mp/kdg")

    # NOTE: Original script contains a typo: DW_DIR_IMP_MP_ZM is assigned but DW_DIR_IMP_MP_TS is exported again instead.
    # We assign and export DW_DIR_IMP_MP_ZM to avoid downstream directory resolution failures.
    os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home_dir, "daten/mp/zm")

    os.environ["DW_DIR_IMP_IF"] = os.path.join(home_dir, "daten/if")
    os.environ["DW_DIR_IMP_NNV"] = os.path.join(home_dir, "daten/nnv")
    os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home_dir, "daten/carmen")

    # Step 4: Define and export generator home
    os.environ["GEN_HOME"] = os.path.join(dw_dir_root, "generator")

    # Step 5: Define and export customer remote settings
    os.environ["DW_DIR_CUSTOMER"] = "<login>"
    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 6: Validate and discover ORACLE_HOME if empty or unset
    oracle_home = os.environ.get("ORACLE_HOME")
    if not oracle_home:
        paths_to_check = [
            "/appl/local/oracle/oracle.8.1.6",
            "/appl/local/oracle/7.3.4",
            "/appl/local/oracle/oracle.7.3.3",
            "/appl/local/oracle/7.3.2",
            "/appl/local/oracle/7.2.3"
        ]
        
        discovered_oracle_home = None
        for path in paths_to_check:
            if os.path.isdir(path):
                # Legacy check maps "oracle.8.1.6" to "8.1.6" path
                if path == "/appl/local/oracle/oracle.8.1.6":
                    discovered_oracle_home = "/appl/local/oracle/8.1.6"
                else:
                    discovered_oracle_home = path
                break
        
        if discovered_oracle_home:
            os.environ["ORACLE_HOME"] = discovered_oracle_home
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
            print("Breche ab ..", file=sys.stderr)
            sys.exit(1)

    # Step 7: Sourcing external environment profiles
    # REVIEW-STRUCT: environment file [.dw_global] not supplied — variables it sets are unknown; do not guess their names or values
    # REVIEW-STRUCT: environment file [.dw_lokal] not supplied — variables it sets are unknown; do not guess their names or values

    # Step 8: Apply umask 022 (read/write/execute for owner, read/execute for group/others)
    os.umask(0o022)

if __name__ == "__main__":
    sys.exit(main())