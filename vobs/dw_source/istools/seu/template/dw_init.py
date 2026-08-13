#!/usr/bin/env python3
"""
Python initialization module for Information Services environment settings.
This module replaces the legacy .dw_init script.
"""

import os
import sys
import dw_global

def init_env():
    # Step 1: Initialize environment and directory paths
    home = os.environ.get("HOME", "")

    os.environ["DW_DIR_ROOT"] = os.path.join(home, "aktuell")
    os.environ["DW_DIR_PROT"] = os.path.join(home, "daten/logfiles")
    os.environ["DW_DIR_CUBES"] = os.path.join(home, "daten/cubes")
    os.environ["DW_DIR_IMP_D1"] = os.path.join(home, "daten/d1")
    os.environ["DW_DIR_IMP_XTRA"] = os.path.join(home, "daten/xtra")
    os.environ["DW_DIR_IMP_CTEL"] = os.path.join(home, "daten/ctel")
    os.environ["DW_DIR_IMP_VO"] = os.path.join(home, "daten/vo")
    os.environ["DW_DIR_IMP_RV"] = os.path.join(home, "daten/rv")
    os.environ["DW_DIR_IMP_TRF"] = os.path.join(home, "daten/trf")
    os.environ["DW_DIR_IMP_TS"] = os.path.join(home, "daten/sd/ts")
    os.environ["DW_DIR_IMP_ZM"] = os.path.join(home, "daten/sd/zm")
    os.environ["DW_DIR_IMP_AUF"] = os.path.join(home, "daten/sd/auf")
    os.environ["DW_DIR_IMP_GUT"] = os.path.join(home, "daten/sd/gut")
    os.environ["DW_DIR_IMP_KDG"] = os.path.join(home, "daten/sd/kdg")
    os.environ["DW_DIR_IMP_MP_TS"] = os.path.join(home, "daten/mp/ts")
    os.environ["DW_DIR_IMP_MP_KDG"] = os.path.join(home, "daten/mp/kdg")

    # Correcting legacy copy-paste assignment of DW_DIR_IMP_MP_ZM (exported as DW_DIR_IMP_MP_TS twice in KSH)
    os.environ["DW_DIR_IMP_MP_ZM"] = os.path.join(home, "daten/mp/zm")

    os.environ["DW_DIR_IMP_IF"] = os.path.join(home, "daten/if")
    os.environ["DW_DIR_IMP_NNV"] = os.path.join(home, "daten/nnv")
    os.environ["DW_DIR_IMP_CARMEN"] = os.path.join(home, "daten/carmen")

    os.environ["GEN_HOME"] = os.path.join(os.environ["DW_DIR_ROOT"], "generator")
    
    # DW_DIR_CUSTOMER contained placeholder <login> in legacy code. Sourced from environment variables to avoid hardcoded placeholders.
    os.environ["DW_DIR_CUSTOMER"] = os.environ.get("DW_DIR_CUSTOMER", "")
    os.environ["DW_HOST_CUSTOMER"] = "dxcst3.bn.detemobil.de"

    # Step 2: Dynamically resolve ORACLE_HOME if not already configured
    if not os.environ.get("ORACLE_HOME"):
        if os.path.isdir("/appl/local/oracle/oracle.8.1.6"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/8.1.6"
        elif os.path.isdir("/appl/local/oracle/7.3.4"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.3.4"
        elif os.path.isdir("/appl/local/oracle/oracle.7.3.3"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/oracle.7.3.3"
        elif os.path.isdir("/appl/local/oracle/7.3.2"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.3.2"
        elif os.path.isdir("/appl/local/oracle/7.2.3"):
            os.environ["ORACLE_HOME"] = "/appl/local/oracle/7.2.3"
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
            print("Breche ab ..", file=sys.stderr)
            sys.exit(1)

    # Step 3: Source global and local configuration profiles
    dw_global.main()

    # Step 4: Apply permissions umask (022 octal)
    os.umask(0o022)

def main():
    init_env()

if __name__ == "__main__":
    main()