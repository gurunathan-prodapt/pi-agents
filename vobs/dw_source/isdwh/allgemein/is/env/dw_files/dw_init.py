#!/usr/bin/env python3
"""
Migration of Shell Environment Initializer to Horizon Python.
"""

import os
import sys
from pathlib import Path

# Required framework environment setup
sys.path.append(os.getenv('DIR_LIB_PY', ''))

def source_config_file(file_path: str):
    """
    Simulates sourcing a shell configuration file.
    In a real Python migration, this reads key-value pairs or executes a Python configuration equivalent.
    """
    path = Path(file_path).expanduser()
    if path.exists():
        print(f"[INFO] Sourcing configurations from: {path}")
        # Parse or load environment settings if necessary
    else:
        print(f"[WARNING] Configuration file {path} not found.")

def main():
    # 1. Resolve HOME directory
    home_dir = os.getenv('HOME', str(Path.home()))
    
    # 2. Define and Export Information Service Directory Paths
    os.environ['DW_DIR_ROOT'] = os.path.join(home_dir, 'aktuell')
    os.environ['DW_DIR_PROT'] = os.path.join(home_dir, 'daten/logfiles')
    os.environ['DW_DIR_CUBES'] = os.path.join(home_dir, 'daten/cubes')

    # Import and Export Directories
    os.environ['DW_DIR_IMP_D1'] = os.path.join(home_dir, 'daten/d1')
    os.environ['DW_DIR_IMP_BWA'] = os.path.join(home_dir, 'daten/dpps/bwa')
    os.environ['DW_DIR_IMP_XTRA'] = os.path.join(home_dir, 'daten/xtra')
    os.environ['DW_DIR_IMP_CTEL'] = os.path.join(home_dir, 'daten/ctel')
    os.environ['DW_DIR_IMP_VO'] = os.path.join(home_dir, 'daten/vo')
    os.environ['DW_DIR_IMP_RV'] = os.path.join(home_dir, 'daten/rv')
    os.environ['DW_DIR_IMP_IF'] = os.path.join(home_dir, 'daten/ees')
    os.environ['DW_DIR_IMP_NNV'] = os.path.join(home_dir, 'daten/nnv')
    os.environ['DW_DIR_IMP_SIGMA'] = os.path.join(home_dir, 'daten/gd/sigma')
    os.environ['DW_DIR_EXP_SIGMA'] = os.path.join(home_dir, 'daten/gd/sigma/export')
    os.environ['DW_DIR_IMP_TRF'] = os.path.join(home_dir, 'daten/trf')
    os.environ['DW_DIR_IMP_AUF'] = os.path.join(home_dir, 'daten/sd/auf')
    os.environ['DW_DIR_IMP_GUT'] = os.path.join(home_dir, 'daten/sd/gut')
    os.environ['DW_DIR_IMP_KDG'] = os.path.join(home_dir, 'daten/sd/kdg')
    os.environ['DW_DIR_IMP_MP_KDG'] = os.path.join(home_dir, 'daten/mp/kdg')
    os.environ['DW_DIR_IMP_MP_TS'] = os.path.join(home_dir, 'daten/mp/ts')
    os.environ['DW_DIR_IMP_MP_ZM'] = os.path.join(home_dir, 'daten/mp/zm')
    os.environ['DW_DIR_IMP_TS'] = os.path.join(home_dir, 'daten/sd/ts')
    os.environ['DW_DIR_IMP_ZM'] = os.path.join(home_dir, 'daten/sd/zm')
    os.environ['DW_DIR_EXP'] = os.path.join(home_dir, 'daten/exporter')
    os.environ['DW_DIR_IMP_BPM'] = os.path.join(home_dir, 'daten/bm')
    os.environ['DW_DIR_IMP_ZTS'] = os.path.join(home_dir, 'daten/zts')
    os.environ['DW_DIR_IMP_VRS'] = os.path.join(home_dir, 'daten/vrs')
    
    os.environ['DW_DIR_IMP_BRUNET'] = os.path.join(home_dir, 'daten/brunet')
    os.environ['DW_DIR_IMP_DWH'] = os.path.join(home_dir, 'daten/dwh')
    os.environ['DW_DIR_IMP_PLATO'] = os.path.join(home_dir, 'daten/dwh/plato')
    
    # DWH 2.5 imports
    os.environ['DW_DIR_IMP_CARMEN'] = os.path.join(home_dir, 'daten/carmen')
    os.environ['DW_DIR_IMP_SAP'] = os.path.join(home_dir, 'daten/sap')
    os.environ['DW_DIR_IMP_SR_RV'] = os.path.join(home_dir, 'daten/sap/sr_rv_dpps')
    os.environ['DW_DIR_IMP_SAP_L'] = os.path.join(home_dir, 'daten/sap/sap_l_gutgr')
    os.environ['DW_DIR_IMP_L_MAHNSTYP_IST'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_MAHNV_FI'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_MAHNV_IST'] = os.path.join(home_dir, 'daten/sap/mahn')
    os.environ['DW_DIR_IMP_L_GUTGR'] = os.path.join(home_dir, 'daten/sd/l_gutschr')
    os.environ['DW_DIR_IMP_L_LEIST'] = os.path.join(home_dir, 'daten/sd/l_leist')
    os.environ['DW_DIR_IMP_L_PROD'] = os.path.join(home_dir, 'daten/sd/l_prod')
    os.environ['DW_DIR_IMP_LKODE'] = os.path.join(home_dir, 'daten/sd/lkode')

    # DWH 7.5 Subscription Server
    os.environ['DW_DIR_IMP_SUBSE'] = os.path.join(home_dir, 'daten/subse')

    # DWH 2.5 SMS Utility Paths
    os.environ['DW_DIR_SMS_PRG'] = os.path.join(home_dir, 'aktuell/allgemein/is/util')
    os.environ['DW_DIR_SMS_ADR'] = os.path.join(home_dir, 'daten/sms/adressen')
    os.environ['DW_DIR_SMS_TMP'] = os.path.join(home_dir, 'daten/sms/tmp')

    # DWH 3.5 Planning
    os.environ['DW_DIR_IMP_DPPS'] = os.path.join(home_dir, 'daten/dpps')
    os.environ['DW_DIR_IMP_PLANF2'] = os.path.join(home_dir, 'daten/planf2')

    # Remote Network Hosts
    os.environ['DW_HOST_CUSTOMER'] = 'dxcst3.bn.detemobil.de'

    # 3. Handle ORACLE_HOME setting and path validation
    oracle_home = os.getenv('ORACLE_HOME', '')
    if not oracle_home:
        path_12_2 = '/appl/local/oracle/12.2.0.1.0'
        path_11_2 = '/appl/local/oracle/11.2.0'
        
        if os.path.isdir(path_12_2):
            oracle_home = path_12_2
        elif os.path.isdir(path_11_2):
            oracle_home = path_11_2
        else:
            print("Fehler in .dw_init:", file=sys.stderr)
            print("   Konnte ORACLE_HOME nicht setzen !", file=sys.stderr)
            
        if oracle_home:
            os.environ['ORACLE_HOME'] = oracle_home

    # 4. Source Global and Local parameters
    source_config_file(os.path.join(home_dir, '.dw_global'))
    
    # SOURCE: NOT FOUND — .DW_LOKAL — no candidate
    source_config_file(os.path.join(home_dir, '.dw_lokal'))

    # 5. Dynamic setting of UTL_FILE directory using active ORACLE_SID
    oracle_sid = os.getenv('ORACLE_SID', 'DEFAULT_SID')
    os.environ['DW_DIR_UTL_FILE'] = f'/appl/local/oracle/admin/{oracle_sid}/utl_file'
    
    print("[SUCCESS] Environment initialization completed.")

if __name__ == '__main__':
    main()