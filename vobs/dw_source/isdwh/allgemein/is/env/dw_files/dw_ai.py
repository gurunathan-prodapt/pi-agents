#!/usr/bin/env python3
"""
Migration of Ab Initio Shell Environment Setup to Horizon Python.
Designed for BigQuery-native Horizon platform execution.
"""

import os
import sys

# Append Horizon library path to access framework resources
sys.path.append(os.getenv('DIR_LIB_PY', ''))
try:
    from framework.core.lib import script
except ImportError:
    pass

def main():
    # ---------------------------------------------------------
    # Step 1: Initialize System Paths and Environment Metadata
    # ---------------------------------------------------------
    
    # AI-Environment Setup
    os.environ['AB_HOME'] = "/appl/local/abinitio/abinitio"
    os.environ['AB_AIR_ROOT'] = "/appl/local/abinitio/TMD_EME/eme_dev/repo"
    os.environ['AB_AIR_HOME'] = "/appl/local/abinitio/abinitio-V2-14"
    
    # Update execution PATH variable
    current_path = os.environ.get('PATH', '')
    ab_bin_path = f"{os.environ['AB_HOME']}/bin"
    os.environ['PATH'] = f"{current_path}.:{ab_bin_path}"
    
    # DWH-AI-Framework Configuration
    os.environ['ETL_Host'] = "dxcsa4.bn.detemobil.de"
    os.environ['ETL_Projekt'] = "BHB"
    
    # Establish Sandbox Roots
    home_dir = os.environ.get('HOME', '/home/default')
    os.environ['AI_PRIV_SAND_ROOT'] = f"{home_dir}/abinitio"
    os.environ['AI_ENV_SAND_ROOT'] = "/appl/local/abinitio/sandboxes/DEV"
    
    # AI_REPOSIT_TRACKING is commented out in the source script;
    # Decoupled matching representation:
    # os.environ['AI_REPOSIT_TRACKING'] = "FALSE"
    
    print("[INFO] Environment variables initialized successfully.")

if __name__ == "__main__":
    main()