#!/usr/bin/env python3
"""
Migration of Ab Initio Shell Environment Setup to Horizon Python.
Registers framework properties, executable paths, and directories.
"""

import os
import sys

# Append execution context's library path
sys.path.append(os.getenv('DIR_LIB_PY', ''))

try:
    from framework.core.lib import script
except ImportError:
    script = None


def configure_ab_initio_env() -> None:
    """
    Registers system and application environment parameters for 
    Ab Initio framework execution in the current OS runtime.
    """
    # Ab Initio Engine Variable Declarations
    os.environ['AB_HOME'] = '/appl/local/abinitio/abinitio'
    os.environ['AB_AIR_ROOT'] = '/appl/local/abinitio/TMD_EME/eme_dev/repo'
    os.environ['AB_AIR_HOME'] = '/appl/local/abinitio/abinitio-V2-14'
    
    # Update execution path
    current_path = os.environ.get('PATH', '')
    ab_bin_path = f"{os.environ['AB_HOME']}/bin"
    if ab_bin_path not in current_path:
        os.environ['PATH'] = f"{current_path}:.:{ab_bin_path}"
    
    # Ab Initio Host Mappings
    os.environ['ETL_Host'] = 'dxcsa4.bn.detemobil.de'
    os.environ['ETL_Projekt'] = 'BHB'
    
    # Resolve Sandbox Directories
    home_dir = os.environ.get('HOME', '/home/default')
    os.environ['AI_PRIV_SAND_ROOT'] = f"{home_dir}/abinitio"
    os.environ['AI_ENV_SAND_ROOT'] = '/appl/local/abinitio/sandboxes/DEV'
    
    print("[SUCCESS] Ab Initio Engine settings updated successfully.")


def main():
    configure_ab_initio_env()


if __name__ == '__main__':
    main()