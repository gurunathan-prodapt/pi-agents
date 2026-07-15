#!/usr/bin/env python3
"""
Migration of Shell Configuration Script to Horizon Python.
Target Environment: BigQuery / Horizon Python Environment.
"""

import os
import sys

# Ensure Horizon Library Path is registered
DIR_LIB_PY = os.getenv('DIR_LIB_PY', '')
if DIR_LIB_PY:
    sys.path.append(DIR_LIB_PY)
else:
    # Fallback to local execution default if DIR_LIB_PY is empty
    sys.path.append('/home/horizon/lib/python')

try:
    from framework.core.lib import script
except ImportError:
    pass

def main():
    try:
        # Step 1: Set Environment Locale/Encoding Parameters
        os.environ['NLS_LANG'] = 'GERMAN_GERMANY.WE8ISO8859P1'
        
        # Step 2: Initialize Database Connection Variables
        db_config = {
            'DB_TNS_NAME_DWH': '@eDWH3.devlab.de.tmo',
            'DB_USER_DWH': 'meyreis',
            'DB_PASSWD_DWH': '<password encrypted with m_password>'
        }
        
        # Step 3: Export variables to environment for downstream execution compatibility
        for key, value in db_config.items():
            os.environ[key] = value
            
        print("Environment and database connection variables initialized successfully.")

    except Exception as e:
        print(f"Error during environment initialization: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()