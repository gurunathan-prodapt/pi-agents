#!/usr/bin/env python3
# Step 1: Import required native modules
import sys
from datetime import date, timedelta

def main():
    try:
        # Step 2: Retrieve system current date
        today = date.today()
        
        # Step 3: Compute yesterday's date natively (handles all calendar boundaries automatically)
        yesterday = today - timedelta(days=1)
        
        # Step 4: Format date outputs
        # # REVIEW: Header states returned format is YYMMDD, but the actual KSH implementation uses a 4-digit year %Y resulting in YYYYMMDD format. The Python conversion will preserve the implemented YYYYMMDD format.
        
        # Var_Datum_Heute (YYYYMMDD)
        var_datum_heute = today.strftime("%Y%m%d")
        
        # Var_Datum_Gestern (YYYYMMDD)
        var_datum_gestern = yesterday.strftime("%Y%m%d")
        
        # Var_Monat_Heute (YYYYMM)
        var_monat_heute = today.strftime("%Y%m")
        
        # Var_Monat_Gestern (YYYYMM)
        var_monat_gestern = yesterday.strftime("%Y%m")
        
        # Step 5: Output variables to stdout matching the exact legacy format
        print(f"{var_datum_heute} {var_datum_gestern} {var_monat_heute} {var_monat_gestern}")
        
    except Exception as e:
        # Step 6: Error handling
        # Literal Output Enforcement (OUTPUT/PRINT LITERAL RULE):
        # The legacy script outputs "Fehler !!!!" to stdout under invalid date-parsing conditions.
        print("Fehler !!!!", file=sys.stderr)
        print(f"Error executing date calculation: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()