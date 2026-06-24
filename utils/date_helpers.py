# Migrated utility: h_alis_date.ksh (Date Check) and gestern.ksh (Date Calculation)
# Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

from datetime import datetime, timedelta

def DWDate_Datum_Check(date_str: str, date_format: str):
    """
    Simulates the DWDate_Datum_Check function from h_alis_date.ksh.
    Checks if a date string matches the expected format.
    Raises ValueError if the date is invalid.
    """
    try:
        datetime.strptime(date_str, date_format)
        print(f"Date '{date_str}' is valid according to format '{date_format}'.")
        return True
    except ValueError:
        raise ValueError(f"Invalid date format for '{date_str}'. Expected '{date_format}'.")

def get_today_and_yesterday_dates(ref_date: datetime = None):
    """
    Simulates the functionality of gestern.ksh.
    Returns today's and yesterday's date as 'YYYYMMDD' strings.
    If ref_date is provided, 'today' is considered that date, otherwise it's the current system date.
    """
    if ref_date is None:
        today = datetime.now()
    else:
        today = ref_date

    yesterday = today - timedelta(days=1)

    p_datum_heute = today.strftime('%Y%m%d')
    p_datum_gestern = yesterday.strftime('%Y%m%d')

    return p_datum_heute, p_datum_gestern

# Example usage
if __name__ == "__main__":
    # Test DWDate_Datum_Check
    try:
        DWDate_Datum_Check("22032023", "%d%m%Y")
        # This should fail
        # DWDate_Datum_Check("2023-03-22", "%d%m%Y")
    except ValueError as e:
        print(f"Error: {e}")

    # Test get_today_and_yesterday_dates
    heute, gestern = get_today_and_yesterday_dates()
    print(f"Today (system date): {heute}, Yesterday (system date): {gestern}")

    # Test with a specific reference date (like p_Stichtag)
    stichtag_str = "22032023"
    stichtag_dt = datetime.strptime(stichtag_str, "%d%m%Y")
    heute_ref, gestern_ref = get_today_and_yesterday_dates(stichtag_dt)
    print(f"Today (from stichtag {stichtag_str}): {heute_ref}, Yesterday (from stichtag {stichtag_str}): {gestern_ref}")