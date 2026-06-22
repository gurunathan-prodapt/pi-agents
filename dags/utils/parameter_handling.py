# Legacy source: vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_parameter.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

from typing import Optional

def validate_parameter(param_name: str, param_value: Optional[str]) -> bool:
    """
    Simulates the pruefeParameterGesetzt function from the legacy ksh script.
    Checks if a parameter value is set (not None and not an empty string).

    Args:
        param_name: The descriptive name of the parameter.
        param_value: The actual value of the parameter.

    Returns:
        True if the parameter is set, False otherwise.
    """
    if param_value is None or str(param_value).strip() == "":
        return False
    return True