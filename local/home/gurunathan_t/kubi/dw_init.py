import os
import sys

# GLOBAL (Environment-Wide)
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
HOME = os.environ.get("HOME", "/home/gurunathan_t")

# Helper function to build paths (either GCS or local)
def get_path(relative_path, use_gcs=True):
    if use_gcs and GCS_BUCKET:
        return f"gs://{GCS_BUCKET}/{relative_path.lstrip('/')}"
    return os.path.join(HOME, relative_path.lstrip('/'))

# JOB-SPECIFIC (Module Configs)
# DW_DIR_ROOT: Sourced as f"{HOME}/aktuell"
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT", os.path.join(HOME, "aktuell"))

# DW_DIR_PROT: Sourced as f"gs://{GCS_BUCKET}/daten/logfiles" or local log path
DW_DIR_PROT = os.environ.get("DW_DIR_PROT", get_path("daten/logfiles", use_gcs=True))

# DW_DIR_CUBES
DW_DIR_CUBES = os.environ.get("DW_DIR_CUBES", get_path("daten/cubes", use_gcs=True))

# Importer directories
DW_DIR_IMP_D1 = get_path("daten/d1")
DW_DIR_IMP_BWA = get_path("daten/dpps/bwa")
DW_DIR_IMP_XTRA = get_path("daten/xtra")
DW_DIR_IMP_CTEL = get_path("daten/ctel")
DW_DIR_IMP_VO = get_path("daten/vo")
DW_DIR_IMP_RV = get_path("daten/rv")
DW_DIR_IMP_IF = get_path("daten/ees")
DW_DIR_IMP_NNV = get_path("daten/nnv")
DW_DIR_IMP_SIGMA = get_path("daten/gd/sigma")
DW_DIR_EXP_SIGMA = get_path("daten/gd/sigma/export")
DW_DIR_IMP_TRF = get_path("daten/trf")
DW_DIR_IMP_AUF = get_path("daten/sd/auf")
DW_DIR_IMP_GUT = get_path("daten/sd/gut")
DW_DIR_IMP_KDG = get_path("daten/sd/kdg")
DW_DIR_IMP_MP_KDG = get_path("daten/mp/kdg")
DW_DIR_IMP_MP_TS = get_path("daten/mp/ts")
DW_DIR_IMP_MP_ZM = get_path("daten/mp/zm")
DW_DIR_IMP_TS = get_path("daten/sd/ts")
DW_DIR_IMP_ZM = get_path("daten/sd/zm")
DW_DIR_EXP = get_path("daten/exporter")
DW_DIR_IMP_BPM = get_path("daten/bm")
DW_DIR_IMP_ZTS = get_path("daten/zts")
DW_DIR_IMP_VRS = get_path("daten/vrs")

DW_DIR_IMP_BRUNET = get_path("daten/brunet")
DW_DIR_IMP_DWH = get_path("daten/dwh")
DW_DIR_IMP_PLATO = get_path("daten/dwh/plato")

# DWH 2.5
DW_DIR_IMP_CARMEN = get_path("daten/carmen")
DW_DIR_IMP_SAP = get_path("daten/sap")
DW_DIR_IMP_SR_RV = get_path("daten/sap/sr_rv_dpps")
DW_DIR_IMP_SAP_L_GUTGR = get_path("daten/sap/sap_l_gutgr")
DW_DIR_IMP_SAP_L = DW_DIR_IMP_SAP_L_GUTGR  # exported as DW_DIR_IMP_SAP_L in source
DW_DIR_IMP_L_MAHNSTYP_IST = get_path("daten/sap/mahn")
DW_DIR_IMP_L_MAHNV_FI = get_path("daten/sap/mahn")
DW_DIR_IMP_L_MAHNV_IST = get_path("daten/sap/mahn")
DW_DIR_IMP_L_GUTGR = get_path("daten/sd/l_gutschr")
DW_DIR_IMP_L_LEIST = get_path("daten/sd/l_leist")
DW_DIR_IMP_L_PROD = get_path("daten/sd/l_prod")
DW_DIR_LKODE = get_path("daten/sd/lkode")

# DWH 7.5 mit Subscription Server
DW_DIR_IMP_SUBSE = get_path("daten/subse")

# DWH 2.5 mit SMS
DW_DIR_SMS_PRG = os.path.join(DW_DIR_ROOT, "allgemein/is/util")
DW_DIR_SMS_ADR = get_path("daten/sms/adressen")
DW_DIR_SMS_TMP = get_path("daten/sms/tmp")

# DWH 3.5
DW_DIR_IMP_DPPS = get_path("daten/dpps")
DW_DIR_IMP_PLANF2 = get_path("daten/planf2")

# Remote Hosts
DW_HOST_CUSTOMER = "dxcst3.bn.detemobil.de"

# Oracle Home setup (retained for compatibility, but stubbed/checked)
ORACLE_HOME = os.environ.get("ORACLE_HOME")
if not ORACLE_HOME:
    # Check legacy directories
    if os.path.isdir("/appl/local/oracle/12.2.0.1.0"):
        ORACLE_HOME = "/appl/local/oracle/12.2.0.1.0"
    elif os.path.isdir("/appl/local/oracle/11.2.0"):
        ORACLE_HOME = "/appl/local/oracle/11.2.0"
    else:
        # Preserve original German error logging output exactly as-is
        print("Fehler in .dw_init:")
        print("   Konnte ORACLE_HOME nicht setzen !")
    
    if ORACLE_HOME:
        os.environ["ORACLE_HOME"] = ORACLE_HOME

# DW_DIR_UTL_FILE
ORACLE_SID = os.environ.get("ORACLE_SID", "")
DW_DIR_UTL_FILE = f"/appl/local/oracle/admin/{ORACLE_SID}/utl_file" if ORACLE_SID else None

# Export variables to os.environ
environ_vars = {
    "DW_DIR_ROOT": DW_DIR_ROOT,
    "DW_DIR_PROT": DW_DIR_PROT,
    "DW_DIR_CUBES": DW_DIR_CUBES,
    "DW_DIR_IMP_D1": DW_DIR_IMP_D1,
    "DW_DIR_IMP_BWA": DW_DIR_IMP_BWA,
    "DW_DIR_IMP_XTRA": DW_DIR_IMP_XTRA,
    "DW_DIR_IMP_CTEL": DW_DIR_IMP_CTEL,
    "DW_DIR_IMP_VO": DW_DIR_IMP_VO,
    "DW_DIR_IMP_RV": DW_DIR_IMP_RV,
    "DW_DIR_IMP_IF": DW_DIR_IMP_IF,
    "DW_DIR_IMP_NNV": DW_DIR_IMP_NNV,
    "DW_DIR_IMP_SIGMA": DW_DIR_IMP_SIGMA,
    "DW_DIR_EXP_SIGMA": DW_DIR_EXP_SIGMA,
    "DW_DIR_IMP_TRF": DW_DIR_IMP_TRF,
    "DW_DIR_IMP_AUF": DW_DIR_IMP_AUF,
    "DW_DIR_IMP_GUT": DW_DIR_IMP_GUT,
    "DW_DIR_IMP_KDG": DW_DIR_IMP_KDG,
    "DW_DIR_IMP_MP_KDG": DW_DIR_IMP_MP_KDG,
    "DW_DIR_IMP_MP_TS": DW_DIR_IMP_MP_TS,
    "DW_DIR_IMP_MP_ZM": DW_DIR_IMP_MP_ZM,
    "DW_DIR_IMP_TS": DW_DIR_IMP_TS,
    "DW_DIR_IMP_ZM": DW_DIR_IMP_ZM,
    "DW_DIR_EXP": DW_DIR_EXP,
    "DW_DIR_IMP_BPM": DW_DIR_IMP_BPM,
    "DW_DIR_IMP_ZTS": DW_DIR_IMP_ZTS,
    "DW_DIR_IMP_VRS": DW_DIR_IMP_VRS,
    "DW_DIR_IMP_BRUNET": DW_DIR_IMP_BRUNET,
    "DW_DIR_IMP_DWH": DW_DIR_IMP_DWH,
    "DW_DIR_IMP_PLATO": DW_DIR_IMP_PLATO,
    "DW_DIR_IMP_CARMEN": DW_DIR_IMP_CARMEN,
    "DW_DIR_IMP_SAP": DW_DIR_IMP_SAP,
    "DW_DIR_IMP_SR_RV": DW_DIR_IMP_SR_RV,
    "DW_DIR_IMP_SAP_L": DW_DIR_IMP_SAP_L,
    "DW_DIR_IMP_L_MAHNSTYP_IST": DW_DIR_IMP_L_MAHNSTYP_IST,
    "DW_DIR_IMP_L_MAHNV_FI": DW_DIR_IMP_L_MAHNV_FI,
    "DW_DIR_IMP_L_MAHNV_IST": DW_DIR_IMP_L_MAHNV_IST,
    "DW_DIR_IMP_L_GUTGR": DW_DIR_IMP_L_GUTGR,
    "DW_DIR_IMP_L_LEIST": DW_DIR_IMP_L_LEIST,
    "DW_DIR_IMP_L_PROD": DW_DIR_IMP_L_PROD,
    "DW_DIR_LKODE": DW_DIR_LKODE,
    "DW_DIR_IMP_SUBSE": DW_DIR_IMP_SUBSE,
    "DW_DIR_SMS_PRG": DW_DIR_SMS_PRG,
    "DW_DIR_SMS_ADR": DW_DIR_SMS_ADR,
    "DW_DIR_SMS_TMP": DW_DIR_SMS_TMP,
    "DW_DIR_IMP_DPPS": DW_DIR_IMP_DPPS,
    "DW_DIR_IMP_PLANF2": DW_DIR_IMP_PLANF2,
    "DW_HOST_CUSTOMER": DW_HOST_CUSTOMER,
}

for key, val in environ_vars.items():
    if val is not None:
        os.environ[key] = val

if DW_DIR_UTL_FILE:
    os.environ["DW_DIR_UTL_FILE"] = DW_DIR_UTL_FILE