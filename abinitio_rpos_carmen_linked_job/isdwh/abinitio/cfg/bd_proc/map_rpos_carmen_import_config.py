# abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import_config.py
# SETTINGS SOURCE RULE: All keys and values are emitted verbatim from EXTRACTED SETTINGS.

import os

DW_DIR_IMP_SAP = os.environ.get("DW_DIR_IMP_SAP", "")

CONFIG = {
    "FWP_Pre_Session": "",
    "FWP_Post_Session": "",
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": f"{DW_DIR_IMP_SAP}/crs/work/",
    "BHB_Zielverzeichnis": f"{DW_DIR_IMP_SAP}/crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
    "BHB_Eintragsnr": "",
    "BHB_Dateiname": "",
    "BHB_Laufzeitvariable": "",
}