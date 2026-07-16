from airflow.models import Variable
from airflow.exceptions import AirflowException

class DynamicPathResolver:
    @staticmethod
    def get_paths():
        """
        Resolves paths dynamically from the central variable container (Airflow Variables).
        Equivalent to UC4 JOBI: DW.HOLE_PFAD_VTRG
        
        Retrieves:
          - DWH_HOME
          - HOME
          - PMS_HOME
        """
        try: 
            paths = {
                "DWH_HOME": Variable.get("dw_variablen_dwh_home"),
                "HOME": Variable.get("dw_variablen_home"),
                "PMS_HOME": Variable.get("dw_variablen_pms_home")
            }
            return paths
        except Exception as e:
            raise AirflowException(f"Failed to resolve path variables: {str(e)}")