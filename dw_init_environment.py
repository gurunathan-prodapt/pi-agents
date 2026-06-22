# Legacy Source: vobs/dw_source/istools/seu/template/.dw_init
# Job: vobs/dw_source/istools/seu/template/.dw_init

import os
import sys

def initialize_environment():
    """
    Initializes environment variables based on the logic of the legacy
    KornShell script vobs/dw_source/istools/seu/template/.dw_init.
    """
    # Read base home directory from environment (or a default if not set)
    # In a cloud-native environment, '/app' is a common default for containerized applications.
    home = os.getenv('HOME', '/app') 

    # Set environment variables
    # These will be set as OS environment variables for the Python process
    os.environ['DW_DIR_ROOT'] = f"{home}/aktuell"
    os.environ['DW_DIR_PROT'] = f"{home}/daten/logfiles"
    os.environ['DW_DIR_CUBES'] = f"{home}/daten/cubes"
    os.environ['DW_DIR_IMP_D1'] = f"{home}/daten/d1"
    os.environ['DW_DIR_IMP_XTRA'] = f"{home}/daten/xtra"
    os.environ['DW_DIR_IMP_CTEL'] = f"{home}/daten/ctel"
    os.environ['DW_DIR_IMP_VO'] = f"{home}/daten/vo"
    os.environ['DW_DIR_IMP_RV'] = f"{home}/daten/rv"
    os.environ['DW_DIR_IMP_TRF'] = f"{home}/daten/trf"
    os.environ['DW_DIR_IMP_TS'] = f"{home}/daten/sd/ts"
    os.environ['DW_DIR_IMP_ZM'] = f"{home}/daten/sd/zm"
    os.environ['DW_DIR_IMP_AUF'] = f"{home}/daten/sd/auf"
    os.environ['DW_DIR_IMP_GUT'] = f"{home}/daten/sd/gut"
    os.environ['DW_DIR_IMP_KDG'] = f"{home}/daten/sd/kdg"
    os.environ['DW_DIR_IMP_MP_TS'] = f"{home}/daten/mp/ts"
    os.environ['DW_DIR_IMP_MP_KDG'] = f"{home}/daten/mp/kdg"
    # Correcting identified typo from the original shell script:
    # Original assigned DW_DIR_IMP_MP_ZM but exported DW_DIR_IMP_MP_TS.
    # Assuming intent was to set and export DW_DIR_IMP_MP_ZM.
    os.environ['DW_DIR_IMP_MP_ZM'] = f"{home}/daten/mp/zm"
    os.environ['DW_DIR_IMP_IF'] = f"{home}/daten/if"
    os.environ['DW_DIR_IMP_NNV'] = f"{home}/daten/nnv"
    os.environ['DW_DIR_IMP_CARMEN'] = f"{home}/daten/carmen"

    os.environ['GEN_HOME'] = f"{os.environ['DW_DIR_ROOT']}/generator"

    # Placeholder value from shell script; requires user confirmation or replacement with a dynamic source.
    # This should ideally be sourced from a secure configuration management system (e.g., Google Secret Manager)
    # or passed as an environment variable in the execution context.
    # For now, it tries to read from an environment variable 'DW_DIR_CUSTOMER' or defaults to a placeholder.
    os.environ['DW_DIR_CUSTOMER'] = os.getenv('DW_DIR_CUSTOMER', '<REPLACE_ME_CUSTOMER_LOGIN>')
    os.environ['DW_HOST_CUSTOMER'] = "dxcst3.bn.detemobil.de"

    # Set ORACLE_HOME only if not already set, using cloud-appropriate logic.
    # In a cloud environment, this logic would likely be replaced by
    # reading ORACLE_HOME from a configuration file, environment variable,
    # or secrets manager. Direct filesystem checks like this are generally not portable
    # or secure for cloud deployments. If Oracle connectivity is still required,
    # secure connection details should be used.
    oracle_home = os.getenv('ORACLE_HOME')
    if not oracle_home:
        candidate_paths = [
            "/appl/local/oracle/oracle.8.1.6",
            "/appl/local/oracle/7.3.4",
            "/appl/local/oracle/oracle.7.3.3",
            "/appl/local/oracle/7.3.2",
            "/appl/local/oracle/7.2.3",
        ]

        selected_oracle_home = None
        for path in candidate_paths:
            if os.path.isdir(path):
                # Match shell script's assignment behavior for specific versions
                if path == "/appl/local/oracle/oracle.8.1.6":
                    selected_oracle_home = "/appl/local/oracle/8.1.6"
                elif path == "/appl/local/oracle/7.3.4":
                    selected_oracle_home = "/appl/local/oracle/7.3.4"
                elif path == "/appl/local/oracle/oracle.7.3.3":
                    selected_oracle_home = "/appl/local/oracle/7.3.3"
                elif path == "/appl/local/oracle/7.3.2":
                    selected_oracle_home = "/appl/local/oracle/7.3.2"
                elif path == "/appl/local/oracle/7.2.3":
                    selected_oracle_home = "/appl/local/oracle/7.2.3"
                # The original shell script had an 'else' that would exit if no specific
                # Oracle home was found, hence no generic assignment here.
                break # Found the first available Oracle home

        if selected_oracle_home is None:
            # As per the original shell script's behavior, exit if ORACLE_HOME cannot be set.
            print("ERROR: Could not set ORACLE_HOME based on legacy paths! Aborting ..", file=sys.stderr)
            sys.exit(1)
        else:
            os.environ['ORACLE_HOME'] = selected_oracle_home
            print(f"INFO: ORACLE_HOME set to {selected_oracle_home}", file=sys.stderr)

    # Sourced scripts ($HOME/.dw_global, $HOME/.dw_lokal):
    # These scripts need separate migration. Their content should be absorbed into
    # this Python script, a dedicated configuration module/file, or a separate
    # Python script that gets called. If they contain shell commands, those
    # must be translated to Python equivalents or external processes.
    # As their content is unknown and cannot be retrieved in this phase,
    # this step is left for manual analysis and integration.
    print("WARNING: The legacy scripts '$HOME/.dw_global' and '$HOME/.dw_lokal' were not migrated. "
          "Their content needs to be analyzed and integrated if required.", file=sys.stderr)

    # umask is a shell-specific permission setting and has no direct Python/BigQuery equivalent.
    # File permissions in a cloud-native environment should be managed by the underlying
    # cloud storage (e.g., GCS permissions, IAM roles) or by the execution environment's configuration.
    print("INFO: 'umask' setting from the legacy script is not directly translatable to this environment. "
          "File permissions should be managed via cloud-native mechanisms.", file=sys.stderr)

if __name__ == "__main__":
    initialize_environment()