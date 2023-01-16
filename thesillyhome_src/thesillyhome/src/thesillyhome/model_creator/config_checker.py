import thesillyhome.model_creator.read_config_json as tsh_config
import logging


def base_config_checks():
    check_mandatory_fields(
        [
            ("api_key", tsh_config.api_key),
            ("actuators", tsh_config.actuators),
            ("db_options", tsh_config.db_options),
            ("db_password", tsh_config.db_password),
            ("db_database", tsh_config.db_database),
            ("db_username", tsh_config.db_username),
            ("db_type", tsh_config.db_type),
            ("db_host", tsh_config.db_host),
            ("db_port", tsh_config.db_port),
        ]
    )
    check_db(tsh_config.db_type)


def check_db(db_type):
    if db_type not in ["mariadb", "postgres", "sqlite", "mysql"]:
        raise Exception("Make sure your dbtype is either `mariadb` or `postgres`.")


def check_mandatory_fields(mandatory_fields: list):
    for name, field in mandatory_fields:
        if field is None:
            raise KeyError(
                f"Missing Mandatory field {name}, please add this to the config file."
            )



