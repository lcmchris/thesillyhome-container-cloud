# Library imports
import subprocess
import json
import os
import logging
import requests

data_dir = "/thesillyhome_src/data"

if os.environ.get("HA_ADDON") == "true":
    config_file = open(f"/data/options.json")
else:
    config_file = open(f"/thesillyhome_src/data/config/options.json")

options = json.load(config_file)

# Mandatory
api_key = options.get("api_key")
actuators = list(options.get("actuactors_id"))
sensors = list(options.get("sensors_id"))
devices = actuators + sensors
db_options = options.get("db_options")[0]
db_password = db_options.get("db_password")
db_database = db_options.get("db_database")
db_username = db_options.get("db_username")
db_type = db_options.get("db_type")
db_host = db_options.get("db_host")
db_port = db_options.get("db_port")

# Defaults
share_data = options.get("share_data", True)
autotrain = options.get("autotrain", True)
autotrain_cadence = options.get("autotrain_cadence", "0 0 * * 0")
startup_disable_all = options.get("startup_disable_all", False)

# External Endpoints
# apigateway_endpoint = "http://host.docker.internal:3000"
apigateway_endpoint = "https://api.thesillyhome.ai"


# Check the prev last_updated date

def get_user_info(api_key):
    url = f"{apigateway_endpoint}/user"
    logging.info(url)
    r = requests.get(url, headers={"Content-Type":"application/json", "Authorization": api_key, "X-Api-Key": api_key})
    response_body = r.json()

    if response_body.get('user', None) != None: 
        user  = response_body['user']
        user_metadata = response_body['user_metadata']
        logging.info('user: ', user)
    else:
        raise ValueError("Invalid API Key user")

    return user, user_metadata

user, user_metadata = get_user_info(api_key)



# Other helpers
def extract_float_sensors(sensors: list):
    float_sensors_types = ["lux"]
    float_sensors = []
    for sensor in sensors:
        if sensor.split("_")[-1] in float_sensors_types:
            float_sensors.append(sensor)
    return float_sensors


float_sensors = extract_float_sensors(sensors)

output_list_og = ["entity_id", "state"]
output_list = ["entity_id", "state", "last_updated"]
output_list_dup = ["entity_id", "state", "last_updated", "duplicate"]


def replace_yaml():
    if os.environ.get("HA_ADDON") == "true" and options.get("ha_options") == None:
        with open("/thesillyhome_src/appdaemon/appdaemon.yaml", "r") as f:
            content = f.read()
            supervisor_token = os.environ["SUPERVISOR_TOKEN"]
            content = content.replace("<ha_url>", "http://supervisor/core")
            content = content.replace("<ha_token>", f"""{supervisor_token}""")

        with open("/thesillyhome_src/appdaemon/appdaemon.yaml", "w") as file:
            file.write(content)
        return
    else:
        ha_options = options["ha_options"][0]
        ha_url = ha_options["ha_url"]
        ha_token = ha_options["ha_token"]

        with open("/thesillyhome_src/appdaemon/appdaemon.yaml", "r") as f:
            content = f.read()
            content = content.replace("<ha_url>", ha_url)
            content = content.replace("<ha_token>", ha_token)

        with open("/thesillyhome_src/appdaemon/appdaemon.yaml", "w") as file:
            file.write(content)
        return


def run_cron():
    if autotrain == True:
        with open("/thesillyhome_src/startup/crontab", "r") as f:
            content = f.read()
            content = content.replace("<autotrain_cadence>", autotrain_cadence)
        with open("/thesillyhome_src/startup/crontab", "w") as file:
            file.write(content)

        subprocess.run(["crontab", "/thesillyhome_src/startup/crontab"])
        logging.info(f"Runnining cron with cadence {autotrain_cadence}")
        return
    else:
        return
