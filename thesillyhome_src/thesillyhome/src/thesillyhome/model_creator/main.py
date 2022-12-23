# Library imports
# Local application imports
from thesillyhome.model_creator.read_config_json import replace_yaml
from thesillyhome.model_creator.read_config_json import run_cron

from thesillyhome.model_creator.home import homedb

from thesillyhome.model_creator.logger import add_logger
from thesillyhome.model_creator.config_checker import base_config_checks
from thesillyhome.model_creator.train_model import start_training_job


if __name__ == "__main__":

    # Setup
    add_logger()
    base_config_checks()
    replace_yaml()

    # Send data to s3
    # homedb().send_data()

    # Train model
    start_training_job()

    # End setup
    run_cron()
