# Library imports

import logging
import requests
import json
import asyncio
import websockets
from io import BytesIO
import tarfile
import os

# Local application imports
import thesillyhome.model_creator.read_config_json as tsh_config


def download_file(url, folder):
    get_response = requests.get(url, stream=True)
    file_name = url.split("/")[-1].split("?")[0]
    with open(f"{folder}/{file_name}", "wb") as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def untarzip(input_file_path: str, output_file_path: str):
    file = tarfile.open(input_file_path)
    file.extractall(output_file_path)
    file.close()


def model_exists_locally():
    model_path = "/thesillyhome_src/data/model"
    return os.path.isfile(f"{model_path}/model.tar.gz")


def start_training_job(force_train=False):
    """
    This function starts the training pipeline job and returns the model.
    """
    logging.info("<---Starting the training sequence--->")

    if model_exists_locally() and force_train == False:
        logging.info(
            "Model already exists locally and force_train is false. Skipping training job."
        )
    else:

        url = f"{tsh_config.apigateway_endpoint}/user/model/train"
        logging.info(f"-> START training pipeline job from {url}")

        data = {
            "user_id": tsh_config.user["id"],
            "force_train": force_train,
            "actuators": tsh_config.actuators,
        }
        r = requests.post(
            url,
            json=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": tsh_config.api_key,
                "X-Api-Key": tsh_config.api_key,
            },
        )
        if r.status_code != 200:
            logging.info(f"<- Training pipeline job failed")
            raise NotImplementedError

        response_body = r.json()
        logging.info(f"<- Training pipeline job succeeded")

        exisiting_model_url = response_body.get("existing_model", None)
        model_path = "/thesillyhome_src/data/model"

        if exisiting_model_url is not None:
            logging.info("->Existng model exists. Downloading and extracting it.")
            download_file(exisiting_model_url, model_path)
            untarzip(f"{model_path}/model.tar.gz", model_path)
            logging.info("<-Exisitng model downloaded and extracted.")
        else:

            logging.info(
                "Connecting to websocket api to wait for model training to complete..."
            )
            pipeline_execution_arn = response_body["PipelineExecutionArn"]
            # Connect to websocket and wait for response
            async def connect_to_websocket():
                try:
                    headers = {
                        "Authorization": tsh_config.api_key,
                    }
                    async with websockets.connect(
                        f"wss://websock.thesillyhome.ai/?pipeline_execution_arn={pipeline_execution_arn}",
                        extra_headers=headers,
                    ) as websocket:
                        async for message in websocket:
                            if message == "end_transmission":
                                await websocket.close()
                            else:
                                try:
                                    logging.info(
                                        f"Received model under {message}. Downloading and unzipping."
                                    )
                                    download_file(message, model_path)
                                    untarzip(f"{model_path}/model.tar.gz", model_path)
                                    logging.info("Model downloaded and unzipped.")
                                except ValueError:
                                    logging.info("Model is empty. No data in buffer")
                except ConnectionError as e:
                    logging.info("Error connecting to websocket.")

            asyncio.run(connect_to_websocket())

            logging.info("Completed training job. Model saved.")


if __name__ == "__main__":
    start_training_job(force_train=False)
