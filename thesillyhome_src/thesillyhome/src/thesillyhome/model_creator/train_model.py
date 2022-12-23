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


def untarzip(self, input_file_path:str, output_file_path:str):
    file = tarfile.open(input_file_path)
    file.extractall(output_file_path)
    file.close()


def model_exists():
    model_path = "/thesillyhome_src/data/model"
    return os.path.isfile(f"{model_path}/model.tar.gz")


def start_training_job(force_train=False):
    if model_exists():
        logging.info("Model already exists. Skipping training job.")
    # Start the training job
    else:
        logging.info("Start training job")
        url = f"{tsh_config.apigateway_endpoint}/user/model/train"

        data = {"user_id": tsh_config.user["id"], "force_train": force_train}
        r = requests.post(
            url,
            json=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": tsh_config.api_key,
                "X-Api-Key": tsh_config.api_key,
            },
        )
        logging.info(r.content)
        logging.info(r.status_code)
        if r.status_code !=200:
            raise NotImplementedError
        
        response_body = r.json()
        logging.info(response_body)
        exisiting_model_url = response_body.get('existing_model',None)
        model_path = "/thesillyhome_src/data/model"
        print (exisiting_model_url)
        if exisiting_model_url is not None:
            download_file(exisiting_model_url, model_path)
            untarzip(f"{model_path}/model.tar.gz",model_path)
            logging.info('Exisitng model downloaded and extracted.')
        else:
            pipeline_execution_arn = response_body["PipelineExecutionArn"]
            # Connect to websocket and wait for response
            async def connect_to_websocket():
                try:
                    async with websockets.connect(
                        f"wss://websock.thesillyhome.ai/?pipeline_execution_arn={pipeline_execution_arn}"
                    ) as websocket:
                        async for message in websocket:
                            if message == "end_transmission":
                                await websocket.close()
                            else:
                                try:
                                    print(f"Received pre-signed url : {message}")
                                    download_file(message, model_path)
                                    untarzip(f"{model_path}/model.tar.gz",model_path)

                                except ValueError:
                                    print("No data in buffer")
                except TimeoutError as e:
                    print("Error connecting to websocket.")

            asyncio.run(connect_to_websocket())

            logging.info("Completed training job. Model saved.")


if __name__ == "__main__":
    start_training_job(force_train=False)
