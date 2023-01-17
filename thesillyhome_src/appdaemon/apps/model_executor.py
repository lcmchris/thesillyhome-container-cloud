# Library imports
import appdaemon.plugins.hass.hassapi as hass
import pickle
import pandas as pd
import datetime
import sqlite3 as sql
import pytz
import numpy as np
import json
import tarfile

import tflite_runtime.interpreter as tflite

# Local application imports
import thesillyhome.model_creator.read_config_json as tsh_config


class ModelExecutor(hass.Hass):
    def initialize(self):
        self.model_version = 1
        self.model_path = f"/thesillyhome_src/data/model/{self.model_version}"
        self.input_columns = list(
            pd.read_parquet(f"{self.model_path}/X_train_entity_ids.parquet")[
                "entity_ids"
            ]
        )
        (
            self.interpreter,
            self.output_columns,
            self.input,
            self.output,
        ) = self.load_model(f"{self.model_path}/model.tflite")

        self.prediction_flow()
        self.handle = self.listen_state(self.state_handler)

    def load_model(self, model_path):
        """
        Loads all models to a dictionary
        """
        # Load TFLite model and allocate tensors.

        interpreter = tflite.Interpreter(model_path=model_path)
        interpreter.allocate_tensors()

        input = interpreter.get_input_details()[0]
        output = interpreter.get_output_details()[0]

        interpreter.allocate_tensors()
        print("Model Interpreter loaded successfully.")

        output_columns = list(
            pd.read_parquet(f"{self.model_path}/Y_train_entity_ids.parquet")[
                "entity_ids"
            ]
        )

        return interpreter, output_columns, input, output

    def prediction_flow(self):
        start = datetime.datetime.now()
        all_states_dict = self.get_state()
        df_states = (
            pd.DataFrame.from_dict(all_states_dict, orient="index")
            .reset_index()
            .rename(columns={"index": "state_id"})
        )

        df_all = df_states.loc[:, ["state_id", "entity_id", "state", "last_updated"]]
        df_all["state"] = df_all["state"].replace(["off", "unavailable", "unknown"], 0)
        df_all["state"] = df_all["state"].fillna(0)
        df_all["state"] = df_all["state"].replace(["on"], 1)
        df_sen_states = self.get_current_states(df_all).reset_index(drop=True).tail(1)

        out_list = []
        for column in df_sen_states.columns:
            try:
                out_list.append(df_sen_states[column].astype(np.int8))
            except:
                try:
                    out_list.append(df_sen_states[column].astype(np.float32))
                except:
                    try:
                        dt_tmp = pd.to_datetime(df_sen_states[column])
                        out_list.append(
                            dt_tmp.dt.hour.rename(f"{column}_hour").astype(np.int8)
                        )
                        out_list.append(
                            dt_tmp.dt.date.apply(lambda x: x.weekday())
                            .rename(f"{column}_day")
                            .astype(np.int8)
                        )

                    except:
                        out_list.append(df_sen_states[column])
                        continue

        df_sen_states = pd.concat(out_list, axis=1)
        df_sen_states = pd.get_dummies(df_sen_states).reset_index(drop=True)

        input_data = pd.DataFrame(columns=self.input_columns)
        input_data.loc[0] = 0.0

        extra_columns = set(df_sen_states.columns) - set(input_data.columns)
        df_sen_states = df_sen_states.drop(columns=extra_columns)

        input_data = pd.merge(
            df_sen_states, input_data, how="left", suffixes=("", "_Temp")
        )
        input_data = input_data.fillna(0)
        input_data = input_data.astype(np.float32)
        end = datetime.datetime.now()
        print(f"time used {end - start}")

        self.get_predictions(input_data)

    def get_predictions(self, input_data):
        self.interpreter.set_tensor(self.input["index"], input_data)
        self.interpreter.invoke()
        prediction = self.interpreter.get_tensor(self.output["index"])
        self.convert_prediction_to_actions(prediction)

    def convert_prediction_to_actions(self, prediction, threshold: float = 0.5):
        prediction_lookup = self.output_columns
        prediction = prediction[0]
        for x, y in zip(prediction, prediction_lookup):
            if x >= threshold:
                self.log(f"Prediction {x} greater than {threshold}")
                self.log(f"Turning on {y}")
                self.turn_on(y)
                pass
            elif x < threshold:
                self.log(f"Prediction {x} less than {threshold}")
                self.log(f"Turning off {y}")
                self.turn_off(y)

        return True

    def get_current_states(self, df_parsed: pd.DataFrame) -> pd.DataFrame:
        """
        Returns pivoted frame of each state id desc
        """
        df_pivot = df_parsed.pivot(
            index=["state_id", "last_updated"], columns=["entity_id"], values=["state"]
        ).sort_values(by="state_id", ascending=False)
        df_pivot.columns = df_pivot.columns.droplevel(0)
        df_pivot = df_pivot.fillna(method="bfill").fillna(method="ffill")
        return df_pivot

    def state_handler(self, entity, attribute, old, new, kwargs):
        # Update input data
        if entity in self.input_columns or f"{entity}_{new}" in self.input_columns:
            print(f"Entity: {entity} is {new}")
            self.prediction_flow()

    def parse_date(self, date=datetime.datetime.now()):
        hour = date.hour
        weekday = date.weekday()
        return hour, weekday
