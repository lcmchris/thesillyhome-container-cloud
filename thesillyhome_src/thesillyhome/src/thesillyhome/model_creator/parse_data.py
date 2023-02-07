# Library imports
from datetime import date, datetime, timezone
from uuid import uuid4
import mysql.connector
import psycopg2
import pandas as pd
import os.path
import os
import logging
from sqlalchemy import create_engine
import bcrypt
import json
import requests
from io import BytesIO
from dateutil import parser

# Local application imports
import thesillyhome.model_creator.read_config_json as tsh_config
import thesillyhome.model_creator.config_checker as config_checker


"""
Get data from DB and store locally
"""


class homedb:
    def __init__(self):
        self.host = tsh_config.db_host
        self.port = tsh_config.db_port
        self.username = tsh_config.db_username
        self.password = tsh_config.db_password
        self.database = tsh_config.db_database
        self.db_type = tsh_config.db_type
        self.mydb = self.connect_internal_db()
        (
            self.ha_schema,
            self.last_changed_string,
            self.last_updated_string,
        ) = self.check_schema()

    def check_schema(self):
        desc_query = f"DESCRIBE homeassistant.states;"
        with self.mydb.connect() as con:
            con = con.execution_options(stream_results=True)
            desc_schema = pd.read_sql(
                desc_query,
                con=con,
            )
            if all(
                x in desc_schema["Field"].unique()
                for x in ["last_changed_ts", "last_updated_ts"]
            ):
                return "new", "last_changed_ts", "last_updated_ts"
            else:
                return "old", "last_changed", "last_updated"

    def connect_internal_db(self):
        if self.db_type == "postgres":
            mydb = create_engine(
                f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
                echo=False,
            )
        elif self.db_type == "mariadb" or self.db_type == "mysql":
            mydb = create_engine(
                f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
                echo=False,
            )
        elif self.db_type == "sqlite":
            mydb = create_engine(
                f"sqlite:////config/{self.database}",
                echo=False,
            )
        else:
            raise Exception(f"Invalid DB type : {self.db_type}.")
        return mydb

    def send_data(self) -> pd.DataFrame:

        logging.info("<---Starting the parsing sequence--->")
        ## Check schema. https://developers.home-assistant.io/blog/2023/01/02/db-schema-v32/

        # Get the last_updated_date to verify if we need to do any uploading.
        logging.info("Querying data from internal homeassistant db.")
        query = f"SELECT \
                    {self.last_updated_string} \
                from states ORDER BY {self.last_updated_string} DESC LIMIT 1;"
        with self.mydb.connect() as con:
            res = con.execute(query)
            if self.ha_schema == "new":
                new_last_updated_date = datetime.fromtimestamp(
                    res.scalar_one()
                ).replace(tzinfo=timezone.utc)
            else:
                new_last_updated_date = res.scalar_one().replace(tzinfo=timezone.utc)

        prev_last_updated_date = parser.parse(
            tsh_config.user_metadata["last_updated_date"]
        )
        user_id = tsh_config.user["id"]

        if new_last_updated_date > prev_last_updated_date:
            logging.info("Uploading data to The Silly Home in chunks.")
            query = f"SELECT \
                        state_id,\
                        entity_id  ,\
                        state  ,\
                        {self.last_changed_string}  ,\
                        {self.last_updated_string}  ,\
                        old_state_id, \
                        attributes_id \
                    from states where {self.last_updated_string} > '{prev_last_updated_date}';"

            unique_actuators = set()

            with self.mydb.connect() as con:
                con = con.execution_options(stream_results=True)
                for df in pd.read_sql(
                    query,
                    con=con,
                    # index_col="state_id",
                    parse_dates=[self.last_changed_string, self.last_updated_string],
                    chunksize=25000,
                ):
                    if self.ha_schema == "new":
                        df["last_updated"] = pd.to_datetime(
                            df[self.last_updated_string], unit="ns"
                        )
                        df["last_changed"] = pd.to_datetime(
                            df[self.last_changed_string], unit="ns"
                        )

                    outputKey = f"{uuid4()}.parquet"
                    url = f"{tsh_config.apigateway_endpoint}/user/states/{user_id}/{outputKey}"
                    unique_actuators = unique_actuators.union(
                        set(df["entity_id"].unique())
                    )

                    # This was done instead of using pyarrow as rasphberrypi has troubles with pyarrow.
                    # Fastparquet also cannot read directly into bytes as the file gets closed hence the use of tmp file.
                    tmp_file = "/tmp/tmp.parquet"
                    df.to_parquet(tmp_file, engine="fastparquet", index=False)
                    with open(tmp_file, mode="rb") as file_data:
                        r = requests.put(
                            url,
                            data=file_data,
                            headers={
                                "Content-Type": "application/binary",
                                "Authorization": tsh_config.api_key,
                            },
                        )
                        if r.status_code != 200:
                            raise ConnectionAbortedError(logging.info(r))

                        # Update last_updated_date so we dont upload the same chunk
                        logging.debug("Updating user last_update date")
                        data = {"last_updated_date": str(new_last_updated_date)}
                        url = f"{tsh_config.apigateway_endpoint}/user"
                        r = requests.put(
                            url,
                            json=data,
                            headers={
                                "Content-Type": "application/json",
                                "Authorization": tsh_config.api_key,
                                "X-Api-Key": tsh_config.api_key,
                            },
                        )
                        if r.status_code != 200:
                            raise ConnectionAbortedError(
                                "Issues with connection to data API."
                            )

        print("Completed data upload")
        return True
