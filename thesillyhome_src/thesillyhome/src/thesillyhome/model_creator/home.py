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
        self.from_cache = False
        self.mydb = self.connect_internal_db()

    def connect_internal_db(self):
        if not self.from_cache:
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
        else:
            return None

    def send_data(self) -> pd.DataFrame:
        # Get the new last_updated date
        logging.info("Querying data from internal homeassistant db.")
        query = f"SELECT \
                    last_updated \
                from states ORDER BY last_updated DESC LIMIT 1;"
        with self.mydb.connect() as con:
            res = con.execute(query)
            new_last_updated_date = res.scalar_one().replace(tzinfo=timezone.utc)

        prev_last_updated_date = parser.parse(
            tsh_config.user_metadata["last_updated_date"]
        )
        user_id = tsh_config.user["id"]

        logging.info(f"Uploading data for {user_id}")

        # Query all new data
        if new_last_updated_date > prev_last_updated_date:
            query = f"SELECT \
                        state_id,\
                        entity_id  ,\
                        state  ,\
                        last_changed  ,\
                        last_updated  ,\
                        old_state_id, \
                        attributes_id \
                    from states where last_updated > '{prev_last_updated_date}';"

            #
            logging.info("Uploading data to The Silly Home")
            with self.mydb.connect() as con:
                con = con.execution_options(stream_results=True)
                for df in pd.read_sql(
                    query,
                    con=con,
                    # index_col="state_id",
                    parse_dates=["last_changed", "last_updated"],
                    chunksize=100000,
                ):
                    # if not (len(df) < 100000):
                    outputKey = f"{uuid4()}.parquet"
                    url = f"{tsh_config.apigateway_endpoint}/user/states/{user_id}/{outputKey}"
                    buffer = BytesIO()

                    df.to_parquet(buffer, engine="pyarrow", index=False)
                    buffer.seek(0)
                    r = requests.put(
                        url,
                        data=buffer.read(),
                        headers={
                            "Content-Type": "application/binary",
                            "Authorization": tsh_config.api_key,
                        },
                    )
                    if r.status_code != 200:
                        raise ConnectionAbortedError
                    buffer.close()

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
                        raise ConnectionAbortedError("Issues with connection to data API.")
        print("Completed data upload")
        return True
