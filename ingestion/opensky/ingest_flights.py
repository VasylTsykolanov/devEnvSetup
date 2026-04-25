# Databricks notebook source
# This pipeline ingest flights data from OpenSky API and store it in json file
import sys
import subprocess
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

import subprocess
repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip()
sys.path.insert(0, repo_root)

from common.utils import configure_logging

logger = configure_logging()

default_config = {
    "unity_catalog": 'data_dev',
    "storage": 'texelstgdev',
    "schema": 'raw',
    "volume": 'opensky'
}

try:

    logger.info("Ingesting opensky files into raw volume...")

    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS
        {default_config['unity_catalog']}.{default_config['schema']}.{default_config['volume']}
        LOCATION 'abfss://{default_config["schema"]}@{default_config["storage"]}.dfs.core.windows.net/{default_config["volume"]}/'
    """)

    # Get current date parts
    now = datetime.now(ZoneInfo("Europe/Paris"))
    year  = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    volume_path = f"/Volumes/{default_config['unity_catalog']}/{default_config['schema']}/{default_config['volume']}"
    file_path = f"{volume_path}/{year}/{month}/flights_day_{day}.json"

    COLUMNS = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

    opensky_url = "https://opensky-network.org/api/states/all"
    PARAMS = {
        # "lamin": 49.0, "lomin": -11.3, "lamax": 61.0, "lomax": 2.0,  # UK example
    }
    response = requests.get(opensky_url, params = PARAMS, timeout=30)
    response.raise_for_status()
    payload = response.json()

    records = [
        {**dict(zip(COLUMNS, state)), "api_timestamp": now.isoformat()}
        for state in payload.get("states", [])
        if state is not None
    ]

    dbutils.fs.put(file_path, json.dumps(records, indent=2, default=str), overwrite=True)

except Exception as e:
    logger.error("Error ingesting OpenSky data: %s", str(e))
    raise

logger.info("Successfully ingested OpenSky data. Pipeline completed.")