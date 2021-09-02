from typing import List
import os
import glob
import tarfile
import bz2
import betfairlightweight
from betfairlightweight import filters
import pandas as pd
import numpy as np
import datetime
import json
from unittest.mock import patch
import matplotlib.pyplot as plt
from typing import Tuple, List, Dict
from scipy.stats import poisson


# Change this certs path to wherever you're storing your certificates
certs_path = '/Users/guillaume_baquiast/Documents/tmp'

# Change these login details to your own
my_username = "your_email"
my_password = "your_password"
my_app_key = "your_app_key"

trading = betfairlightweight.APIClient(username=my_username,
                                       password=my_password,
                                       app_key=my_app_key,
                                       certs=certs_path)

listener = betfairlightweight.StreamListener(max_latency=None)

# loading from tar and extracting files
def load_markets(file_paths: List[str]):
    for file_path in file_paths:
        if os.path.isdir(file_path):
            for path in glob.iglob(file_path + '**/**/*.bz2', recursive=True):
                f = bz2.BZ2File(path, 'rb')
                yield f
                f.close()
        elif os.path.isfile(file_path):
            ext = os.path.splitext(file_path)[1]
            # iterate through a tar archive
            if ext == '.tar':
                with tarfile.TarFile(file_path) as archive:
                    for file in archive:
                        yield bz2.open(archive.extractfile(file))
            # or a zip archive
            elif ext == '.zip':
                with zipfile.ZipFile(file_path) as archive:
                    for file in archive.namelist():
                        yield bz2.open(archive.open(file))

    return None


def retrieve_game_data(market_paths: List) -> Dict:
    data_dict = {
        "event_name": [],
        "event_id": [],
        "market_type": [],
        "market_time": [],
        "open_date": [],
        "market_id": [],
        "publish_time": [],
        "runner_name": [],
        "ltp": [],
        "total_matched": [],
        "in_play": [],
    }

    for file_obj in load_markets(market_paths):
        stream = trading.streaming.create_historical_generator_stream(
            file_path=file_obj,
            listener=listener,
        )

        with patch("builtins.open", lambda f, _: f):
            gen = stream.get_generator()

            for market_books in gen():
                for market_book in market_books:
                    for runner_idx in range(len(market_book.runners)):
                        data_dict["event_name"].append(market_book.market_definition.event_name)
                        data_dict["event_id"].append(market_book.market_definition.event_id)
                        data_dict["market_type"].append(market_book.market_definition.market_type)
                        data_dict["market_time"].append(market_book.market_definition.market_time)
                        data_dict["open_date"].append(market_book.market_definition.open_date)
                        data_dict["market_id"].append(market_book.market_id)
                        data_dict["publish_time"].append(market_book.publish_time)
                        data_dict["runner_name"].append(market_book.market_definition.runners[runner_idx].name)
                        data_dict["ltp"].append(market_book.runners[runner_idx].last_price_traded)
                        data_dict["total_matched"].append(market_book.runners[runner_idx].total_matched)
                        data_dict["in_play"].append(market_book.inplay)

    return data_dict


def preprocess_game_data(data_dict: Dict) -> pd.DataFrame:
    # Create dataframe from data dict
    df_game = pd.DataFrame(data_dict).drop_duplicates()

    # Ensure to have all information at each tick
    df_time_ids = df_game[["publish_time"]].drop_duplicates()
    df_market_ids = df_game[["market_type", "runner_name"]].drop_duplicates()

    df_time_ids["key"] = 1
    df_market_ids["key"] = 1

    df_ids = df_time_ids.merge(df_market_ids, on="key", how="inner").drop(columns="key")

    df_game = df_ids.merge(df_game, on=["publish_time", "market_type", "runner_name"], how="left")

    # Sort and fill na
    df_game = df_game.sort_values(["market_type", "runner_name", "publish_time"])
    df_game["ltp"] = df_game.groupby(["market_type", "runner_name"])["ltp"].fillna(method="ffill")

    # Add odd and proba
    df_game["odd"] = df_game["ltp"] - 1
    df_game["proba"] = 1 / df_game["ltp"]

    # Add time from game
    start_in_play_t = df_game[(df_game["in_play"] == True) & (df_game["market_type"]=="MATCH_ODDS")]["publish_time"].min()
    game_start_time = start_in_play_t.replace(second=0, microsecond=0, minute=0)
    df_game["time_from_game_start"] = df_game["publish_time"] - game_start_time

    return df_game
