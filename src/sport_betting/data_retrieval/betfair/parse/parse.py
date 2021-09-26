# coding: utf-8

import bz2
import glob
import os
import tarfile
import zipfile
from unittest.mock import patch

import betfairlightweight
import pandas as pd


# loading from tar and extracting files
from sport_betting.data_retrieval.config.config import APIConfig


def load_markets(file_paths: list):
    for file_path in file_paths:
        if os.path.isdir(file_path):
            for path in glob.iglob(file_path + '**/**/*.bz2', recursive=True):
                f = bz2.BZ2File(path, 'rb')
                yield f, os.path.basename(os.path.dirname(file_path))
                f.close()
        elif os.path.isfile(file_path):
            ext = os.path.splitext(file_path)[1]
            # iterate through a tar archive
            if ext == '.tar':
                with tarfile.TarFile(file_path) as archive:
                    for file in archive:
                        yield bz2.open(archive.extractfile(file)), os.path.basename(os.path.dirname(file_path))
            # or a zip archive
            elif ext == '.zip':
                with zipfile.ZipFile(file_path) as archive:
                    for file in archive.namelist():
                        yield bz2.open(archive.open(file)), os.path.basename(os.path.dirname(file_path))

    return None


def retrieve_game_data(raw_files_directory: str, betfair_trading: betfairlightweight.APIClient) -> dict:
    # Get list of files corresponding to markets
    list_markets = [os.path.join(raw_files_directory, daily_dir, file)
                    for daily_dir in os.listdir(raw_files_directory)
                    for file in os.listdir(os.path.join(raw_files_directory, daily_dir))]

    # Initialize dictionary
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

    for file_obj, daily_dir in load_markets(list_markets):
        stream = betfair_trading.streaming.create_historical_generator_stream(
            file_path=file_obj,
            listener=betfairlightweight.StreamListener(max_latency=None),
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


def preprocess_game_data(data_dict: dict) -> pd.DataFrame:
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
    start_in_play_t = df_game[(df_game["in_play"] == True) & (df_game["market_type"] == "MATCH_ODDS")]["publish_time"].min()
    game_start_time = start_in_play_t.replace(second=0, microsecond=0, minute=0)
    df_game["time_from_game_start"] = df_game["publish_time"] - game_start_time

    return df_game


def parse_game_files(raw_files_directory: str) -> pd.DataFrame:
    # Create session
    api_cfg = APIConfig()
    trading = betfairlightweight.APIClient(username=api_cfg.betfair_user,
                                           password=api_cfg.betfair_pwd,
                                           app_key=api_cfg.betfair_certs,
                                           certs=api_cfg.betfair_certs)

    # Parse files
    games_dict = retrieve_game_data(raw_files_directory=raw_files_directory,
                                    betfair_trading=trading)

    # Preprocess data and convert to pandas DataFrame
    df_preprocessed = preprocess_game_data(data_dict=games_dict)

    return df_preprocessed
