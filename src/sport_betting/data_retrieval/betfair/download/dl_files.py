# coding: utf-8
import asyncio
import datetime
import functools
import os
from collections import namedtuple

import betfairlightweight
from pandas import DataFrame

from sport_betting.data_retrieval.config.config import APIConfig

DatedListFiles = namedtuple("DatedListFiles", ["date", "file_name"])


def run_in_executor(f):
    @functools.wraps(f)
    async def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: f(*args, **kwargs))

    return inner


def get_list_game_days(df_games: DataFrame) -> list:
    """
    Get list of game days
    :param df_games: games dataframe
    :return: list of game days
    """
    # Get list of game days from dataframe
    list_dates = df_games['date'].drop_duplicates().tolist()

    return list_dates


@run_in_executor
def get_files_to_dl_day(game_day: datetime.datetime, betfair_trading: betfairlightweight.APIClient) -> list:
    """
    Retrieve list of files to download for a specific game day
    :param game_day: date
    :param betfair_trading: instance of the betfair trading API
    :return: list of files to be downloaded
    """
    # Define date used for the request
    data = {
        "sport": "Soccer",
        "plan": "Basic Plan",
        "from_day": game_day.day,
        "from_month": game_day.month,
        "from_year": game_day.year,
        "to_day": game_day.day,
        "to_month": game_day.month,
        "to_year": game_day.year,
        "event_id": None,
        "event_name": None,
        "market_types_collection": [],
        "countries_collection": ["GB"],
        "file_type_collection": []
    }

    # Get list files
    list_files = betfair_trading.historic.get_file_list(**data)

    return [DatedListFiles(game_day.strftime('%Y%m%d'), file) for file in list_files]


@run_in_executor
def download_file(file_path: DatedListFiles, dl_directory: str, betfair_trading: betfairlightweight.APIClient):
    """
    Download file from betfair
    :param file_path: Betfair file path
    :param dl_directory: local directory where the file will be stored
    :param betfair_trading: instance of the betfair trading API
    :return:
    """
    # Create directory path
    directory_path = os.path.join(dl_directory, file_path.date)

    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

    # Download file
    betfair_trading.historic.download_file(file_path=file_path.file_name,
                                           store_directory=directory_path)


async def download_betfair_files(df_games: DataFrame, dl_directory: str):
    """
    Download Betfair game day files based on games dataframe
    :param df_games: games dataframe
    :param dl_directory: local directory where the files will be stored
    :return:
    """
    # Get list of game days
    list_game_days = get_list_game_days(df_games=df_games)

    # Create session
    api_cfg = APIConfig()
    trading = betfairlightweight.APIClient(username=api_cfg.betfair_user,
                                           password=api_cfg.betfair_pwd,
                                           app_key=api_cfg.betfair_api_key,
                                           certs=api_cfg.betfair_certs)
    trading.login()

    # Get list of files to DL
    list_files_tasks = [asyncio.create_task(get_files_to_dl_day(day, trading)) for day in list_game_days]
    list_files_nested = [await t for t in asyncio.as_completed(list_files_tasks)]

    # Unnest list of files
    list_files = [item for sublist in list_files_nested for item in sublist]

    # Download files
    list_dl_tasks = [asyncio.create_task(download_file(file, dl_directory, trading)) for file in list_files]
    results = [await t for t in asyncio.as_completed(list_dl_tasks)]

    trading.logout()
