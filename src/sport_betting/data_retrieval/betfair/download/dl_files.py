# coding: utf-8
import asyncio
import datetime
import json
import os
from collections import namedtuple

import betfairlightweight
import requests
import tqdm

from sport_betting.data_retrieval import run_in_executor
from sport_betting.data_retrieval.config.config import APIConfig

DatedListFiles = namedtuple("DatedListFiles", ["date", "file_name"])


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
        "market_types_collection": None,
        "countries_collection": None,
        "file_type_collection": None
    }

    # Get list files
    list_files = None
    while list_files is None:
        try:
            list_files = betfair_trading.historic.get_file_list(**data)
        except (requests.exceptions.Timeout, json.decoder.JSONDecodeError,
                betfairlightweight.exceptions.InvalidResponse, betfairlightweight.exceptions.APIError):
            continue

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
    if not os.path.exists(dl_directory):
        os.makedirs(dl_directory, exist_ok=True)

    # Download file
    filename = None
    while filename is None:
        try:
            filename = betfair_trading.historic.download_file(file_path=file_path.file_name,
                                                              store_directory=dl_directory)
        except (requests.exceptions.Timeout, betfairlightweight.exceptions.APIError):
            continue

    return filename


async def download_betfair_files(day: datetime.datetime, dl_directory: str):
    """
    Download Betfair game day files based on games dataframe
    :param day: game day
    :param dl_directory: local directory where the files will be stored
    :return: list of files downloaded
    """
    # Create session
    api_cfg = APIConfig()
    trading = betfairlightweight.APIClient(username=api_cfg.betfair_user,
                                           password=api_cfg.betfair_pwd,
                                           app_key=api_cfg.betfair_api_key,
                                           certs=api_cfg.betfair_certs)
    trading.login()

    # Get list of files to DL
    list_files = get_files_to_dl_day(day, trading)

    # Download files
    list_dl_tasks = [asyncio.create_task(download_file(file, dl_directory, trading)) for file in list_files]
    files_downloaded = [await t for t in tqdm.tqdm(asyncio.as_completed(list_dl_tasks), total=len(list_dl_tasks))]

    trading.logout()

    return files_downloaded
