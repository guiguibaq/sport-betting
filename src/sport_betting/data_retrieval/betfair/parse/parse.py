# coding: utf-8
import bz2
import gc
import multiprocessing
import os
import re
from collections import namedtuple
from datetime import datetime
from functools import partial
from heapq import heapreplace
from typing import List
from unittest.mock import patch

import betfairlightweight
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from betfairlightweight.endpoints import Streaming

MarketFile = namedtuple("MarketFile", ("path", "size"))


def create_equal_size_chunk(list_market_files: List[MarketFile], n_chunks: int, sort=True) -> List[List[str]]:
    """
    Separate files into chunks of roughly equal sizes (as in total file sizes)
    :param list_market_files: original list of files
    :param n_chunks: number of chunks to be created
    :param sort: boolean to sort files by size if not done previously
    :return: list of chunks
    """
    bins = [[0] for _ in range(n_chunks)]
    if sort:
        list_market_files = sorted(list_market_files, key=lambda x: x.size)
    for market_file in list_market_files:
        least = bins[0]
        least[0] += market_file.size
        least.append(market_file)
        heapreplace(bins, least)
    bins = [x[1:] for x in bins]
    return [[market_file.path for market_file in bin] for bin in bins]


def pd_df_to_parquet(df: pd.DataFrame, path_save: str):
    """
    Write pandas dataframe to parquet file
    :param df: pandas dataframe
    :param path_save: saving path
    :return:
    """
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=path_save, compression='snappy')


def load_markets(file_paths: list) -> (bz2.BZ2File, str):
    """
    Decompress Betfair market file
    :param file_paths: list of file paths
    :return:
    """
    for file_path in [file_path for file_path in file_paths if isinstance(file_path, str)]:
        if re.search(r"\.bz2$", file_path):
            f = bz2.BZ2File(file_path, 'rb')
            yield f, os.path.basename(os.path.dirname(file_path))


def retrieve_game_data(market_paths: list) -> List[dict]:
    """
    Parse market data from Betfair
    :param market_paths: list of Betfair market files
    :return: list of parsed dictionaries
    """
    # Instantiate list of dicts
    dict_list = list()

    for file_obj, game_day in load_markets(market_paths):
        stream = Streaming.create_historical_generator_stream(
            file_path=file_obj,
            listener=betfairlightweight.StreamListener(max_latency=None),
        )

        with patch("builtins.open", lambda f, _: f):
            gen = stream.get_generator()
            try:
                for market_books in gen():
                    for market_book in market_books:
                        for runner_idx in range(len(market_book.runners)):
                            data_dict = {
                                "event_name": market_book.market_definition.event_name,
                                "event_id": market_book.market_definition.event_id,
                                "market_type": market_book.market_definition.market_type,
                                "market_time": market_book.market_definition.market_time,
                                "open_date": market_book.market_definition.open_date,
                                "market_id": market_book.market_id,
                                "publish_time": market_book.publish_time,
                                "runner_name": market_book.market_definition.runners[runner_idx].name,
                                "ltp": market_book.runners[runner_idx].last_price_traded,
                                "total_matched": market_book.runners[runner_idx].total_matched,
                                "in_play": market_book.inplay,
                                "game_day": datetime.strptime(game_day, "%Y%m%d").date()
                            }
                            dict_list.append(data_dict)
            except OSError:
                pass

    return dict_list


def process_chunk(chunk: List[str], path_save: str):
    """
    Parse Betfair market data and write as parquet file
    :param chunk: list of Betfair files
    :param path_save: saving path
    :return:
    """
    dict_list = retrieve_game_data(chunk)
    df_games = pd.DataFrame(dict_list)
    df_games = df_games[~df_games.event_id.isnull()]
    del dict_list

    pd_df_to_parquet(df=df_games, path_save=path_save)
    del df_games

    gc.collect()


def parse_game_files(raw_files_directory: str, path_save: str):
    """
    Parse Betfair market files and save as parquet file
    :param raw_files_directory: directory used to store Betfair files
    :param path_save: saving path
    :return:
    """
    # Get list of files corresponding to markets
    daily_dirs = [os.path.join(raw_files_directory, daily_dir)
                  for daily_dir in os.listdir(raw_files_directory)
                  if os.path.isdir(os.path.join(raw_files_directory, daily_dir))]
    list_market_files = [os.path.join(daily_dir, file)
                         for daily_dir in daily_dirs
                         for file in os.listdir(os.path.join(raw_files_directory, daily_dir))]
    list_market_files = [MarketFile(path, os.path.getsize(path)) for path in list_market_files]

    # Chunk to process files in multiple iterations
    chunked_files = create_equal_size_chunk(list_market_files=list_market_files,
                                            n_chunks=int(len(list_market_files) / 100) + 1)

    # Parse files
    f = partial(process_chunk, path_save=path_save)
    try:
        pool = multiprocessing.Pool(processes=os.cpu_count() - 2)
        pool.map(f, chunked_files)
    finally:
        pool.close()
        pool.join()
