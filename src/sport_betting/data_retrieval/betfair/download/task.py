# coding: utf-8
import asyncio
import os
import subprocess

import luigi
import pandas as pd

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.download.dl_files import download_betfair_files
from sport_betting.data_retrieval.games.task import TaskGamesRetrieval


class TaskDownloadSingleDay(luigi.Task):
    directory = luigi.Parameter()
    game_day = luigi.DateSecondParameter()

    def output(self):
        output_path = os.path.join(self.directory, self.game_day.strftime('%Y%m%d'))
        return luigi.LocalTarget(path=output_path)

    def run(self):
        with self.output().temporary_path() as tmp_path:
            asyncio.run(download_betfair_files(day=self.game_day,
                                               dl_directory=tmp_path)
                        )


class TaskDownloadFiles(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        reqs = {
            "games": TaskGamesRetrieval(competition_id=self.competition_id, year=self.year)
        }
        return reqs

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "betfair", "raw_files", "download.done")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        df_games = pd.read_parquet(self.input()['games'].path)

        # Get list of dates
        list_dates = df_games['date'].drop_duplicates().tolist()

        # Create dir export
        dir_export = os.path.dirname(self.output().path)
        os.makedirs(dir_export, exist_ok=True)

        for date in list_dates:
            yield TaskDownloadSingleDay(directory=dir_export, game_day=date)

        subprocess.call(['touch', self.output().path])
