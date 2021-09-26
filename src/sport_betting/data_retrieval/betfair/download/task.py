# coding: utf-8
import asyncio
import os

import luigi
import pandas as pd

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.download.dl_files import download_betfair_files
from sport_betting.data_retrieval.games.task import TaskGamesRetrieval


class TaskDownloadFiles(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        reqs = {
            "games": TaskGamesRetrieval(competition_id=self.competition_id, year=self.year)
        }

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "betfair", "raw_files")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        df_games = pd.read_parquet(self.input()['games'].path)

        with self.output().temporary_path() as tmp_path:
            os.makedirs(tmp_path)

            loop = asyncio.get_event_loop()
            dls = loop.run_until_complete(download_betfair_files(df_games=df_games,
                                                                 dl_directory=tmp_path)
                                          )
