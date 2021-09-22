# coding: utf-8
import os

import luigi

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.config.config import APIConfig
from sport_betting.data_retrieval.games.games import get_list_matches


class TaskGamesRetrieval(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "football-data", "games_list.parquet")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        api_token = APIConfig().football_data_token

        df_games = get_list_matches(api_token=api_token,
                                    competition_id=self.competition_id,
                                    year=self.year)

        with self.output().temporary_path() as tmp_path:
            df_games.to_parquet(tmp_path)
