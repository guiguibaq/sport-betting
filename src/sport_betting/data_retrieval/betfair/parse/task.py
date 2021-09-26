# coding: utf-8
import os

import luigi

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.download.task import TaskDownloadFiles
from sport_betting.data_retrieval.betfair.parse.parse import parse_game_files


class TaskParseFiles(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        reqs = {
            "raw_files": TaskDownloadFiles(competition_id=self.competition_id, year=self.year)
        }
        return reqs

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "betfair", "parsed_games.parquet")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        with self.output().temporary_path() as tmp_path:
            df_preprocessed = parse_game_files(raw_files_directory=self.input()['raw_files'].path)

            df_preprocessed.to_parquet(tmp_path)
