# coding: utf-8
import os

import luigi
import pandas as pd

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.event_matching.match import matching_events
from sport_betting.data_retrieval.betfair.parse.task import TaskParseFiles
from sport_betting.data_retrieval.games.task import TaskGamesRetrieval


class TaskMatchingEvents(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        reqs = {
            "games": TaskGamesRetrieval(competition_id=self.competition_id, year=self.year),
            "parsed_files": TaskParseFiles(competition_id=self.competition_id, year=self.year),
        }
        return reqs

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "event_games.parquet")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        # Load games dataframe
        df_games = pd.read_parquet(self.input()['games'].path)

        # Load parsed files
        list_df = list()
        for file in os.listdir(self.input()['parsed_files'].path):
            df = pd.read_parquet(os.path.join(self.input()['parsed_files'].path, file))
            df = df[['event_id', 'event_name', 'game_day']].drop_duplicates()
            list_df.append(df)
        df_betfair_events = pd.concat(list_df, axis=0, ignore_index=True).drop_duplicates()

        # Match events
        df_games_events = matching_events(df_games=df_games,
                                          df_betfair_events=df_betfair_events)

        with self.output().temporary_path() as tmp_path:
            df_games_events.to_parquet(tmp_path)
