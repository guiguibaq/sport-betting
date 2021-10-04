# coding: utf-8
import os

import luigi
import pandas as pd

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.event_matching.task import TaskMatchingEvents
from sport_betting.data_retrieval.betfair.parse.task import TaskParseFiles


class TaskMarkets(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        reqs = {
            "games": TaskMatchingEvents(competition_id=self.competition_id, year=self.year),
            "parsed_files": TaskParseFiles(competition_id=self.competition_id, year=self.year),
        }
        return reqs

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "event_markets.parquet")
        return luigi.LocalTarget(path=output_path)

    def run(self):
        # Load games dataframe and get event_ids
        df_games = pd.read_parquet(self.input()['games'].path)
        list_event_ids = [event_id for event_id in df_games['event_id'].unique() if event_id]

        # Load parsed files
        list_df = list()
        for file in os.listdir(self.input()['parsed_files'].path):
            df = pd.read_parquet(os.path.join(self.input()['parsed_files'].path, file))
            df = df[df['event_id'].isin(list_event_ids)]
            list_df.append(df)
        df_relevant_events = pd.concat(list_df, axis=0, ignore_index=True).drop_duplicates()

        with self.output().temporary_path() as tmp_path:
            df_relevant_events.to_parquet(tmp_path)
