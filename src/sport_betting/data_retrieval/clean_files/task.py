# coding: utf-8
import os
import subprocess

import luigi
from luigi.mock import MockTarget

from sport_betting import DATA_DIR
from sport_betting.data_retrieval.betfair.event_matching.task import TaskMatchingEvents
from sport_betting.data_retrieval.betfair.markets.task import TaskMarkets


class TaskCleanFiles(luigi.Task):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def output(self):
        output_path = os.path.join(DATA_DIR, self.competition_id, str(self.year), "clean_files")
        return MockTarget(output_path)

    def requires(self):
        yield TaskMarkets(year=self.year, competition_id=self.competition_id)
        yield TaskMatchingEvents(year=self.year, competition_id=self.competition_id)

    def run(self):
        data_dir = os.path.dirname(self.output().path)

        # Delete all temporary files directories
        for directory in [os.path.join(data_dir, el) for el in os.listdir(data_dir)
                          if os.path.isdir(os.path.join(data_dir, el))]:
            subprocess.call(['rm', '-r', directory])

        with self.output().temporary_path() as tmp_path:
            with open(tmp_path, 'w') as writer:
                writer.write("Cleaning done :)")
