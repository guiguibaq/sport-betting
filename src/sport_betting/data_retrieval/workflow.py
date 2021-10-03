# coding: utf-8

import luigi

from sport_betting.data_retrieval.betfair.markets.task import TaskMarkets


class WorkflowDataRetrieval(luigi.WrapperTask):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        yield TaskMarkets(year=self.year, competition_id=self.competition_id)
