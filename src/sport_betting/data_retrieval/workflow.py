# coding: utf-8

import luigi

from sport_betting.data_retrieval.clean_files.task import TaskCleanFiles


class WorkflowDataRetrieval(luigi.WrapperTask):
    competition_id = luigi.Parameter(default="CL")
    year = luigi.IntParameter()

    def requires(self):
        yield TaskCleanFiles(year=self.year, competition_id=self.competition_id)
