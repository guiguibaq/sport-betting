# coding: utf-8

import luigi

from sport_betting.data_retrieval.betfair.download.task import TaskDownloadFiles


class WorkflowDataRetrieval(luigi.WrapperTask):
    competition_id = luigi.Parameter(default="CL")

    def requires(self):
        for year in [2018]:
            yield TaskDownloadFiles(year=year, competition_id=self.competition_id)


if __name__ == '__main__':
    t = WorkflowDataRetrieval()
    luigi.build([t], local_scheduler=True)
