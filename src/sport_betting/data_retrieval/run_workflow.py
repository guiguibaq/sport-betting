# coding: utf-8

import argparse

import luigi

from sport_betting.data_retrieval.workflow import WorkflowDataRetrieval

parser = argparse.ArgumentParser()
parser.add_argument("year", help="Year used for data retrieval", type=int)
parser.add_argument("-c", "--competition", help="Code of the competition to be parsed")


if __name__ == '__main__':
    # Parse args
    args = parser.parse_args()

    # Create the task that will be run
    if args.competition:
        task = WorkflowDataRetrieval(year=args.year, competition_id=args.competition)
    else:
        task = WorkflowDataRetrieval(year=args.year)

    luigi.build([task], local_scheduler=True)
