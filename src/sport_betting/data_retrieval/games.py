# coding: utf-8

import requests
import dateutil.parser
import pandas as pd


def parse_matches(matches_json: dict) -> list:
    """
    Parse matches data from request response
    :param matches_json: json from request
    :return: list of game dictionaries
    """
    # Open list of matches
    list_matches = list()

    # Parse all matches
    for match in matches_json.get('matches'):
        match_data = {
            "id": match.get("id"),
            "timestamp": dateutil.parser.parse(match.get('utcDate')),
            "date": dateutil.parser.parse(match.get('utcDate')).date(),
            "stage": match.get("stage"),
            "home_team": match.get('homeTeam').get('name'),
            "away_team": match.get('awayTeam').get('name'),
            "home_score": match.get('score').get('fullTime').get('homeTeam'),
            "away_score": match.get('score').get('fullTime').get('awayTeam'),
        }
        list_matches.append(match_data)

    return list_matches


def get_list_matches(api_token: str, competition_id: str = "CL", year: int = 2020) -> pd.DataFrame:
    """
    Get list of matches for a specific competition and year
    :param api_token: api.football-data API token
    :param competition_id: id of the competition (Champions league by default)
    :param year: starting year of the competition (for example, 2012 will fetch the 2012-2013 season)
    :return:
    """
    # Format API endpoint to be called
    api_endpoint = "https://api.football-data.org/v2/competitions/{}/matches?season={}".format(competition_id,
                                                                                               year)

    # Fetch data from the API
    r_matches = requests.get(url=api_endpoint, headers={'X-Auth-Token': api_token})
    matches_json = r_matches.json()

    # Parse matches
    list_games = parse_matches(matches_json=matches_json)

    # Return data as pandas dataframe
    return pd.DataFrame(list_games)
