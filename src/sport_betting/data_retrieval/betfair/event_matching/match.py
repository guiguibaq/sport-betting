# coding: utf-8
import re
from difflib import SequenceMatcher

import pandas as pd
import unidecode


def unaccent_str(text: str) -> str:
    """
    Remove accent from string
    :param text: string
    :return: string without special characters
    """
    text = re.sub(r"\s*\b(A?FC|CF|FK|SC|AS|AC)\b\s*", " ", text)
    return unidecode.unidecode(text).strip()


def matching_events(df_games: pd.DataFrame, df_betfair_events: pd.DataFrame) -> pd.DataFrame:
    """
    Process matching between Betfair events and soccer games
    :param df_games: dataframe containing relevant games
    :param df_betfair_events: dataframe containing Betfair event markets
    :return: dataframe containing relevant games with relevant Betfair event_id associated
    """
    # For each game in df_games, get best matching event
    list_idx = list()
    list_event_ids = list()
    for idx, row in df_games.iterrows():
        # Create game name
        game_name = unaccent_str("{} v. {}".format(row.get('home_team'), row.get('away_team')))
        # Get game day
        game_day = row.get('date')

        # Filter events on the right day and compute similarity with game name
        df_events_day = df_betfair_events[df_betfair_events['game_day'] == game_day]
        df_events_day['similarity'] = df_events_day['event_name'].apply(lambda x: SequenceMatcher(None, unaccent_str(x), game_name).ratio())

        try:
            # Get best match
            df_best_match = df_events_day.loc[df_events_day['similarity'].idxmax()]

            # Add best matches to list
            list_idx.append(idx)
            list_event_ids.append(df_best_match['event_id'])
        except TypeError:
            list_idx.append(idx)
            list_event_ids.append(None)

    # Add event_id to each game
    df_games['event_id'] = pd.Series(data=list_event_ids, index=list_idx)

    return df_games
