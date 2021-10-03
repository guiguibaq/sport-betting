import pandas as pd

from sport_betting._old.retreive_data import retrieve_game_data, preprocess_game_data


def retrieve_avg_game_data(df_metadata, timedelta_min=None, timedelta_max=None) -> pd.DataFrame:
    """Retrieve avg game proba (one line per market).
    Apply time filter if timedelta_min and timedelta_max are specified.
    """

    subset_game_df = pd.DataFrame \
        (columns=["event_name", "event_id", "market_type", "runner_name", "ltp", "total_matched", "odd", "proba"])

    for event_name in df_metadata["event_name"]:
        print(f"Retrieving info for {event_name}")

        market_paths = df_metadata[df_metadata["event_name" ]==event_name]["path"].tolist()

        if not isinstance(market_paths[0], str):
            print(f"Game {event_name} not processed because of missing path")
            continue

        data_dict = retrieve_game_data(market_paths)
        df_game = preprocess_game_data(data_dict)

        if timedelta_min:
            df_game = df_game[df_game["time_from_game_start"] >= timedelta_min]
        if timedelta_max:
            df_game = df_game[df_game["time_from_game_start"] <= timedelta_max]

        subset_game_i = df_game.groupby(["event_name", "event_id", "market_type", "runner_name"]).mean().reset_index()

        subset_game_df = subset_game_df.append(subset_game_i)

    return subset_game_df