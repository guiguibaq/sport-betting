from typing import Tuple

import numpy as np
import pandas as pd
from scipy.stats import poisson


def get_lambdas_for_proba_score(
        proba_00: float, proba_10: float
) -> Tuple[float]:
    lambda_1 = proba_10 / proba_00
    lambda_2 = -np.log(proba_00) - proba_10 / proba_00

    lambdas = (lambda_1, lambda_2)

    return lambdas


def get_proba_score(
        score: Tuple,
        lambdas: Tuple,
) -> float:
    return np.product(poisson.pmf(k=score, mu=lambdas, loc=0))


def get_proba_scores_matrix(lambdas: Tuple) -> pd.DataFrame():
    proba_scores = {
        "score_1": [],
        "score_2": [],
        "proba": [],
    }

    for score_1 in range(0, 9):
        for score_2 in range(0, 9):
            proba_scores["score_1"].append(score_1)
            proba_scores["score_2"].append(score_2)
            proba_scores["proba"].append(get_proba_score((score_1, score_2), lambdas))

    return pd.DataFrame(proba_scores)


def get_proba_match(df_proba_scores: pd.DataFrame) -> pd.DataFrame:
    proba_win_1 = df_proba_scores.loc[df_proba_scores["score_1"] > df_proba_scores["score_2"], "proba"].sum()
    proba_win_2 = df_proba_scores.loc[df_proba_scores["score_2"] > df_proba_scores["score_1"], "proba"].sum()
    proba_draw = df_proba_scores.loc[df_proba_scores["score_1"] == df_proba_scores["score_2"], "proba"].sum()

    return pd.DataFrame(
        {
            "outcome": ["win_1", "win_2", "draw"],
            "proba": [proba_win_1, proba_win_2, proba_draw],
        }
    )


def get_proba_under_k_goals(df_proba_scores: pd.DataFrame):
    proba_score = {
        "market_type": [],
        "runner_name": [],
        "proba": [],
    }

    for k in range(0, 9):
        proba_under_k_goals = df_proba_scores.loc[
            (df_proba_scores["score_1"] + df_proba_scores["score_2"]) <= k, "proba"
        ].sum()
        proba_score["market_type"].append(f"OVER_UNDER_{k}5")
        proba_score["market_type"].append(f"OVER_UNDER_{k}5")
        proba_score["runner_name"].append(f"Under {k}.5 Goals")
        proba_score["proba"].append(proba_under_k_goals)
        proba_score["runner_name"].append(f"Over {k}.5 Goals")
        proba_score["proba"].append(1 - proba_under_k_goals)

    return pd.DataFrame(proba_score)


def get_odds_poisson_vs_actual(
        pre_game_odds: pd.DataFrame,
        event_name: str,
):
    """Computes all odds given a poisson distribution, and compares them with observed odds."""
    pre_game_odds = pre_game_odds.copy()
    pre_game_odds = pre_game_odds[pre_game_odds["event_name"] == event_name]

    proba_00 = pre_game_odds[
        (pre_game_odds["market_type"] == "CORRECT_SCORE")
        & (pre_game_odds["runner_name"] == "0 - 0")
        ]["proba"].tolist()[0]

    proba_10 = pre_game_odds[
        (pre_game_odds["market_type"] == "CORRECT_SCORE")
        & (pre_game_odds["runner_name"] == "1 - 0")
        ]["proba"].tolist()[0]

    lambdas = get_lambdas_for_proba_score(proba_00, proba_10)

    df_proba_per_market = pd.DataFrame(
        {
            "market_type": [],
            "runner_name": [],
            "proba": [],
        }
    )

    # Add CORRECT_SCORE
    df_proba_scores_matrix = get_proba_scores_matrix(lambdas)

    df_proba_per_market = df_proba_scores_matrix.copy()
    df_proba_per_market["runner_name"] = (
            df_proba_per_market["score_1"].astype(str) + " - " +
            df_proba_per_market["score_2"].astype(str)
    )
    df_proba_per_market["market_type"] = "CORRECT_SCORE"
    df_proba_per_market = df_proba_per_market[["market_type", "runner_name", "proba"]]

    # Add MATCH_ODDS
    df_proba_match = get_proba_match(df_proba_scores_matrix)

    df_proba_match["market_type"] = "MATCH_ODDS"
    df_proba_match["runner_name"] = [event_name.split(" v ")[0], event_name.split(" v ")[1], "The Draw"]
    df_proba_per_market = df_proba_per_market.append(df_proba_match[["market_type", "runner_name", "proba"]])

    # Add OVER_UNDER_05
    df_proba_over_under = get_proba_under_k_goals(df_proba_scores_matrix)
    df_proba_per_market = df_proba_per_market.append(df_proba_over_under[["market_type", "runner_name", "proba"]])

    # Add column for plot colors
    df_proba_per_market["market_type_col"] = df_proba_per_market["market_type"]
    df_proba_per_market.loc[
        df_proba_per_market["market_type_col"].str.contains("OVER_UNDER"), "market_type_col"] = "OVER_UNDER"

    # Merge with obs proba
    df_compare_proba = (
        df_proba_per_market.rename(columns={"proba": "proba_poisson"})
            .merge(pre_game_odds.rename(columns={"proba": "proba_obs"}), on=["market_type", "runner_name"], how="inner")
    )

    return df_compare_proba