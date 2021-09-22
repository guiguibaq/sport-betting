from typing import List
import os
import glob
import tarfile
import bz2
import betfairlightweight
from betfairlightweight import filters
import pandas as pd
import numpy as np
import datetime
import json
from unittest.mock import patch
import matplotlib.pyplot as plt
from typing import Tuple, List, Dict
from scipy.stats import poisson


def plot_proba_through_time(
        df_game: pd.DataFrame,
        df_game_filter: pd.Series = None,
        date_time_start_game: str = None,
        x_lim_hours_start_game: Tuple = (5, 2),
        y_lim: Tuple = (0, 1.1),
        title: str = None,
        plot_game_time_limits: bool = True,
        goal_times: List = [],
        arbitrage_bound: int = None,
):
    xlim = None
    if x_lim_hours_start_game:
        xlim = (
            np.datetime64(date_time_start_game) - np.timedelta64(x_lim_hours_start_game[0], 'h'),
            np.datetime64(date_time_start_game) + np.timedelta64(x_lim_hours_start_game[1], 'h'),
        )

    if df_game_filter is None:
        df_game_filter = [True] * df_game.shape[0]

    df_game[df_game_filter].plot(
        x="publish_time",
        y="proba",
        xlim=xlim,
        ylim=y_lim,
        figsize=(15, 7)
    )

    if plot_game_time_limits:
        plt.axvline(x=np.datetime64(date_time_start_game), color="orange", label="game_in")
        plt.axvline(x=np.datetime64(date_time_start_game) + np.timedelta64(45 + 15 + 45, 'm'), color="orange")

    if goal_times:
        for goal_time in goal_times:
            if goal_time > 45:
                goal_time += 15
            plt.axvline(x=np.datetime64(date_time_start_game) + np.timedelta64(goal_time, 'm'), color="grey",
                        linestyle="--")

    if arbitrage_bound:
        plt.axhline(y=1, color="red")
        plt.axhline(y=1 - arbitrage_bound, color="red", linestyle="--")
        plt.axhline(y=1 + arbitrage_bound, color="red", linestyle="--")

    plt.legend()
    plt.title(title)
    plt.show()