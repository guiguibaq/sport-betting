# README

Some code on BetFair strategies.

Leverages BetFair API (see https://betfair-datascientists.github.io/api/apiPythontutorial/) to get access to data.

Historical data was downloaded manually from Betfair historical data for the 2020 Champions League. More data should be added for robust backtest.

- /data: contains some metadata
- /src: util functions
- /notebook/retrieve_data: contains code that processes the historical data
- /notebook/arbitrage: explores arbitrage opportunities on past games. Found that:
  - (i) arbitrage opportunities exist, but most of them disappear quickly. Good engineering (low latency) would be key to capture them
  - (ii) some arbitrage opportunity remain for a long time on markets UNDER / OVER X goals, with X above 4. Still to test if those are useable in real life.
- notebook/poisson_distribution: explores pricing of different market. In particular found that:
  - (i) all markets seem to be aligned on a poisson process: given the lambdas on the two opposing teams, all quotes can be retrieved very closely. That is impressive, because it shows that all quotes are governed by only two parameters.
  - (ii) backtests a strategy to explore mis-pricing between markets based on the lambdas of other markets. Good backtest results (SHARP: 1.4), but need more data to confirm 