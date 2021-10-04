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

## Data retrieval
Data can be fetched automatically using the data retrieval module.

### Configuration
Configuration for the data retrieval module is located in the `config/config.cfg` file.

#### Football-data
An API key from football-data (https://www.football-data.org/) is needed and requires prior registration.  
Your personal API key has to be stored in the `[FootballData]` section of the configuration file.

#### Betfair
Betfair account credentials should also be stored in `[Betfair]` section of the configuration file.  
Required credentials are :  
- `email`: email address from your Betfair account
- `password`: Betfair account password
- `api_key`: Betfair API key (see https://developer.betfair.com/get-started/#exchange-api)
- `certs_dir`: directory where your certificates registered with Betfair are stored (see https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Non-Interactive+%28bot%29+login)

### Execution
Data can be retrieved by launching the command `make data $year` where `$year` corresponds to the desired Champion's league season.  
For example, `make data 2020` retrieves data from the 2020-2021 season of the CL.

### Output
The output of the data retrieval module consists of two files :
- `./data/CL/$year/event_markets.parquet`: parquet file with market data for CL games of the year.
- `./data/CL/$year/event_games.parquet`: parquet file with list of games, dates, teams and scores corresponding to each betting `event_id`.
