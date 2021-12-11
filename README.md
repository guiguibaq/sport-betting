# README

Explore betting strategies on BetFair exchange. 

Organised as such:
* `/src`: data retrieval and utils for strategies
* `/notebook`: explore pricing along time and attempt to model prices
* `/data`: some metadata

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

## Pricing and strategies

### Arbitrage accross market
Arbitrage opportunities exist, although:
i) they either disaper very quickly (low latency is key to benefit from them)
ii) or they involve exotic market with low liquidy
For more details see `/notebook/arbitrage`

### Poisson distribution
Although a football game contains many markets, it seems that all markets align on the same Poisson distribution. Therefore, all markets are linked, and knowing the parameters of the Poisson implies knowing all prices. This can be leveraged for arbitrage.
For more details see `/notebook/poisson_distribution`

### Backtest
Applying a simple strategy following the Poisson idea leads to good backtest results (SHARP: 1.4). Further backtesting required to confirm the robstness of the strategy.

