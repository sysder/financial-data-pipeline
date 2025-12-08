from dagster import Definitions, load_assets_from_modules

from .assets import market_data, news_data

# Load assets
market_assets = load_assets_from_modules([market_data])
news_assets = load_assets_from_modules([news_data])

defs = Definitions(
    assets=market_assets + news_assets,
    # Register resources to be injected into assets based on argument names
    resources={"market_client": market_data.YahooFinanceClient()},
)
