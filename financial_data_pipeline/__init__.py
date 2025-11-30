from dagster import Definitions, load_assets_from_modules

from .assets import market_data

# Automatically load all assets defined in the market_data module
all_assets = load_assets_from_modules([market_data])

defs = Definitions(
    assets=all_assets,
    # Register resources to be injected into assets based on argument names
    resources={"market_client": market_data.YahooFinanceClient()},
)
