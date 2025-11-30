import polars as pl
import yfinance as yf
from dagster import AssetExecutionContext, ConfigurableResource, Output, asset
from pydantic import Field


class YahooFinanceClient(ConfigurableResource):
    """
    A Dagster Resource for fetching market data from Yahoo Finance.

    This class encapsulates the data fetching logic, allowing the underlying
    API implementation to be swapped (e.g., to Alpha Vantage or Bloomberg)
    without modifying the downstream assets.
    """

    # Configuration for connection settings (injectable at runtime)
    connect_timeout: int = Field(
        default=30, description="Connection timeout in seconds."
    )

    def fetch_ohlcv(self, ticker: str, period: str) -> pl.DataFrame:
        """
        Downloads historical OHLCV data and converts it to a Polars DataFrame.

        Args:
            ticker (str): The stock ticker symbol (e.g., "AAPL").
            period (str): The data period to download (e.g., "1y", "1mo").

        Returns:
            pl.DataFrame: A DataFrame containing daily stock data with snake_case columns.
                          Schema: [date, open, high, low, close, volume]
        """
        # auto_adjust=True handles stock splits and dividends automatically
        pdf = yf.download(ticker, period=period, auto_adjust=True, progress=False)

        if pdf.empty:
            raise ValueError(f"No data found for ticker {ticker}.")

        # Convert pandas DataFrame to Polars for performance
        # include_index=True ensures the 'Date' index is preserved as a column
        df = pl.from_pandas(pdf, include_index=True)

        # Normalize column names to snake_case for consistency
        df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})

        return df


@asset(
    description="Ingests historical OHLCV data using a pluggable market client.",
    group_name="ingestion",
    compute_kind="python",
)
def raw_stock_data(
    context: AssetExecutionContext, market_client: YahooFinanceClient
) -> Output[pl.DataFrame]:
    """
    Ingests market data via the injected `market_client` resource.

    This asset is agnostic to the data source implementation, relying on
    dependency injection for fetching logic.

    Args:
        context: Dagster execution context for logging.
        market_client: The configured data fetching resource.

    Returns:
        Output[pl.DataFrame]: Raw stock data with metadata for observability.
    """
    # TODO: Externalize these parameters to a config file or asset partition in the future
    ticker = "AAPL"
    period = "1y"

    context.log.info(
        f"Fetching data for {ticker} using {market_client.__class__.__name__}..."
    )

    # Delegate the fetching logic to the resource
    df = market_client.fetch_ohlcv(ticker=ticker, period=period)

    # Return with rich metadata for lineage tracking
    return Output(
        value=df,
        metadata={
            "num_records": len(df),
            "columns": list(df.columns),
            "ticker": ticker,
            "source": "Yahoo Finance",
            "preview": df.head(5).to_pandas().to_markdown(),
        },
    )
