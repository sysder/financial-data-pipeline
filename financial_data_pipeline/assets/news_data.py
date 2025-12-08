import datetime

import feedparser
import jaconv
import pandas as pd
import polars as pl
from dagster import AssetExecutionContext, Output, asset


# Helper Functions (Domain Logic)
def normalize_text(text: str) -> str:
    """
    Normalizes Japanese text for Entity Resolution (Name Matching).

    This function handles Japanese orthographical variants to improve matching recall.

    Examples of handled variations:
        - Full-width vs Half-width: "NEC" vs "ＮＥＣ"
        - Kana variations: "キヤノン" (Big 'Ya') vs "キャノン" (Small 'ya')

    Args:
        text (str): Raw text input.

    Returns:
        str: Normalized text (NFKC normalization + Katakana standardization).
    """
    if not text:
        return ""

    # 1. Normalize characters (NFKC equivalent + custom rules)
    # Convert Half-width Kana to Full-width, Full-width Alphanumeric to Half-width
    normalized = jaconv.z2h(text, kana=False, digit=True, ascii=True)
    normalized = jaconv.h2z(normalized, kana=True, digit=False, ascii=False)

    # 2. Lowercase conversion (Case-insensitive matching)
    normalized = normalized.lower()

    return normalized


@asset(
    description="Fetches the list of listed companies from JPX (Japan Exchange Group).",
    group_name="master_data",
    compute_kind="pandas",
)
def jpx_company_list(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
    Downloads and parses the official Excel file of listed companies from JPX.
    This serves as the Master Data for entity resolution.
    """
    # Official JPX data URL (Subject to monthly updates)
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

    context.log.info(f"Downloading master data from {url}...")

    # Read Excel using pandas (Polars read_excel depends on engine availability)
    pdf = pd.read_excel(url)
    df = pl.from_pandas(pdf)

    # Column selection and renaming for clarity
    # Note: Column names are hardcoded based on the current JPX format.
    # Schema: [Date, Code, Company Name, Market Segment, Sector Code, Sector Name, ...]
    df_clean = df.select(
        [
            pl.col("コード").alias("ticker"),
            pl.col("銘柄名").alias("company_name"),
            pl.col("33業種区分").alias("sector_name"),
        ]
    )

    # Cast Ticker to String (e.g., 7203 -> "7203")
    df_clean = df_clean.with_columns(pl.col("ticker").cast(pl.Utf8))

    # Pre-calculate normalized names for faster matching
    # Using map_elements with python function (acceptable for small master data < 10k rows)
    df_clean = df_clean.with_columns(
        pl.col("company_name")
        .map_elements(normalize_text, return_dtype=pl.Utf8)
        .alias("normalized_name")
    )

    return Output(
        value=df_clean,
        metadata={
            "count": len(df_clean),
            "preview": df_clean.head(5).to_pandas().to_markdown(),
        },
    )


@asset(
    description="Fetches latest business news headlines via RSS feeds.",
    group_name="ingestion",
    compute_kind="python",
)
def raw_news_feed(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
    Ingests unstructured text data (news headlines) from Yahoo! News Business RSS.
    """
    rss_url = "https://news.yahoo.co.jp/rss/topics/business.xml"

    context.log.info(f"Fetching RSS from {rss_url}...")
    feed = feedparser.parse(rss_url)

    news_items = []
    for entry in feed.entries:
        news_items.append(
            {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get(
                    "published", datetime.datetime.now().isoformat()
                ),
                "source": "Yahoo News Business",
            }
        )

    df = pl.DataFrame(news_items)

    return Output(
        value=df,
        metadata={
            "num_articles": len(df),
            "latest_article": df["title"][0] if not df.isEmpty() else "None",
        },
    )


@asset(
    description="Maps news headlines to stock tickers (Entity Resolution).",
    group_name="transformation",
    compute_kind="polars",
)
def news_ticker_linkage(
    context: AssetExecutionContext,
    raw_news_feed: pl.DataFrame,
    jpx_company_list: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Performs Entity Resolution by matching news titles against the company master.

    Strategy:
    1. Normalize news titles (handle Japanese variants).
    2. Perform exact keyword matching against normalized company names.

    Future Improvement:
        - Implement Aho-Corasick algorithm for O(n) complexity scaling.
        - Use vector embeddings (e.g., OpenAI text-embedding-3) for semantic matching.
    """

    # 1. Normalize news titles
    news_df = raw_news_feed.with_columns(
        pl.col("title")
        .map_elements(normalize_text, return_dtype=pl.Utf8)
        .alias("normalized_title")
    )

    # 2. Matching Logic
    # Converting master data to list of dicts for iteration
    master_dict = jpx_company_list.select(
        ["ticker", "normalized_name", "company_name"]
    ).to_dicts()

    def find_tickers(text):
        found_tickers = []
        found_names = []

        for company in master_dict:
            # Simple inclusion check: is company name inside the news title?
            # Note: This may cause false positives for short names (e.g., "Mori" in "Mori Building").
            if company["normalized_name"] in text:
                found_tickers.append(company["ticker"])
                found_names.append(company["company_name"])

        return {"tickers": found_tickers, "matched_companies": found_names}

    # Apply matching logic
    # Returning structured data to keep the schema clean
    result_df = news_df.with_columns(
        pl.col("normalized_title")
        .map_elements(
            lambda x: find_tickers(x),
            return_dtype=pl.Struct(
                [
                    pl.Field("tickers", pl.List(pl.Utf8)),
                    pl.Field("matched_companies", pl.List(pl.Utf8)),
                ]
            ),
        )
        .alias("match_result")
    )

    # Flatten and select final columns
    final_df = result_df.select(
        [
            pl.col("published"),
            pl.col("title"),
            pl.col("link"),
            pl.col("source"),
            pl.col("match_result").struct.field("tickers").alias("related_tickers"),
            pl.col("match_result")
            .struct.field("matched_companies")
            .alias("matched_companies_debug"),
        ]
    )

    # Filter for news with identified tickers
    linked_news = final_df.filter(pl.col("related_tickers").list.len() > 0)

    return Output(
        value=linked_news,
        metadata={
            "total_news": len(news_df),
            "linked_news": len(linked_news),
            "match_rate": f"{len(linked_news) / len(news_df):.2%}"
            if len(news_df) > 0
            else "0%",
            # Show preview of successful matches in Dagster UI
            "preview": linked_news.select(["title", "related_tickers"])
            .head(5)
            .to_pandas()
            .to_markdown(),
        },
    )
