import pandas as pd
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ── Lookup: WMO-style market sentiment ───────────────────────────────────
def _sentiment_label(change_24h: float | None) -> str:
    """
    Classify 24h price change into sentiment label.
    """
    if change_24h is None:
        return "Unknown"
    if change_24h >= 5.0:
        return "Strongly Bullish"
    if change_24h >= 2.0:
        return "Bullish"
    if change_24h >= -2.0:
        return "Neutral"
    if change_24h >= -5.0:
        return "Bearish"
    return "Strongly Bearish"


# ── Lookup: Market cap category ──────────────────────────────────────────
def _market_cap_category(market_cap: float | None) -> str:
    """
    Classify coin by market cap into size category.
    """
    if market_cap is None:
        return "Unknown"
    if market_cap >= 10_000_000_000:
        return "Mega Cap"
    if market_cap >= 1_000_000_000:
        return "Large Cap"
    if market_cap >= 100_000_000:
        return "Mid Cap"
    if market_cap >= 10_000_000:
        return "Small Cap"
    return "Micro Cap"


# ── Lookup: Volatility flag ──────────────────────────────────────────────
def _volatility_flag(change_24h: float | None) -> str:
    """
    Classify volatility based on absolute 24h price change.
    """
    if change_24h is None:
        return "Unknown"
    abs_change = abs(change_24h)
    if abs_change >= 5.0:
        return "High"
    if abs_change >= 2.0:
        return "Medium"
    return "Low"


# ── Lookup: Primary category ─────────────────────────────────────────────
def _primary_category(categories: list) -> str | None:
    """
    Extract the first meaningful category from CoinGecko categories list.
    """
    if not categories:
        return None
    # Filter out empty strings
    clean = [c for c in categories if c and len(c.strip()) > 0]
    return clean[0] if clean else None


# ── Transform: Build markets DataFrame ───────────────────────────────────
def transform_markets(
    markets_raw: list,
    details_map: dict,
    exchange_rates: dict,
    target_currencies: list
) -> pd.DataFrame:
    """
    Main transformation function.

    Steps:
    1. Build base DataFrame from raw markets data
    2. Join with coin details (category, genesis_date)
    3. Compute price conversions using exchange rates
    4. Compute derived KPIs
    5. Apply business rules
    6. Add audit fields

    Returns clean transformed DataFrame ready for loading.
    """
    logger.info("TRANSFORM — Starting transformation for %d coins ...", len(markets_raw))

    if not markets_raw:
        logger.error("TRANSFORM — No market data to transform")
        return pd.DataFrame()

    load_ts = datetime.now(timezone.utc)

    rows = []

    for coin in markets_raw:
        coin_id = coin.get("id")

        # ── Base fields from CoinGecko markets ──────────────────
        symbol      = str(coin.get("symbol", "")).upper()
        name        = coin.get("name")
        image_url   = coin.get("image")
        rank        = coin.get("market_cap_rank")
        price_usd   = coin.get("current_price")
        market_cap  = coin.get("market_cap")
        volume_24h  = coin.get("total_volume")
        high_24h    = coin.get("high_24h")
        low_24h     = coin.get("low_24h")
        change_24h  = coin.get("price_change_percentage_24h")
        change_7d   = coin.get("price_change_percentage_7d_in_currency")
        ath         = coin.get("ath")
        ath_date    = coin.get("ath_date")
        atl         = coin.get("atl")
        circulating = coin.get("circulating_supply")
        total_supply= coin.get("total_supply")
        last_updated= coin.get("last_updated")

        # ── JOIN: Coin details ───────────────────────────────────
        detail       = details_map.get(coin_id, {})
        categories   = detail.get("categories", [])
        genesis_date = detail.get("genesis_date")
        description  = detail.get("description")

        # ── Derived: Price conversions ───────────────────────────
        price_eur = None
        price_gbp = None
        price_jpy = None
        price_chf = None

        if price_usd is not None and exchange_rates:
            if "EUR" in target_currencies and "EUR" in exchange_rates:
                price_eur = round(price_usd * exchange_rates["EUR"], 6)
            if "GBP" in target_currencies and "GBP" in exchange_rates:
                price_gbp = round(price_usd * exchange_rates["GBP"], 6)
            if "JPY" in target_currencies and "JPY" in exchange_rates:
                price_jpy = round(price_usd * exchange_rates["JPY"], 6)
            if "CHF" in target_currencies and "CHF" in exchange_rates:
                price_chf = round(price_usd * exchange_rates["CHF"], 6)

        # ── Derived: ATH drawdown ────────────────────────────────
        ath_drawdown_pct = None
        if ath and price_usd and ath > 0:
            ath_drawdown_pct = round(((price_usd - ath) / ath) * 100, 2)

        # ── Derived: Volume / Market Cap ratio ───────────────────
        volume_to_mcap = None
        if volume_24h and market_cap and market_cap > 0:
            volume_to_mcap = round(volume_24h / market_cap, 6)

        # ── Derived: Supply ratio ────────────────────────────────
        supply_ratio = None
        if circulating and total_supply and total_supply > 0:
            supply_ratio = round(circulating / total_supply, 4)

        # ── Business rules ───────────────────────────────────────
        sentiment        = _sentiment_label(change_24h)
        market_cap_cat   = _market_cap_category(market_cap)
        volatility       = _volatility_flag(change_24h)
        primary_category = _primary_category(categories)

        # ── Data quality flag ────────────────────────────────────
        dq_flag = 0
        if price_usd is None:
            dq_flag = 2
        elif change_24h is None or market_cap is None:
            dq_flag = 1

        rows.append({
            # Identity
            "coin_id":              coin_id,
            "symbol":               symbol,
            "name":                 name,
            "rank":                 rank,
            "image_url":            image_url,

            # Price
            "price_usd":            price_usd,
            "price_eur":            price_eur,
            "price_gbp":            price_gbp,
            "price_jpy":            price_jpy,
            "price_chf":            price_chf,

            # Market data
            "market_cap_usd":       market_cap,
            "volume_24h_usd":       volume_24h,
            "high_24h_usd":         high_24h,
            "low_24h_usd":          low_24h,

            # Changes
            "change_24h_pct":       change_24h,
            "change_7d_pct":        change_7d,

            # ATH / ATL
            "ath_usd":              ath,
            "ath_date":             ath_date,
            "ath_drawdown_pct":     ath_drawdown_pct,
            "atl_usd":              atl,

            # Supply
            "circulating_supply":   circulating,
            "total_supply":         total_supply,
            "supply_ratio":         supply_ratio,

            # Derived KPIs
            "volume_to_mcap_ratio": volume_to_mcap,
            "market_cap_category":  market_cap_cat,
            "volatility_flag":      volatility,
            "sentiment_label":      sentiment,

            # From coin detail (JOIN)
            "primary_category":     primary_category,
            "genesis_date":         genesis_date,
            "description":          description,

            # Audit
            "last_updated_api":     last_updated,
            "data_quality_flag":    dq_flag,
            "load_ts":              load_ts,
        })

    df = pd.DataFrame(rows)

    # ── Deduplication ────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["coin_id"], keep="first")
    dupes = before - len(df)
    if dupes > 0:
        logger.warning("TRANSFORM — Removed %d duplicate coin_id rows", dupes)

    logger.info(
        "TRANSFORM — Done. %d rows ready. DQ flags: 0=%d, 1=%d, 2=%d",
        len(df),
        len(df[df["data_quality_flag"] == 0]),
        len(df[df["data_quality_flag"] == 1]),
        len(df[df["data_quality_flag"] == 2]),
    )

    return df