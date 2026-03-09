import requests
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────
COINGECKO_BASE    = "https://api.coingecko.com/api/v3"
EXCHANGE_RATE_URL = "https://open.er-api.com/v6/latest/USD"
MAX_RETRIES       = 3
RETRY_BACKOFF     = 2


# ── Helper ───────────────────────────────────────────────────────────────
def _get(url: str, params: dict = None, label: str = "") -> dict | list | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=10)

            # If rate limited, wait longer before retrying
            if response.status_code == 429:
                wait = 60 * attempt  # 60s, 120s, 180s
                logger.warning(
                    "Attempt %d/%d failed for [%s]: 429 rate limit — waiting %ds",
                    attempt, MAX_RETRIES, label, wait
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except Exception as e:
            wait = RETRY_BACKOFF ** attempt
            logger.warning(
                "Attempt %d/%d failed for [%s]: %s — retrying in %ds",
                attempt, MAX_RETRIES, label, e, wait
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)

    logger.error("All retries exhausted for [%s]", label)
    return None


# ── Source 1: CoinGecko Markets ──────────────────────────────────────────
def extract_markets(top_n: int = 50, vs_currency: str = "usd") -> list:
    logger.info("EXTRACT [CoinGecko Markets] Waiting 60s to avoid rate limit ...")
    time.sleep(60)
    logger.info("EXTRACT [CoinGecko Markets] Fetching top %d coins ...", top_n)

    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency":             vs_currency,
        "order":                   "market_cap_desc",
        "per_page":                top_n,
        "page":                    1,
        "sparkline":               "false",
        "price_change_percentage": "24h,7d"
    }

    data = _get(url, params=params, label="CoinGecko/markets")

    if data is None:
        logger.error("EXTRACT [CoinGecko Markets] Failed — returning empty list")
        return []

    logger.info("EXTRACT [CoinGecko Markets] Got %d coins", len(data))
    return data


# ── Source 2: Coin Details (skipped — returns empty stubs) ───────────────
def extract_coin_detail(coin_id: str) -> dict | None:
    return {
        "coin_id":      coin_id,
        "categories":   [],
        "genesis_date": None,
        "description":  None
    }


def extract_all_coin_details(coin_ids: list) -> dict:
    logger.info(
        "EXTRACT [Details] Skipping detail calls — using stubs for %d coins",
        len(coin_ids)
    )
    return {
        coin_id: {
            "coin_id":      coin_id,
            "categories":   [],
            "genesis_date": None,
            "description":  None
        }
        for coin_id in coin_ids
    }


# ── Source 3: Exchange Rates ──────────────────────────────────────────────
def extract_exchange_rates() -> dict:
    logger.info("EXTRACT [ExchangeRates] Fetching USD exchange rates ...")

    data = _get(EXCHANGE_RATE_URL, label="ExchangeRates/USD")

    if data is None or "rates" not in data:
        logger.error("EXTRACT [ExchangeRates] Failed — returning empty dict")
        return {}

    rates = data["rates"]
    logger.info("EXTRACT [ExchangeRates] Got %d currency rates", len(rates))
    return rates