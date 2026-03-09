import os
import logging
import json
from datetime import datetime, timezone

from extract import extract_markets, extract_all_coin_details, extract_exchange_rates
from transform import transform_markets
from load import get_engine, setup_database, load_snapshot, log_pipeline_run

# ── Load .env with explicit UTF-8 handling ───────────────────────────────
env_path = os.path.join(os.path.dirname(__file__), "../.env")
if os.path.exists(env_path):
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
with open(os.path.join(os.path.dirname(__file__), "../config/coins_config.json"), encoding="utf-8") as f:
    CONFIG = json.load(f)

PIPELINE_CODE     = CONFIG["pipeline_code"]
TOP_N             = CONFIG["top_n_coins"]
VS_CURRENCY       = CONFIG["vs_currency"]
TARGET_CURRENCIES = CONFIG["target_currencies"]

# ── Database URL from .env ────────────────────────────────────────────────
DB_URL = os.environ.get("ETL_TARGET_DB_URL", "").replace("postgresql://", "postgresql+pg8000://")


# ── Main pipeline ─────────────────────────────────────────────────────────
def run_pipeline():
    start_ts    = datetime.now(timezone.utc)
    status      = "failed"
    rows_loaded = 0
    error_count = 0
    warnings    = []

    logger.info("=" * 60)
    logger.info("  %s - STARTING", PIPELINE_CODE)
    logger.info("  %s", start_ts.isoformat())
    logger.info("=" * 60)

    engine = get_engine(DB_URL)

    try:
        # Step 1: Setup database
        setup_database(engine)

        # Step 2: Extract
        markets_raw    = extract_markets(top_n=TOP_N, vs_currency=VS_CURRENCY)
        rows_extracted = len(markets_raw)

        if not markets_raw:
            raise Exception("No market data extracted - aborting pipeline")

        coin_ids       = [c["id"] for c in markets_raw]
        details_map    = extract_all_coin_details(coin_ids)
        exchange_rates = extract_exchange_rates()

        if not exchange_rates:
            warnings.append("Exchange rates unavailable - price conversions will be NULL")
            error_count += 1

        # Step 3: Transform
        df = transform_markets(
            markets_raw=markets_raw,
            details_map=details_map,
            exchange_rates=exchange_rates,
            target_currencies=TARGET_CURRENCIES,
        )

        if df.empty:  
            raise Exception("Transformed DataFrame is empty - aborting load")

        # Step 4: Load
        rows_loaded = load_snapshot(df, engine)
        status      = "success" if error_count == 0 else "partial"

    except Exception as e:
        logger.exception("Pipeline FAILED: %s", e)
        status       = "failed"
        error_count += 1
        warnings.append(str(e))

    finally:
        end_ts  = datetime.now(timezone.utc)
        elapsed = (end_ts - start_ts).total_seconds()

        log_pipeline_run(
            engine=engine,
            pipeline_code=PIPELINE_CODE,
            start_ts=start_ts,
            end_ts=end_ts,
            status=status,
            rows_extracted=rows_extracted if "rows_extracted" in dir() else 0,
            rows_loaded=rows_loaded,
            error_count=error_count,
            warnings=warnings,
        )

        logger.info("=" * 60)
        logger.info("  Status  : %s", status.upper())
        logger.info("  Elapsed : %.1f seconds", elapsed)
        logger.info("  Loaded  : %d rows", rows_loaded)
        logger.info("  Errors  : %d", error_count)
        logger.info("  Warnings: %d", len(warnings))
        logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()