import logging
import json
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
import pandas as pd

# Force UTF-8 encoding for psycopg2/libpq
os.environ["PYTHONIOENCODING"] = "utf-8"

logger = logging.getLogger(__name__)


DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS manual;"

DDL_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS manual.pipeline_runs (
    run_id          SERIAL          PRIMARY KEY,
    pipeline_code   VARCHAR(50)     NOT NULL,
    start_ts        TIMESTAMPTZ     NOT NULL,
    end_ts          TIMESTAMPTZ,
    status          VARCHAR(20),
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_count     INTEGER,
    warnings        JSONB
);
"""

DDL_CRYPTO_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS manual.crypto_market_snapshot (
    coin_id                 VARCHAR(100)    NOT NULL,
    symbol                  VARCHAR(20)     NOT NULL,
    name                    VARCHAR(100),
    rank                    INTEGER,
    image_url               TEXT,
    price_usd               NUMERIC(20,8),
    price_eur               NUMERIC(20,8),
    price_gbp               NUMERIC(20,8),
    price_jpy               NUMERIC(20,8),
    price_chf               NUMERIC(20,8),
    market_cap_usd          NUMERIC(24,2),
    volume_24h_usd          NUMERIC(24,2),
    high_24h_usd            NUMERIC(20,8),
    low_24h_usd             NUMERIC(20,8),
    change_24h_pct          NUMERIC(10,4),
    change_7d_pct           NUMERIC(10,4),
    ath_usd                 NUMERIC(20,8),
    ath_date                TIMESTAMPTZ,
    ath_drawdown_pct        NUMERIC(10,2),
    atl_usd                 NUMERIC(20,8),
    circulating_supply      NUMERIC(24,4),
    total_supply            NUMERIC(24,4),
    supply_ratio            NUMERIC(8,4),
    volume_to_mcap_ratio    NUMERIC(12,6),
    market_cap_category     VARCHAR(20),
    volatility_flag         VARCHAR(10),
    sentiment_label         VARCHAR(30),
    primary_category        VARCHAR(100),
    genesis_date            VARCHAR(20),
    description             TEXT,
    last_updated_api        TIMESTAMPTZ,
    data_quality_flag       SMALLINT        NOT NULL DEFAULT 0,
    load_ts                 TIMESTAMPTZ     NOT NULL
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_cms_symbol  ON manual.crypto_market_snapshot(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_cms_rank     ON manual.crypto_market_snapshot(rank);",
    "CREATE INDEX IF NOT EXISTS idx_cms_load_ts  ON manual.crypto_market_snapshot(load_ts);",
    "CREATE INDEX IF NOT EXISTS idx_cms_mcap_cat ON manual.crypto_market_snapshot(market_cap_category);",
]


def get_engine(db_url: str):
    if "pg8000" in db_url:
        engine = create_engine(
            db_url,
            future=True,
            pool_pre_ping=True
        )
    else:
        engine = create_engine(
            db_url,
            future=True,
            connect_args={
                "options": "-c client_encoding=UTF8"
            },
            pool_pre_ping=True
        )
    logger.info("LOAD -- Database engine created.")
    return engine


def setup_database(engine) -> None:
    logger.info("LOAD -- Setting up database schema and tables ...")
    with engine.begin() as conn:
        conn.execute(text(DDL_SCHEMA))
        conn.execute(text(DDL_PIPELINE_RUNS))
        conn.execute(text(DDL_CRYPTO_SNAPSHOT))
        for idx in DDL_INDEXES:
            conn.execute(text(idx))
    logger.info("LOAD -- Database setup complete.")


def load_snapshot(df: pd.DataFrame, engine) -> int:
    if df.empty:
        logger.error("LOAD -- DataFrame is empty. Nothing to load.")
        return 0

    logger.info("LOAD -- Deleting existing rows ...")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM manual.crypto_market_snapshot"))

    logger.info("LOAD -- Inserting %d rows ...", len(df))
    with engine.begin() as conn:
        for _, row in df.iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()
            cols = ", ".join(row_dict.keys())
            vals = ", ".join([f":{ k}" for k in row_dict.keys()])
            conn.execute(text(f"INSERT INTO manual.crypto_market_snapshot ({cols}) VALUES ({vals})"), row_dict)

    logger.info("LOAD -- Done. %d rows inserted.", len(df))
    return len(df)

def log_pipeline_run(
    engine,
    pipeline_code: str,
    start_ts: datetime,
    end_ts: datetime,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    error_count: int,
    warnings: list,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO manual.pipeline_runs (
                    pipeline_code, start_ts, end_ts, status,
                    rows_extracted, rows_loaded, error_count, warnings
                ) VALUES (
                    :pipeline_code, :start_ts, :end_ts, :status,
                    :rows_extracted, :rows_loaded, :error_count, cast(:warnings as jsonb)
                )
            """),
            {
                "pipeline_code":  pipeline_code,
                "start_ts":       start_ts,
                "end_ts":         end_ts,
                "status":         status,
                "rows_extracted": rows_extracted,
                "rows_loaded":    rows_loaded,
                "error_count":    error_count,
                "warnings":       json.dumps(warnings),
            }
        )
    logger.info("LOAD -- Run logged. Status: %s", status)