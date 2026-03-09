#  POC ETL Manual — Crypto Data Pipeline

A manual ETL (Extract, Transform, Load) pipeline that fetches cryptocurrency data from public APIs, transforms it, and loads it into a local database.

---

##  Project Structure

```
poc_etl_manual/
├── config/
│   └── coins_config.json       # List of coins to track
├── pipeline/
│   ├── extract.py              # Fetch data from CoinGecko & Exchange Rate APIs
│   ├── transform.py            # Clean and transform raw data
│   ├── load.py                 # Load transformed data into the database
│   └── main.py                 # Pipeline entry point
├── tests/                      # Unit tests
├── spec/                       # Specifications / documentation
├── .env                        # Environment variables (not committed)
├── docker-compose.yml          # Docker setup for the database
├── init_db.py                  # Initialize database schema
├── setup_db.py                 # Setup database configuration
├── reset_password.py           # Utility to reset DB password
├── check_count.py              # Verify row counts in DB
├── check_db.py                 # Check database connectivity
└── requirements.txt            # Python dependencies
```

---

##  Tech Stack

- **Python 3.x**
- **PostgreSQL** (via Docker)
- **CoinGecko API** — crypto market data
- **Exchange Rate API** — USD conversion rates
- **Docker / Docker Compose**

---

##  Getting Started

### 1. Prerequisites

- Python 3.x
- Docker & Docker Compose
- Git

### 2. Clone the repository

```bash
git clone https://github.com/meriamammari/poc_etl_manual.git
cd poc_etl_manual
```

### 3. Create a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Configure environment variables

Create a `.env` file at the root of the project:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_user
DB_PASSWORD=your_password
```

### 6. Start the database

```bash
docker-compose up -d
```

### 7. Initialize the database

```bash
python init_db.py
```

### 8. Run the ETL pipeline

```bash
python pipeline/main.py
```

---

##  Pipeline Overview

```
[CoinGecko API]          [Exchange Rate API]
       │                         │
       └──────────┬──────────────┘
                  ▼
            extract.py         ← Fetch raw data with retry logic
                  │
                  ▼
           transform.py        ← Clean, normalize, convert currencies
                  │
                  ▼
             load.py           ← Insert into PostgreSQL database
```

### Key features of the extraction step:
- Retry mechanism with configurable `MAX_RETRIES` and `RETRY_BACKOFF`
- Fetches coin details from `https://api.coingecko.com/api/v3`
- Fetches USD exchange rates from `https://open.er-api.com/v6/latest/USD`

---

##  Utility Scripts

| Script | Description |
|---|---|
| `init_db.py` | Creates database tables |
| `setup_db.py` | Configures database settings |
| `reset_password.py` | Resets the database user password |
| `check_db.py` | Tests database connectivity |
| `check_count.py` | Prints row counts for all tables |

---

##  Running Tests

```bash
pytest tests/
```

---

##  Coins Configuration

Edit `config/coins_config.json` to add or remove coins tracked by the pipeline.

```json
{
  "coins": ["bitcoin", "ethereum", "solana"]
}
```

---

##  Security Notes

- Never commit your `.env` file — it is listed in `.gitignore`
- Keep your database credentials private
- API keys (if any) should always be stored in `.env`

---

##  Author

**Meriam Ammari**  
[github.com/meriamammari](https://github.com/meriamammari)
