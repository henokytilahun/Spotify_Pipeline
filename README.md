# Spotify Listening Insights Pipeline

A fully automated data pipeline that ingests both Spotify API data and GDPR streaming history exports, transforms the data into analytics-ready formats, and stores everything in a PostgreSQL database. The pipeline is orchestrated via Apache Airflow and runs on WSL2.

---

## Features

- Ingests `StreamingHistory*.json` files from Spotify GDPR export
- Pulls daily listening history, top tracks, and top artists via the Spotify Web API
- Stores normalized data in PostgreSQL with conflict-safe upserts
- Runs fully automated via Apache Airflow DAGs
- Enables long-term analytics including genre trends, play counts, and listening behavior

---

## Architecture Overview


- **Data Sources**:
  - Spotify API endpoints:
    - `/me/top/artists`
    - `/me/top/tracks`
    - `/me/player/recently-played`
  - GDPR Streaming History export files

- **Pipeline Layers**:
  - Data Extraction (API and JSON)
  - Transformation (normalization and feature engineering)
  - Loading (SQLAlchemy ORM with deduplication)
  - Orchestration (Airflow DAGs: daily + historical)

---

## Technologies Used

| Technology        | Purpose                                 |
|-------------------|-----------------------------------------|
| Python            | Core language for scripting             |
| Spotipy           | Spotify API and OAuth handling          |
| SQLAlchemy        | ORM and schema modeling                 |
| psycopg2          | PostgreSQL driver                       |
| PostgreSQL        | Data warehousing                        |
| Apache Airflow    | DAG orchestration                       |
| dotenv            | Secret management for credentials       |
| WSL2              | Linux runtime for PostgreSQL and Airflow|

---

## Setup Instructions

1. Clone the repo
2. Install dependencies in a virtual environment (e.g. `spotify-airflow-venv`)
3. Set up PostgreSQL (locally or on WSL2)
4. Create a `.env.local` file with your Spotify API keys and DB URL
5. Run the initial data model setup:
   ```bash
   python model_loader.py
