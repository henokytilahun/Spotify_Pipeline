from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Date, SmallInteger, Text, Integer, String
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy.dialects.postgresql import insert as pg_insert

def init_db_and_load_historical():
    # ── 0. Load DB URL & connect ────────────────────────────────────────────────────
    load_dotenv(dotenv_path=".env.local")
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base = declarative_base()

    # ── 1. Define your table (same as before) ───────────────────────────────────────
    class ListeningHistory(Base):
        __tablename__ = "listening_history"
        played_at    = Column(DateTime, primary_key=True)
        date         = Column(Date,    nullable=False)
        hour         = Column(SmallInteger, nullable=False)
        weekday      = Column(String(10),   nullable=False)
        track_name   = Column(Text,   nullable=False)
        artist_name  = Column(Text,   nullable=False)
        album_name   = Column(Text,   nullable=False)
        ms_played    = Column(Integer, nullable=False)
        platform     = Column(Text)
        conn_country = Column(String(5))

    # Create tables if not exist
    Base.metadata.create_all(engine)

    # ── 2. Load your CSV into a DataFrame ───────────────────────────────────────────
    df = pd.read_csv("spotify_streaming_history.csv", parse_dates=["played_at","date"])
    records = df.to_dict(orient="records")

    # ── 3. Fetch existing played_at keys ────────────────────────────────────────────
    existing = set(
        r[0] for r in session.execute(
            select(ListeningHistory.played_at)
        ).all()
    )

    # ── 4. Filter out duplicates ────────────────────────────────────────────────────
    new_records = [r for r in records if r["played_at"].to_pydatetime() not in existing]

    print(f"{len(new_records)} new rows to insert (out of {len(records)})")

    # ── 5. Bulk-insert only the new rows ────────────────────────────────────────────
    inserted = 0
    for record in new_records:
        stmt = pg_insert(ListeningHistory).values(**record).on_conflict_do_nothing(index_elements=['played_at'])
        result = session.execute(stmt)
        if result.rowcount:
            inserted += 1
    session.commit()
    print(f"Inserted {inserted} new rows into listening_history (duplicates skipped).")

init_db_and_load_historical()
