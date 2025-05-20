from sqlalchemy import create_engine, Column, Integer, String, DateTime, Date, SmallInteger, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Adjust this to your Postgres connection
engine = create_engine("postgresql+psycopg2://postgres:6@localhost:5432/spotify_db")
Base = declarative_base()

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

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Load CSV
df = pd.read_csv("spotify_streaming_history.csv", parse_dates=["played_at","date"])
records = df.to_dict(orient="records")

# Bulk insert
session.bulk_insert_mappings(ListeningHistory, records)
session.commit()
print(f"Inserted {len(records)} rows into listening_history.")
