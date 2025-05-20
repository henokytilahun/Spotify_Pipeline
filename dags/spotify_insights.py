from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
import glob

def fetch_recently_played(**kwargs):
    sp = get_spotify_client()
    recent = sp.current_user_recently_played(limit=50)["items"]
    df = pd.DataFrame([
        {
            "played_at": item["played_at"],
            "track_name": item["track"]["name"],
            "artist_name": item["track"]["artists"][0]["name"],
            "album_name": item["track"]["album"]["name"],
            "ms_played": item["track"]["duration_ms"],
        }
        for item in recent
    ])
    df["played_at"] = pd.to_datetime(df["played_at"])
    df["date"] = df["played_at"].dt.date
    df["hour"] = df["played_at"].dt.hour
    df["weekday"] = df["played_at"].dt.day_name()
    # Upsert to avoid duplicates
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = pg_insert(engine.table_names()[0]).values(**row.to_dict()).on_conflict_do_nothing(index_elements=['played_at'])
            conn.execute(stmt)

def backfill_past_exports(**kwargs):
    files = glob.glob("data/spotify_export/Streaming_History_Audio_*.json")
    dfs = [pd.read_json(f) for f in files]
    history = pd.concat(dfs, ignore_index=True)
    history["played_at"] = pd.to_datetime(history["ts"])
    history = history.rename(columns={
        "master_metadata_track_name":  "track_name",
        "master_metadata_album_artist_name": "artist_name",
        "master_metadata_album_album_name":  "album_name",
        "ms_played":                   "ms_played"
    })
    history["date"]    = history["played_at"].dt.date
    history["hour"]    = history["played_at"].dt.hour
    history["weekday"] = history["played_at"].dt.day_name()
    output = history[[
        "played_at", "date", "hour", "weekday",
        "track_name", "artist_name", "album_name",
        "ms_played", "platform", "conn_country"
    ]]
    # Upsert to avoid duplicates
    with engine.begin() as conn:
        for _, row in output.iterrows():
            stmt = pg_insert(engine.table_names()[0]).values(**row.to_dict()).on_conflict_do_nothing(index_elements=['played_at'])
            conn.execute(stmt) 