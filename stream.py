import glob
import pandas as pd

# 1. Load & concatenate all your exports
files = glob.glob("data/spotify_export/Streaming_History_Audio_*.json")
dfs = [pd.read_json(f) for f in files]
history = pd.concat(dfs, ignore_index=True)

# 2. Parse the timestamp (ts) into a datetime
history["played_at"] = pd.to_datetime(history["ts"])

# 3. Rename the columns you care about
history = history.rename(columns={
    "master_metadata_track_name":  "track_name",
    "master_metadata_album_artist_name": "artist_name",
    "master_metadata_album_album_name":  "album_name",
    "ms_played":                   "ms_played"
})

# 4. Extract date parts if desired
history["date"]    = history["played_at"].dt.date
history["hour"]    = history["played_at"].dt.hour
history["weekday"] = history["played_at"].dt.day_name()

# 5. Select & reorder only the columns you need
output = history[[
    "played_at", "date", "hour", "weekday",
    "track_name", "artist_name", "album_name",
    "ms_played", "platform", "conn_country"
]]

# 6. Save to CSV
output.to_csv("spotify_streaming_history.csv", index=False)
print(f"Saved {len(output)} rows to spotify_streaming_history.csv")


