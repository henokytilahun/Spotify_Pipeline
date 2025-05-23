from dotenv import load_dotenv
import os
import time
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

#Load & validate creds
load_dotenv(dotenv_path=".env.local")
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI]):
    raise RuntimeError("Missing Spotify creds in .env.local")

#Auth & token retrieval
auth_manager = SpotifyOAuth(
    client_id =CLIENT_ID,
    client_secret =CLIENT_SECRET,
    redirect_uri =REDIRECT_URI,
    scope ="user-top-read user-read-recently-played user-read-private",
    cache_path =".cache"
)
sp = spotipy.Spotify(auth_manager=auth_manager)

token_info = auth_manager.cache_handler.get_cached_token()
if token_info:
    print("Access Token Expires At:",
          time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(token_info["expires_at"])))

#Fetch Top Tracks & Top Artists
top_tracks = sp.current_user_top_tracks(limit=50)
track_items = top_tracks["items"]
track_ids = [t["id"] for t in track_items if t.get("id")]

top_artists = sp.current_user_top_artists(limit=20)
artist_items = top_artists["items"]
artist_ids = [a["id"] for a in artist_items]

print(f"\nFetched {len(track_ids)} top tracks and {len(artist_ids)} top artists.")

#Extract Artist Genres
artist_info = sp.artists(artist_ids)["artists"]
artist_genres = [g for artist in artist_info for g in artist.get("genres", [])]
genres_df = pd.DataFrame({"genre": artist_genres})

#Track Popularity
pop_df = pd.DataFrame({"popularity": [t["popularity"] for t in track_items]})

#Album Release Years
release_years = []
seen_albums = {}
for t in track_items:
    aid = t["album"]["id"]
    if aid not in seen_albums:
        seen_albums[aid] = sp.album(aid)["release_date"][:4]
    release_years.append(int(seen_albums[aid]))
year_df = pd.DataFrame({"release_year": release_years})

#Listening Time Trends
recent = sp.current_user_recently_played(limit=50)["items"]
# times is a DatetimeIndex, so use .hour and .day_name() directly
times = pd.to_datetime([item["played_at"] for item in recent])
time_df = pd.DataFrame({
    "hour": times.hour,
    "weekday": times.day_name()
})


#Diversity & Collab Network
diversity_score = len({t["artists"][0]["id"] for t in track_items}) / len(track_items)

G = nx.Graph()
for t in track_items:
    ids = [a["id"] for a in t["artists"]]
    for i in range(len(ids)):
        for j in range(i+1, len(ids)):
            G.add_edge(ids[i], ids[j])

print(f"\nArtist Diversity Score: {diversity_score:.2f}")
print(f"Collaboration network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")