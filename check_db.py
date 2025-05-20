from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/henok/Spotify_Pipeline/.env.local")
engine = create_engine(os.getenv("DATABASE_URL"))
print(engine.execute("SELECT COUNT(*) FROM listening_history").scalar())
