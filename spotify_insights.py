from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import glob
from model_loader import init_db_and_load_historical

DEFAULT_ARGS = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_insights',
    default_args=DEFAULT_ARGS,
    description='ETL and analytics pipeline for Spotify listening insights',
    start_date=datetime(2025, 5, 20),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    #Initialize DB & load full history
    t_init_db = PythonOperator(
        task_id="init_db_and_load_historical",
        python_callable=init_db_and_load_historical,
    )

    #Init Spotify client
    def init_spotify_client():
        import spotipy
        from spotipy.oauth2 import SpotifyOAuth
        sp_oauth = SpotifyOAuth(
            client_id='{{ var.value.SPOTIFY_CLIENT_ID }}',
            client_secret='{{ var.value.SPOTIFY_CLIENT_SECRET }}',
            redirect_uri='{{ var.value.SPOTIFY_REDIRECT_URI }}',
            scope='user-top-read user-read-recently-played'
        )
        return spotipy.Spotify(auth_manager=sp_oauth)

    t_init_sp = PythonOperator(
        task_id='init_spotify_client',
        python_callable=init_spotify_client,
    )

    #Fetch recently played
    def fetch_recently_played(**context):
        sp = context['ti'].xcom_pull(task_ids='init_spotify_client')
        results = sp.current_user_recently_played(limit=50)
        df = pd.DataFrame(results['items'])
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        df.to_sql('listening_history', engine, if_exists='append', index=False)

    t_fetch_recent = PythonOperator(
        task_id='fetch_recently_played',
        python_callable=fetch_recently_played,
    )

    #Fetch top tracks
    def fetch_top_tracks(**context):
        sp = context['ti'].xcom_pull(task_ids='init_spotify_client')
        results = sp.current_user_top_tracks(limit=50, time_range='long_term')
        df = pd.json_normalize(results['items'])
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        df.to_sql('top_tracks', engine, if_exists='replace', index=False)

    t_fetch_top_tracks = PythonOperator(
        task_id='fetch_top_tracks',
        python_callable=fetch_top_tracks,
    )

    #Fetch top artists
    def fetch_top_artists(**context):
        sp = context['ti'].xcom_pull(task_ids='init_spotify_client')
        results = sp.current_user_top_artists(limit=20, time_range='long_term')
        df = pd.json_normalize(results['items'])
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        df.to_sql('top_artists', engine, if_exists='replace', index=False)

    t_fetch_top_artists = PythonOperator(
        task_id='fetch_top_artists',
        python_callable=fetch_top_artists,
    )

    #Load GDPR export
    def load_streaming_export():
        files = glob.glob('/opt/airflow/data/spotify_export/StreamingHistory*.json')
        df = pd.concat((pd.read_json(f) for f in files), ignore_index=True)
        df['played_at'] = pd.to_datetime(df['ts']).dt.tz_localize('UTC').dt.tz_convert(None)
        df.rename(columns={
            'master_metadata_track_name':'track_name',
            'master_metadata_album_artist_name':'artist_name',
            'ms_played':'ms_played'
        }, inplace=True)
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        df.to_sql('streaming_export', engine, if_exists='replace', index=False)

    t_load_export = PythonOperator(
        task_id='load_streaming_export',
        python_callable=load_streaming_export,
    )

    #Compute daily metrics
    def compute_daily_metrics():
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        df = pd.read_sql(
            'SELECT date(played_at) AS day, COUNT(*) AS plays '
            'FROM listening_history GROUP BY day',
            engine
        )
        df.to_sql('daily_metrics', engine, if_exists='replace', index=False)

    t_daily_metrics = PythonOperator(
        task_id='compute_daily_metrics',
        python_callable=compute_daily_metrics,
    )

    #Cleanup old data
    def cleanup_old_data():
        engine = PostgresHook(postgres_conn_id='spotify_db').get_sqlalchemy_engine()
        engine.execute(
            "DELETE FROM listening_history "
            "WHERE played_at < NOW() - INTERVAL '2 years'"
        )

    t_cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
    )

    #DAG wiring
    t_init_db >> t_init_sp
    t_init_sp >> [t_fetch_recent, t_fetch_top_tracks, t_fetch_top_artists]
    t_fetch_recent >> t_load_export >> t_daily_metrics >> t_cleanup
