CREATE TABLE listening_history (
  played_at    TIMESTAMP     NOT NULL,
  date         DATE          NOT NULL,
  hour         SMALLINT      NOT NULL,
  weekday      VARCHAR(10)   NOT NULL,
  track_name   TEXT          NOT NULL,
  artist_name  TEXT          NOT NULL,
  album_name   TEXT          NOT NULL,
  ms_played    INTEGER       NOT NULL,
  platform     TEXT,
  conn_country VARCHAR(5)
);
