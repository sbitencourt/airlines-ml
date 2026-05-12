\connect dst_airlines;

CREATE TABLE IF NOT EXISTS flight_snapshot_insights (
  run_id TEXT PRIMARY KEY,
  snapshot_at TIMESTAMPTZ NOT NULL,

  source TEXT NOT NULL,
  endpoint TEXT NOT NULL,

  observed_flights_total INTEGER NOT NULL,

  observed_flights_active INTEGER NOT NULL,
  observed_flights_scheduled INTEGER NOT NULL,
  observed_flights_landed INTEGER NOT NULL,
  observed_flights_cancelled INTEGER NOT NULL,
  observed_flights_diverted INTEGER NOT NULL,
  observed_flights_incident INTEGER NOT NULL,
  observed_flights_unknown INTEGER NOT NULL,

  observed_flights_active_pct NUMERIC(6,2),

  observed_departure_delays_total INTEGER NOT NULL,
  observed_arrival_delays_total INTEGER NOT NULL,
  observed_delayed_flights_total INTEGER NOT NULL,

  observed_unique_airlines INTEGER NOT NULL,
  observed_unique_routes INTEGER NOT NULL,
  observed_unique_departure_airports INTEGER NOT NULL,
  observed_unique_arrival_airports INTEGER NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_flight_snapshot_insights_snapshot_at
ON flight_snapshot_insights (snapshot_at DESC);