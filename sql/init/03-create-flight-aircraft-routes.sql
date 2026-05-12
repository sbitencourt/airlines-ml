\connect dst_airlines;

CREATE TABLE IF NOT EXISTS flight_aircraft_routes (
  id BIGSERIAL PRIMARY KEY,

  run_id TEXT NOT NULL,
  source TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  loaded_at TIMESTAMPTZ,

  flight_date DATE,
  flight_number TEXT,
  flight_iata TEXT,
  flight_icao TEXT,
  flight_status TEXT,

  airline_name TEXT,
  airline_iata TEXT,
  airline_icao TEXT,

  aircraft_registration TEXT,
  aircraft_iata TEXT,
  aircraft_icao TEXT,
  aircraft_icao24 TEXT,

  departure_airport TEXT,
  departure_iata TEXT,
  departure_icao TEXT,
  departure_timezone TEXT,
  departure_region TEXT,

  arrival_airport TEXT,
  arrival_iata TEXT,
  arrival_icao TEXT,
  arrival_timezone TEXT,
  arrival_region TEXT,

  departure_scheduled TIMESTAMPTZ,
  departure_estimated TIMESTAMPTZ,
  departure_actual TIMESTAMPTZ,

  arrival_scheduled TIMESTAMPTZ,
  arrival_estimated TIMESTAMPTZ,
  arrival_actual TIMESTAMPTZ,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_flight_aircraft_routes UNIQUE (
    run_id,
    flight_iata,
    departure_iata,
    arrival_iata,
    aircraft_icao24,
    flight_date
  )
);

CREATE INDEX IF NOT EXISTS idx_flight_aircraft_routes_run_id
ON flight_aircraft_routes (run_id);

CREATE INDEX IF NOT EXISTS idx_flight_aircraft_routes_flight_date
ON flight_aircraft_routes (flight_date DESC);

CREATE INDEX IF NOT EXISTS idx_flight_aircraft_routes_regions
ON flight_aircraft_routes (departure_region, arrival_region);

CREATE INDEX IF NOT EXISTS idx_flight_aircraft_routes_aircraft_icao24
ON flight_aircraft_routes (aircraft_icao24);