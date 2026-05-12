import os

import psycopg
from dotenv import load_dotenv


env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file)


def insert_flight_snapshot_insights(insights: dict) -> None:
    postgres_uri = os.getenv("POSTGRES_URI")

    if not postgres_uri:
        raise RuntimeError("POSTGRES_URI environment variable is required")

    sql = """
    INSERT INTO flight_snapshot_insights (
      run_id,
      snapshot_at,
      source,
      endpoint,

      observed_flights_total,

      observed_flights_active,
      observed_flights_scheduled,
      observed_flights_landed,
      observed_flights_cancelled,
      observed_flights_diverted,
      observed_flights_incident,
      observed_flights_unknown,

      observed_flights_active_pct,

      observed_departure_delays_total,
      observed_arrival_delays_total,
      observed_delayed_flights_total,

      observed_unique_airlines,
      observed_unique_routes,
      observed_unique_departure_airports,
      observed_unique_arrival_airports
    )
    VALUES (
      %(run_id)s,
      %(snapshot_at)s,
      %(source)s,
      %(endpoint)s,

      %(observed_flights_total)s,

      %(observed_flights_active)s,
      %(observed_flights_scheduled)s,
      %(observed_flights_landed)s,
      %(observed_flights_cancelled)s,
      %(observed_flights_diverted)s,
      %(observed_flights_incident)s,
      %(observed_flights_unknown)s,

      %(observed_flights_active_pct)s,

      %(observed_departure_delays_total)s,
      %(observed_arrival_delays_total)s,
      %(observed_delayed_flights_total)s,

      %(observed_unique_airlines)s,
      %(observed_unique_routes)s,
      %(observed_unique_departure_airports)s,
      %(observed_unique_arrival_airports)s
    )
    ON CONFLICT (run_id)
    DO UPDATE SET
      snapshot_at = EXCLUDED.snapshot_at,
      source = EXCLUDED.source,
      endpoint = EXCLUDED.endpoint,

      observed_flights_total = EXCLUDED.observed_flights_total,

      observed_flights_active = EXCLUDED.observed_flights_active,
      observed_flights_scheduled = EXCLUDED.observed_flights_scheduled,
      observed_flights_landed = EXCLUDED.observed_flights_landed,
      observed_flights_cancelled = EXCLUDED.observed_flights_cancelled,
      observed_flights_diverted = EXCLUDED.observed_flights_diverted,
      observed_flights_incident = EXCLUDED.observed_flights_incident,
      observed_flights_unknown = EXCLUDED.observed_flights_unknown,

      observed_flights_active_pct = EXCLUDED.observed_flights_active_pct,

      observed_departure_delays_total = EXCLUDED.observed_departure_delays_total,
      observed_arrival_delays_total = EXCLUDED.observed_arrival_delays_total,
      observed_delayed_flights_total = EXCLUDED.observed_delayed_flights_total,

      observed_unique_airlines = EXCLUDED.observed_unique_airlines,
      observed_unique_routes = EXCLUDED.observed_unique_routes,
      observed_unique_departure_airports = EXCLUDED.observed_unique_departure_airports,
      observed_unique_arrival_airports = EXCLUDED.observed_unique_arrival_airports;
    """

    with psycopg.connect(postgres_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, insights)
        conn.commit()

def insert_flight_aircraft_routes(routes: list[dict]) -> None:
    postgres_uri = os.getenv("POSTGRES_URI")

    if not postgres_uri:
        raise RuntimeError("POSTGRES_URI environment variable is required")

    if not routes:
        return

    sql = """
    INSERT INTO flight_aircraft_routes (
      run_id,
      source,
      endpoint,
      loaded_at,

      flight_date,
      flight_number,
      flight_iata,
      flight_icao,
      flight_status,

      airline_name,
      airline_iata,
      airline_icao,

      aircraft_registration,
      aircraft_iata,
      aircraft_icao,
      aircraft_icao24,

      departure_airport,
      departure_iata,
      departure_icao,
      departure_timezone,
      departure_region,

      arrival_airport,
      arrival_iata,
      arrival_icao,
      arrival_timezone,
      arrival_region,

      departure_scheduled,
      departure_estimated,
      departure_actual,

      arrival_scheduled,
      arrival_estimated,
      arrival_actual
    )
    VALUES (
      %(run_id)s,
      %(source)s,
      %(endpoint)s,
      %(loaded_at)s,

      %(flight_date)s,
      %(flight_number)s,
      %(flight_iata)s,
      %(flight_icao)s,
      %(flight_status)s,

      %(airline_name)s,
      %(airline_iata)s,
      %(airline_icao)s,

      %(aircraft_registration)s,
      %(aircraft_iata)s,
      %(aircraft_icao)s,
      %(aircraft_icao24)s,

      %(departure_airport)s,
      %(departure_iata)s,
      %(departure_icao)s,
      %(departure_timezone)s,
      %(departure_region)s,

      %(arrival_airport)s,
      %(arrival_iata)s,
      %(arrival_icao)s,
      %(arrival_timezone)s,
      %(arrival_region)s,

      %(departure_scheduled)s,
      %(departure_estimated)s,
      %(departure_actual)s,

      %(arrival_scheduled)s,
      %(arrival_estimated)s,
      %(arrival_actual)s
    )
    ON CONFLICT (
      run_id,
      flight_iata,
      departure_iata,
      arrival_iata,
      aircraft_icao24,
      flight_date
    )
    DO UPDATE SET
      source = EXCLUDED.source,
      endpoint = EXCLUDED.endpoint,
      loaded_at = EXCLUDED.loaded_at,

      flight_number = EXCLUDED.flight_number,
      flight_icao = EXCLUDED.flight_icao,
      flight_status = EXCLUDED.flight_status,

      airline_name = EXCLUDED.airline_name,
      airline_iata = EXCLUDED.airline_iata,
      airline_icao = EXCLUDED.airline_icao,

      aircraft_registration = EXCLUDED.aircraft_registration,
      aircraft_iata = EXCLUDED.aircraft_iata,
      aircraft_icao = EXCLUDED.aircraft_icao,

      departure_airport = EXCLUDED.departure_airport,
      departure_icao = EXCLUDED.departure_icao,
      departure_timezone = EXCLUDED.departure_timezone,
      departure_region = EXCLUDED.departure_region,

      arrival_airport = EXCLUDED.arrival_airport,
      arrival_icao = EXCLUDED.arrival_icao,
      arrival_timezone = EXCLUDED.arrival_timezone,
      arrival_region = EXCLUDED.arrival_region,

      departure_scheduled = EXCLUDED.departure_scheduled,
      departure_estimated = EXCLUDED.departure_estimated,
      departure_actual = EXCLUDED.departure_actual,

      arrival_scheduled = EXCLUDED.arrival_scheduled,
      arrival_estimated = EXCLUDED.arrival_estimated,
      arrival_actual = EXCLUDED.arrival_actual;
    """

    with psycopg.connect(postgres_uri) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, routes)
        conn.commit()