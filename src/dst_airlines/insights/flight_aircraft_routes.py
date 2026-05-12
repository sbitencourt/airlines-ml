def timezone_to_region(timezone: str | None) -> str | None:
    if not timezone:
        return None

    return timezone.split("/", 1)[0]


def build_flight_aircraft_routes(
    payload: dict,
    run_id: str,
    source: str = "aviationstack",
    endpoint: str = "flights",
) -> list[dict]:
    flights = payload.get("data", [])

    if isinstance(payload, list):
        flights = payload

    routes = []

    for item in flights:
        aircraft = item.get("aircraft") or {}
        airline = item.get("airline") or {}
        arrival = item.get("arrival") or {}
        departure = item.get("departure") or {}
        flight = item.get("flight") or {}
        meta = item.get("_meta") or {}

        departure_timezone = departure.get("timezone")
        arrival_timezone = arrival.get("timezone")

        routes.append({
            "run_id": run_id,
            "source": source,
            "endpoint": endpoint,
            "loaded_at": meta.get("loaded_at"),

            "flight_date": item.get("flight_date"),
            "flight_number": item.get("flight_number") or flight.get("number"),
            "flight_iata": flight.get("iata") or item.get("flight_number"),
            "flight_icao": flight.get("icao"),
            "flight_status": item.get("flight_status") or flight.get("flight_status"),

            "airline_name": airline.get("name"),
            "airline_iata": airline.get("iata") or item.get("airline_iata"),
            "airline_icao": airline.get("icao"),

            "aircraft_registration": aircraft.get("registration"),
            "aircraft_iata": aircraft.get("iata"),
            "aircraft_icao": aircraft.get("icao"),
            "aircraft_icao24": aircraft.get("icao24"),

            "departure_airport": departure.get("airport"),
            "departure_iata": departure.get("iata") or item.get("departure_iata"),
            "departure_icao": departure.get("icao"),
            "departure_timezone": departure_timezone,
            "departure_region": timezone_to_region(departure_timezone),

            "arrival_airport": arrival.get("airport"),
            "arrival_iata": arrival.get("iata") or item.get("arrival_iata"),
            "arrival_icao": arrival.get("icao"),
            "arrival_timezone": arrival_timezone,
            "arrival_region": timezone_to_region(arrival_timezone),

            "departure_scheduled": departure.get("scheduled"),
            "departure_estimated": departure.get("estimated"),
            "departure_actual": departure.get("actual"),

            "arrival_scheduled": arrival.get("scheduled"),
            "arrival_estimated": arrival.get("estimated"),
            "arrival_actual": arrival.get("actual"),
        })

    return routes