import pandas as pd
import streamlit as st

from api_client import API_URL, call_api
from utils import (
    get_display_value,
    get_source_config,
    infer_flight_status_message,
    normalize_flights,
)


st.set_page_config(
    page_title="DST Airlines - Flight Status Analyzer",
    layout="wide",
)


# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.title("DST Airlines")
st.sidebar.caption("Single flight status analyzer")

st.sidebar.divider()

st.sidebar.subheader("API Configuration")
st.sidebar.code(API_URL)

data_source = st.sidebar.radio(
    "Data source",
    options=[
        "MongoDB raw flights",
        "SQL business table",
    ],
)

limit = st.sidebar.slider(
    "Maximum matching records",
    min_value=1,
    max_value=50,
    value=10,
    step=1,
)

refresh = st.sidebar.button("Refresh data")

if refresh:
    st.cache_data.clear()


# -----------------------------
# Header
# -----------------------------
st.title("Flight Status Analyzer")
st.caption(
    "Search a specific flight using a dedicated FastAPI search endpoint."
)

st.divider()


# -----------------------------
# Health check
# -----------------------------
health_data, health_error = call_api("/health")

if health_error:
    st.error("FastAPI is not reachable.")
    st.warning("Check that the FastAPI container is running and that port 8000 is exposed.")
    st.code("docker compose up -d fastapi")
    st.stop()

st.success("API online")


# -----------------------------
# Search endpoint selection
# -----------------------------
if data_source == "MongoDB raw flights":
    search_endpoint = "/mongo/flights/search"
else:
    search_endpoint = "/flights/search"

_, source_description = get_source_config(data_source)

st.subheader("Selected data source")
st.info(source_description)


# -----------------------------
# Search box
# -----------------------------
st.subheader("Search flight")

flight_query = st.text_input(
    "Flight identifier",
    placeholder="Example: AF123, DL45, U24567",
)

if not flight_query:
    st.info("Enter a flight identifier to analyze its status.")
    st.stop()


flights_data, flights_error = call_api(
    search_endpoint,
    params={
        "flight_iata": flight_query,
        "limit": limit,
    },
)

if flights_error:
    st.error(f"Error while calling {search_endpoint}")
    st.code(flights_error)
    st.stop()

if not flights_data:
    st.warning("No matching flight was found by the API.")
    st.stop()


matches = normalize_flights(flights_data)

if matches.empty:
    st.warning("The API returned data, but it could not be normalized into a table.")
    st.stop()


# -----------------------------
# Match selection
# -----------------------------
st.subheader("Matching flights")

if len(matches) > 1:
    matches = matches.reset_index(drop=True)

    def build_option_label(row: pd.Series) -> str:
        flight_id = get_display_value(
            row,
            ["flight.iata", "flight.icao", "flight.number", "flight_iata", "flight_icao", "flight_number"],
        )
        route = (
            get_display_value(row, ["departure.iata", "departure_iata", "origin"], "?")
            + " → "
            + get_display_value(row, ["arrival.iata", "arrival_iata", "destination"], "?")
        )
        status = get_display_value(row, ["flight_status", "status", "flight.status"])
        return f"{flight_id} | {route} | {status}"

    selected_index = st.selectbox(
        "Several matches were found. Select one flight:",
        options=list(matches.index),
        format_func=lambda idx: build_option_label(matches.loc[idx]),
    )

    selected_flight = matches.loc[selected_index]
else:
    selected_flight = matches.iloc[0]


# -----------------------------
# Flight status summary
# -----------------------------
flight_id = get_display_value(
    selected_flight,
    ["flight.iata", "flight.icao", "flight.number", "flight_iata", "flight_icao", "flight_number"],
)

airline = get_display_value(
    selected_flight,
    [
        "airline.name",
        "airline.airline_name",
        "airline_name",
        "airline",
        "airline.iata_code",
        "airline.icao_code",
    ],
)

status = get_display_value(
    selected_flight,
    ["flight_status", "status", "flight.status"],
)

departure_airport = get_display_value(
    selected_flight,
    ["departure.airport", "departure_airport"],
)

departure_iata = get_display_value(
    selected_flight,
    ["departure.iata", "departure_iata", "origin"],
)

arrival_airport = get_display_value(
    selected_flight,
    ["arrival.airport", "arrival_airport"],
)

arrival_iata = get_display_value(
    selected_flight,
    ["arrival.iata", "arrival_iata", "destination"],
)

departure_scheduled = get_display_value(
    selected_flight,
    ["departure.scheduled", "departure_scheduled"],
)

departure_estimated = get_display_value(
    selected_flight,
    ["departure.estimated", "departure_estimated"],
)

departure_actual = get_display_value(
    selected_flight,
    ["departure.actual", "departure_actual"],
)

departure_delay = get_display_value(
    selected_flight,
    ["departure.delay", "departure_delay"],
    default="0",
)

arrival_scheduled = get_display_value(
    selected_flight,
    ["arrival.scheduled", "arrival_scheduled"],
)

arrival_estimated = get_display_value(
    selected_flight,
    ["arrival.estimated", "arrival_estimated"],
)

arrival_actual = get_display_value(
    selected_flight,
    ["arrival.actual", "arrival_actual"],
)

arrival_delay = get_display_value(
    selected_flight,
    ["arrival.delay", "arrival_delay"],
    default="0",
)

departure_terminal = get_display_value(
    selected_flight,
    ["departure.terminal", "departure_terminal"],
)

departure_gate = get_display_value(
    selected_flight,
    ["departure.gate", "departure_gate"],
)

arrival_terminal = get_display_value(
    selected_flight,
    ["arrival.terminal", "arrival_terminal"],
)

arrival_gate = get_display_value(
    selected_flight,
    ["arrival.gate", "arrival_gate"],
)

baggage = get_display_value(
    selected_flight,
    ["arrival.baggage", "arrival_baggage"],
)

aircraft_registration = get_display_value(
    selected_flight,
    ["aircraft.registration", "aircraft_registration"],
)

is_ground = get_display_value(
    selected_flight,
    ["live.is_ground", "is_ground"],
)

live_updated = get_display_value(
    selected_flight,
    ["live.updated", "live_updated"],
)

prediction_label = get_display_value(
    selected_flight,
    ["prediction_label"],
)

predicted_is_delayed = get_display_value(
    selected_flight,
    ["predicted_is_delayed"],
)

delay_probability = get_display_value(
    selected_flight,
    ["delay_probability"],
)

model_name = get_display_value(
    selected_flight,
    ["model_name"],
)

model_version = get_display_value(
    selected_flight,
    ["model_version"],
)

prediction_created_at = get_display_value(
    selected_flight,
    ["prediction_created_at"],
)

st.subheader("Flight status result")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.metric("Flight", flight_id)

with kpi2:
    st.metric("Airline", airline)

with kpi3:
    st.metric("Status", status)

with kpi4:
    st.metric("Route", f"{departure_iata} → {arrival_iata}")

st.info(infer_flight_status_message(selected_flight))

st.subheader("ML delay prediction")

prediction_col_1, prediction_col_2, prediction_col_3, prediction_col_4 = st.columns(4)

with prediction_col_1:
    st.metric("Prediction", prediction_label)

with prediction_col_2:
    try:
        probability_value = float(delay_probability)
        st.metric("Delay probability", f"{probability_value:.1%}")
    except (TypeError, ValueError):
        st.metric("Delay probability", delay_probability)

with prediction_col_3:
    st.metric("Predicted delayed", predicted_is_delayed)

with prediction_col_4:
    st.metric("Model version", model_version)

st.caption(f"Prediction generated at: {prediction_created_at}")
st.caption(f"Model: {model_name}")


# -----------------------------
# Detailed status
# -----------------------------
st.subheader("Detailed flight information")

detail_col_1, detail_col_2 = st.columns(2)

with detail_col_1:
    st.write("### Departure")
    st.write(f"**Airport:** {departure_airport}")
    st.write(f"**IATA:** {departure_iata}")
    st.write(f"**Terminal:** {departure_terminal}")
    st.write(f"**Gate:** {departure_gate}")
    st.write(f"**Scheduled:** {departure_scheduled}")
    st.write(f"**Estimated:** {departure_estimated}")
    st.write(f"**Actual:** {departure_actual}")
    st.write(f"**Delay:** {departure_delay} minutes")

with detail_col_2:
    st.write("### Arrival")
    st.write(f"**Airport:** {arrival_airport}")
    st.write(f"**IATA:** {arrival_iata}")
    st.write(f"**Terminal:** {arrival_terminal}")
    st.write(f"**Gate:** {arrival_gate}")
    st.write(f"**Baggage:** {baggage}")
    st.write(f"**Scheduled:** {arrival_scheduled}")
    st.write(f"**Estimated:** {arrival_estimated}")
    st.write(f"**Actual:** {arrival_actual}")
    st.write(f"**Delay:** {arrival_delay} minutes")


# -----------------------------
# Raw selected record
# -----------------------------
with st.expander("Selected flight raw record"):
    st.json(selected_flight.dropna().to_dict())

with st.expander("All matching records"):
    st.dataframe(matches, use_container_width=True, hide_index=True)


st.subheader("Aircraft and live data")

aircraft_col, live_col = st.columns(2)

with aircraft_col:
    st.write("### Aircraft")
    st.write(f"**Registration:** {aircraft_registration}")

with live_col:
    st.write("### Live status")
    st.write(f"**Is ground:** {is_ground}")
    st.write(f"**Last update:** {live_updated}")


# -----------------------------
# Footer
# -----------------------------
st.divider()

st.caption(
    "This screen uses a dedicated FastAPI endpoint instead of filtering locally in Streamlit."
)
