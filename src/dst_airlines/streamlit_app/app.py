import streamlit as st

from api_client import API_URL, call_api
from utils import (
    calculate_status_kpis,
    find_first_existing_column,
    get_source_config,
    normalize_flights,
)


st.set_page_config(
    page_title="DST Airlines - Flight Explorer",
    layout="wide",
)


def render_metric_card(label: str, value: str | int, help_text: str | None = None) -> None:
    """Render a simple Streamlit metric card."""
    st.metric(label=label, value=value, help=help_text)


# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.title("DST Airlines")
st.sidebar.caption("Flight data explorer powered by FastAPI")

st.sidebar.divider()

st.sidebar.subheader("API Configuration")
st.sidebar.code(API_URL)

limit = st.sidebar.slider(
    "Number of recent flights",
    min_value=5,
    max_value=100,
    value=20,
    step=5,
)

data_source = st.sidebar.radio(
    "Data source",
    options=[
        "MongoDB raw flights",
        "SQL business table",
    ],
)

st.sidebar.divider()

refresh = st.sidebar.button("Refresh data")

if refresh:
    st.cache_data.clear()


# -----------------------------
# Header
# -----------------------------
st.title("DST Airlines Flight Explorer")
st.caption(
    "User-facing interface that consumes FastAPI endpoints without connecting directly "
    "to MongoDB or PostgreSQL."
)

st.divider()


# -----------------------------
# API Health
# -----------------------------
health_data, health_error = call_api("/health")

col_status, col_api, col_limit = st.columns([1, 2, 1])

with col_status:
    if health_error:
        st.error("API offline")
    else:
        st.success("API online")

with col_api:
    st.write("**FastAPI endpoint**")
    st.code(API_URL)

with col_limit:
    st.write("**Records limit**")
    st.metric("Limit", limit)


if health_error:
    st.warning(
        "FastAPI is not reachable. Check that the container is running and that port 8000 is exposed."
    )
    st.code("docker compose up -d fastapi")
    st.stop()


# -----------------------------
# Data loading
# -----------------------------
endpoint, source_description = get_source_config(data_source)

flights_data, flights_error = call_api(endpoint, params={"limit": limit})

st.subheader("Selected data source")
st.info(source_description)

if flights_error:
    st.error(f"Error while calling {endpoint}")
    st.code(flights_error)
    st.stop()

if not flights_data:
    st.warning("No data returned by the API.")
    st.stop()

df = normalize_flights(flights_data)

if df.empty:
    st.warning("The API returned data, but it could not be normalized into a table.")
    st.stop()


# -----------------------------
# Column detection
# -----------------------------
status_column = find_first_existing_column(
    df,
    ["flight_status", "status", "flight.status"],
)

airline_column = find_first_existing_column(
    df,
    ["airline.name", "airline_name", "airline"],
)

departure_column = find_first_existing_column(
    df,
    ["departure.iata", "departure_iata", "origin"],
)

arrival_column = find_first_existing_column(
    df,
    ["arrival.iata", "arrival_iata", "destination"],
)


# -----------------------------
# Filters
# -----------------------------
st.subheader("Explore flights")

filtered_df = df.copy()

filter_cols = st.columns(3)

with filter_cols[0]:
    if status_column:
        status_options = sorted(filtered_df[status_column].dropna().astype(str).unique())
        selected_statuses = st.multiselect(
            "Filter by status",
            options=status_options,
            default=status_options,
        )
        filtered_df = filtered_df[
            filtered_df[status_column].astype(str).isin(selected_statuses)
        ]
    else:
        st.caption("Status column not available.")

with filter_cols[1]:
    if airline_column:
        airline_options = sorted(filtered_df[airline_column].dropna().astype(str).unique())
        selected_airlines = st.multiselect(
            "Filter by airline",
            options=airline_options,
            default=airline_options,
        )
        filtered_df = filtered_df[
            filtered_df[airline_column].astype(str).isin(selected_airlines)
        ]
    else:
        st.caption("Airline column not available.")

with filter_cols[2]:
    search_text = st.text_input(
        "Search in table",
        placeholder="Example: CDG, JFK, Air France",
    )

    if search_text:
        mask = filtered_df.astype(str).apply(
            lambda row: row.str.contains(search_text, case=False, na=False).any(),
            axis=1,
        )
        filtered_df = filtered_df[mask]


# -----------------------------
# KPI Section
# -----------------------------
st.subheader("Overview")

active_count, delayed_count, scheduled_count = calculate_status_kpis(
    filtered_df,
    status_column,
)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    render_metric_card("Records displayed", len(filtered_df))

with kpi2:
    render_metric_card("Active flights", active_count)

with kpi3:
    render_metric_card("Delayed flights", delayed_count)

with kpi4:
    render_metric_card("Scheduled flights", scheduled_count)


st.divider()


# -----------------------------
# Route summary
# -----------------------------
if departure_column and arrival_column and not filtered_df.empty:
    route_df = filtered_df.copy()
    route_df["route"] = (
        route_df[departure_column].astype(str)
        + " → "
        + route_df[arrival_column].astype(str)
    )

    top_routes = (
        route_df["route"]
        .value_counts()
        .head(10)
        .reset_index()
    )
    top_routes.columns = ["route", "count"]

    st.subheader("Top routes")
    st.bar_chart(
        top_routes,
        x="route",
        y="count",
        use_container_width=True,
    )


# -----------------------------
# Chart
# -----------------------------
if status_column and not filtered_df.empty:
    st.subheader("Flight status distribution")

    status_counts = (
        filtered_df[status_column]
        .fillna("unknown")
        .astype(str)
        .value_counts()
        .reset_index()
    )
    status_counts.columns = ["status", "count"]

    st.bar_chart(
        status_counts,
        x="status",
        y="count",
        use_container_width=True,
    )


# -----------------------------
# Data table
# -----------------------------
st.subheader("Recent flights")

st.dataframe(
    filtered_df,
    use_container_width=True,
    hide_index=True,
)

with st.expander("Raw API response"):
    st.json(flights_data)


# -----------------------------
# Footer
# -----------------------------
st.divider()

st.caption(
    "Grafana remains the operational dashboard for business and pipeline monitoring. "
    "Streamlit is an example of a user-facing application consuming FastAPI."
)
