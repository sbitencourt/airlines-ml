import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from api_client import call_api

st.set_page_config(
    page_title="DST Airlines - BI & Analytics",
    layout="wide",
)

st.title("📊 Flight Analytics & Business Intelligence")
st.caption("Historical insights, delays, and punctuality performance based on EUROCONTROL data.")

st.divider()

# -----------------------------
# Sidebar: Filters
# -----------------------------
st.sidebar.title("BI Controls")

load_all = st.sidebar.checkbox("Load FULL French Dataset", value=True, help="Uncheck to limit the number of rows for faster loading.")

if load_all:
    limit_records = 0  # 0 signals the API to return everything
else:
    limit_records = st.sidebar.slider(
        "Sample Size (Records)",
        min_value=1000,
        max_value=50000,
        value=10000,
        step=1000,
    )

refresh = st.sidebar.button("🔄 Refresh Data")
if refresh:
    st.cache_data.clear()

# -----------------------------
# Data Loading
# -----------------------------
with st.spinner("Fetching analytical data from PostgreSQL via FastAPI..."):
    data, error = call_api("/analytics/historical", params={"limit": limit_records})

if error:
    st.error("Failed to load analytics from FastAPI.")
    st.code(error)
    st.stop()

if not data:
    st.warning("No historical records found in PostgreSQL.")
    st.stop()

df = pd.DataFrame(data)

# Date processing to extract day of the week, hour, and 3-hour blocks
df["filed_dep_time"] = pd.to_datetime(df["filed_dep_time"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
df["hour"] = df["filed_dep_time"].dt.hour
df["day_name"] = df["filed_dep_time"].dt.day_name()
df["day_of_week"] = df["filed_dep_time"].dt.dayofweek

# Create 3-hour buckets (e.g., 00h-02h, 03h-05h, etc.)
df["hour_group"] = df["hour"].apply(lambda h: f"{(h // 3) * 3:02d}h-{((h // 3) * 3) + 2:02d}h")

days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
hour_buckets = [f"{i:02d}h-{i+2:02d}h" for i in range(0, 24, 3)]

# -----------------------------
# Global Screen Filters
# -----------------------------
col_f1, col_f2, col_f3 = st.columns(3)

with col_f1:
    operators = sorted(df["operator"].dropna().unique().tolist())
    selected_operators = st.multiselect("Filter by Airline (Operator):", operators, default=operators[:5] if len(operators) > 5 else operators)

with col_f2:
    aircraft_types = sorted(df["aircraft_type"].dropna().unique().tolist())
    selected_aircraft = st.multiselect("Filter by Aircraft:", aircraft_types, default=aircraft_types[:5] if len(aircraft_types) > 5 else aircraft_types)

with col_f3:
    delay_filter = st.radio("Flight Status:", ["All", "Only Delayed (>15m)", "Only On-Time"], horizontal=True)

# Apply filters
filtered_df = df[df["operator"].isin(selected_operators) & df["aircraft_type"].isin(selected_aircraft)]

if delay_filter == "Only Delayed (>15m)":
    filtered_df = filtered_df[filtered_df["is_delayed"] == True]
elif delay_filter == "Only On-Time":
    filtered_df = filtered_df[filtered_df["is_delayed"] == False]

if filtered_df.empty:
    st.warning("No records match the selected filters.")
    st.stop()

# -----------------------------
# Top KPIs
# -----------------------------
total_flights = len(filtered_df)
delayed_flights = int(filtered_df["is_delayed"].sum())
on_time_flights = total_flights - delayed_flights
delay_rate = (delayed_flights / total_flights) * 100 if total_flights > 0 else 0
avg_delay = filtered_df[filtered_df["is_delayed"] == True]["delay_minutes"].mean()
avg_delay = 0 if pd.isna(avg_delay) else avg_delay

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Analyzed Flights", f"{total_flights:,}")
kpi2.metric("Punctuality Rate", f"{100 - delay_rate:.1f}%")
kpi3.metric("Delay Rate (>15m)", f"{delay_rate:.1f}%")
kpi4.metric("Average Delay Time", f"{avg_delay:.0f} min")

st.divider()

# -----------------------------
# Charts and Visualizations
# -----------------------------
tab_overview, tab_time, tab_fleet = st.tabs(["✈️ Routes & Operators", "⏰ Time Patterns", "🛩️ Fleet & Aircraft"])

with tab_overview:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Delays by Operator")
        op_agg = filtered_df.groupby(["operator", "is_delayed"]).size().reset_index(name="count")
        op_agg["status"] = op_agg["is_delayed"].map({True: "Delayed", False: "On-Time"})
        fig_op = px.bar(
            op_agg, x="operator", y="count", color="status", barmode="stack",
            color_discrete_map={"On-Time": "#2ecc71", "Delayed": "#e74c3c"}
        )
        st.plotly_chart(fig_op, use_container_width=True)

    with c2:
        st.subheader("Top 10 Routes by Volume")
        filtered_df["route"] = filtered_df["origin"] + " → " + filtered_df["destination"]
        top_routes = filtered_df["route"].value_counts().head(10).reset_index()
        top_routes.columns = ["route", "count"]
        fig_routes = px.bar(top_routes, x="count", y="route", orientation="h", color="count", color_continuous_scale="Blues")
        st.plotly_chart(fig_routes, use_container_width=True)

with tab_time:
    t1, t2 = st.columns(2)
    with t1:
        st.subheader("Volume & Delay Rate by Day")
        
        day_agg = filtered_df.groupby(["day_name", "is_delayed"]).size().reset_index(name="count")
        day_agg["status"] = day_agg["is_delayed"].map({True: "Delayed", False: "On-Time"})
        
        day_totals = day_agg.groupby("day_name")["count"].transform("sum")
        day_agg["percentage"] = (day_agg["count"] / day_totals) * 100
        
        fig_day = px.bar(
            day_agg, x="day_name", y="count", color="status", 
            text=day_agg["percentage"].apply(lambda x: f"{x:.1f}%"),
            category_orders={"day_name": days_order},
            color_discrete_map={"On-Time": "#2ecc71", "Delayed": "#e74c3c"}
        )
        
        fig_day.update_traces(
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=13),
            hovertemplate="<b>%{x}</b><br>Status: %{data.name}<br>Total Flights: %{y}<br>Proportion: %{text}<extra></extra>"
        )
        
        st.plotly_chart(fig_day, use_container_width=True)

    with t2:
        st.subheader("Delay Volume by Hour of Day")
        hour_agg = filtered_df.groupby(["hour", "is_delayed"]).size().reset_index(name="count")
        hour_agg["status"] = hour_agg["is_delayed"].map({True: "Delayed", False: "On-Time"})
        fig_hour = px.line(
            hour_agg, x="hour", y="count", color="status", markers=True,
            color_discrete_map={"On-Time": "#2ecc71", "Delayed": "#e74c3c"}
        )
        st.plotly_chart(fig_hour, use_container_width=True)
        
    st.divider()
    
    # -----------------------------
    # Heatmap: Day vs Time Block Delay Rate
    # -----------------------------
    st.subheader("Heatmap: Delay Rate (%) by Day and Time Block")
    
    # Group by 3-hour blocks and day
    heatmap_data = filtered_df.groupby(["hour_group", "day_name"]).agg(
        total_flights=("flight_id", "count"),
        delayed_flights=("is_delayed", "sum")
    ).reset_index()
    
    # Calculate delay percentage
    heatmap_data["delay_pct"] = (heatmap_data["delayed_flights"] / heatmap_data["total_flights"]) * 100
    
    # Create pivot tables for Z values (percentage) and Custom Data (flight counts)
    pivot_pct = heatmap_data.pivot(index="hour_group", columns="day_name", values="delay_pct")
    pivot_count = heatmap_data.pivot(index="hour_group", columns="day_name", values="total_flights")
    
    # Reindex to ensure strict matrix shapes and order, filling missing slots with 0
    pivot_pct = pivot_pct.reindex(index=hour_buckets, columns=days_order).fillna(0)
    pivot_count = pivot_count.reindex(index=hour_buckets, columns=days_order).fillna(0)
    
    # Plot the Heatmap
    fig_heatmap = px.imshow(
        pivot_pct,
        labels=dict(x="Day of the Week", y="Time Block", color="Delay Rate (%)"),
        x=days_order,
        y=hour_buckets,
        color_continuous_scale="Reds",
        aspect="auto",
        text_auto=".1f"  # Show number inside square with 1 decimal place
    )
    
    # Inject the flight count matrix into the hover tooltip using customdata
    fig_heatmap.update_traces(
        customdata=pivot_count.values,
        hovertemplate="Day: %{x}<br>Time Block: %{y}<br>Delay Rate: %{z:.1f}%<br>Total Flights Analyzed: %{customdata}<extra></extra>"
    )
    
    # Invert Y axis so 00h-02h is at the top
    fig_heatmap.update_yaxes(autorange="reversed")
    
    st.plotly_chart(fig_heatmap, use_container_width=True)

with tab_fleet:
    st.subheader("Delay Rate vs Volume by Aircraft Type")
    aircraft_summary = filtered_df.groupby("aircraft_type").agg(
        total_flights=("flight_id", "count"),
        delayed_flights=("is_delayed", "sum")
    ).reset_index()
    
    aircraft_summary["delay_percentage"] = (aircraft_summary["delayed_flights"] / aircraft_summary["total_flights"]) * 100
    # Filters out aircraft with low flight volume (>= 10 flights) for significance
    aircraft_summary = aircraft_summary[aircraft_summary["total_flights"] >= 10].sort_values(by="delay_percentage", ascending=False)

    # Cria a figura com dois eixos Y (Eixo Primário e Eixo Secundário)
    fig_fleet = make_subplots(specs=[[{"secondary_y": True}]])

    # Adiciona as Barras (Taxa de Atraso em % no Eixo Y da esquerda)
    fig_fleet.add_trace(
        go.Bar(
            x=aircraft_summary["aircraft_type"],
            y=aircraft_summary["delay_percentage"],
            name="Delay Rate (%)",
            marker_color="#e74c3c", # Cor vermelha para as barras de atraso
            text=aircraft_summary["delay_percentage"].round(1).astype(str) + "%",
            textposition="auto",
            hovertemplate="Model: %{x}<br>Delay Rate: %{y:.1f}%<extra></extra>"
        ),
        secondary_y=False,
    )

    # Adiciona a Linha (Contagem de Voos no Eixo Y da direita)
    fig_fleet.add_trace(
        go.Scatter(
            x=aircraft_summary["aircraft_type"],
            y=aircraft_summary["total_flights"],
            name="Total Flights",
            mode="lines+markers",
            line=dict(color="#2980b9", width=3), # Linha azul para contraste
            marker=dict(size=8),
            hovertemplate="Model: %{x}<br>Total Flights: %{y}<extra></extra>"
        ),
        secondary_y=True,
    )

    # Configura o layout e os títulos dos eixos
    fig_fleet.update_layout(
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
        margin=dict(t=50) # Adiciona um espacinho no topo para a legenda
    )
    
    fig_fleet.update_xaxes(title_text="Aircraft Model")
    fig_fleet.update_yaxes(title_text="Delay Rate (%)", secondary_y=False, range=[0, 100]) # Trava o Y1 de 0 a 100%
    fig_fleet.update_yaxes(title_text="Total Flights Analysed", secondary_y=True, showgrid=False) # Remove o grid do Y2 para não poluir

    st.plotly_chart(fig_fleet, use_container_width=True)

# -----------------------------
# Raw Data Table
# -----------------------------
st.subheader("Consolidated Historical Data")
st.dataframe(
    filtered_df[["flight_id", "operator", "aircraft_type", "origin", "destination", "filed_dep_time", "delay_minutes", "is_delayed"]],
    use_container_width=True,
    hide_index=True,
)