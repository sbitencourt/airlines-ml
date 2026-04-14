# Aviationstack API Integration

Primary aviation data source used to retrieve flight status, airline routes, airport metadata, and airline information.

---

## Endpoints Used in This Project

The following Aviationstack endpoints are used:

| Endpoint | Purpose | Data Type |
|--------|--------|--------|
| flights | Flight status snapshots | Variable data |
| routes | Airline route network | Semi-static |
| airports | Airport metadata | Fixed data |
| airlines | Airline metadata | Fixed data |
| timetable | Daily flight schedules | Operational / temporal |

---

## Data Usage in This Project

The endpoints are used to retrieve different categories of aviation data:

- **Fixed data**
  - airports
  - airlines

- **Semi-static data**
  - routes (updated approximately every 24 hours)

- **Variable data**
  - flights (used to build periodic snapshots of air traffic activity)

- **Operational data**
  - timetable (daily flight schedules for airports)

This classification supports the project data architecture:

- **MongoDB** → flight snapshots (variable data)
- **SQL database** → structured metadata and derived reporting tables

---

# Endpoints

## /flights

The **Flights endpoint** is capable of tracking flights and retrieving flight status information in real time.

It can be used to look up information about one or multiple flights using optional query parameters to filter the result set.


### Query Parameters

#### Airline filters
- **airline_iata** – Filter by airline IATA code.
- **airline_icao** – Filter by airline ICAO code.
- **airline_name** – Filter by airline name.

#### Arrival filters
- **arr_iata** – Filter by arrival IATA code (e.g., `DFW`).
- **arr_icao** – Filter by arrival ICAO code (e.g., `KDFW`).
- **arr_scheduled_time_arr** – Filter by scheduled arrival date (`YYYY-MM-DD`).

#### Departure filters
- **dep_iata** – Filter by departure IATA code (e.g., `SFO`).
- **dep_icao** – Filter by departure ICAO code (e.g., `KSFO`).
- **dep_scheduled_time_dep** – Filter by scheduled departure date (`YYYY-MM-DD`).

#### Flight identifiers
- **flight_iata** – Filter by flight IATA (e.g., `AA1004`).
- **flight_icao** – Filter by flight ICAO (e.g., `AAL1004`).
- **flight_num** – Filter by flight number.

#### Flight status

Allowed values:

- `scheduled`
- `active`
- `landed`
- `cancelled`
- `incident`
- `diverted`

#### Historical data
- **flight_date** – Historical flight date (`YYYY-MM-DD`).

#### Delay filters
- **max_delay_arr**
- **max_delay_dep**
- **min_delay_arr**
- **min_delay_dep**

#### Pagination
- **limit** – Results per page (default `100`)
- **offset** – Pagination offset (default `0`)

#### Other
- **callback** – Optional JSONP callback function.

---

## /routes

The **Routes endpoint** provides airline route data updated approximately every **24 hours**.

This endpoint allows retrieving information about routes operated by airlines between departure and arrival airports.


### Query Parameters

#### Airline filters
- **airline_iata** – Filter by airline IATA code.
- **airline_icao** – Filter by airline ICAO code.

#### Arrival filters
- **arr_iata** – Filter by arrival IATA code (e.g., `DFW`).
- **arr_icao** – Filter by arrival ICAO code (e.g., `KDFW`).

#### Departure filters
- **dep_iata** – Filter by departure IATA code (e.g., `SFO`).
- **dep_icao** – Filter by departure ICAO code (e.g., `KSFO`).

#### Flight identifier
- **flight_num** – Filter by flight number.

#### Pagination
- **limit** – Number of results per page (default `100`)
- **offset** – Pagination offset (default `0`)

#### Other
- **callback** – Optional JSONP callback function.

---

## /airports

The **Airports endpoint** provides information about global airports, including location data, IATA codes, ICAO codes, and airport names.

Customers subscribed to the **Basic Plan or higher** can also use the `search` parameter to retrieve airport autocomplete suggestions.


### Query Parameters

#### Pagination
- **limit** – Number of results per page (default `100`)
- **offset** – Pagination offset (default `0`)

#### Search
- **search** – Free-text search used for airport lookup or autocomplete (available on certain paid plans).

#### Other
- **callback** – Optional JSONP callback function name.

---

## /airlines

The **Airlines endpoint** provides information about global airlines, including identifiers such as IATA code, ICAO code, callsign, airline name, and operational status.

Subscribers to paid plans may also use the `search` parameter to retrieve airline autocomplete suggestions.


### Query Parameters

#### Pagination
- **limit** – Number of results per page (default `100`)
- **offset** – Pagination offset (default `0`)

#### Search
- **search** – Free-text search used for airline lookup or autocomplete (available on certain paid plans).

#### Other
- **callback** – Optional JSONP callback function name.

---

## /timetable (Flight Schedules)

The **Flight Schedules endpoint** provides real-time timetable information for flights on the current day, including both departures and arrivals.

Rate limits:

- **Paid plans:** 1 request every 10 seconds  
- **Free plans:** 1 request every 60 seconds  


### Required Parameters

- **access_key** – API authentication key.
- **iataCode** – Airport IATA code (e.g., `JFK`, `DXB`).
- **type** – Schedule type:
  - `arrival`
  - `departure`

### Airline Filters

- **airline_iata**
- **airline_icao**
- **airline_name**

### Flight Identifiers

- **flight_iata**
- **flight_icao**
- **flight_num**

### Arrival Filters

- **arr_schTime**
- **arr_estTime**
- **arr_actTime**
- **arr_estRunway**
- **arr_actRunway**
- **arr_delay**
- **arr_terminal**

### Departure Filters

- **dep_schTime**
- **dep_estTime**
- **dep_actTime**
- **dep_estRunway**
- **dep_actRunway**
- **dep_delay**
- **dep_terminal**

### Flight Status

Allowed values:

- `scheduled`
- `active`
- `landed`
- `cancelled`
- `incident`
- `diverted`
- `redirected`
- `unknown`

### Pagination

- **limit** – Number of results per page (default `100`)
- **offset** – Pagination offset (default `0`)

### Other

- **callback** – Optional JSONP callback function name.




