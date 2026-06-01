# DST Airlines - carpeta `api`

Esta carpeta agrega una FastAPI mínima al proyecto sin cambiar el flujo actual de Grafana.

## Rol

- **Grafana Business** continúa leyendo la tabla SQL preparada para exponer insights a responsables del proceso.
- **Grafana Health + Prometheus** continúan monitoreando la salud del pipeline.
- **FastAPI** expone endpoints REST para consumidores como Dash, Streamlit, Swagger o clientes externos.

## Ubicación recomendada

Copiar esta carpeta en:

```text
src/dst_airlines/api/
```

## Dependencias a agregar en `pyproject.toml`

```toml
"fastapi",
"uvicorn[standard]",
"psycopg2-binary"
```

## Variables de entorno útiles

Los defaults están alineados con tu `docker-compose.yml` actual.

```env
POSTGRES_URI=postgresql://root:passwd@postgres:5432/dst_airlines
SQL_BUSINESS_TABLE=flight_predictions

MONGO_URI=mongodb://root:passwd@mongodb:27017/?authSource=admin
MONGO_DB=dst_airlines
MONGO_FLIGHTS_COLLECTION=flights

SQL_BUSINESS_TABLE=business_flights
```

Cambia `SQL_BUSINESS_TABLE` al nombre real de tu tabla SQL preparada para Grafana.

## Servicio sugerido para `docker-compose.yml`

```yaml
  fastapi:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: dst-airlines-api
    depends_on:
      - postgres
      - mongodb
    env_file:
      - .env.docker
    environment:
      PYTHONPATH: /opt/airflow/project/src
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DB: airflow
      POSTGRES_USER: root
      POSTGRES_PASSWORD: passwd
      MONGO_URI: mongodb://root:passwd@mongodb:27017/?authSource=admin
      SQL_BUSINESS_TABLE: business_flights
    volumes:
      - ./:/opt/airflow/project
    working_dir: /opt/airflow/project
    command: uvicorn dst_airlines.api.main:app --host 0.0.0.0 --port 8000 --reload
    ports:
      - "8000:8000"
```

## Endpoints incluidos


GET /health
GET /flights/latest
GET /flights/search?flight_iata=IW1851&limit=10
GET /mongo/flights/latest
GET /mongo/flights/search?flight_iata=IW1851&limit=10
GET /business/kpis
GET /flights/status-summary



## Documentación Swagger

Una vez levantado el servicio:

```text
http://localhost:8000/docs
```
