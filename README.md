# Weather Analytics Platform — AWS

AWS-hosted, containerized weather analytics platform using Python, Kafka,
PostgreSQL, Docker Compose, Prometheus, and Grafana for real-time ingestion,
processing, visualization, reconciliation, and monitoring.

## Architecture

The initial backfill and continuous producer publish weather observations to
Kafka. The consumer writes idempotent records to PostgreSQL, the aggregator
refreshes historical windows, FastAPI serves the data, and Dash renders the
dashboard.

```text
Weather source --> backfill / producer --> Kafka --> consumer --> PostgreSQL
                                                               |           |
                                                               v           v
                                                          aggregator    FastAPI
                                                                          |
                                                                          v
                                                                    Dash dashboard
```

PostgreSQL, Kafka, Prometheus, and Grafana state is stored under
`local-data/`. Running `./stop` removes containers and the Compose network but
retains this data.

## Requirements

- Docker Engine and Docker Compose v2
- Git
- At least 2 GB RAM; 4 GB is recommended for continuous monitoring
- Approximately 20 GB of free disk space to begin

## First run

```bash
cp .env.example .env
nano .env
./start
```

Replace every `replace-with-...` value before starting. Never commit `.env`.
Startup builds the current source, starts PostgreSQL and Kafka, starts the
consumer, publishes missing history through Kafka, and starts the continuous
application services.

Open `http://SERVER_IP:8050`.

## Optional services

On a 2 GB server, run these only when needed:

```bash
./start --monitoring
./start --tools
./start --all
```

Prometheus, Grafana, and Kafka UI bind only to server loopback. Access them
through an SSH tunnel:

```bash
ssh -L 29090:127.0.0.1:29090 \
    -L 23000:127.0.0.1:23000 \
    -L 18080:127.0.0.1:18080 lightsail
```

- Prometheus: `http://localhost:29090`
- Grafana: `http://localhost:23000`
- Kafka UI: `http://localhost:18080`

## Operations

```bash
./status
./stop
```

`./status` reports service state, current container resources, and persistent
storage. `./stop` retains everything under `local-data/`. The next `./start`
compares PostgreSQL timestamps with the upstream source and republishes only
the required reconciliation window through Kafka.

### Check Kafka consumer lag

Run the diagnostic CLI with a small heap so it can coexist with the broker on
a 2 GB server:

```bash
docker compose exec \
  -e KAFKA_HEAP_OPTS="-Xms32m -Xmx64m" \
  kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group weather-postgres-consumer-aws
```

`LAG` is the number of messages waiting for the consumer group. A value of
zero means PostgreSQL ingestion has caught up with the Kafka topic.

## Persistence

```text
local-data/
├── postgres/
├── kafka/
├── prometheus/
└── grafana/
```

Deleting `local-data/postgres` deletes the database. Deleting
`local-data/kafka` deletes retained Kafka messages and broker metadata. Back
up PostgreSQL before server replacement or destructive maintenance.

## AWS Lightsail

- Open TCP port `8050` only for the public dashboard.
- Keep database, Kafka, monitoring, and administration ports closed.
- Attach a static IP before configuring DNS.
- Use SSH tunnels for monitoring and administrative tools.
- On a 2 GB plan, enable swap and avoid monitoring during a full backfill.

## Security

- `.env` and `local-data/` are excluded from Git and Docker build context.
- Administrative interfaces bind only to the server loopback interface.
- PostgreSQL and Kafka are available only on the private Compose network.
- Rotate credentials if an `.env` value is exposed.
