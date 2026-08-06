# Weather Analytics Platform — AWS

A containerized, event-driven weather analytics platform designed to run on a
single AWS Lightsail Linux instance with Docker Compose. The platform collects
historical and live weather observations, routes every observation through
Apache Kafka, stores idempotent records in PostgreSQL, calculates aggregates,
serves data through FastAPI, and presents current conditions and historical
trends through a Dash web application.

The public dashboard is served through Nginx. PostgreSQL, Kafka, Prometheus,
Grafana, and Kafka UI remain private.

## Architecture

```text
Historical weather source                         Live weather source
          |                                                |
          v                                                v
   backfill container                               producer container
          |                                                |
          +--------------------> Kafka <--------------------+
                                  |
                                  v
                            consumer container
                                  |
                                  v
                     PostgreSQL container + host data
                            |             |
                            v             v
                      aggregator       FastAPI
                                          |
                                          v
                                    Dash dashboard
                                          |
                                          v
Internet --> Lightsail firewall --> Nginx :80/:443 --> Dash :8050
```

### Components

| Component | Responsibility |
| --- | --- |
| Backfill | Reads missing historical observations and publishes them to Kafka. It never writes weather records directly to PostgreSQL. |
| Producer | Polls the configured weather sources and publishes new observations to Kafka continuously. |
| Kafka | Retains weather events and consumer-group offsets so ingestion can resume after an interruption. |
| Consumer | Subscribes to the weather topic and performs idempotent PostgreSQL writes. |
| PostgreSQL | Stores weather observations, ingestion state, and aggregate tables. |
| Aggregator | Refreshes the historical aggregate windows used by analytics queries. |
| FastAPI | Provides health, metrics, current-condition, and historical-data endpoints. |
| Dash/Gunicorn | Renders the browser-based weather dashboard. |
| Nginx | Accepts public HTTP/HTTPS traffic and reverse-proxies it to Dash. |
| Prometheus and exporters | Collect application, PostgreSQL, Kafka, and container metrics. |
| Grafana | Displays the provisioned platform monitoring dashboard. |
| Kafka UI | Provides an optional private interface for Kafka topics and messages. |

## Repository structure

```text
.
├── compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example
├── start
├── stop
├── status
├── src/
│   └── weather_platform/
│       ├── backfill.py
│       ├── producer.py
│       ├── consumer.py
│       ├── aggregation.py
│       ├── ingestion_state.py
│       ├── api.py
│       ├── config.py
│       └── dashboard/
├── monitoring/
│   ├── prometheus/
│   └── grafana/
└── local-data/                 # created at runtime and excluded from Git
    ├── postgres/
    ├── kafka/
    ├── prometheus/
    └── grafana/
```

## Runtime and persistence model

The repository contains source code and deployment configuration. Docker builds
the Python application image from the checked-out source and runs separate
containers from that image for the producer, consumer, backfill, aggregator,
API, and dashboard.

PostgreSQL and Kafka use their official images. Their persistent directories
are bind-mounted from `local-data/` on the host:

```text
Lightsail disk                       Container
local-data/postgres/  <---------->   /var/lib/postgresql/data
local-data/kafka/     <---------->   /var/lib/kafka/data
```

Removing or recreating a container does not remove these directories. Running
`./stop`, closing SSH, or rebooting the instance does not intentionally delete
the data. Deleting `local-data/postgres`, deleting `local-data/kafka`, or
deleting the Lightsail instance can destroy the corresponding state.

Persistent storage is not a backup. Use PostgreSQL backups and Lightsail
snapshots before destructive maintenance or instance replacement.

## Requirements

### Local development

- Git
- Docker Desktop or Docker Engine
- Docker Compose v2 (`docker compose`)
- At least 2 GB of available memory
- Approximately 20 GB of free storage to begin

### AWS Lightsail

- A 64-bit Ubuntu Lightsail instance
- 2 GB RAM for the core platform; 4 GB is recommended when monitoring tools run continuously
- At least 60 GB of SSD storage for the initial deployment and retained Kafka data
- A Lightsail SSH key
- A static IPv4 address
- A domain or subdomain if HTTPS is required

## Configuration

Create the runtime environment file:

```bash
cp .env.example .env
nano .env
```

Replace every `replace-with-...` placeholder. At minimum, set strong,
independent values for:

```dotenv
POSTGRES_PASSWORD=replace-with-a-strong-password
GRAFANA_ADMIN_PASSWORD=replace-with-another-strong-password
```

Important settings include:

| Variable | Purpose |
| --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | PostgreSQL database and credentials |
| `KAFKA_TOPIC` | Weather-event topic |
| `KAFKA_CONSUMER_GROUP` | Consumer group whose offsets track database ingestion |
| `PRODUCER_POLL_SECONDS` | Live-source polling interval |
| `RECONCILIATION_LOOKBACK_DAYS` | Overlap used when reconciling source and database timestamps |
| `API_BASE_URL` | Internal API URL used by the dashboard |
| Station names and URLs | Weather data sources |
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | Grafana administrator credentials |

For an Nginx-hosted Lightsail deployment, add this line to `.env` so Dash is
reachable only from the instance itself:

```dotenv
DASHBOARD_BIND_ADDRESS=127.0.0.1
```

Never commit `.env`. Both `.env` and `local-data/` are excluded from Git and
the Docker build context.

## Run locally

From the repository root:

```bash
cp .env.example .env
nano .env
./start
```

Open:

```text
http://localhost:8050
```

The first startup can take a long time because the historical source is read,
events are published to Kafka, and the consumer inserts them into PostgreSQL.
The terminal running `./start` remains attached to the one-shot backfill
container. Closing that terminal during the backfill interrupts that one
process, but already-published Kafka messages and consumed database records
remain persisted.

### What `./start` does

`./start` performs these steps in order:

1. Verifies Docker, Docker Compose, and `.env`.
2. Creates the persistent `local-data/` directories.
3. Builds `weather-platform-aws:local` from the current source.
4. Starts PostgreSQL and Kafka and waits for their health checks.
5. Starts the consumer before any backfill events are published.
6. Compares the upstream source with stored ingestion/database timestamps.
7. Publishes the required historical or reconciliation window through Kafka.
8. Starts the producer, consumer, aggregator, API, and dashboard.
9. Starts optional monitoring or administration profiles when requested.

Because the consumer uses idempotent writes and the reconciliation process
uses an overlap window, restarting safely rechecks recent data without creating
duplicate weather records.

## Operations

### View service and resource status

```bash
./status
```

The report includes container health, memory and CPU usage, and persistent
directory sizes. PostgreSQL files may be owned by the container user; if its
reported directory size appears too small, run:

```bash
sudo du -sh local-data/postgres
```

### Stop the platform

```bash
./stop
```

This removes application containers and the Compose network but retains
everything under `local-data/`.

### Start again

```bash
./start
```

The platform reuses PostgreSQL and Kafka data, checks the latest timestamps,
publishes only the required reconciliation window, and resumes live ingestion.

### View logs

```bash
docker compose logs --tail=100
docker compose logs -f consumer
docker compose logs -f kafka
docker compose logs -f dashboard
```

Press `Ctrl+C` to stop following logs. This does not stop the containers.

### Check Kafka consumer lag

Use the small diagnostic heap shown below. Kafka command-line utilities launch
another JVM inside the broker container, so allowing them to inherit the
broker heap can exceed the container memory limit on a small server.

```bash
docker compose exec \
  -e KAFKA_HEAP_OPTS="-Xms32m -Xmx64m" \
  kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group weather-postgres-consumer-aws
```

Key columns:

- `LOG-END-OFFSET`: newest offset in the Kafka partition
- `CURRENT-OFFSET`: offset committed by the consumer group
- `LAG`: events that have not yet been committed by the consumer

`LAG = 0` means the database consumer is caught up.

### Back up PostgreSQL

```bash
mkdir -p ~/weather-backups
docker compose exec -T postgres sh -c \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  > "$HOME/weather-backups/weather-$(date +%Y%m%d-%H%M%S).sql"
```

The database variables are read inside the PostgreSQL container. Store
important backups somewhere other than the Lightsail instance.

## Optional monitoring and administration

Start the core platform only:

```bash
./start
```

Start Prometheus, Grafana, and exporters:

```bash
./start --monitoring
```

Start Kafka UI:

```bash
./start --tools
```

Start everything:

```bash
./start --all
```

On a 2 GB instance, wait for the initial backfill and consumer catch-up to
finish before starting monitoring or Kafka UI.

Prometheus, Grafana, and Kafka UI bind to the instance loopback address. Open
SSH tunnels from the local computer:

```bash
ssh \
  -L 29090:127.0.0.1:29090 \
  -L 23000:127.0.0.1:23000 \
  -L 18080:127.0.0.1:18080 \
  lightsail
```

Then open:

- Prometheus: `http://127.0.0.1:29090`
- Grafana: `http://127.0.0.1:23000`
- Kafka UI: `http://127.0.0.1:18080`

Do not expose these ports publicly.

## Deploy to AWS Lightsail

### 1. Create the instance

In Amazon Lightsail:

1. Create a Linux/Unix instance.
2. Select **Linux operating system** and an Ubuntu LTS image.
3. Select a plan with at least 2 GB RAM; use 4 GB if monitoring will run continuously.
4. Give the instance a descriptive name.
5. Download and protect the Lightsail SSH private key.

AWS Lightsail documentation:

- [Launch a Lightsail Linux instance](https://docs.aws.amazon.com/lightsail/)
- [Lightsail networking overview](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-networking.html)

### 2. Configure SSH on the local computer

Protect the downloaded key:

```bash
chmod 600 /absolute/path/to/LightsailDefaultKey.pem
```

Add an entry to `~/.ssh/config`:

```sshconfig
Host lightsail
    HostName LIGHTSAIL_PUBLIC_OR_STATIC_IP
    User ubuntu
    IdentityFile /absolute/path/to/LightsailDefaultKey.pem
    Port 22
```

Connect:

```bash
ssh lightsail
```

After attaching a static IP, update `HostName` if the address changed.

### 3. Install Git and Docker

Install basic packages:

```bash
sudo apt update
sudo apt install -y git ca-certificates curl
```

Install Docker Engine and the Docker Compose plugin by following Docker's
official Ubuntu instructions:

- [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)
- [Install the Docker Compose plugin](https://docs.docker.com/compose/install/linux/)

Verify the installation:

```bash
sudo systemctl enable --now docker
docker --version
docker compose version
```

If Docker requires `sudo`, add the Ubuntu user to the Docker group:

```bash
sudo usermod -aG docker "$USER"
exit
```

Reconnect with `ssh lightsail`, then verify:

```bash
docker run --rm hello-world
```

Membership in the Docker group grants root-equivalent control of Docker on
that server. Only trusted users should receive SSH access.

### 4. Give the instance read access to the private repository

For a private GitHub repository, create a dedicated read-only deploy key on the
Lightsail instance:

```bash
ssh-keygen -t ed25519 -C "weather-analytics-lightsail"
cat ~/.ssh/id_ed25519.pub
```

In the GitHub repository, open **Settings → Deploy keys → Add deploy key**,
paste the public key, and leave write access disabled.

Test:

```bash
ssh -T git@github.com
```

Clone:

```bash
git clone git@github.com:OWNER/weather-analytics-platform-aws.git
cd weather-analytics-platform-aws
```

Never copy or expose `~/.ssh/id_ed25519`; only the `.pub` file belongs in
GitHub.

### 5. Configure and start the platform

```bash
cp .env.example .env
nano .env
```

Replace the passwords and add:

```dotenv
DASHBOARD_BIND_ADDRESS=127.0.0.1
```

Start:

```bash
./start
```

In another SSH session:

```bash
cd ~/weather-analytics-platform-aws
./status
```

Confirm the dashboard responds inside the instance:

```bash
curl -I http://127.0.0.1:8050
```

### 6. Install and configure Nginx

Install Nginx:

```bash
sudo apt update
sudo apt install -y nginx
sudo systemctl enable --now nginx
```

Create `/etc/nginx/sites-available/weather-platform`:

```nginx
server {
    listen 80;
    listen [::]:80;

    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8050;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 120s;
        proxy_connect_timeout 10s;
    }
}
```

Enable the site and disable the default welcome page:

```bash
sudo ln -s /etc/nginx/sites-available/weather-platform \
  /etc/nginx/sites-enabled/weather-platform
sudo unlink /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

Test the proxy:

```bash
curl -I http://127.0.0.1
```

The response should report `Server: nginx` and an HTTP success status.

### 7. Attach a static IP and configure the firewall

In Lightsail, open **Networking → Create static IP**, select the instance, and
attach the address. A static IPv4 address prevents DNS and SSH configuration
from breaking when the instance is stopped and started.

Configure the Lightsail IPv4 and, if enabled, IPv6 firewalls:

| Port | Exposure | Purpose |
| --- | --- | --- |
| `22/tcp` | Restrict to trusted source IPs when practical | SSH |
| `80/tcp` | Public | HTTP and certificate validation |
| `443/tcp` | Public | HTTPS |

Do not publicly open:

```text
5432  PostgreSQL
9092  Kafka
8050  Direct Dash/Gunicorn
18080 Kafka UI tunnel
23000 Grafana tunnel
29090 Prometheus tunnel
```

AWS references:

- [Create and attach a static IP](https://docs.aws.amazon.com/lightsail/latest/userguide/lightsail-create-static-ip.html)
- [Control Lightsail traffic with firewalls](https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-firewall-and-port-mappings-in-amazon-lightsail.html)

Test from another computer:

```text
http://LIGHTSAIL_STATIC_IP
```

### 8. Connect a portfolio subdomain

Keep the existing portfolio at its current host and point only a subdomain to
Lightsail. In the DNS provider, create:

```text
Type:  A
Name:  weather
Value: LIGHTSAIL_STATIC_IP
TTL:   300 or Automatic
```

This produces a URL such as:

```text
weather.example.com
```

Check DNS:

```bash
dig +short weather.example.com
```

It should return the Lightsail static IPv4 address. AWS also documents
[mapping a domain to Lightsail with DNS](https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-dns-in-amazon-lightsail.html).

Update the Nginx configuration:

```nginx
server_name weather.example.com;
```

Validate and reload:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

The portfolio can now link to `http://weather.example.com`.

### 9. Enable HTTPS

Ensure DNS resolves to the static IP and ports `80` and `443` are open. Then
install Certbot:

```bash
sudo apt update
sudo apt install -y certbot python3-certbot-nginx
```

Request and install the certificate:

```bash
sudo certbot --nginx -d weather.example.com
```

Test automatic renewal:

```bash
sudo certbot renew --dry-run
```

The public application is then available at:

```text
https://weather.example.com
```

## Deploy application updates

Commit and push tested changes from the development computer. On Lightsail:

```bash
ssh lightsail
cd ~/weather-analytics-platform-aws
git pull origin main
./start
```

`./start` rebuilds the application image from the new source and reconciles the
source/database timestamps before resuming continuous services. It does not
delete `.env` or `local-data/`.

If only the Nginx configuration changes, Docker does not need to restart:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## Reboots and SSH disconnections

Closing the SSH terminal does not stop Lightsail or the containers. Compose
services use `restart: unless-stopped`, Nginx is enabled through systemd, and
Docker starts at boot. After a reboot:

```bash
ssh lightsail
cd ~/weather-analytics-platform-aws
./status
sudo systemctl status nginx --no-pager
```

Run `./start` only if the application is stopped or an explicit reconciliation
and rebuild is required.

## Troubleshooting

### Kafka repeatedly restarts

Check:

```bash
docker compose ps kafka
docker inspect \
  --format='RestartCount={{.RestartCount}} OOMKilled={{.State.OOMKilled}}' \
  weather-analytics-aws-kafka-1
docker compose logs --tail=200 kafka
```

Search recent Docker events for memory kills:

```bash
docker events \
  --since 20m \
  --until "$(date -Iseconds)" \
  --filter container=weather-analytics-aws-kafka-1
```

An `oom` event or exit code `137` means the container exceeded its memory
allowance. Always run Kafka diagnostic utilities with the small heap shown in
the lag command. Do not delete `local-data/kafka` while troubleshooting.

### Nginx returns `502 Bad Gateway`

Nginx is running but cannot reach Dash. Check:

```bash
docker compose ps dashboard
curl -I http://127.0.0.1:8050
docker compose logs --tail=100 dashboard
```

### Public IP works but the domain does not

Check:

```bash
dig +short weather.example.com
sudo nginx -T
```

The DNS result must match the attached Lightsail static IP, and `server_name`
must match the requested hostname.

### SSH stops working after attaching a static IP

Update the `HostName` field for `Host lightsail` in `~/.ssh/config`. Also
confirm that the Lightsail firewall permits TCP port `22` from the current
public IP.

### Dashboard shows unavailable values during first startup

The web container can become available before the historical consumer reaches
zero lag. Check consumer logs and Kafka lag. Values populate as PostgreSQL
catches up.

## Security checklist

- Keep `.env`, private keys, and `local-data/` out of Git.
- Use strong, unique PostgreSQL and Grafana passwords.
- Use a read-only GitHub deploy key on the server.
- Bind Dash to `127.0.0.1` when Nginx is the public entry point.
- Expose only ports `80` and `443` publicly.
- Restrict SSH to trusted source addresses where practical.
- Never expose PostgreSQL, Kafka, Prometheus, Grafana, or Kafka UI publicly.
- Apply Ubuntu, Docker, and container-image security updates.
- Use HTTPS before sharing the dashboard publicly.
- Store backups and snapshots outside the running application instance.
