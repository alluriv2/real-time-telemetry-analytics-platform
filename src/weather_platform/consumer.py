import json
import os
import sys
import time
from datetime import datetime

import psycopg
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

from weather_platform.config import (
    POSTGRES_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    CONSUMER_BATCH_SIZE,
    CONSUMER_BATCH_TIMEOUT_SECONDS,
    CONSUMER_STALL_TIMEOUT_SECONDS,
)
from weather_platform.ingestion_state import (
    create_ingestion_state_table,
    record_consumer_watermark,
)


# ---------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------

def parse_dt(raw_dt):
    if not raw_dt:
        return None

    # Convert timezone offset such as -0400 to -04:00.
    if (
        len(raw_dt) > 5
        and raw_dt[-5] in ["+", "-"]
        and raw_dt[-3] != ":"
    ):
        raw_dt = raw_dt[:-2] + ":" + raw_dt[-2:]

    try:
        return datetime.fromisoformat(raw_dt).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------
# PostgreSQL schema creation
# ---------------------------------------------------------

def create_weather_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            station TEXT NOT NULL,
            dt TIMESTAMP NOT NULL,
            vel_avg_mph DOUBLE PRECISION,
            vel_min_mph DOUBLE PRECISION,
            vel_max_mph DOUBLE PRECISION,
            dir_avg_deg BIGINT,
            temps_avg_f DOUBLE PRECISION,
            temps_min_f DOUBLE PRECISION,
            temps_max_f DOUBLE PRECISION,
            tempb_avg_f DOUBLE PRECISION,
            tempb_min_f DOUBLE PRECISION,
            tempb_max_f DOUBLE PRECISION,
            hum_avg_pct DOUBLE PRECISION,
            hum_min_pct DOUBLE PRECISION,
            hum_max_pct DOUBLE PRECISION,
            pres_avg_pa BIGINT,
            pres_min_pa BIGINT,
            pres_max_pa BIGINT,
            lux_avg_lx BIGINT,
            lux_min_lx BIGINT,
            lux_max_lx BIGINT,
            rain_inc_count BIGINT,
            rain_inc_in DOUBLE PRECISION,
            uptime_seconds BIGINT,
            millis BIGINT,
            node_ip TEXT,
            wifi_ssid TEXT,
            hostname TEXT,
            server_rmt_ip TEXT,
            server_svr_dt TIMESTAMP,
            PRIMARY KEY (station, dt)
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_dt
        ON weather (dt);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_station_dt
        ON weather (station, dt DESC);
        """)

    conn.commit()


def create_latest_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_latest (
            station TEXT PRIMARY KEY,
            dt TIMESTAMP NOT NULL,

            temperature DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            lux DOUBLE PRECISION,
            rain_inches DOUBLE PRECISION,

            updated_at TIMESTAMP NOT NULL
                DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Add these columns if weather_latest already existed
        # before humidity and pressure were introduced.
        cur.execute("""
        ALTER TABLE weather_latest
        ADD COLUMN IF NOT EXISTS humidity DOUBLE PRECISION;
        """)

        cur.execute("""
        ALTER TABLE weather_latest
        ADD COLUMN IF NOT EXISTS pressure DOUBLE PRECISION;
        """)

    conn.commit()


# ---------------------------------------------------------
# Kafka JSON extraction
# ---------------------------------------------------------

def extract_record(obj):
    node = obj.get("node", {})
    server = obj.get("server", {})

    dt = parse_dt(obj.get("dt"))

    if dt is None:
        return None

    station = obj.get("station") or node.get("hostname")

    if not station:
        return None

    return {
        "station": station,
        "dt": dt,

        "vel_avg_mph": obj.get("vel_avg_mph"),
        "vel_min_mph": obj.get("vel_min_mph"),
        "vel_max_mph": obj.get("vel_max_mph"),
        "dir_avg_deg": obj.get("dir_avg_deg"),

        "temps_avg_f": obj.get("temps_avg_f"),
        "temps_min_f": obj.get("temps_min_f"),
        "temps_max_f": obj.get("temps_max_f"),

        "tempb_avg_f": obj.get("tempb_avg_f"),
        "tempb_min_f": obj.get("tempb_min_f"),
        "tempb_max_f": obj.get("tempb_max_f"),

        "hum_avg_pct": obj.get("hum_avg_pct"),
        "hum_min_pct": obj.get("hum_min_pct"),
        "hum_max_pct": obj.get("hum_max_pct"),

        "pres_avg_pa": obj.get("pres_avg_pa"),
        "pres_min_pa": obj.get("pres_min_pa"),
        "pres_max_pa": obj.get("pres_max_pa"),

        "lux_avg_lx": obj.get("lux_avg_lx"),
        "lux_min_lx": obj.get("lux_min_lx"),
        "lux_max_lx": obj.get("lux_max_lx"),

        "rain_inc_count": obj.get("rain_inc_count"),
        "rain_inc_in": obj.get("rain_inc_in"),

        "uptime_seconds": node.get("uptime_seconds"),
        "millis": node.get("millis"),
        "node_ip": node.get("ip"),
        "wifi_ssid": node.get("wifi_ssid"),
        "hostname": node.get("hostname") or station,

        "server_rmt_ip": server.get("rmt_ip"),
        "server_svr_dt": parse_dt(server.get("svr_dt")),
    }


# ---------------------------------------------------------
# Raw weather upsert
# ---------------------------------------------------------

WEATHER_UPSERT_SQL = """
INSERT INTO weather (
    station,
    dt,

    vel_avg_mph,
    vel_min_mph,
    vel_max_mph,
    dir_avg_deg,

    temps_avg_f,
    temps_min_f,
    temps_max_f,

    tempb_avg_f,
    tempb_min_f,
    tempb_max_f,

    hum_avg_pct,
    hum_min_pct,
    hum_max_pct,

    pres_avg_pa,
    pres_min_pa,
    pres_max_pa,

    lux_avg_lx,
    lux_min_lx,
    lux_max_lx,

    rain_inc_count,
    rain_inc_in,

    uptime_seconds,
    millis,
    node_ip,
    wifi_ssid,
    hostname,

    server_rmt_ip,
    server_svr_dt
)
VALUES (
    %(station)s,
    %(dt)s,

    %(vel_avg_mph)s,
    %(vel_min_mph)s,
    %(vel_max_mph)s,
    %(dir_avg_deg)s,

    %(temps_avg_f)s,
    %(temps_min_f)s,
    %(temps_max_f)s,

    %(tempb_avg_f)s,
    %(tempb_min_f)s,
    %(tempb_max_f)s,

    %(hum_avg_pct)s,
    %(hum_min_pct)s,
    %(hum_max_pct)s,

    %(pres_avg_pa)s,
    %(pres_min_pa)s,
    %(pres_max_pa)s,

    %(lux_avg_lx)s,
    %(lux_min_lx)s,
    %(lux_max_lx)s,

    %(rain_inc_count)s,
    %(rain_inc_in)s,

    %(uptime_seconds)s,
    %(millis)s,
    %(node_ip)s,
    %(wifi_ssid)s,
    %(hostname)s,

    %(server_rmt_ip)s,
    %(server_svr_dt)s
)
ON CONFLICT (station, dt)
DO UPDATE SET
    vel_avg_mph = EXCLUDED.vel_avg_mph,
    vel_min_mph = EXCLUDED.vel_min_mph,
    vel_max_mph = EXCLUDED.vel_max_mph,
    dir_avg_deg = EXCLUDED.dir_avg_deg,

    temps_avg_f = EXCLUDED.temps_avg_f,
    temps_min_f = EXCLUDED.temps_min_f,
    temps_max_f = EXCLUDED.temps_max_f,

    tempb_avg_f = EXCLUDED.tempb_avg_f,
    tempb_min_f = EXCLUDED.tempb_min_f,
    tempb_max_f = EXCLUDED.tempb_max_f,

    hum_avg_pct = EXCLUDED.hum_avg_pct,
    hum_min_pct = EXCLUDED.hum_min_pct,
    hum_max_pct = EXCLUDED.hum_max_pct,

    pres_avg_pa = EXCLUDED.pres_avg_pa,
    pres_min_pa = EXCLUDED.pres_min_pa,
    pres_max_pa = EXCLUDED.pres_max_pa,

    lux_avg_lx = EXCLUDED.lux_avg_lx,
    lux_min_lx = EXCLUDED.lux_min_lx,
    lux_max_lx = EXCLUDED.lux_max_lx,

    rain_inc_count = EXCLUDED.rain_inc_count,
    rain_inc_in = EXCLUDED.rain_inc_in,

    uptime_seconds = EXCLUDED.uptime_seconds,
    millis = EXCLUDED.millis,
    node_ip = EXCLUDED.node_ip,
    wifi_ssid = EXCLUDED.wifi_ssid,
    hostname = EXCLUDED.hostname,

    server_rmt_ip = EXCLUDED.server_rmt_ip,
    server_svr_dt = EXCLUDED.server_svr_dt;
"""


# ---------------------------------------------------------
# Latest-state upsert
# ---------------------------------------------------------

LATEST_UPSERT_SQL = """
INSERT INTO weather_latest (
    station,
    dt,
    temperature,
    wind_speed,
    wind_direction,
    humidity,
    pressure,
    lux,
    rain_inches,
    updated_at
)
VALUES (
    %(station)s,
    %(dt)s,
    %(temps_avg_f)s,
    %(vel_avg_mph)s,
    %(dir_avg_deg)s,
    %(hum_avg_pct)s,
    %(pressure_hpa)s,
    %(lux_avg_lx)s,
    %(rain_inc_in)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (station)
DO UPDATE SET
    dt = EXCLUDED.dt,
    temperature = EXCLUDED.temperature,
    wind_speed = EXCLUDED.wind_speed,
    wind_direction = EXCLUDED.wind_direction,
    humidity = EXCLUDED.humidity,
    pressure = EXCLUDED.pressure,
    lux = EXCLUDED.lux,
    rain_inches = EXCLUDED.rain_inches,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.dt >= weather_latest.dt;
"""


def write_weather_batch(
    conn,
    batch,
):
    """
    Write raw observations, latest-state rows, and ingestion watermarks
    in one PostgreSQL transaction.
    """
    if not batch:
        return

    records = [item["record"] for item in batch]

    try:
        with conn.cursor() as cur:
            cur.executemany(WEATHER_UPSERT_SQL, records)
            cur.executemany(LATEST_UPSERT_SQL, records)

            for item in batch:
                record = item["record"]
                record_consumer_watermark(
                    cur=cur,
                    station=record["station"],
                    timestamp=record["dt"],
                    partition=item["partition"],
                    offset=item["offset"],
                )

        conn.commit()

    except Exception:
        conn.rollback()
        raise


def kafka_offsets_after(messages):
    """
    Return the next offset to commit for each partition represented in
    a successfully processed batch.
    """
    next_offsets = {}

    for msg in messages:
        key = (msg.topic(), msg.partition())
        next_offset = msg.offset() + 1
        next_offsets[key] = max(
            next_offset,
            next_offsets.get(key, next_offset),
        )

    return [
        TopicPartition(topic, partition, offset)
        for (topic, partition), offset in next_offsets.items()
    ]


class StallWatchdog:
    """
    Detect a consumer that is running but no longer part of its group.

    librdkafka can lose the group coordinator and keep returning empty
    polls forever while the process stays up. In that state the consumer
    holds no partition assignment, so a long period without one means
    it is stuck.
    """

    def __init__(self, timeout_seconds, now):
        self.timeout_seconds = timeout_seconds
        self.last_assigned_at = now

    def is_stalled(self, has_assignment, now):
        if has_assignment:
            self.last_assigned_at = now
            return False
        return now - self.last_assigned_at > self.timeout_seconds


# ---------------------------------------------------------
# Main consumer
# ---------------------------------------------------------

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_CONSUMER_GROUP}")
    print(
        f"Batching up to {CONSUMER_BATCH_SIZE} messages "
        f"for {CONSUMER_BATCH_TIMEOUT_SECONDS:g} seconds."
    )
    print("Press Ctrl+C to stop.\n")

    try:
        with psycopg.connect(**POSTGRES_CONFIG) as conn:
            create_weather_table(conn)
            create_latest_table(conn)
            create_ingestion_state_table(conn)

            watchdog = StallWatchdog(
                CONSUMER_STALL_TIMEOUT_SECONDS,
                time.monotonic(),
            )

            while True:
                messages = consumer.consume(
                    num_messages=CONSUMER_BATCH_SIZE,
                    timeout=CONSUMER_BATCH_TIMEOUT_SECONDS,
                )

                if watchdog.is_stalled(
                    bool(consumer.assignment()),
                    time.monotonic(),
                ):
                    print(
                        "Consumer has held no partition assignment for "
                        f"over {CONSUMER_STALL_TIMEOUT_SECONDS:g} seconds; "
                        "exiting so Docker restarts it. Uncommitted "
                        "Kafka offsets will be replayed safely."
                    )
                    sys.stdout.flush()
                    # Skip consumer.close(): it can block on the same
                    # unresponsive coordinator that caused the stall.
                    os._exit(1)

                if not messages:
                    continue

                try:
                    batch = []
                    processed_messages = []

                    for msg in messages:
                        if msg.error():
                            if (
                                msg.error().code()
                                == KafkaError.UNKNOWN_TOPIC_OR_PART
                            ):
                                print(
                                    "Kafka topic is not ready yet; "
                                    "waiting for the producer to "
                                    "create it."
                                )
                                continue
                            raise KafkaException(msg.error())

                        processed_messages.append(msg)

                        try:
                            obj = json.loads(
                                msg.value().decode("utf-8")
                            )
                        except (
                            json.JSONDecodeError,
                            UnicodeDecodeError,
                        ):
                            print(
                                "Skipping malformed message "
                                f"at partition {msg.partition()} "
                                f"offset {msg.offset()}"
                            )
                            continue

                        record = extract_record(obj)

                        if record is None:
                            print(
                                "Skipping malformed message "
                                f"at partition {msg.partition()} "
                                f"offset {msg.offset()}"
                            )
                            continue

                        # Keep raw pressure in Pa in the weather table.
                        # Store latest/dashboard pressure in hPa.
                        if record["pres_avg_pa"] is not None:
                            record["pressure_hpa"] = (
                                record["pres_avg_pa"] / 100
                            )
                        else:
                            record["pressure_hpa"] = None

                        batch.append({
                            "record": record,
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        })

                    # All database writes in the batch commit together.
                    write_weather_batch(
                        conn=conn,
                        batch=batch,
                    )

                    # Commit each partition's next Kafka offset only after
                    # PostgreSQL commits. A crash between the two commits
                    # can replay the batch, so writes remain idempotent.
                    offsets = kafka_offsets_after(processed_messages)
                    if offsets:
                        consumer.commit(
                            offsets=offsets,
                            asynchronous=False,
                        )

                    if batch:
                        print(
                            f"Stored batch of {len(batch)} records "
                            f"from {len(offsets)} partition(s)"
                        )

                except Exception as exc:
                    print(
                        "Error processing Kafka batch: "
                        f"{exc}"
                    )
                    print(
                        "The batch stopped; any uncommitted Kafka "
                        "offsets will be replayed safely."
                    )
                    break

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
