"""
sensor_consumer.py
------------------
Reads sensor messages from Kafka topics in real time.
Applies anomaly detection logic, stores data in SQLite,
and writes a JSON snapshot file for the Flask backend to serve.

Topics consumed:
  - sensor-temperature
  - sensor-gas

Run: python sensor_consumer.py
"""

import json
import sqlite3
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

KAFKA_BROKER      = "localhost:9092"
TOPICS            = ["sensor-temperature", "sensor-gas"]
CONSUMER_GROUP    = "cooling-system-group"
DB_PATH           = Path("data/sensor_data.db")
SNAPSHOT_PATH     = Path("data/latest_snapshot.json")

TEMP_WARNING      = 30.0      # °C
TEMP_CRITICAL     = 40.0      # °C
GAS_WARNING       = 400       # ppm
GAS_CRITICAL      = 700       # ppm
MAX_HISTORY       = 50        # data points kept in snapshot

# In-memory state shared between consumer thread and snapshot writer
_lock    = threading.Lock()
_state   = {
    "temperature":  [],
    "gas":          [],
    "alerts":       [],
    "latest_temp":  None,
    "latest_gas":   None,
}


# ─── Database ────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS temperature_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id   TEXT,
            temperature REAL,
            fan_status  TEXT,
            fan_pwm     INTEGER,
            alert_level TEXT,
            recorded_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gas_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id   TEXT,
            gas_ppm     INTEGER,
            alert_level TEXT,
            recorded_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_type TEXT,
            alert_level TEXT,
            message     TEXT,
            value       TEXT,
            created_at  TEXT
        )
    """)
    conn.commit()
    log.info("Database initialised at %s", DB_PATH)


def store_temperature(conn: sqlite3.Connection, msg: dict):
    conn.execute(
        "INSERT INTO temperature_readings (sensor_id, temperature, fan_status, fan_pwm, alert_level, recorded_at) VALUES (?,?,?,?,?,?)",
        (msg["sensor_id"], msg["temperature"], msg["fan_status"], msg["fan_pwm"], msg["alert_level"], msg["timestamp"])
    )
    conn.commit()


def store_gas(conn: sqlite3.Connection, msg: dict):
    conn.execute(
        "INSERT INTO gas_readings (sensor_id, gas_ppm, alert_level, recorded_at) VALUES (?,?,?,?)",
        (msg["sensor_id"], msg["gas_ppm"], msg["alert_level"], msg["timestamp"])
    )
    conn.commit()


def store_alert(conn: sqlite3.Connection, sensor_type: str, level: str, message: str, value: str):
    conn.execute(
        "INSERT INTO alerts (sensor_type, alert_level, message, value, created_at) VALUES (?,?,?,?,?)",
        (sensor_type, level, message, value, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()


# ─── Processing logic ─────────────────────────────────────────────────────────

def process_temperature(msg: dict, conn: sqlite3.Connection):
    temp  = msg["temperature"]
    level = msg["alert_level"]
    fan   = msg["fan_status"]
    pwm   = msg["fan_pwm"]
    ts    = msg["timestamp"]

    store_temperature(conn, msg)

    point = {"time": ts, "value": temp, "fan": fan, "pwm": pwm, "level": level}

    with _lock:
        _state["temperature"].append(point)
        if len(_state["temperature"]) > MAX_HISTORY:
            _state["temperature"].pop(0)
        _state["latest_temp"] = point

        if level in ("WARNING", "CRITICAL"):
            alert = {
                "type":    "temperature",
                "level":   level,
                "message": f"Temperature {level.lower()}: {temp}°C — Fan {fan} (PWM {pwm}%)",
                "value":   f"{temp}°C",
                "time":    ts,
            }
            _state["alerts"].insert(0, alert)
            if len(_state["alerts"]) > 30:
                _state["alerts"].pop()

    if level in ("WARNING", "CRITICAL"):
        store_alert(conn, "temperature", level,
                    f"Temp {level}: {temp}°C", f"{temp}°C")
        log.warning("ALERT [%s] Temperature = %.1f°C | Fan = %s (%d%%)", level, temp, fan, pwm)
    else:
        log.info("Temp = %.1f°C | Fan = %s (%d%%)", temp, fan, pwm)


def process_gas(msg: dict, conn: sqlite3.Connection):
    ppm   = msg["gas_ppm"]
    level = msg["alert_level"]
    ts    = msg["timestamp"]

    store_gas(conn, msg)

    point = {"time": ts, "value": ppm, "level": level}

    with _lock:
        _state["gas"].append(point)
        if len(_state["gas"]) > MAX_HISTORY:
            _state["gas"].pop(0)
        _state["latest_gas"] = point

        if level in ("WARNING", "CRITICAL"):
            alert = {
                "type":    "gas",
                "level":   level,
                "message": f"Gas {level.lower()}: {ppm} ppm detected",
                "value":   f"{ppm} ppm",
                "time":    ts,
            }
            _state["alerts"].insert(0, alert)
            if len(_state["alerts"]) > 30:
                _state["alerts"].pop()

    if level in ("WARNING", "CRITICAL"):
        store_alert(conn, "gas", level, f"Gas {level}: {ppm} ppm", f"{ppm} ppm")
        log.warning("ALERT [%s] Gas = %d ppm", level, ppm)
    else:
        log.info("Gas = %d ppm", ppm)


# ─── Snapshot writer ──────────────────────────────────────────────────────────

def snapshot_writer():
    """Writes the in-memory state to a JSON file every second for Flask to read."""
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    while True:
        with _lock:
            snapshot = {
                "temperature": _state["temperature"][-MAX_HISTORY:],
                "gas":         _state["gas"][-MAX_HISTORY:],
                "alerts":      _state["alerts"][:20],
                "latest_temp": _state["latest_temp"],
                "latest_gas":  _state["latest_gas"],
                "updated_at":  datetime.utcnow().isoformat() + "Z",
            }
        SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2))
        time.sleep(1)


# ─── Main consumer loop ───────────────────────────────────────────────────────

def connect_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            log.info("Consumer connected. Listening on topics: %s", TOPICS)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not reachable. Retrying in 5s...")
            time.sleep(5)


def main():
    log.info("Starting IoT sensor consumer")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    init_db(conn)

    # Start snapshot writer in background
    t = threading.Thread(target=snapshot_writer, daemon=True)
    t.start()
    log.info("Snapshot writer started -> %s", SNAPSHOT_PATH)

    consumer = connect_consumer()

    try:
        for message in consumer:
            topic = message.topic
            data  = message.value

            if topic == "sensor-temperature":
                process_temperature(data, conn)
            elif topic == "sensor-gas":
                process_gas(data, conn)

    except KeyboardInterrupt:
        log.info("Consumer stopped.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
