"""
sensor_producer.py
------------------
Simulates an ESP32 environmental monitoring system.
Reads temperature (DHT11) and gas (MQ-2) sensor data,
then publishes JSON messages to Kafka topics.

Topics produced:
  - sensor-temperature
  - sensor-gas

Run: python sensor_producer.py
"""

import json
import time
import random
import math
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

KAFKA_BROKER   = "localhost:9092"
TOPIC_TEMP     = "sensor-temperature"
TOPIC_GAS      = "sensor-gas"
SENSOR_ID      = "ESP32-S1"
PUBLISH_INTERVAL = 2          # seconds between readings

# Realistic simulation parameters
BASE_TEMP      = 26.0         # baseline temperature (°C)
BASE_GAS       = 300          # baseline gas level (ppm)
TEMP_DRIFT     = 0.3          # max random drift per tick
GAS_DRIFT      = 15           # max random drift per tick
SPIKE_CHANCE   = 0.04         # 4% chance of a sudden spike per tick


def connect_producer() -> KafkaProducer:
    """Connect to Kafka broker with retries."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka broker at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not reachable. Retrying in 5s...")
            time.sleep(5)


def simulate_temperature(t: float, current: float) -> float:
    """
    Simulates a DHT11 temperature sensor.
    Uses a slow sine wave for natural variation + random noise.
    Occasionally injects a heat spike (simulates server overload).
    """
    # Natural slow oscillation (room heating/cooling cycle)
    cycle = 2.0 * math.sin(t / 120.0)
    noise = random.uniform(-TEMP_DRIFT, TEMP_DRIFT)
    spike = random.uniform(5.0, 12.0) if random.random() < SPIKE_CHANCE else 0.0

    new_val = current + (cycle * 0.05) + noise + spike

    # Drift back toward baseline slowly if too far away
    if new_val > BASE_TEMP + 15:
        new_val -= 0.8
    if new_val < BASE_TEMP - 5:
        new_val += 0.5

    return round(max(18.0, min(55.0, new_val)), 1)


def simulate_gas(current: int) -> int:
    """
    Simulates an MQ-2 gas/smoke sensor (reads in ppm).
    Occasionally spikes to simulate smoke detection.
    """
    noise = random.randint(-GAS_DRIFT, GAS_DRIFT)
    spike = random.randint(200, 600) if random.random() < SPIKE_CHANCE else 0

    new_val = current + noise + spike

    # Drift back toward baseline
    if new_val > BASE_GAS + 300:
        new_val -= 30
    if new_val < BASE_GAS - 50:
        new_val += 10

    return max(100, min(1500, new_val))


def determine_fan_status(temp: float, temp_threshold: float = 30.0) -> dict:
    """
    Replicates the ESP32 fan control logic from the embedded TP:
    - OFF   below threshold
    - LOW   30–35°C
    - HIGH  above 35°C
    """
    if temp < temp_threshold:
        return {"status": "OFF", "pwm": 0}
    elif temp < 35.0:
        pwm = int(50 + (temp - temp_threshold) * 10)
        return {"status": "LOW", "pwm": min(pwm, 80)}
    else:
        pwm = int(80 + (temp - 35.0) * 5)
        return {"status": "HIGH", "pwm": min(pwm, 100)}


def build_temp_message(temp: float, fan: dict) -> dict:
    return {
        "sensor_id":    SENSOR_ID,
        "sensor_type":  "DHT11",
        "temperature":  temp,
        "unit":         "C",
        "fan_status":   fan["status"],
        "fan_pwm":      fan["pwm"],
        "alert":        temp > 30.0,
        "alert_level":  "CRITICAL" if temp > 40.0 else ("WARNING" if temp > 30.0 else "OK"),
        "timestamp":    datetime.utcnow().isoformat() + "Z",
    }


def build_gas_message(gas: int) -> dict:
    return {
        "sensor_id":    SENSOR_ID,
        "sensor_type":  "MQ-2",
        "gas_ppm":      gas,
        "alert":        gas > 400,
        "alert_level":  "CRITICAL" if gas > 700 else ("WARNING" if gas > 400 else "OK"),
        "timestamp":    datetime.utcnow().isoformat() + "Z",
    }


def main():
    log.info("Starting IoT sensor simulator — ESP32 Environmental Monitor")
    log.info("Topics: %s | %s", TOPIC_TEMP, TOPIC_GAS)
    log.info("Publish interval: %ds", PUBLISH_INTERVAL)

    producer = connect_producer()

    current_temp = BASE_TEMP
    current_gas  = BASE_GAS
    t = 0.0

    try:
        while True:
            # Simulate sensor readings
            current_temp = simulate_temperature(t, current_temp)
            current_gas  = simulate_gas(current_gas)
            fan          = determine_fan_status(current_temp)

            temp_msg = build_temp_message(current_temp, fan)
            gas_msg  = build_gas_message(current_gas)

            # Publish to Kafka
            producer.send(TOPIC_TEMP, value=temp_msg)
            producer.send(TOPIC_GAS,  value=gas_msg)
            producer.flush()

            log.info(
                "Temp=%.1f°C [%s] | Gas=%dppm [%s] | Fan=%s (PWM %d%%)",
                current_temp, temp_msg["alert_level"],
                current_gas,  gas_msg["alert_level"],
                fan["status"], fan["pwm"],
            )

            t += PUBLISH_INTERVAL
            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Sensor producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
