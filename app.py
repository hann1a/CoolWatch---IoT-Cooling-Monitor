"""
app.py
------
Flask backend that serves the web dashboard and exposes
REST API endpoints for the frontend to poll in real time.

Data comes from the JSON snapshot written by sensor_consumer.py.

Run: python app.py
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime

from flask import Flask, jsonify, render_template, abort
from flask_cors import CORS

app = Flask(
    __name__,
    template_folder="frontend/templates",
    static_folder="frontend/static",
    static_url_path="/static"
)
CORS(app)

SNAPSHOT_PATH = Path("data/latest_snapshot.json")
DB_PATH       = Path("data/sensor_data.db")


def read_snapshot() -> dict:
    """Read the latest snapshot file written by the consumer."""
    if not SNAPSHOT_PATH.exists():
        return {
            "temperature": [], "gas": [], "alerts": [],
            "latest_temp": None, "latest_gas": None,
            "updated_at": None,
        }
    return json.loads(SNAPSHOT_PATH.read_text())


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ─── Pages ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ─── API ──────────────────────────────────────────────────────────────────────

@app.route("/api/snapshot")
def api_snapshot():
    """Full state snapshot — polled every 2s by the frontend."""
    data = read_snapshot()
    return jsonify(data)


@app.route("/api/latest")
def api_latest():
    """Only the most recent reading from each sensor."""
    data = read_snapshot()
    return jsonify({
        "temperature": data.get("latest_temp"),
        "gas":         data.get("latest_gas"),
        "updated_at":  data.get("updated_at"),
    })


@app.route("/api/alerts")
def api_alerts():
    """Recent alerts list."""
    data = read_snapshot()
    return jsonify({"alerts": data.get("alerts", [])})


@app.route("/api/history/temperature")
def api_history_temp():
    """Last N temperature readings from SQLite."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT temperature, fan_status, fan_pwm, alert_level, recorded_at "
            "FROM temperature_readings ORDER BY id DESC LIMIT 100"
        ).fetchall()
        conn.close()
        return jsonify({"data": [dict(r) for r in reversed(rows)]})
    except Exception:
        return jsonify({"data": []})


@app.route("/api/history/gas")
def api_history_gas():
    """Last N gas readings from SQLite."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT gas_ppm, alert_level, recorded_at "
            "FROM gas_readings ORDER BY id DESC LIMIT 100"
        ).fetchall()
        conn.close()
        return jsonify({"data": [dict(r) for r in reversed(rows)]})
    except Exception:
        return jsonify({"data": []})


@app.route("/api/history/alerts")
def api_history_alerts():
    """All stored alerts from SQLite."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT sensor_type, alert_level, message, value, created_at "
            "FROM alerts ORDER BY id DESC LIMIT 50"
        ).fetchall()
        conn.close()
        return jsonify({"alerts": [dict(r) for r in rows]})
    except Exception:
        return jsonify({"alerts": []})


@app.route("/api/stats")
def api_stats():
    """Summary statistics from the database."""
    try:
        conn = get_db()
        total_temp = conn.execute("SELECT COUNT(*) FROM temperature_readings").fetchone()[0]
        total_gas  = conn.execute("SELECT COUNT(*) FROM gas_readings").fetchone()[0]
        total_alerts = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        crit_alerts  = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE alert_level='CRITICAL'"
        ).fetchone()[0]
        avg_temp = conn.execute(
            "SELECT AVG(temperature) FROM temperature_readings"
        ).fetchone()[0]
        avg_gas  = conn.execute(
            "SELECT AVG(gas_ppm) FROM gas_readings"
        ).fetchone()[0]
        conn.close()
        return jsonify({
            "total_temp_readings": total_temp,
            "total_gas_readings":  total_gas,
            "total_alerts":        total_alerts,
            "critical_alerts":     crit_alerts,
            "avg_temperature":     round(avg_temp, 1) if avg_temp else None,
            "avg_gas_ppm":         round(avg_gas, 0) if avg_gas else None,
        })
    except Exception:
        return jsonify({})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
