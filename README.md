# IoT Auto Cooling System — Apache Kafka Middleware Project
Université de Blida 1 — Technologies du Middleware 2025/2026

## Architecture
```
sensor_producer.py  →  Kafka (Docker)  →  sensor_consumer.py  →  SQLite
                                                  ↓
                                        data/latest_snapshot.json
                                                  ↓
                                            app.py (Flask)
                                                  ↓
                                        Web Dashboard (browser)
```

## Project structure
```
iot_cooling/
├── docker-compose.yml       # Kafka + Zookeeper
├── requirements.txt
├── sensor_producer.py       # Simulated ESP32 sensors → Kafka
├── sensor_consumer.py       # Kafka consumer → SQLite + snapshot
├── app.py                   # Flask REST API + dashboard
├── data/                    # Created automatically
│   ├── sensor_data.db
│   └── latest_snapshot.json
└── frontend/
    ├── templates/index.html
    └── static/
        ├── css/style.css
        └── js/dashboard.js
```

## Step-by-step setup

### 1. Install Docker Desktop
Download from https://www.docker.com/products/docker-desktop
Make sure it is running before continuing.

### 2. Start Kafka
```bash
docker-compose up -d
```
Wait ~15 seconds for Kafka to fully start.
Verify with: `docker ps`  (you should see zookeeper and kafka containers)

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the consumer (Terminal 1)
Start this BEFORE the producer so it is ready to receive.
```bash
python sensor_consumer.py
```
You should see: "Consumer connected. Listening on topics..."

### 5. Run the producer (Terminal 2)
```bash
python sensor_producer.py
```
You should see readings every 2 seconds:
  Temp=34.2°C [WARNING] | Gas=312ppm [OK] | Fan=LOW (PWM 62%)

### 6. Start the Flask backend (Terminal 3)
```bash
python app.py
```
Open your browser at: http://localhost:5000

## API endpoints
| Endpoint                  | Description                        |
|---------------------------|------------------------------------|
| GET /api/snapshot         | Full real-time state snapshot      |
| GET /api/latest           | Most recent reading per sensor     |
| GET /api/alerts           | Active alerts list                 |
| GET /api/history/temperature | Last 100 temperature readings   |
| GET /api/history/gas      | Last 100 gas readings              |
| GET /api/stats            | Summary statistics from SQLite     |

## Kafka topics
| Topic                | Producer key  | Content                       |
|----------------------|---------------|-------------------------------|
| sensor-temperature   | ESP32-S1      | temperature, fan status, PWM  |
| sensor-gas           | ESP32-S1      | gas ppm, alert level          |

## Alert thresholds (defined in sensor_consumer.py)
| Sensor      | Warning | Critical |
|-------------|---------|----------|
| Temperature | 30°C    | 40°C     |
| Gas (MQ-2)  | 400 ppm | 700 ppm  |

## Stop everything
- Ctrl+C in each terminal
- `docker-compose down` to stop Kafka
