/**
 * dashboard.js
 * Polls the Flask /api/snapshot endpoint every 2 seconds.
 * Updates all UI elements with real data from the Kafka consumer.
 */

const API_BASE    = "";          // same origin as Flask
const POLL_MS     = 2000;
const CHART_POINTS = 30;

// ─── State ───────────────────────────────────────────────────────────────────
let tempLabels  = [];
let tempData    = [];
let gasLabels   = [];
let gasData     = [];
let logBuffer   = [];
let alertBuffer = [];
let msgCount    = 0;
let connected   = false;

// ─── Chart.js instances ───────────────────────────────────────────────────────
let chartTemp, chartGas;

function initCharts() {
  const baseOpts = (yLabel) => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 300 },
    plugins: { legend: { display: false }, tooltip: {
      backgroundColor: "#1e2330", titleFont: { family: "'DM Mono', monospace", size: 11 },
      bodyFont: { family: "'DM Mono', monospace", size: 11 }, padding: 8,
    }},
    scales: {
      x: { ticks: { font: { family: "'DM Mono', monospace", size: 10 }, color: "#8a93a6", maxTicksLimit: 8 },
           grid: { color: "rgba(0,0,0,0.04)" } },
      y: { title: { display: true, text: yLabel, font: { size: 10 }, color: "#8a93a6" },
           ticks: { font: { family: "'DM Mono', monospace", size: 10 }, color: "#8a93a6" },
           grid: { color: "rgba(0,0,0,0.04)" } }
    }
  });

  chartTemp = new Chart(document.getElementById("chart-temp"), {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        data: [], borderColor: "#5499d4", backgroundColor: "rgba(84,153,212,0.08)",
        borderWidth: 2, pointRadius: 2, tension: 0.4, fill: true
      }]
    },
    options: baseOpts("°C")
  });

  chartGas = new Chart(document.getElementById("chart-gas"), {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        data: [], borderColor: "#45a87c", backgroundColor: "rgba(69,168,124,0.08)",
        borderWidth: 2, pointRadius: 2, tension: 0.4, fill: true
      }]
    },
    options: baseOpts("ppm")
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function fmtTime(isoStr) {
  if (!isoStr) return "—";
  const d = new Date(isoStr);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function badgeClass(level) {
  if (!level) return "";
  if (level === "CRITICAL") return "badge-critical";
  if (level === "WARNING")  return "badge-warning";
  return "badge-ok";
}

function alertIcon(type, level) {
  if (type === "temperature") return level === "CRITICAL" ? "fa-fire" : "fa-thermometer-half";
  if (type === "gas")         return level === "CRITICAL" ? "fa-skull-crossbones" : "fa-smog";
  return "fa-check";
}

function alertLevelClass(level) {
  if (!level || level === "OK") return "level-ok";
  if (level === "CRITICAL") return "level-critical";
  return "level-warning";
}

// ─── UI updaters ─────────────────────────────────────────────────────────────

function updateMetricCard(cardId, value, unit, level, badgeId, badgeText) {
  const card  = document.getElementById(cardId);
  const badge = document.getElementById(badgeId);
  card.classList.remove("alert-warning", "alert-critical");
  badge.classList.remove("badge-ok", "badge-warning", "badge-critical", "badge-neutral");

  if (level === "CRITICAL") {
    card.classList.add("alert-critical");
    badge.classList.add("badge-critical");
  } else if (level === "WARNING") {
    card.classList.add("alert-warning");
    badge.classList.add("badge-warning");
  } else {
    badge.classList.add("badge-ok");
  }

  badge.textContent = badgeText;
}

function updateTemp(point) {
  if (!point) return;
  const val   = point.value;
  const level = point.level;
  const fan   = point.fan;
  const pwm   = point.pwm;

  document.getElementById("val-temp").textContent     = val + "°C";
  document.getElementById("extra-temp").textContent   = "";
  updateMetricCard("card-temp", val, "°C", level, "badge-temp", level);

  // Fan card
  document.getElementById("val-fan").textContent = fan || "OFF";
  document.getElementById("badge-pwm").textContent = "PWM " + (pwm || 0) + "%";
  const pwmBar = document.getElementById("pwm-bar");
  pwmBar.style.width = (pwm || 0) + "%";
  pwmBar.style.background = pwm > 80 ? "#c84040" : pwm > 50 ? "#d49020" : "#5499d4";

  const fanIcon = document.getElementById("fan-icon");
  if (fan !== "OFF" && pwm > 0) {
    fanIcon.classList.add("fan-spinning");
    fanIcon.style.color = "#5499d4";
  } else {
    fanIcon.classList.remove("fan-spinning");
    fanIcon.style.color = "#d0d5de";
  }

  document.getElementById("val-health").textContent = level === "OK" ? "Good" : level === "WARNING" ? "Elevated" : "Alert";
}

function updateGas(point) {
  if (!point) return;
  const level = point.level;
  document.getElementById("val-gas").textContent = point.value;
  updateMetricCard("card-gas", point.value, "ppm", level, "badge-gas", level);
}

function updateCharts(tempHistory, gasHistory) {
  if (tempHistory && tempHistory.length > 0) {
    const labels = tempHistory.map(p => fmtTime(p.time));
    const data   = tempHistory.map(p => p.value);
    chartTemp.data.labels = labels;
    chartTemp.data.datasets[0].data = data;
    chartTemp.update("none");
  }
  if (gasHistory && gasHistory.length > 0) {
    const labels = gasHistory.map(p => fmtTime(p.time));
    const data   = gasHistory.map(p => p.value);
    chartGas.data.labels = labels;
    chartGas.data.datasets[0].data = data;
    chartGas.update("none");
  }
}

function updateAlerts(alerts) {
  if (!alerts) return;

  // Dashboard preview (3 most recent)
  const previewEl = document.getElementById("alert-preview-list");
  if (alerts.length === 0) {
    previewEl.innerHTML = '<div class="alert-empty">No alerts. System nominal.</div>';
  } else {
    previewEl.innerHTML = alerts.slice(0, 4).map(a => `
      <div class="alert-item ${alertLevelClass(a.level)}">
        <i class="fa-solid ${alertIcon(a.type, a.level)}"></i>
        <div>
          <div class="alert-msg">${a.message}</div>
          <div class="alert-time">${fmtTime(a.time)}</div>
        </div>
      </div>
    `).join("");
  }

  // Full alerts page
  const allEl = document.getElementById("all-alerts-list");
  if (alerts.length === 0) {
    allEl.innerHTML = '<div class="alert-empty">No alerts recorded yet.</div>';
  } else {
    allEl.innerHTML = alerts.map(a => `
      <div class="alert-item ${alertLevelClass(a.level)}">
        <i class="fa-solid ${alertIcon(a.type, a.level)}"></i>
        <div>
          <div class="alert-msg">${a.message}</div>
          <div class="alert-time">${fmtTime(a.time)}</div>
        </div>
      </div>
    `).join("");
  }
}

function updateLogs(tempHistory, gasHistory) {
  const rows = [];
  const tSlice = (tempHistory || []).slice(-20);
  const gSlice = (gasHistory  || []).slice(-20);

  for (let i = tSlice.length - 1; i >= 0; i--) {
    const p = tSlice[i];
    rows.push({ time: p.time, sensor: "DHT11 — Temperature", value: p.value + " °C",   level: p.level, topic: "sensor-temperature" });
  }
  for (let i = gSlice.length - 1; i >= 0; i--) {
    const p = gSlice[i];
    rows.push({ time: p.time, sensor: "MQ-2 — Gas",         value: p.value + " ppm",   level: p.level, topic: "sensor-gas" });
  }

  rows.sort((a, b) => new Date(b.time) - new Date(a.time));

  const tbody = document.getElementById("log-table-body");
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-row">Waiting for sensor data...</td></tr>';
    return;
  }

  tbody.innerHTML = rows.slice(0, 40).map(r => `
    <tr>
      <td>${fmtTime(r.time)}</td>
      <td>${r.sensor}</td>
      <td>${r.value}</td>
      <td><span class="badge ${badgeClass(r.level)}">${r.level}</span></td>
      <td>${r.topic}</td>
    </tr>
  `).join("");
}

function updateKafkaPage(snap) {
  const tempLen = (snap.temperature || []).length;
  const gasLen  = (snap.gas || []).length;
  document.getElementById("k-total").textContent = tempLen + gasLen;
  document.getElementById("tc-temp").textContent = tempLen + " msgs";
  document.getElementById("tc-gas").textContent  = gasLen  + " msgs";
}

async function fetchStats() {
  try {
    const res  = await fetch(API_BASE + "/api/stats");
    const data = await res.json();
    const el = (id, val, unit) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val != null ? val + unit : "—";
    };
    el("stat-avg-temp",  data.avg_temperature, "°C");
    el("stat-avg-gas",   data.avg_gas_ppm,     " ppm");
    el("stat-alerts",    data.total_alerts,    "");
    el("stat-critical",  data.critical_alerts, "");
  } catch (_) {}
}

// ─── Connection status ────────────────────────────────────────────────────────
function setConnected(ok) {
  const dot  = document.getElementById("status-dot");
  const text = document.getElementById("status-text");
  dot.classList.remove("connected", "error");
  if (ok) {
    dot.classList.add("connected");
    text.textContent = "Kafka live";
    document.getElementById("badge-health").textContent = "Kafka OK";
    document.getElementById("badge-health").className = "metric-badge badge-ok";
  } else {
    dot.classList.add("error");
    text.textContent = "No data yet";
  }
  connected = ok;
}

// ─── Main poll loop ───────────────────────────────────────────────────────────
async function poll() {
  try {
    const res  = await fetch(API_BASE + "/api/snapshot");
    const snap = await res.json();

    const hasData = snap.latest_temp || snap.latest_gas;
    setConnected(!!hasData);

    if (snap.latest_temp) updateTemp(snap.latest_temp);
    if (snap.latest_gas)  updateGas(snap.latest_gas);

    updateCharts(snap.temperature, snap.gas);
    updateAlerts(snap.alerts || []);
    updateLogs(snap.temperature, snap.gas);
    updateKafkaPage(snap);

    if (snap.updated_at) {
      document.getElementById("last-update-time").textContent = fmtTime(snap.updated_at);
    }

    msgCount++;
    document.getElementById("extra-msgs").textContent = msgCount + " msgs received";

  } catch (e) {
    setConnected(false);
    console.warn("API unreachable:", e.message);
  }
}

// ─── Navigation ───────────────────────────────────────────────────────────────
const PAGE_TITLES = {
  dashboard: ["Dashboard",     "Real-time environmental monitoring"],
  logs:      ["Sensor Logs",   "Raw readings from Kafka consumer"],
  alerts:    ["Alerts",        "Threshold violations and anomalies"],
  kafka:     ["Kafka Monitor", "Broker topics and message statistics"],
  settings:  ["Settings",      "System configuration"],
};

document.querySelectorAll(".nav-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const page = btn.dataset.page;
    document.querySelectorAll(".nav-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("page-" + page).classList.add("active");
    const [title, desc] = PAGE_TITLES[page] || [page, ""];
    document.getElementById("page-title").textContent = title;
    document.getElementById("page-desc").textContent  = desc;
    if (page === "kafka") fetchStats();
  });
});

// ─── Init ─────────────────────────────────────────────────────────────────────
initCharts();
poll();
setInterval(poll, POLL_MS);
setInterval(fetchStats, 10000);
