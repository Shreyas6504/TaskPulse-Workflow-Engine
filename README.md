# TaskPulse — Asynchronous Task Orchestration & Distributed Worker Queue

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python Version" />
  <img src="https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white" alt="CI" />
  <img src="https://img.shields.io/badge/Architecture-Threaded%20Worker%20Pool-005571?style=for-the-badge&logo=cpu&logoColor=white" alt="Worker Pool" />
  <img src="https://img.shields.io/badge/Resilience-Exponential%20Backoff-F59E0B?style=for-the-badge" alt="Backoff" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License" />
</p>

> **TaskPulse** is a lightweight, fault-tolerant asynchronous background task engine and job scheduler built in Python. Designed as a high-efficiency alternative to heavyweight broker setups, it provides thread-safe priority dispatching, automatic exponential backoff retries with randomized jitter, a Dead-Letter Queue (DLQ) for poison pill isolation, and a real-time web observability dashboard.

---

## ⚡ Core Engineering Capabilities

- 🎯 **Priority-Based Heap Scheduling:** Min-heap (`heapq`) based task ordering ensures urgent jobs (`Priority 1`) preempt standard work (`Priority 4`).
- 🔄 **Exponential Backoff with Full Jitter:** Resilient retry scheduling prevents thundering-herd network spikes when connecting to third-party APIs:  
  $$T_{\text{backoff}} = (2^{\text{retry}} \times 0.4s) + \text{Uniform}(0.1, 0.3)$$
- ☠️ **Dead-Letter Queue (DLQ):** Poison-pill tasks that exceed `max_retries` are safely quarantined without stalling concurrent worker threads.
- 🧵 **Dynamic Worker Pool:** Non-blocking multi-threaded executor threads (`threading.Condition`) with graceful shutdown semantics.
- 📊 **Real-time Observability UI:** Embedded single-page dashboard with 1-second auto-polling for execution latency, queue depth, and failure telemetry.

---

## 🏗️ Architecture Design

```text
                  [ Client / API Trigger ]
                             │
                             ▼
              [ TaskPulse Priority Min-Heap ]
                             │
     ┌───────────────────────┼───────────────────────┐
     ▼                       ▼                       ▼
 [ Worker 1 ]           [ Worker 2 ]            [ Worker 3 ]
     │                       │                       │
     ├──────── SUCCESS ──────┴──> [ Result Ledger ]
     │
     └── FAILED ──> [ Retry Count < Max? ]
                           ├── YES ──> Exponential Backoff Delay ──> Re-enqueue
                           └── NO  ──> [ Dead-Letter Queue (DLQ) ]
```

---

## 🚀 Quickstart

### 1. Clone & Install
```bash
git clone https://github.com/Shreyas6504/TaskPulse-Workflow-Engine.git
cd TaskPulse-Workflow-Engine
pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
python -m unittest discover -s tests
```

### 3. Launch Observability Dashboard
```bash
python server.py
```
Open **[http://localhost:5001](http://localhost:5001)** in your browser to inspect the live worker pool and trigger sample background jobs.

---

## 🔌 Programmatic Usage

```python
from engine import TaskEngine

# 1. Initialize engine with 4 concurrent worker threads
engine = TaskEngine(num_workers=4)

# 2. Register job handler
def generate_pdf_report(payload):
    user_id = payload["user_id"]
    # ... computation / network calls ...
    return f"Report generated for user {user_id}"

engine.register_handler("pdf_generator", generate_pdf_report)

# 3. Enqueue job with High Priority (Priority 1) & 3 retries
job_id = engine.submit_job(
    name="Monthly Financial Report",
    handler="pdf_generator",
    payload={"user_id": 4092},
    priority=1,
    max_retries=3
)
```

---

## 👤 Author
**Shreyas Shetty**  
- GitHub: [@Shreyas6504](https://github.com/Shreyas6504)
- Email: [jshreyasshetty@gmail.com](mailto:jshreyasshetty@gmail.com)
