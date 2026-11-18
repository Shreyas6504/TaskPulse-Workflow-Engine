"""
TaskPulse Web Dashboard & Monitoring API
"""

import time
import random
from flask import Flask, render_template, jsonify, request
from engine import TaskEngine

app = Flask(__name__)
engine = TaskEngine(num_workers=3)

# Sample Handlers
def process_payment(payload):
    time.sleep(random.uniform(0.2, 0.6))
    amount = payload.get("amount", 100)
    return f"Charged ${amount} successfully. Transaction ID: txn_{random.randint(10000, 99999)}"

def send_notification(payload):
    time.sleep(random.uniform(0.1, 0.4))
    user = payload.get("recipient", "user@example.com")
    return f"Dispatch notification sent to {user}"

def unstable_api_sync(payload):
    # Simulates an unstable third-party webhook service to demonstrate Exponential Backoff & Retries
    time.sleep(0.3)
    if random.random() < 0.65:
        raise ConnectionResetError("Remote server 503 Service Unavailable (Simulated Network Spike)")
    return "Synced 42 records from external vendor successfully."

engine.register_handler("payment_processor", process_payment)
engine.register_handler("notification_service", send_notification)
engine.register_handler("unstable_sync", unstable_api_sync)

# Seed baseline demo jobs
engine.submit_job("Reconcile Daily Subscriptions", "payment_processor", {"amount": 250}, priority=1)
engine.submit_job("Send User Welcome Email", "notification_service", {"recipient": "alex@startup.io"}, priority=3)
engine.submit_job("Vendor Inventory Sync", "unstable_sync", {"batch_id": 901}, priority=2)

@app.route("/")
def dashboard():
    return render_template("index.html")

@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    return jsonify({"success": True, "jobs": engine.get_all_jobs()})

@app.route("/api/stats", methods=["GET"])
def stats():
    return jsonify({"success": True, "stats": engine.get_stats()})

@app.route("/api/jobs/trigger", methods=["POST"])
def trigger_job():
    data = request.get_json() or {}
    job_type = data.get("type", "payment")
    
    if job_type == "payment":
        jid = engine.submit_job("Process Customer Charge", "payment_processor", {"amount": random.randint(50, 500)}, priority=2)
    elif job_type == "flaky":
        jid = engine.submit_job("Unstable Cloud Sync (Testing Backoff)", "unstable_sync", {"attempt": 1}, priority=1)
    else:
        jid = engine.submit_job("Broadcast Notification", "notification_service", {"recipient": "team@dev.com"}, priority=3)

    return jsonify({"success": True, "job_id": jid})

if __name__ == "__main__":
    print("✓ TaskPulse Engine Running on http://127.0.0.1:5001")
    app.run(port=5001, debug=True)
