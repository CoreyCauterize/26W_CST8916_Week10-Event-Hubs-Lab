# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads the last N events from Event Hubs and serves a live dashboard
#
# Routes:
#   GET  /              → serves the demo e-commerce store (client.html)
#   POST /track         → receives a click event and publishes it to Event Hubs
#   GET  /dashboard     → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent events as JSON (polled by the dashboard)

import os
import json
import threading
import dotenv
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Azure Event Hubs SDK
# EventHubProducerClient  – sends events to Event Hubs
# EventHubConsumerClient  – reads events from Event Hubs
# EventData               – wraps a single event payload
# ---------------------------------------------------------------------------
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

# Load environment variables from .env file
dotenv.load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables so secrets never live in code
#
# Set these before running locally:
#   export EVENT_HUB_CONNECTION_STR="Endpoint=sb://..."
#   export EVENT_HUB_NAME="clickstream"
#
# On Azure App Service, set them as Application Settings in the portal.
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream")
EVENT_HUB_ANALYTICS_NAME = os.environ.get("EVENT_HUB_ANALYTICS_NAME", "analytics-results")

# In-memory buffers
_event_buffer = []          # Raw clickstream events (for live feed + KPIs)
_analytics_buffer = []      # Stream Analytics results (device breakdown + spikes)
_buffer_lock = threading.Lock()
_analytics_lock = threading.Lock()
MAX_BUFFER = 50


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    if not CONNECTION_STR:
        # Gracefully skip if the connection string is not configured yet
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – skipping Event Hubs publish")
        return

    # EventHubProducerClient is created fresh per request here for simplicity.
    # In a high-throughput production app you would keep a shared client instance.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        # create_batch() lets the SDK manage event size limits automatically
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer thread – reads events from Event Hubs and buffers them
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    """Callback invoked by the consumer client for each incoming event."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _event_buffer.append(data)
        # Keep the buffer at MAX_BUFFER entries (drop the oldest)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    # Acknowledge the event so Event Hubs advances the consumer offset
    partition_context.update_checkpoint(event)


def _on_analytics_event(partition_context, event):
    """Callback for Stream Analytics results from the analytics Event Hub."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _analytics_lock:
        _analytics_buffer.append(data)
        if len(_analytics_buffer) > MAX_BUFFER:
            _analytics_buffer.pop(0)

    partition_context.update_checkpoint(event)


def start_consumer():
    """Start two Event Hubs consumers in separate background daemon threads.

    Thread 1: reads raw clickstream events (for the live feed + KPIs)
    Thread 2: reads Stream Analytics output (device breakdown + spike detection)

    Each consumer.receive() blocks forever, so they must run on separate threads.
    """
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – consumer threads not started")
        return

    # Thread 1 – raw clickstream events
    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
    )

    def run_clickstream():
        with consumer:
            consumer.receive(
                on_event=_on_event,
                starting_position="-1",
            )

    t1 = threading.Thread(target=run_clickstream, daemon=True)
    t1.start()
    app.logger.info(f"Clickstream consumer started on {EVENT_HUB_NAME}")

    # Thread 2 – Stream Analytics results
    analytics_consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_ANALYTICS_NAME,
    )

    def run_analytics():
        with analytics_consumer:
            analytics_consumer.receive(
                on_event=_on_analytics_event,
                starting_position="-1",
            )

    t2 = threading.Thread(target=run_analytics, daemon=True)
    t2.start()
    app.logger.info(f"Analytics consumer started on {EVENT_HUB_ANALYTICS_NAME}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    """Health check – used by Azure App Service to verify the app is running."""
    return jsonify({"status": "healthy"}), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.

    Expected JSON body:
    {
        "event_type": "page_view" | "product_click" | "add_to_cart" | "purchase",
        "page":       "/products/shoes",
        "product_id": "p_shoe_42",       (optional)
        "user_id":    "u_1234"
    }
    """
    if not request.json:
        abort(400)

    # Enrich the event with a server-side timestamp
    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "device":     request.json.get("device"),
        "browser":    request.json.get("browser"),
        "os":         request.json.get("os")

    }

    send_to_event_hubs(event)

    # Also buffer locally so the dashboard works even without a consumer thread
    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return the buffered events as JSON.
    The dashboard polls this endpoint every 2 seconds.

    Optional query param:  ?limit=20  (default 20, max 50)
    """
    try:
        # request.args.get("limit", 20) reads ?limit=N from the URL, defaulting to 20.
        # int() converts it from a string (all URL params are strings) to an integer.
        # min(..., MAX_BUFFER) clamps the value so callers can never request more
        # events than the buffer holds — e.g. ?limit=999 silently becomes 50.
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        # int() raises ValueError if the param is non-numeric (e.g. ?limit=abc)
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    # Build a simple summary for the dashboard
    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify({"events": recent, "summary": summary, "total": len(recent)}), 200

#get analytics for spikes and device breakdown, which are polled by the dashboard to update the UI
@app.route("/api/analytics", methods=["GET"]) # ai disclosure: use to recreate the buffered analytics results from the Stream Analytics output events hub.
def get_analytics():
    """Return both device breakdown and spike detection results."""
    with _buffer_lock:
        recent = list(_event_buffer)

    # Device breakdown
    breakdown = {}
    for e in recent:
        device = e.get("device", "unknown") or "unknown"
        breakdown[device] = breakdown.get(device, 0) + 1

    # Spike detection
    count = len(recent)
    if count >= 40:
        status = "critical"
    elif count >= 25:
        status = "spike"
    else:
        status = "normal"

    return jsonify({
        "device_breakdown": breakdown,
        "spike_status": status,
        "event_count": count,
    }), 200

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background consumer so the dashboard receives live events
    start_consumer()
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
