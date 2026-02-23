"""Monarch Connection Status Scraper - Flask Web Application."""

import functools
import json
import os
import queue
import secrets
import threading

from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for

from models import Connection, ScrapeSession, db
from scraper import scrape_connections

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///monarch.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))


class ReverseProxyMiddleware:
    """Set SCRIPT_NAME from X-Forwarded-Prefix header so Flask generates
    correct URLs when running behind a reverse proxy at a subpath."""

    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        prefix = environ.get("HTTP_X_FORWARDED_PREFIX", "")
        if prefix:
            environ["SCRIPT_NAME"] = prefix.rstrip("/")
            path_info = environ.get("PATH_INFO", "")
            if path_info.startswith(prefix):
                environ["PATH_INFO"] = path_info[len(prefix.rstrip("/")):]
        return self.wsgi_app(environ, start_response)


app.wsgi_app = ReverseProxyMiddleware(app.wsgi_app)

# Password gate — set APP_PASSWORD env var to enable.
# If unset the app runs without auth (local-only use).
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

db.init_app(app)

with app.app_context():
    db.create_all()
    # Clean up any stale sessions from previous crashes
    stale = ScrapeSession.query.filter(
        ScrapeSession.status.in_(["starting", "running"])
    ).all()
    for s in stale:
        s.status = "failed"
        s.error_message = "Server restarted before completion"
    if stale:
        db.session.commit()

# Global dict to track active scrape progress queues
_progress_queues: dict[int, queue.Queue] = {}
_progress_lock = threading.Lock()


def _get_or_create_queue(session_id: int) -> queue.Queue:
    with _progress_lock:
        if session_id not in _progress_queues:
            _progress_queues[session_id] = queue.Queue()
        return _progress_queues[session_id]


def _remove_queue(session_id: int):
    with _progress_lock:
        _progress_queues.pop(session_id, None)


# --- Auth helpers ---


def login_required(f):
    """Decorator: redirect to /login if APP_PASSWORD is set and user isn't authenticated."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if APP_PASSWORD and not session.get("authenticated"):
            # For API endpoints return 401 JSON; for pages redirect
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# --- Routes ---


@app.route("/login", methods=["GET", "POST"])
def login():
    """Simple password gate."""
    if not APP_PASSWORD:
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        if secrets.compare_digest(request.form.get("password", ""), APP_PASSWORD):
            session["authenticated"] = True
            return redirect(url_for("index"))
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    """Main page showing scrape history and statistics."""
    return render_template("index.html")


@app.route("/api/scrape", methods=["POST"])
@login_required
def start_scrape():
    """Start a new scrape in a background thread, return session ID."""
    # Prevent concurrent scrapes
    running = ScrapeSession.query.filter(
        ScrapeSession.status.in_(["starting", "running"])
    ).first()
    if running:
        return jsonify({"error": "A scrape is already in progress", "session_id": running.id}), 409

    # Create a placeholder session to get the ID
    session = ScrapeSession(status="starting")
    db.session.add(session)
    db.session.commit()
    session_id = session.id

    q = _get_or_create_queue(session_id)

    def progress_callback(event_type, data):
        q.put({"event": event_type, "data": data})

    def run_scrape():
        try:
            scrape_connections(app, progress_callback=progress_callback, session_id=session_id)
        except Exception as e:
            q.put({"event": "error", "data": {"message": str(e)}})
        finally:
            q.put(None)  # Sentinel to signal end

    thread = threading.Thread(target=run_scrape, daemon=True)
    thread.start()

    return jsonify({"session_id": session_id})


@app.route("/api/scrape/<int:session_id>/progress")
@login_required
def scrape_progress(session_id):
    """SSE endpoint for real-time scrape progress."""
    q = _get_or_create_queue(session_id)

    def generate():
        while True:
            try:
                msg = q.get(timeout=120)
                if msg is None:
                    # Scrape finished
                    yield f"data: {json.dumps({'event': 'done'})}\n\n"
                    break
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty:
                # Keep-alive
                yield f"data: {json.dumps({'event': 'keepalive'})}\n\n"

        _remove_queue(session_id)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/sessions")
@login_required
def list_sessions():
    """List all scrape sessions with at least 1 institution."""
    sessions = (
        ScrapeSession.query
        .filter(ScrapeSession.total_institutions > 0)
        .order_by(ScrapeSession.started_at.desc())
        .all()
    )
    return jsonify([s.to_dict() for s in sessions])


@app.route("/api/sessions/<int:session_id>")
@login_required
def get_session(session_id):
    """Get details of a specific scrape session."""
    session = db.session.get(ScrapeSession, session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    return jsonify(session.to_dict())


@app.route("/api/sessions/<int:session_id>/connections")
@login_required
def get_connections(session_id):
    """Get all connections for a specific scrape session."""
    session = db.session.get(ScrapeSession, session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    connections = (
        Connection.query.filter_by(scrape_session_id=session_id)
        .order_by(Connection.rank)
        .all()
    )
    return jsonify(
        {
            "session": session.to_dict(),
            "connections": [c.to_dict() for c in connections],
        }
    )


@app.route("/api/sessions/<int:session_id>/stats")
@login_required
def get_session_stats(session_id):
    """Get summary statistics for a scrape session."""
    session = db.session.get(ScrapeSession, session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    connections = Connection.query.filter_by(scrape_session_id=session_id).all()

    # Provider distribution & per-provider weighted averages
    providers = {}
    provider_metrics = {}  # provider -> { success: [], longevity: [], update: [] }
    statuses = {}
    for c in connections:
        prov = c.data_provider or "Unknown"
        providers[prov] = providers.get(prov, 0) + 1

        if prov not in provider_metrics:
            provider_metrics[prov] = {"success": [], "longevity": [], "update": []}
        if c.success_pct is not None:
            provider_metrics[prov]["success"].append(c.success_pct)
        if c.longevity_pct is not None:
            provider_metrics[prov]["longevity"].append(c.longevity_pct)
        if c.update_pct is not None:
            provider_metrics[prov]["update"].append(c.update_pct)

        st = c.connection_status or "OK"
        statuses[st] = statuses.get(st, 0) + 1

    # Weighted average per provider: 40% success, 30% longevity, 30% updates
    W_SUCCESS, W_LONGEVITY, W_UPDATE = 0.4, 0.3, 0.3
    provider_scores = {}
    for prov, metrics in provider_metrics.items():
        avg_s = (
            round(sum(metrics["success"]) / len(metrics["success"]), 2)
            if metrics["success"]
            else None
        )
        avg_l = (
            round(sum(metrics["longevity"]) / len(metrics["longevity"]), 2)
            if metrics["longevity"]
            else None
        )
        avg_u = (
            round(sum(metrics["update"]) / len(metrics["update"]), 2)
            if metrics["update"]
            else None
        )

        # Compute weighted average from available metrics
        parts = []
        weights = []
        if avg_s is not None:
            parts.append(avg_s * W_SUCCESS)
            weights.append(W_SUCCESS)
        if avg_l is not None:
            parts.append(avg_l * W_LONGEVITY)
            weights.append(W_LONGEVITY)
        if avg_u is not None:
            parts.append(avg_u * W_UPDATE)
            weights.append(W_UPDATE)

        weighted_avg = (
            round(sum(parts) / sum(weights), 2) if weights else None
        )

        provider_scores[prov] = {
            "avg_success": avg_s,
            "avg_longevity": avg_l,
            "avg_update": avg_u,
            "weighted_avg": weighted_avg,
            "institution_count": providers.get(prov, 0),
            "success_count": len(metrics["success"]),
            "longevity_count": len(metrics["longevity"]),
            "update_count": len(metrics["update"]),
            "weights": {
                "success": W_SUCCESS,
                "longevity": W_LONGEVITY,
                "update": W_UPDATE,
            },
        }

    # Overall average metrics
    success_vals = [c.success_pct for c in connections if c.success_pct is not None]
    longevity_vals = [c.longevity_pct for c in connections if c.longevity_pct is not None]
    update_vals = [c.update_pct for c in connections if c.update_pct is not None]

    return jsonify(
        {
            "session": session.to_dict(),
            "total_institutions": len(connections),
            "provider_distribution": providers,
            "status_distribution": statuses,
            "provider_scores": provider_scores,
            "avg_success_pct": (
                round(sum(success_vals) / len(success_vals), 2) if success_vals else None
            ),
            "avg_longevity_pct": (
                round(sum(longevity_vals) / len(longevity_vals), 2)
                if longevity_vals
                else None
            ),
            "avg_update_pct": (
                round(sum(update_vals) / len(update_vals), 2) if update_vals else None
            ),
        }
    )


@app.route("/api/stats/latest")
@login_required
def get_latest_stats():
    """Get stats from the most recent completed scrape."""
    session = (
        ScrapeSession.query.filter_by(status="completed")
        .order_by(ScrapeSession.started_at.desc())
        .first()
    )
    if not session:
        return jsonify({"error": "No completed scrapes found"}), 404

    return get_session_stats(session.id)


@app.route("/api/history")
@login_required
def get_history():
    """Return per-provider stats for all completed scrape sessions (newest first)."""
    sessions = (
        ScrapeSession.query.filter_by(status="completed")
        .filter(ScrapeSession.total_institutions > 0)
        .order_by(ScrapeSession.started_at.desc())
        .all()
    )

    W_SUCCESS, W_LONGEVITY, W_UPDATE = 0.4, 0.3, 0.3
    results = []

    for sess in sessions:
        connections = Connection.query.filter_by(scrape_session_id=sess.id).all()
        provider_metrics: dict[str, dict] = {}
        provider_counts: dict[str, int] = {}

        for c in connections:
            prov = c.data_provider or "Unknown"
            provider_counts[prov] = provider_counts.get(prov, 0) + 1
            if prov not in provider_metrics:
                provider_metrics[prov] = {"success": [], "longevity": [], "update": []}
            if c.success_pct is not None:
                provider_metrics[prov]["success"].append(c.success_pct)
            if c.longevity_pct is not None:
                provider_metrics[prov]["longevity"].append(c.longevity_pct)
            if c.update_pct is not None:
                provider_metrics[prov]["update"].append(c.update_pct)

        providers = {}
        for prov, m in provider_metrics.items():
            avg_s = round(sum(m["success"]) / len(m["success"]), 2) if m["success"] else None
            avg_l = round(sum(m["longevity"]) / len(m["longevity"]), 2) if m["longevity"] else None
            avg_u = round(sum(m["update"]) / len(m["update"]), 2) if m["update"] else None

            parts, weights = [], []
            if avg_s is not None:
                parts.append(avg_s * W_SUCCESS); weights.append(W_SUCCESS)
            if avg_l is not None:
                parts.append(avg_l * W_LONGEVITY); weights.append(W_LONGEVITY)
            if avg_u is not None:
                parts.append(avg_u * W_UPDATE); weights.append(W_UPDATE)

            providers[prov] = {
                "success": avg_s,
                "longevity": avg_l,
                "update": avg_u,
                "weighted": round(sum(parts) / sum(weights), 2) if weights else None,
                "count": provider_counts.get(prov, 0),
            }

        results.append({
            "session_id": sess.id,
            "started_at": sess.started_at.isoformat() if sess.started_at else None,
            "total_institutions": sess.total_institutions or len(connections),
            "providers": providers,
        })

    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True, port=5555, threaded=True, use_reloader=False)
