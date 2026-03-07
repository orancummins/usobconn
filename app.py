"""Monarch Connection Status Scraper - Flask Web Application."""

import functools
import json
import logging
import os
import queue
import secrets
import threading
import ssl
import urllib.request

from flask import Flask, Response, jsonify, redirect, render_template, request, send_file, session, url_for

from models import Connection, ScrapeSession, db
from scheduler import start_scheduler
from json_fetcher import fetch_json_connections

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

app = Flask(__name__, static_folder="static", static_url_path="/static")
MIN_SESSION_FIS = 5000  # Ignore sessions with fewer than this many institutions
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


def _launch_scrape(source="manual"):
    """Start a scrape in a background thread. Returns session_id.

    Used by both the /api/scrape endpoint and the scheduler.
    """
    with app.app_context():
        # Prevent concurrent scrapes
        running = ScrapeSession.query.filter(
            ScrapeSession.status.in_(["starting", "running"])
        ).first()
        if running:
            return running.id

        sess = ScrapeSession(status="starting")
        db.session.add(sess)
        db.session.commit()
        session_id = sess.id

    q = _get_or_create_queue(session_id)

    def progress_callback(event_type, data):
        q.put({"event": event_type, "data": data})

    def run_retrieval():
        try:
            fetch_json_connections(app, progress_callback=progress_callback, session_id=session_id)
        except Exception as e:
            q.put({"event": "error", "data": {"message": str(e)}})
        finally:
            q.put(None)  # Sentinel to signal end

    thread = threading.Thread(target=run_retrieval, daemon=True, name=f"retrieve-{source}")
    thread.start()
    logging.getLogger(__name__).info("Retrieval started (source=%s, session=%d)", source, session_id)
    return session_id


# --- Auth helpers ---


def login_required(f):
    """Decorator: auth is currently deactivated — always passes through."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
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

    session_id = _launch_scrape(source="manual")
    return jsonify({"session_id": session_id})


@app.route("/api/scrape/active")
@login_required
def active_scrape():
    """Check if a scrape is currently running. Returns session info or null."""
    running = ScrapeSession.query.filter(
        ScrapeSession.status.in_(["starting", "running"])
    ).first()
    next_scheduled = app.config.get("NEXT_SCHEDULED_SCRAPE")
    if running:
        return jsonify({
            "active": True,
            "session_id": running.id,
            "status": running.status,
            "started_at": running.started_at.isoformat() if running.started_at else None,
            "next_scheduled": next_scheduled,
        })
    return jsonify({"active": False, "next_scheduled": next_scheduled})


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
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
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
    """Return per-provider stats for all completed scrape sessions (newest first).

    Optional query param ?decile=0..9 filters to that 10% rank bucket
    (0 = top 10%, 9 = bottom 10%).  Omit for all institutions.
    """
    decile = request.args.get("decile", type=int)

    sessions = (
        ScrapeSession.query.filter_by(status="completed")
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
        .order_by(ScrapeSession.started_at.desc())
        .all()
    )

    W_SUCCESS, W_LONGEVITY, W_UPDATE = 0.4, 0.3, 0.3
    results = []

    for sess in sessions:
        connections = (
            Connection.query.filter_by(scrape_session_id=sess.id)
            .order_by(Connection.rank)
            .all()
        )

        # Apply decile filter if requested
        if decile is not None and 0 <= decile <= 9:
            n = len(connections)
            bucket_size = n / 10
            start = int(decile * bucket_size)
            end = int((decile + 1) * bucket_size)
            connections = connections[start:end]

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


@app.route("/api/logo/<path:name>")
@login_required
def get_logo(name):
    """Serve the logo PNG for a given institution name."""
    import hashlib
    logo_hash = hashlib.md5(name.encode()).hexdigest()
    logo_path = os.path.join(app.instance_path, "logos", f"{logo_hash}.png")
    if os.path.exists(logo_path):
        return send_file(logo_path, mimetype="image/png",
                         max_age=86400)  # cache 24 hours
    return "", 404


@app.route("/api/score-changes")
@login_required
def get_score_changes():
    """Compare consecutive completed sessions and return per-day score-change summaries.

    Optional query param ?decile=0..9 filters to that 10% rank bucket.
    """
    from collections import defaultdict

    decile = request.args.get("decile", type=int)

    sessions = (
        ScrapeSession.query.filter_by(status="completed")
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
        .order_by(ScrapeSession.started_at.asc())
        .all()
    )
    if len(sessions) < 2:
        return jsonify([])

    W_SUCCESS, W_LONGEVITY, W_UPDATE = 0.4, 0.3, 0.3

    def _weighted(c):
        parts, weights = [], []
        if c.success_pct is not None:
            parts.append(c.success_pct * W_SUCCESS); weights.append(W_SUCCESS)
        if c.longevity_pct is not None:
            parts.append(c.longevity_pct * W_LONGEVITY); weights.append(W_LONGEVITY)
        if c.update_pct is not None:
            parts.append(c.update_pct * W_UPDATE); weights.append(W_UPDATE)
        return round(sum(parts) / sum(weights), 2) if weights else None

    # Build lookup: session_id -> { institution_name: Connection }
    session_ids = [s.id for s in sessions]
    all_conns = (
        Connection.query
        .filter(Connection.scrape_session_id.in_(session_ids))
        .order_by(Connection.rank)
        .all()
    )

    # Group by session, then apply optional decile filter
    from collections import defaultdict as _dd
    _by_session = _dd(list)
    for c in all_conns:
        _by_session[c.scrape_session_id].append(c)

    lookup = defaultdict(dict)
    for sid, conns in _by_session.items():
        if decile is not None and 0 <= decile <= 9:
            n = len(conns)
            bucket_size = n / 10
            start = int(decile * bucket_size)
            end = int((decile + 1) * bucket_size)
            conns = conns[start:end]
        for c in conns:
            lookup[sid][c.institution_name] = c

    results = []
    for i in range(1, len(sessions)):
        prev_sess = sessions[i - 1]
        curr_sess = sessions[i]
        prev_map = lookup.get(prev_sess.id, {})
        curr_map = lookup.get(curr_sess.id, {})

        common = set(prev_map.keys()) & set(curr_map.keys())
        if not common:
            continue

        changed_count = 0
        improved_count = 0
        declined_count = 0
        total_abs_change = 0.0
        buckets = {"0": 0, "0-5": 0, "5-10": 0, "10-25": 0, "25+": 0}
        biggest_improvements = []
        biggest_declines = []

        for name in common:
            prev_w = _weighted(prev_map[name])
            curr_w = _weighted(curr_map[name])
            if prev_w is None or curr_w is None:
                continue
            delta = round(curr_w - prev_w, 2)
            abs_d = abs(delta)

            if abs_d < 0.01:
                buckets["0"] += 1
                continue

            changed_count += 1
            total_abs_change += abs_d
            curr_prov = curr_map[name].data_provider
            prev_prov = prev_map[name].data_provider
            entry = {
                "name": name,
                "delta": delta,
                "provider": curr_prov,
                "prev_provider": prev_prov if prev_prov != curr_prov else None,
            }
            if delta > 0:
                improved_count += 1
                biggest_improvements.append(entry)
            else:
                declined_count += 1
                biggest_declines.append(entry)

            if abs_d < 5:
                buckets["0-5"] += 1
            elif abs_d < 10:
                buckets["5-10"] += 1
            elif abs_d < 25:
                buckets["10-25"] += 1
            else:
                buckets["25+"] += 1

        biggest_improvements.sort(key=lambda x: x["delta"], reverse=True)
        biggest_declines.sort(key=lambda x: x["delta"])

        results.append({
            "date": curr_sess.started_at.isoformat() if curr_sess.started_at else None,
            "session_id": curr_sess.id,
            "prev_session_id": prev_sess.id,
            "common_fis": len(common),
            "changed": changed_count,
            "improved": improved_count,
            "declined": declined_count,
            "unchanged": len(common) - changed_count,
            "avg_abs_change": round(total_abs_change / changed_count, 2) if changed_count else 0,
            "buckets": buckets,
            "top_improvements": biggest_improvements[:5],
            "top_declines": biggest_declines[:5],
        })

    return jsonify(results)


@app.route("/api/institutions/<path:name>/history")
@login_required
def get_institution_history(name):
    """Return metric history for a specific institution across all completed scrapes."""
    sessions = (
        ScrapeSession.query.filter_by(status="completed")
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
        .order_by(ScrapeSession.started_at.asc())
        .all()
    )

    history = []
    for sess in sessions:
        conn = (
            Connection.query
            .filter_by(scrape_session_id=sess.id, institution_name=name)
            .first()
        )
        if conn:
            history.append({
                "session_id": sess.id,
                "date": sess.started_at.isoformat() if sess.started_at else None,
                "rank": conn.rank,
                "data_provider": conn.data_provider,
                "additional_providers": conn.additional_providers,
                "success_pct": conn.success_pct,
                "longevity_pct": conn.longevity_pct,
                "update_pct": conn.update_pct,
                "connection_status": conn.connection_status,
            })

    return jsonify({"institution_name": name, "history": history})


@app.route("/api/sessions/<int:session_id>/provider-changes")
@login_required
def get_provider_changes(session_id):
    """Return FIs whose primary provider changed compared to the previous session."""
    current = db.session.get(ScrapeSession, session_id)
    if not current:
        return jsonify({"error": "Session not found"}), 404

    # Find the previous completed session
    prev = (
        ScrapeSession.query
        .filter(ScrapeSession.status == "completed")
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
        .filter(ScrapeSession.started_at < current.started_at)
        .order_by(ScrapeSession.started_at.desc())
        .first()
    )
    if not prev:
        return jsonify({"changes": [], "prev_session_id": None})

    cur_conns = {
        c.institution_name: c.data_provider
        for c in Connection.query.filter_by(scrape_session_id=session_id).all()
    }
    prev_conns = {
        c.institution_name: c.data_provider
        for c in Connection.query.filter_by(scrape_session_id=prev.id).all()
    }

    changes = []
    for name, cur_prov in cur_conns.items():
        prev_prov = prev_conns.get(name)
        if prev_prov and cur_prov and prev_prov != cur_prov:
            changes.append({
                "institution_name": name,
                "prev_provider": prev_prov,
                "new_provider": cur_prov,
            })

    changes.sort(key=lambda x: x["institution_name"])
    return jsonify({
        "changes": changes,
        "prev_session_id": prev.id,
        "prev_date": prev.started_at.isoformat() if prev.started_at else None,
        "cur_date": current.started_at.isoformat() if current.started_at else None,
    })


@app.route("/api/sessions/range-diff")
@login_required
def range_diff():
    """Return per-step diffs for a range of consecutive sessions (from_id → latest).
    Includes provider switches, added/removed FIs, and service degradation changes.
    """
    from_id = request.args.get("from", type=int)
    to_id = request.args.get("to", type=int)
    if not from_id:
        return jsonify({"error": "Provide ?from=<session_id>"}), 400

    W_S, W_L, W_U = 0.4, 0.3, 0.3

    def _weighted(c):
        parts, weights = [], []
        if c.success_pct is not None:
            parts.append(c.success_pct * W_S); weights.append(W_S)
        if c.longevity_pct is not None:
            parts.append(c.longevity_pct * W_L); weights.append(W_L)
        if c.update_pct is not None:
            parts.append(c.update_pct * W_U); weights.append(W_U)
        return round(sum(parts) / sum(weights), 2) if weights else None

    sessions = (
        ScrapeSession.query.filter_by(status="completed")
        .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
        .filter(ScrapeSession.id >= from_id)
        .order_by(ScrapeSession.started_at.asc())
        .all()
    )
    if to_id:
        sessions = [s for s in sessions if s.id <= to_id]

    if len(sessions) < 2:
        return jsonify([])

    session_ids = [s.id for s in sessions]
    all_conns = (
        Connection.query
        .filter(Connection.scrape_session_id.in_(session_ids))
        .order_by(Connection.rank)
        .all()
    )
    from collections import defaultdict
    by_sess = defaultdict(dict)
    for c in all_conns:
        by_sess[c.scrape_session_id][c.institution_name] = c

    steps = []
    for i in range(1, len(sessions)):
        prev_sess = sessions[i - 1]
        curr_sess = sessions[i]
        prev_map = by_sess.get(prev_sess.id, {})
        curr_map = by_sess.get(curr_sess.id, {})

        prev_names = set(prev_map.keys())
        curr_names = set(curr_map.keys())
        common = prev_names & curr_names
        added = sorted(curr_names - prev_names)
        removed = sorted(prev_names - curr_names)

        provider_switches = []
        degraded = []
        improved = []

        for name in sorted(common):
            pc = prev_map[name]
            cc = curr_map[name]
            pp = pc.data_provider
            cp = cc.data_provider
            if pp and cp and pp != cp:
                provider_switches.append({
                    "name": name,
                    "from_provider": pp,
                    "to_provider": cp,
                })
            pw = _weighted(pc)
            cw = _weighted(cc)
            if pw is not None and cw is not None:
                delta = round(cw - pw, 2)
                if delta <= -5:
                    degraded.append({"name": name, "delta": delta, "provider": cp})
                elif delta >= 5:
                    improved.append({"name": name, "delta": delta, "provider": cp})

            # Connection status changes
            if pc.connection_status != cc.connection_status:
                if cc.connection_status in ("Issues reported", "Unavailable"):
                    degraded.append({
                        "name": name,
                        "status_change": f"{pc.connection_status or 'OK'} → {cc.connection_status}",
                        "provider": cp,
                    })

        steps.append({
            "prev_session": prev_sess.to_dict(),
            "curr_session": curr_sess.to_dict(),
            "added": [{"name": n, "provider": curr_map[n].data_provider} for n in added],
            "removed": [{"name": n, "provider": prev_map[n].data_provider} for n in removed],
            "added_count": len(added),
            "removed_count": len(removed),
            "provider_switches": provider_switches,
            "switch_count": len(provider_switches),
            "degraded": sorted(degraded, key=lambda x: x.get("delta", 0)),
            "improved": sorted(improved, key=lambda x: -x.get("delta", 0)),
        })

    return jsonify(steps)


@app.route("/api/sessions/diff")
@login_required
def diff_sessions():
    """Compare two sessions and return provider changes + summary."""
    session_a = request.args.get("a", type=int)
    session_b = request.args.get("b", type=int)
    if not session_a or not session_b:
        return jsonify({"error": "Provide ?a=<id>&b=<id>"}), 400

    sa = db.session.get(ScrapeSession, session_a)
    sb = db.session.get(ScrapeSession, session_b)
    if not sa or not sb:
        return jsonify({"error": "Session not found"}), 404

    conns_a = {
        c.institution_name: c.data_provider
        for c in Connection.query.filter_by(scrape_session_id=session_a).all()
    }
    conns_b = {
        c.institution_name: c.data_provider
        for c in Connection.query.filter_by(scrape_session_id=session_b).all()
    }

    changes = []
    for name in set(conns_a) & set(conns_b):
        prov_a = conns_a.get(name)
        prov_b = conns_b.get(name)
        if prov_a and prov_b and prov_a != prov_b:
            changes.append({
                "institution_name": name,
                "provider_a": prov_a,
                "provider_b": prov_b,
            })

    changes.sort(key=lambda x: x["institution_name"])

    # Summarise net gains/losses per provider
    summary = {}
    for ch in changes:
        pa = ch["provider_a"]
        pb = ch["provider_b"]
        summary.setdefault(pa, {"gained": 0, "lost": 0})
        summary.setdefault(pb, {"gained": 0, "lost": 0})
        summary[pa]["lost"] += 1
        summary[pb]["gained"] += 1

    # FIs added/removed between sessions
    added = []
    removed = []
    for name in set(conns_b) - set(conns_a):
        added.append({"institution_name": name, "provider": conns_b[name]})
    for name in set(conns_a) - set(conns_b):
        removed.append({"institution_name": name, "provider": conns_a[name]})
    added.sort(key=lambda x: x["institution_name"])
    removed.sort(key=lambda x: x["institution_name"])

    return jsonify({
        "session_a": sa.to_dict(),
        "session_b": sb.to_dict(),
        "changes": changes,
        "summary": summary,
        "total_changes": len(changes),
        "added": added,
        "removed": removed,
    })


if __name__ == "__main__":
    # Start the daily scheduler
    start_scheduler(app, lambda: _launch_scrape(source="scheduled"))
    app.run(debug=True, port=5555, threaded=True, use_reloader=False)
