"""JSON API fetcher — retrieves institution data from Monarch ISS API and saves to database.

Replaces the Playwright web scraper as the primary data source.  The ISS
API at https://iss-api.monarch.com/api/v1/institutions/top is paginated
(~100 items per page, ~11 000 total).  Each item contains per-provider
metrics with pill_value scores (0-4) which we convert to percentages:

    pill_value 0 → None (not rated)
    pill_value 1 → 25%
    pill_value 2 → 50%
    pill_value 3 → 75%
    pill_value 4 → 100%
"""

import json
import logging
import os
import ssl
import urllib.request
from datetime import datetime, timezone

from models import Connection, ScrapeSession, db

log = logging.getLogger(__name__)

ISS_URL = "https://iss-api.monarch.com/api/v1/institutions/top"

PILL_TO_PCT = {0: None, 1: 25.0, 2: 50.0, 3: 75.0, 4: 100.0}

LEVEL_LABELS = {
    25.0: "Low",
    50.0: "Medium",
    75.0: "Good",
    100.0: "Excellent",
}


def _pill_to_pct(metrics, key):
    """Convert a pill_value from the API metrics to a percentage."""
    if not metrics:
        return None
    m = metrics.get(key)
    if not m:
        return None
    pill = m.get("pill_value")
    if pill is None or pill == 0:
        return None
    return PILL_TO_PCT.get(pill)


def _capitalize_provider(name):
    """Normalize provider name: plaid→Plaid, finicity→Finicity, mx→MX."""
    if not name:
        return "Unknown"
    if name.lower() == "mx":
        return "MX"
    return name.capitalize()


def fetch_json_connections(app, progress_callback=None, session_id=None):
    """Fetch all institutions from Monarch ISS API and save to database.

    Uses the same ScrapeSession / Connection models as the old Playwright
    scraper so all existing dashboard, history, and chart endpoints work
    unchanged.

    Args:
        app: Flask application instance.
        progress_callback: callable(event_type, data_dict) for SSE progress.
        session_id: existing ScrapeSession ID to populate.

    Returns:
        session_id
    """

    def emit(event_type, data):
        if progress_callback:
            progress_callback(event_type, data)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    emit("status", {"message": "Connecting to Monarch ISS API..."})

    try:
        # ── Phase 1: Fetch all pages ──────────────────────────────
        all_items = []
        offset = 0

        while True:
            url = f"{ISS_URL}?offset={offset}"
            req = urllib.request.Request(
                url, headers={"User-Agent": "MonarchScraper/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            items = data.get("items", [])
            next_offset = data.get("next_offset")
            all_items.extend(items)

            # Build compact FI summaries for the live-feed panel
            fi_batch = []
            for it in items:
                fi_batch.append({
                    "name": it.get("name", "Unknown"),
                    "prov": _capitalize_provider(it.get("preferred_data_provider")),
                    "logo": it.get("logo") if isinstance(it.get("logo"), str) and it["logo"].startswith("data:image") else None,
                })

            emit(
                "progress",
                {
                    "message": f"Fetched {len(all_items)} institutions...",
                    "phase": "scrolling",       # reuse existing phase name
                    "count": len(all_items),
                    "fi_batch": fi_batch,
                },
            )

            if not next_offset or len(items) == 0:
                break
            offset = next_offset

        total = len(all_items)
        emit(
            "progress",
            {
                "message": f"Retrieved {total} institutions. Saving to database...",
                "phase": "saving",
                "current": 0,
                "total": total,
                "count": total,
            },
        )

        # ── Phase 2: Save to database ────────────────────────────
        with app.app_context():
            session = db.session.get(ScrapeSession, session_id)
            session.status = "running"
            session.total_institutions = total
            db.session.commit()

            for idx, item in enumerate(all_items):
                pref_prov = _capitalize_provider(
                    item.get("preferred_data_provider")
                )

                # Find metrics for the preferred provider
                dpm_list = item.get("data_provider_metrics", [])
                pref_metrics = None
                for dpm in dpm_list:
                    if dpm.get("data_provider") == item.get(
                        "preferred_data_provider"
                    ):
                        pref_metrics = dpm.get("metrics", {})
                        break

                # Primary provider metrics
                success_pct = _pill_to_pct(
                    pref_metrics, "first_connection_success"
                )
                longevity_pct = _pill_to_pct(
                    pref_metrics, "connection_longevity"
                )
                update_pct = _pill_to_pct(
                    pref_metrics, "average_update_time"
                )

                # Connection status
                if item.get("new_connections_disabled"):
                    status = "Unavailable"
                elif item.get("has_issues_reported"):
                    status = "Issues reported"
                elif not item.get("active"):
                    status = "Unavailable"
                else:
                    status = "OK"

                # Build per-provider detail (same shape the scraper stored)
                provider_details = []
                for dpm in dpm_list:
                    dp_name = _capitalize_provider(dpm.get("data_provider"))
                    metrics = dpm.get("metrics", {})

                    s_pct = _pill_to_pct(metrics, "first_connection_success")
                    l_pct = _pill_to_pct(metrics, "connection_longevity")
                    u_pct = _pill_to_pct(metrics, "average_update_time")

                    provider_details.append(
                        {
                            "name": dp_name,
                            "success_pct": s_pct,
                            "success_rate": LEVEL_LABELS.get(s_pct),
                            "longevity_pct": l_pct,
                            "longevity": LEVEL_LABELS.get(l_pct),
                            "update_pct": u_pct,
                            "update_frequency": LEVEL_LABELS.get(u_pct),
                        }
                    )

                # Ensure preferred provider is listed first
                provider_details.sort(
                    key=lambda pd: 0 if pd["name"] == pref_prov else 1
                )

                # Save logo to disk (instance/logos/<hash>.png) — skip if cached
                logo_data = item.get("logo")
                if logo_data and isinstance(logo_data, str):
                    import base64, hashlib
                    inst_name = item.get("name", "Unknown")
                    logo_hash = hashlib.md5(inst_name.encode()).hexdigest()
                    logo_dir = os.path.join(app.instance_path, "logos")
                    os.makedirs(logo_dir, exist_ok=True)
                    logo_path = os.path.join(logo_dir, f"{logo_hash}.png")
                    if not os.path.exists(logo_path):
                        try:
                            if logo_data.startswith("data:image"):
                                _, b64data = logo_data.split(",", 1)
                                raw = base64.b64decode(b64data)
                            elif logo_data.startswith(("http://", "https://")):
                                req = urllib.request.Request(
                                    logo_data,
                                    headers={"User-Agent": "MonarchScraper/1.0"},
                                )
                                with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                                    raw = resp.read()
                            else:
                                b64data = logo_data
                                raw = base64.b64decode(b64data)
                            if len(raw) > 100:
                                with open(logo_path, "wb") as lf:
                                    lf.write(raw)
                        except Exception:
                            pass

                conn = Connection(
                    scrape_session_id=session_id,
                    rank=item.get("popularity", idx + 1),
                    institution_name=item.get("name", "Unknown"),
                    data_provider=pref_prov,
                    additional_providers=json.dumps(provider_details),
                    success_pct=success_pct,
                    success_rate=LEVEL_LABELS.get(success_pct),
                    longevity_pct=longevity_pct,
                    longevity=LEVEL_LABELS.get(longevity_pct),
                    update_pct=update_pct,
                    update_frequency=LEVEL_LABELS.get(update_pct),
                    connection_status=status,
                )
                db.session.add(conn)

                if (idx + 1) % 100 == 0 or idx == total - 1:
                    emit(
                        "progress",
                        {
                            "message": f"Saving {idx + 1} / {total} institutions",
                            "phase": "saving",
                            "current": idx + 1,
                            "total": total,
                            "count": total,
                        },
                    )

            session.status = "completed"
            session.finished_at = datetime.now(timezone.utc)
            db.session.commit()

        emit(
            "complete",
            {
                "message": f"Retrieval complete! {total} institutions saved.",
                "session_id": session_id,
                "total": total,
            },
        )
        return session_id

    except Exception as e:
        with app.app_context():
            session = db.session.get(ScrapeSession, session_id)
            if session:
                session.status = "failed"
                session.error_message = str(e)
                session.finished_at = datetime.now(timezone.utc)
                db.session.commit()

        emit("error", {"message": f"Retrieval failed: {str(e)}"})
        raise
