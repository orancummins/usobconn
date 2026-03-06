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
import base64
import hashlib
import threading
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


def _fetch_all_institutions(ctx, emit=None):
    """Fetch all institution items from the ISS API."""
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

        if emit:
            fi_batch = []
            for it in items:
                fi_batch.append(
                    {
                        "name": it.get("name", "Unknown"),
                        "prov": _capitalize_provider(
                            it.get("preferred_data_provider")
                        ),
                        "logo": it.get("logo")
                        if isinstance(it.get("logo"), str)
                        and it["logo"].startswith("data:image")
                        else None,
                    }
                )
            emit(
                "progress",
                {
                    "message": f"Fetched {len(all_items)} institutions...",
                    "phase": "scrolling",
                    "count": len(all_items),
                    "fi_batch": fi_batch,
                },
            )

        if not next_offset or len(items) == 0:
            break
        offset = next_offset
    return all_items


def _save_logo_to_disk(logo_dir, institution_name, logo_data, ctx=None, force=False):
    """Persist a logo for an institution if possible."""
    if not logo_data or not isinstance(logo_data, str):
        return False

    logo_hash = hashlib.md5(institution_name.encode()).hexdigest()
    logo_path = os.path.join(logo_dir, f"{logo_hash}.png")
    if (not force) and os.path.exists(logo_path):
        return False

    try:
        if logo_data.startswith("data:image"):
            _, b64data = logo_data.split(",", 1)
            raw = base64.b64decode(b64data)
        elif logo_data.startswith(("http://", "https://")):
            req = urllib.request.Request(
                logo_data, headers={"User-Agent": "MonarchScraper/1.0"}
            )
            with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                raw = resp.read()
        else:
            b64data = logo_data
            raw = base64.b64decode(b64data)
        if len(raw) <= 100:
            return False
        with open(logo_path, "wb") as lf:
            lf.write(raw)
        return True
    except Exception:
        return False


def refresh_logo_for_institution(app, institution_name, force=False):
    """Refresh a single institution logo from ISS API."""
    target = (institution_name or "").strip().lower()
    if not target:
        return {"found": False, "saved": False}

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    offset = 0
    while True:
        url = f"{ISS_URL}?offset={offset}"
        req = urllib.request.Request(
            url, headers={"User-Agent": "MonarchScraper/1.0"}
        )
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        items = data.get("items", [])
        for item in items:
            name = (item.get("name") or "").strip()
            if name.lower() != target:
                continue
            logo_data = item.get("logo")
            with app.app_context():
                logo_dir = os.path.join(app.instance_path, "logos")
                os.makedirs(logo_dir, exist_ok=True)
                saved = _save_logo_to_disk(
                    logo_dir, institution_name, logo_data, ctx=ctx, force=force
                )
                return {"found": True, "saved": saved}

        next_offset = data.get("next_offset")
        if not next_offset or len(items) == 0:
            break
        offset = next_offset

    return {"found": False, "saved": False}


def _schedule_full_logo_upgrade(app, institution_names):
    """Upgrade cached base64 logos to full remote logos in the background."""
    names = [n for n in institution_names if n]
    if not names:
        return

    def _run():
        log.info("Starting background full-logo upgrade for %d institutions", len(names))
        for name in names:
            try:
                refresh_logo_for_institution(app, name, force=True)
            except Exception:
                log.exception("Full-logo upgrade failed for institution: %s", name)
        log.info("Background full-logo upgrade complete")

    threading.Thread(
        target=_run, daemon=True, name="logo-upgrade-after-scrape"
    ).start()


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
        all_items = _fetch_all_institutions(ctx, emit=emit)

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

            logo_dir = os.path.join(app.instance_path, "logos")
            os.makedirs(logo_dir, exist_ok=True)

            batch_size = 2000
            pending_rows = []
            pending_logos = []
            insert_stmt = Connection.__table__.insert()

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

                logo_data = item.get("logo")
                if logo_data and isinstance(logo_data, str):
                    pending_logos.append((item.get("name", "Unknown"), logo_data))

                pending_rows.append(
                    {
                        "scrape_session_id": session_id,
                        "rank": item.get("popularity", idx + 1),
                        "institution_name": item.get("name", "Unknown"),
                        "data_provider": pref_prov,
                        "additional_providers": json.dumps(provider_details),
                        "success_pct": success_pct,
                        "success_rate": LEVEL_LABELS.get(success_pct),
                        "longevity_pct": longevity_pct,
                        "longevity": LEVEL_LABELS.get(longevity_pct),
                        "update_pct": update_pct,
                        "update_frequency": LEVEL_LABELS.get(update_pct),
                        "connection_status": status,
                    }
                )

                if len(pending_rows) >= batch_size:
                    db.session.execute(insert_stmt, pending_rows)
                    pending_rows.clear()

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

            if pending_rows:
                db.session.execute(insert_stmt, pending_rows)

            session.status = "completed"
            session.finished_at = datetime.now(timezone.utc)
            db.session.commit()

            # Cache logos after DB commit so "saving" phase reflects DB work only.
            if pending_logos:
                base64_saved_names = set()
                emit(
                    "progress",
                    {
                        "message": "Database save complete. Caching logos...",
                        "phase": "logos",
                        "current": 0,
                        "total": len(pending_logos),
                        "count": total,
                    },
                )
                for idx, (inst_name, logo_data) in enumerate(pending_logos, start=1):
                    # Keep scrape-time caching lightweight: embedded/base64 only.
                    if not logo_data.startswith(("http://", "https://")):
                        saved = _save_logo_to_disk(logo_dir, inst_name, logo_data)
                        if saved and logo_data.startswith("data:image"):
                            base64_saved_names.add(inst_name)
                    if idx % 500 == 0 or idx == len(pending_logos):
                        emit(
                            "progress",
                            {
                                "message": f"Caching logos {idx} / {len(pending_logos)}",
                                "phase": "logos",
                                "current": idx,
                                "total": len(pending_logos),
                                "count": total,
                            },
                        )

                # Upgrade newly-written base64 logos to full remote logos asynchronously.
                _schedule_full_logo_upgrade(app, sorted(base64_saved_names))

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


def refresh_logo_cache(app, progress_callback=None, force=False):
    """Refresh cached institution logos independently of DB retrieval.

    Args:
        app: Flask app.
        progress_callback: optional callable(event_type, data).
        force: if True, re-fetch even if logo file already exists.
    """

    def emit(event_type, data):
        if progress_callback:
            progress_callback(event_type, data)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    emit("status", {"message": "Starting logo refresh..."})
    all_items = _fetch_all_institutions(ctx)
    total = len(all_items)
    emit(
        "progress",
        {
            "message": f"Refreshing logo cache for {total} institutions...",
            "phase": "logos",
            "current": 0,
            "total": total,
            "count": total,
        },
    )

    saved = 0
    skipped = 0
    errors = 0
    with app.app_context():
        logo_dir = os.path.join(app.instance_path, "logos")
        os.makedirs(logo_dir, exist_ok=True)

        for idx, item in enumerate(all_items, start=1):
            name = item.get("name", "Unknown")
            logo_data = item.get("logo")
            logo_hash = hashlib.md5(name.encode()).hexdigest()
            logo_path = os.path.join(logo_dir, f"{logo_hash}.png")

            if (not force) and os.path.exists(logo_path):
                skipped += 1
            else:
                try:
                    if _save_logo_to_disk(logo_dir, name, logo_data, ctx=ctx):
                        saved += 1
                    else:
                        skipped += 1
                except Exception:
                    errors += 1

            if idx % 250 == 0 or idx == total:
                emit(
                    "progress",
                    {
                        "message": (
                            f"Logo refresh {idx} / {total} "
                            f"(saved={saved}, skipped={skipped}, errors={errors})"
                        ),
                        "phase": "logos",
                        "current": idx,
                        "total": total,
                        "count": total,
                    },
                )

    emit(
        "complete",
        {
            "message": (
                f"Logo refresh complete: saved={saved}, skipped={skipped}, errors={errors}"
            ),
            "total": total,
            "saved": saved,
            "skipped": skipped,
            "errors": errors,
        },
    )
    return {"total": total, "saved": saved, "skipped": skipped, "errors": errors}
