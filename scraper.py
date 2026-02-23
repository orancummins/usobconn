"""Scraper module for Monarch connection status page using Playwright.

The page uses a CSS grid layout (not a <table>). Each institution row is a div
with class 'sc-gahYZc cGmkTm' containing 5 child cells:
  Cell 0: Institution name + logo
  Cell 1: Data provider (Plaid / Finicity / MX) + additional providers
  Cell 2: Connection success    (4 colored blocks)
  Cell 3: Connection longevity  (4 colored blocks)
  Cell 4: Average update time   (4 colored blocks)

Metric blocks use background-color to indicate level:
  rgb(43, 154, 102)   = dark green   (filled)
  rgb(141, 182, 84)   = light green  (filled)
  rgb(255, 197, 61)   = yellow       (filled)
  rgb(220, 62, 66)    = red          (filled)
  rgb(235, 232, 229)  = gray         (empty)

Score mapping (filled blocks out of 4):
  4/4 -> 100%   3/4 -> 75%   2/4 -> 50%   1/4 -> 25%   0/4 -> 0%
"""

import json
import random
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright

from models import Connection, ScrapeSession, db

TARGET_URL = "https://www.monarch.com/connection-status"

# Gray is the only "empty" color; anything else counts as filled
GRAY_COLOR = "rgb(235, 232, 229)"

# 0 filled blocks = not rated (null), not 0%.
# On Monarch's scale, all-gray means "no data" for that metric.
SCORE_MAP = {0: None, 1: 25.0, 2: 50.0, 3: 75.0, 4: 100.0}

LEVEL_LABELS = {
    25.0: "Low",
    50.0: "Medium",
    75.0: "Good",
    100.0: "Excellent",
}


def scrape_connections(app, progress_callback=None, session_id=None):
    """
    Scrape the Monarch connection status page.

    Args:
        app: Flask app instance (for db context)
        progress_callback: callable(event_type, data_dict) for SSE updates
        session_id: existing ScrapeSession id to update (if None, creates new)

    Returns:
        scrape_session_id
    """

    def emit(event_type, data):
        if progress_callback:
            progress_callback(event_type, data)

    with app.app_context():
        if session_id:
            session = db.session.get(ScrapeSession, session_id)
            session.started_at = datetime.now(timezone.utc)
            session.status = "running"
            db.session.commit()
        else:
            session = ScrapeSession(
                started_at=datetime.now(timezone.utc), status="running"
            )
            db.session.add(session)
            db.session.commit()
            session_id = session.id

    emit("status", {"message": "Starting browser...", "session_id": session_id})

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
            )

            # Remove automation indicators
            page = context.new_page()
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            emit(
                "status",
                {"message": "Navigating to Monarch connection status page..."},
            )
            # Use domcontentloaded instead of networkidle — the page has
            # long-running analytics/tracking requests that prevent networkidle
            # from ever firing within the timeout.
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=90000)
            # Extra settle time to let JS hydrate and render
            page.wait_for_timeout(random.randint(3000, 5000))

            # Dismiss cookie banner if present
            try:
                reject_btn = page.query_selector(
                    "button:has-text('Reject Non-Essential')"
                )
                if reject_btn:
                    reject_btn.click()
                    page.wait_for_timeout(1000)
            except Exception:
                pass

            emit(
                "status",
                {"message": "Page loaded. Scrolling to load all rows..."},
            )

            # Wait for data rows to appear — use the actual styled-component class
            page.wait_for_selector(".sc-gahYZc.cGmkTm", timeout=30000)
            page.wait_for_timeout(random.randint(1500, 2500))

            # JavaScript to extract visible rows.
            # getComputedStyle only returns accurate colors for elements that
            # are on-screen (painted). Off-screen blocks return the default
            # gray, which would incorrectly score every metric as 0%.
            # We extract visible rows at each scroll position during the
            # single scroll pass that also triggers lazy loading.
            JS_EXTRACT_VISIBLE = """() => {
                const GRAY = 'rgb(235, 232, 229)';
                // 0 filled blocks = not rated (null), not 0%
                const SCORE_MAP = {0: null, 1: 25, 2: 50, 3: 75, 4: 100};
                const results = [];
                const vTop = window.scrollY;
                const vBot = vTop + window.innerHeight;

                const rows = document.querySelectorAll('.sc-gahYZc.cGmkTm');
                rows.forEach((row, idx) => {
                    const rect = row.getBoundingClientRect();
                    const absTop = rect.top + window.scrollY;
                    const absBot = rect.bottom + window.scrollY;
                    // Only process rows at least partially in the viewport
                    if (absBot < vTop || absTop > vBot) return;

                    try {
                        const cells = row.children;
                        if (cells.length < 5) return;

                        const nameEl = cells[0];
                        const name = nameEl.innerText.trim().split('\\n')[0].trim();
                        if (!name || name.length < 2) return;

                        const provCell = cells[1];
                        const provText = provCell.innerText.trim();
                        const lines = provText.split('\\n').map(l => l.trim()).filter(l => l);

                        let provider = '';
                        let additionalProviders = [];
                        const knownProviders = ['Plaid', 'Finicity', 'MX'];
                        for (const line of lines) {
                            for (const kp of knownProviders) {
                                if (line === kp || line.startsWith(kp)) {
                                    if (!provider) provider = kp;
                                    else additionalProviders.push(kp);
                                }
                            }
                            const moreMatch = line.match(/\\+(\\d+)\\s*more/i);
                            if (moreMatch) additionalProviders.push(line);
                        }

                        function readMetric(cell) {
                            const blocks = cell.querySelectorAll('.sc-dYwGCk');
                            if (blocks.length === 0) return null;
                            let filled = 0;
                            for (const b of blocks) {
                                const bg = window.getComputedStyle(b).backgroundColor;
                                if (bg !== GRAY) filled++;
                            }
                            return SCORE_MAP[filled] !== undefined ? SCORE_MAP[filled] : null;
                        }

                        const success_pct = readMetric(cells[2]);
                        const longevity_pct = readMetric(cells[3]);
                        const update_pct = readMetric(cells[4]);

                        let status = 'OK';
                        const rowText = row.innerText;
                        if (rowText.includes('Unavailable')) status = 'Unavailable';
                        else if (rowText.includes('Issues reported')) status = 'Issues reported';

                        results.push({
                            idx: idx,
                            name: name,
                            provider: provider,
                            additional_providers: additionalProviders,
                            success_pct: success_pct,
                            longevity_pct: longevity_pct,
                            update_pct: update_pct,
                            status: status
                        });
                    } catch(e) {}
                });
                return results;
            }"""

            def _merge_batch(batch, all_extracted):
                """Merge a batch of extracted rows into the accumulated dict."""
                for inst in batch:
                    key = (inst["name"], inst["provider"])
                    existing = all_extracted.get(key)
                    if existing is None:
                        all_extracted[key] = inst
                    else:
                        # Update if this batch has better (non-null) metrics
                        for field in ("success_pct", "longevity_pct", "update_pct"):
                            new_val = inst.get(field)
                            old_val = existing.get(field)
                            if new_val is not None and (old_val is None or old_val == 0) and new_val > 0:
                                existing[field] = new_val

            # Single combined scroll pass: triggers lazy loading AND extracts
            # visible row data at each position, so we only traverse once.
            prev_count = 0
            stall_streak = 0
            STALL_LIMIT = 5  # require 5 consecutive stalls to stop
            max_scroll_attempts = 120
            all_extracted = {}  # keyed by (name, provider) to deduplicate

            for scroll_attempt in range(max_scroll_attempts):
                # Scroll incrementally (viewport-height steps) instead of
                # jumping straight to the bottom, which looks more natural
                # and gives the lazy loader time to fetch batches.
                page.evaluate(
                    "window.scrollBy(0, window.innerHeight * (0.8 + Math.random() * 0.4))"
                )
                page.wait_for_timeout(random.randint(800, 1800))

                current_count = page.evaluate(
                    "document.querySelectorAll('.sc-gahYZc.cGmkTm').length"
                )

                # Extract visible rows at this scroll position
                batch = page.evaluate(JS_EXTRACT_VISIBLE)
                _merge_batch(batch, all_extracted)

                if current_count > prev_count:
                    stall_streak = 0
                    prev_count = current_count
                else:
                    stall_streak += 1
                    # On stall, try jump-to-bottom then back up to unstick
                    if stall_streak == 2:
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(1500, 2500))
                        batch = page.evaluate(JS_EXTRACT_VISIBLE)
                        _merge_batch(batch, all_extracted)
                    elif stall_streak == 3:
                        # Scroll up a bit and back down to trigger observers
                        page.evaluate(
                            "window.scrollBy(0, -window.innerHeight * 2)"
                        )
                        page.wait_for_timeout(random.randint(1000, 1800))
                        batch = page.evaluate(JS_EXTRACT_VISIBLE)
                        _merge_batch(batch, all_extracted)
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        page.wait_for_timeout(random.randint(1500, 2500))
                        batch = page.evaluate(JS_EXTRACT_VISIBLE)
                        _merge_batch(batch, all_extracted)

                if stall_streak >= STALL_LIMIT:
                    break

                # Emit progress every 3rd scroll attempt
                if current_count > 0 and scroll_attempt % 3 == 0:
                    emit(
                        "progress",
                        {
                            "message": f"Scrolling... {len(all_extracted)} / ~{current_count} institutions captured",
                            "phase": "scrolling",
                            "count": len(all_extracted),
                        },
                    )

            # Quick cleanup sweep: scroll top-to-bottom to catch any rows
            # that were still loading during the first pass.
            emit(
                "status",
                {
                    "message": f"Verifying {len(all_extracted)} institutions..."
                },
            )
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(random.randint(800, 1200))

            for _ in range(200):
                page.wait_for_timeout(random.randint(200, 400))
                batch = page.evaluate(JS_EXTRACT_VISIBLE)
                _merge_batch(batch, all_extracted)

                at_bottom = page.evaluate(
                    """() => {
                    window.scrollBy(0, window.innerHeight * 0.8);
                    return window.scrollY + window.innerHeight >= document.body.scrollHeight - 50;
                }"""
                )
                if at_bottom:
                    page.wait_for_timeout(random.randint(200, 400))
                    batch = page.evaluate(JS_EXTRACT_VISIBLE)
                    _merge_batch(batch, all_extracted)
                    break

            institutions_data = list(all_extracted.values())

            total = len(institutions_data)
            emit(
                "progress",
                {
                    "message": f"Extracted {total} institutions. Saving to database...",
                    "phase": "saving",
                    "current": 0,
                    "total": total,
                    "count": total,
                },
            )

            # Save to database
            with app.app_context():
                session = db.session.get(ScrapeSession, session_id)
                session.total_institutions = total

                for idx, inst in enumerate(institutions_data):
                    success = inst.get("success_pct")
                    longevity = inst.get("longevity_pct")
                    update = inst.get("update_pct")

                    conn = Connection(
                        scrape_session_id=session_id,
                        rank=idx + 1,
                        institution_name=inst.get("name", "Unknown"),
                        data_provider=inst.get("provider", ""),
                        additional_providers=json.dumps(
                            inst.get("additional_providers", [])
                        ),
                        success_pct=success,
                        success_rate=LEVEL_LABELS.get(success),
                        longevity_pct=longevity,
                        longevity=LEVEL_LABELS.get(longevity),
                        update_pct=update,
                        update_frequency=LEVEL_LABELS.get(update),
                        connection_status=inst.get("status", "OK"),
                    )
                    db.session.add(conn)

                    if (idx + 1) % 25 == 0 or idx == total - 1:
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

            browser.close()

        emit(
            "complete",
            {
                "message": f"Scraping complete! {total} institutions saved.",
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

        emit("error", {"message": f"Scraping failed: {str(e)}"})
        raise
