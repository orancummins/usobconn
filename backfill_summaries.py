"""Backfill SessionSummary rows for all existing completed sessions.

Run once after deploying the SessionSummary optimisation to populate
pre-computed aggregates for historical scrape sessions.

Usage:
    python backfill_summaries.py
"""

import sys
from app import app, db
from models import ScrapeSession, SessionSummary
from json_fetcher import _build_session_summaries

MIN_SESSION_FIS = 5000


def backfill():
    with app.app_context():
        # Find completed sessions that have no summaries yet
        already = {
            r[0]
            for r in db.session.query(SessionSummary.scrape_session_id).distinct().all()
        }
        sessions = (
            ScrapeSession.query
            .filter_by(status="completed")
            .filter(ScrapeSession.total_institutions >= MIN_SESSION_FIS)
            .order_by(ScrapeSession.id)
            .all()
        )
        to_process = [s for s in sessions if s.id not in already]
        total = len(to_process)
        if total == 0:
            print("All sessions already have summaries — nothing to do.")
            return

        print(f"Backfilling summaries for {total} session(s)...")
        for i, sess in enumerate(to_process, 1):
            _build_session_summaries(sess.id)
            print(f"  [{i}/{total}] Session {sess.id} done")

        print("Backfill complete.")


if __name__ == "__main__":
    backfill()
