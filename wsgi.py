"""WSGI entrypoint for Gunicorn."""

import os

from app import _launch_scrape, app
from scheduler import start_scheduler


if os.environ.get("ENABLE_SCHEDULER", "true").lower() in ("1", "true", "yes"):
    start_scheduler(app, lambda: _launch_scrape(source="scheduled"))

