"""Database models for Monarch connection status scraper."""

from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class ScrapeSession(db.Model):
    """Represents a single scrape run, timestamped for historical tracking."""

    __tablename__ = "scrape_sessions"
    __table_args__ = (
        db.Index("ix_sessions_status", "status"),
        db.Index("ix_sessions_started_at", "started_at"),
    )

    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(
        db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    finished_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(
        db.String(20), nullable=False, default="running"
    )  # running | completed | failed
    total_institutions = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)

    connections = db.relationship(
        "Connection", backref="session", lazy=True, cascade="all, delete-orphan"
    )
    summaries = db.relationship(
        "SessionSummary", backref="session", lazy=True, cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": self.status,
            "total_institutions": self.total_institutions,
            "error_message": self.error_message,
        }


class Connection(db.Model):
    """Represents a single financial institution's connection status."""

    __tablename__ = "connections"
    __table_args__ = (
        db.Index("ix_connections_session_id", "scrape_session_id"),
        db.Index("ix_connections_institution_name", "institution_name"),
        db.Index("ix_connections_session_name", "scrape_session_id", "institution_name"),
        db.Index("ix_connections_session_rank", "scrape_session_id", "rank"),
        db.Index("ix_connections_status", "connection_status"),
    )

    id = db.Column(db.Integer, primary_key=True)
    scrape_session_id = db.Column(
        db.Integer, db.ForeignKey("scrape_sessions.id"), nullable=False
    )
    rank = db.Column(db.Integer, nullable=False)  # popularity rank order
    institution_name = db.Column(db.String(255), nullable=False)
    data_provider = db.Column(db.String(100), nullable=True)  # Plaid, Finicity, MX
    additional_providers = db.Column(
        db.Text, nullable=True
    )  # JSON list of extra providers
    success_rate = db.Column(db.String(50), nullable=True)  # e.g. "High", "Medium"
    success_pct = db.Column(db.Float, nullable=True)  # numeric percentage if available
    longevity = db.Column(db.String(50), nullable=True)
    longevity_pct = db.Column(db.Float, nullable=True)
    update_frequency = db.Column(db.String(50), nullable=True)
    update_pct = db.Column(db.Float, nullable=True)
    connection_status = db.Column(
        db.String(50), nullable=True
    )  # "OK", "Issues reported", "Unavailable"
    status_detail = db.Column(db.Text, nullable=True)  # e.g. "Crypto accounts are not supported."

    def to_dict(self):
        return {
            "id": self.id,
            "rank": self.rank,
            "institution_name": self.institution_name,
            "data_provider": self.data_provider,
            "additional_providers": self.additional_providers,
            "success_rate": self.success_rate,
            "success_pct": self.success_pct,
            "longevity": self.longevity,
            "longevity_pct": self.longevity_pct,
            "update_frequency": self.update_frequency,
            "update_pct": self.update_pct,
            "connection_status": self.connection_status,
            "status_detail": self.status_detail,
        }


class SessionSummary(db.Model):
    """Pre-computed per-session, per-provider aggregate metrics.

    Populated at the end of each scrape to avoid re-scanning all Connection
    rows for history/stats endpoints.
    """

    __tablename__ = "session_summaries"
    __table_args__ = (
        db.Index("ix_summary_session_id", "scrape_session_id"),
        db.UniqueConstraint("scrape_session_id", "provider", name="uq_summary_session_provider"),
    )

    id = db.Column(db.Integer, primary_key=True)
    scrape_session_id = db.Column(
        db.Integer, db.ForeignKey("scrape_sessions.id"), nullable=False
    )
    provider = db.Column(db.String(100), nullable=False)
    count = db.Column(db.Integer, nullable=False, default=0)
    avg_success = db.Column(db.Float, nullable=True)
    avg_longevity = db.Column(db.Float, nullable=True)
    avg_update = db.Column(db.Float, nullable=True)
    weighted_avg = db.Column(db.Float, nullable=True)
    issues_count = db.Column(db.Integer, nullable=False, default=0)
    unavailable_count = db.Column(db.Integer, nullable=False, default=0)
    ok_count = db.Column(db.Integer, nullable=False, default=0)

    def to_dict(self):
        return {
            "provider": self.provider,
            "count": self.count,
            "avg_success": self.avg_success,
            "avg_longevity": self.avg_longevity,
            "avg_update": self.avg_update,
            "weighted_avg": self.weighted_avg,
            "issues_count": self.issues_count,
            "unavailable_count": self.unavailable_count,
            "ok_count": self.ok_count,
        }
