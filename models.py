"""Database models for Monarch connection status scraper."""

from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class ScrapeSession(db.Model):
    """Represents a single scrape run, timestamped for historical tracking."""

    __tablename__ = "scrape_sessions"

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

    # classification based on Mastercard Open Finance attributes
    institution_type = db.Column(
        db.String(20), nullable=False, default="UnMatched"
    )  # OAuth | Legacy | UnMatched

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
            "institution_type": self.institution_type,
        }
