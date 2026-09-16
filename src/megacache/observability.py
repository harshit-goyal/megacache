"""Structured logging configuration."""

import datetime
import json
import logging
from typing import Any, Dict

_EXTRA_FIELDS = (
    "protocol",
    "operation",
    "status",
    "duration_ms",
    "remote",
    "username",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        document: Dict[str, Any] = {
            "timestamp": datetime.datetime.fromtimestamp(
                record.created, datetime.timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                document[field] = value
        if record.exc_info:
            document["exception"] = self.formatException(record.exc_info)
        return json.dumps(document, separators=(",", ":"))


def configure_logging(format_name: str) -> None:
    handler = logging.StreamHandler()
    if format_name == "json":
        handler.setFormatter(JsonFormatter())
    elif format_name == "text":
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
        )
    else:
        raise ValueError("MEGACACHE_LOG_FORMAT must be 'json' or 'text'")
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

