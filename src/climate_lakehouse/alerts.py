"""Der einzige Alarmkanal: immer das Log, optional zusätzlich ein Slack-Webhook."""

import logging

import httpx

from climate_lakehouse.config import Settings

log = logging.getLogger(__name__)


def send_alert(settings: Settings, title: str, detail: str) -> None:
    """Meldet einen gescheiterten Lauf. Wirft nie: Ein kaputter Kanal darf ihn nicht verdecken."""
    log.error("ALERT %s", title, extra={"alert": title, "detail": detail})
    if settings.alert_channel != "slack":
        return
    try:
        response = httpx.post(
            settings.slack_webhook_url,
            json={"text": f"*{title}*\n{detail}"},
            timeout=settings.http_timeout_s,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        log.error("slack alert failed", extra={"error": str(exc)})
