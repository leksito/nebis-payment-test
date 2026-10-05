import dataclasses
import json
from collections.abc import Iterable
from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from app.adapters.postgres.tables import outbox
from app.domain.events import DomainEvent

_ENVELOPE_FIELDS = {"event_id", "occurred_at"}


async def insert_events(conn: AsyncConnection, events: Iterable[DomainEvent]) -> None:
    """Write domain events to the outbox, in the caller's transaction."""
    rows = [
        {
            "id": event.event_id,
            "event_type": event.event_type,
            "payload": event_payload(event),
            "created_at": event.occurred_at,
        }
        for event in events
    ]
    if rows:
        await conn.execute(insert(outbox), rows)


def event_payload(event: DomainEvent) -> dict[str, Any]:
    body = {
        f.name: getattr(event, f.name)
        for f in dataclasses.fields(event)
        if f.name not in _ENVELOPE_FIELDS
    }
    # Round-trip through JSON so UUIDs, datetimes, Decimals become JSON-native
    return json.loads(json.dumps(body, default=str))
