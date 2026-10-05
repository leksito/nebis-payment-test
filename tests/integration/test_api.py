import asyncio
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import Table, func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.tables import outbox, payments
from tests.integration.conftest import fetch_all

URL = "/api/v1/payments"

PAYLOAD = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/webhook",
}


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(engine: AsyncEngine, table: Table) -> int:
    async with engine.connect() as conn:
        return await conn.scalar(select(func.count()).select_from(table)) or 0


@pytest.mark.parametrize("api_key", [None, "wrong"])
async def test_requires_api_key(client: AsyncClient, api_key: str | None) -> None:
    client.headers.pop("X-API-Key")
    headers = {"X-API-Key": api_key} if api_key else {}

    create = await client.post(URL, json=PAYLOAD, headers={**key(), **headers})
    get = await client.get(f"{URL}/{uuid.uuid4()}", headers=headers)

    assert create.status_code == 401
    assert get.status_code == 401


async def test_create_payment_writes_payment_and_outbox_event(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    response = await client.post(URL, json=PAYLOAD, headers=key())

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"

    [event] = await fetch_all(engine, outbox)
    assert event.published_at is None
    assert event.event_type == "payment.created"
    assert event.payload == {"payment_id": body["payment_id"]}


async def test_get_payment(client: AsyncClient) -> None:
    headers = key()
    created = (await client.post(URL, json=PAYLOAD, headers=headers)).json()

    response = await client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "id": created["payment_id"],
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "status": "pending",
        "idempotency_key": headers["Idempotency-Key"],
        "webhook_url": "https://example.com/webhook",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_unknown_payment_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_same_key_and_body_replays_original_payment(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    headers = key()
    first = await client.post(URL, json=PAYLOAD, headers=headers)
    # Key order and number formatting do not matter, only the parsed values
    second = await client.post(
        URL, json={**dict(reversed(PAYLOAD.items())), "amount": 100.5}, headers=headers
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert await count(engine, payments) == 1
    assert await count(engine, outbox) == 1


async def test_same_key_with_different_body_conflicts(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    headers = key()
    await client.post(URL, json=PAYLOAD, headers=headers)

    response = await client.post(URL, json={**PAYLOAD, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409
    assert await count(engine, payments) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: AsyncClient, engine: AsyncEngine
) -> None:
    headers = key()

    responses = await asyncio.gather(
        *(client.post(URL, json=PAYLOAD, headers=headers) for _ in range(10))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(engine, payments) == 1
    assert await count(engine, outbox) == 1


async def test_idempotency_key_header_is_required(client: AsyncClient) -> None:
    response = await client.post(URL, json=PAYLOAD)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "0"},
        {"amount": "-5"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"unexpected": "field"},
    ],
)
async def test_invalid_body_is_rejected(client: AsyncClient, override: dict) -> None:
    response = await client.post(URL, json={**PAYLOAD, **override}, headers=key())
    assert response.status_code == 422
