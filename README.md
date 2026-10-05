# Payments Service

Асинхронный процессинг платежей: FastAPI, PostgreSQL, RabbitMQ (FastStream), outbox, retry, DLQ.

## Запуск

```bash
cp .env.example .env          # при желании поменяйте API_KEY
docker compose up -d --build
```

- API и Swagger: http://localhost:8000/docs
- RabbitMQ UI: http://localhost:15672 (guest / guest)

Миграции применяются автоматически (сервис `migrate`). Остановить и удалить данные:
`docker compose down -v`.

## Примеры

Создать платеж (`X-API-Key` — значение `API_KEY` из `.env`):

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.00", "currency": "RUB", "description": "Order #42",
       "metadata": {"order_id": 42}, "webhook_url": "https://webhook.site/<your-id>"}'
```

Получить платеж:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
```

Повтор с тем же `Idempotency-Key` и тем же телом вернет тот же платеж, с другим телом — `409`.

Retry и DLQ: укажите недоступный `webhook_url` (например, `http://localhost:1/hook`) и смотрите
`docker compose logs -f consumer` — после 3 попыток сообщение окажется в очереди `payments.new.dlq`.

## Тесты

```bash
uv sync
uv run pytest        # нужен Docker: Postgres поднимается через testcontainers
```
