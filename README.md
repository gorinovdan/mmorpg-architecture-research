# MMORPG Architecture Research Stand

Практический стенд для проверки архитектурных решений, применимых к серверной части MMORPG: синхронная транзакционная обработка, Outbox/Inbox, Redis Pub/Sub, Redis Streams, WebSocket replay/reconnect, saga-оркестрация и cache-aside.

Репозиторий используется как воспроизводимое приложение к НИР:

```text
https://github.com/gorinovdan/mmorpg-architecture-research
```

## Состав стенда

- `cmd/researchd` — HTTP/WebSocket backend и worker.
- `cmd/wscheck` — проверка доставки WebSocket-событий после reconnect/replay.
- `internal/expstats` — проверяемые функции расчета экспериментальных метрик.
- `scripts/run_experiments.py` — нагрузочные и отказоустойчивые сценарии.
- `scripts/verify_results.py` — автоматическая проверка инвариантов результатов.
- `docker-compose.yml` — PostgreSQL, Redis, backend, worker и профиль load generator.

## Проверяемые решения

| Решение | Что проверяется |
| --- | --- |
| Синхронная транзакция | Задержка команды и сохранение состояния в PostgreSQL как source of truth. |
| Inbox | Идемпотентность callback при повторной доставке одной команды. |
| Outbox | Сохранение бизнес-события в одной транзакции с изменением состояния. |
| Redis Pub/Sub | Эфемерная доставка и потеря сообщения для позднего подписчика. |
| Redis Streams | Durable replay событий и обнаружение транспортного дубля. |
| WebSocket reconnect | Повторное чтение событий из Redis Streams после подключения клиента. |
| Saga | Завершение успешной покупки и компенсация невыполнимого сценария. |
| Cache-aside | Обнаружение устаревшего значения при изменении PostgreSQL в обход инвалидации. |

## Запуск

Требуются Docker, Docker Compose v2, Go 1.25 и Python 3.

```bash
make up
make migrate
make test
make experiment
make verify-results
make down
```

Результаты эксперимента сохраняются в `results/latest/`:

- `summary.json` — сводные метрики и результаты проверок;
- `metrics.csv` — табличная форма для отчета;
- `metrics_plot.png` или `metrics_plot.svg` — график ключевых метрик.

## Метрики

Стенд фиксирует:

- p50/p95 latency для синхронной и Outbox-обработки;
- throughput в запросах в секунду;
- количество уникальных, потерянных и дублированных событий;
- backlog Outbox и возраст старейшей записи;
- lag Redis Streams;
- число завершенных и компенсированных saga.

## Интерпретация

Стенд не моделирует всю MMORPG-платформу. Его задача — изолировать архитектурные свойства интеграционных паттернов и проверить критичные инварианты: сохранность состояния, идемпотентность, восстановление после сбоя доставки и различие между ephemeral и durable messaging.
