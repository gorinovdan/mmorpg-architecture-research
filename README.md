# MMORPG Architecture Research Stand

Исследовательский стенд проверяет архитектурные решения серверной платформы текстовой MMORPG: PostgreSQL как source of truth, Inbox/Outbox, Redis Streams/Pub/Sub, WebSocket replay, Saga orchestration и cache-aside. Репозиторий предназначен для воспроизводимого запуска экспериментов, результаты которых используются в отчете НИР.

Публичный URL: <https://github.com/gorinovdan/mmorpg-architecture-research>

## Состав стенда

| Компонент | Назначение |
| --- | --- |
| Go backend | HTTP API, WebSocket gateway, обработка команд, чат, Saga worker, административные сценарии эксперимента. |
| PostgreSQL | Источник истины для состояния игроков, Inbox, Outbox, чата и состояний Saga. |
| Redis Streams | Durable event log для replay, учета дублей и восстановления WebSocket-клиента после reconnect. |
| Redis Pub/Sub | Низколатентный transient-канал для live fan-out активным клиентам. |
| Python runner | Серии нагрузки, fault-injection, CSV/JSON-артефакты и графики. |
| GitHub Actions | Go-тесты, Docker smoke test, генерация и проверка экспериментальных артефактов. |

## Быстрый запуск

```bash
make deps
make test
make up
make migrate
make experiment
make verify-results
make down
```

`make deps` создает локальное Python-окружение `.venv` и устанавливает зависимости для построения графиков. `make experiment` использует фиксированный seed `4116`, выполняет серии нагрузки и fault-сценарии, затем сохраняет результаты в `results/latest/`.

## Экспериментальная методика

Стенд строит сравнение не на экспертной оценке, а на цепочке:

`критерий -> сценарий стенда -> метрика -> результат -> вывод -> рекомендация`.

Проверяемые критерии:

- масштабируемость: серии нагрузки `100/500/1000` для `sync_transaction`, `outbox_transaction` и `chat_fanout`, по 5 повторов;
- надежность: duplicate callback, publish-no-ack, потеря Pub/Sub для позднего подписчика, WebSocket replay, stale cache, успешная и компенсированная Saga;
- производительность: p50/p95/p99 latency, throughput и error rate;
- сложность внедрения: число реализованных компонентов, состояний, контрактов и worker-путей;
- стоимость эксплуатации: stateful-компоненты, длина stream, attempts, backlog, требования к истории;
- наблюдаемость: наличие метрик, по которым можно доказать или опровергнуть архитектурный вывод.

## Артефакты

После `make experiment` создаются:

| Файл | Содержание |
| --- | --- |
| `summary.json` | Машиночитаемая сводка запуска: параметры, серии нагрузки, fault-сценарии, матрица сравнения, evidence trace и рекомендации. |
| `metrics.csv` | Нормализованная выгрузка метрик для повторной обработки. |
| `scenario_runs.csv` | 45 строк отдельных запусков: 3 сценария x 3 размера нагрузки x 5 повторов. |
| `comparison_matrix.csv` | Итоговая экспериментальная матрица по решениям и критериям. |
| `criteria_scores.csv` | Развернутые оценки каждого решения по каждому критерию. |
| `evidence_trace.json` | Связи между критерием, сценарием, метрикой, результатом и выводом. |
| `recommendations.json` | Практические рекомендации с привязкой к экспериментальному доказательству. |

Графики:

- `latency_comparison.png`;
- `throughput_comparison.png`;
- `reliability_matrix.png`;
- `outbox_backlog.png`;
- `cache_consistency.png`;
- `saga_outcomes.png`;
- `criteria_radar.png`;
- `metrics_plot.png`.

## Наблюдаемые метрики

Endpoint `/metrics/summary` возвращает показатели, используемые в отчете и автоматической проверке:

- `inbox_count`;
- `outbox_published`;
- `outbox_attempts_total`;
- `outbox_oldest_pending_ms`;
- `outbox_pending`;
- `chat_messages`;
- `chat_stream_len`;
- `redis_stream_len`;
- `redis_duplicate_events`;
- `saga_duration_avg_ms`;
- `saga_duration_p95_ms`;
- `saga_completed`;
- `saga_compensated`.

В ответах `/chat` и `/saga/purchase` дополнительно возвращается `latency_ms`, чтобы latency измерялась на уровне прикладного сценария, а не только внешним таймером Python runner.

## Интерпретация результатов

Стенд не моделирует весь production-кластер MMORPG. Его назначение другое: воспроизвести классы отказов и проверить причинные свойства паттернов.

Основные подтверждаемые выводы:

- PostgreSQL остается source of truth для экономики, прогресса, инвентаря и аудита;
- Inbox/idempotency предотвращает повторный бизнес-эффект при повторной доставке callback;
- Transactional Outbox устраняет риск двойной записи между PostgreSQL и брокером, но требует идемпотентных потребителей;
- Redis Pub/Sub пригоден для live fan-out, но не является durable-журналом;
- Redis Streams нужен для replay и восстановления WebSocket-клиента;
- Saga оправдана только для долгих экономических цепочек с явными компенсациями;
- cache-aside не должен быть источником решений для критичной экономики из-за риска stale read;
- архитектурная рекомендация считается доказанной только при наличии наблюдаемой метрики или fault-сценария.

## Ограничения валидности

Локальные значения latency и throughput не являются универсальным benchmark. Они зависят от машины, Docker, версии runtime и фоновой нагрузки. В отчете используются не абсолютные значения как промышленная норма, а воспроизводимая структура доказательств: инварианты, fault-сценарии, наличие backlog, повторная доставка, replay, Saga outcomes и stale cache.

## Структура

```text
.
├── cmd/researchd          # backend, worker, API, WebSocket, Saga, cache-aside
├── cmd/wscheck            # проверка WebSocket replay
├── docs                   # архитектура и методика экспериментов
├── internal/expstats      # тестируемые функции статистики
├── scripts                # запуск экспериментов и проверка результатов
└── results/latest         # CSV/JSON/PNG-артефакты последнего запуска
```
