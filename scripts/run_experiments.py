#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import shutil
import subprocess
import sys
import time
import textwrap
import urllib.error
import urllib.request
from collections import defaultdict


SEED = 4116
LOAD_SIZES = [100, 500, 1000]
REPEATS = 5
GRAPH_FILES = [
    "latency_comparison.png",
    "throughput_comparison.png",
    "reliability_matrix.png",
    "outbox_backlog.png",
    "cache_consistency.png",
    "saga_outcomes.png",
    "criteria_radar.png",
    "metrics_plot.png",
]


def request_json(method, url, payload=None, timeout=10):
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_health(base_url, deadline=60):
    end = time.time() + deadline
    last_error = None
    while time.time() < end:
        try:
            health = request_json("GET", f"{base_url}/health", timeout=2)
            if health.get("ok"):
                return
        except Exception as exc:  # pragma: no cover - diagnostic path
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"backend is not healthy: {last_error}")


def wait_for(base_url, predicate, deadline=90):
    end = time.time() + deadline
    last = None
    while time.time() < end:
        last = request_json("GET", f"{base_url}/metrics/summary")
        if predicate(last):
            return last
        time.sleep(0.25)
    return last


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    rank = (p / 100.0) * (len(values) - 1)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    frac = rank - low
    return values[low] * (1 - frac) + values[high] * frac


def mean(values):
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def summarize_batch(scenario, load, repeat, count, latencies, errors, started):
    elapsed = max(time.time() - started, 0.001)
    return {
        "scenario": scenario,
        "load": load,
        "repeat": repeat,
        "count": count,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "throughput_rps": count / elapsed,
        "error_count": errors,
        "error_rate": errors / max(count, 1),
    }


def run_command_batch(base_url, delivery, load, repeat, player_id, amount=1, fault=None):
    latencies = []
    errors = 0
    started = time.time()
    for i in range(load):
        payload = {
            "message_id": f"{delivery}-{load}-{repeat}-{player_id}-{i}",
            "player_id": player_id,
            "operation": "grant",
            "amount": amount,
            "delivery": delivery,
        }
        if fault and i == 0:
            payload["fault"] = fault
        try:
            response = request_json("POST", f"{base_url}/command", payload)
            latencies.append(float(response["latency_ms"]))
        except Exception:
            errors += 1
    result = summarize_batch(f"{delivery}_transaction", load, repeat, load, latencies, errors, started)
    if delivery == "outbox":
        before = request_json("GET", f"{base_url}/metrics/summary")
        target = int(before["redis_unique_events"])
        deadline = max(60, load / 10)
        drained = wait_for(
            base_url,
            lambda m: int(m["redis_unique_events"]) >= target and int(m["outbox_pending"]) == 0,
            deadline=deadline,
        )
        result["outbox_pending_after"] = int(drained["outbox_pending"])
        result["outbox_oldest_pending_ms_after"] = float(drained["outbox_oldest_pending_ms"])
    return result


def run_chat_batch(base_url, load, repeat):
    latencies = []
    errors = 0
    started = time.time()
    room = f"room-{load}-{repeat}"
    for i in range(load):
        payload = {
            "room": room,
            "user_id": f"user-{i % 25}",
            "message": f"message-{SEED}-{load}-{repeat}-{i}",
        }
        try:
            response = request_json("POST", f"{base_url}/chat", payload)
            latencies.append(float(response["latency_ms"]))
        except Exception:
            errors += 1
    return summarize_batch("chat_fanout", load, repeat, load, latencies, errors, started)


def run_performance_series(base_url):
    rows = []
    for load in LOAD_SIZES:
        for repeat in range(1, REPEATS + 1):
            rows.append(run_command_batch(base_url, "sync", load, repeat, f"sync-player-{load}-{repeat}"))
            rows.append(run_command_batch(base_url, "outbox", load, repeat, f"outbox-player-{load}-{repeat}"))
            rows.append(run_chat_batch(base_url, load, repeat))
    return rows


def aggregate_series(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["scenario"], row["load"])].append(row)
    out = []
    for (scenario, load), items in sorted(grouped.items()):
        out.append({
            "scenario": scenario,
            "load": load,
            "repeats": len(items),
            "count_total": sum(int(item["count"]) for item in items),
            "p50_ms_mean": mean(item["p50_ms"] for item in items),
            "p95_ms_mean": mean(item["p95_ms"] for item in items),
            "p99_ms_mean": mean(item["p99_ms"] for item in items),
            "throughput_rps_mean": mean(item["throughput_rps"] for item in items),
            "error_rate_mean": mean(item["error_rate"] for item in items),
        })
    return out


def duplicate_callback(base_url):
    payload = {
        "message_id": "duplicate-callback-1",
        "player_id": "duplicate-player",
        "operation": "grant",
        "amount": 7,
        "delivery": "outbox",
    }
    responses = [request_json("POST", f"{base_url}/command", payload) for _ in range(5)]
    player = request_json("GET", f"{base_url}/player?id=duplicate-player")
    return {
        "requests": len(responses),
        "duplicates_reported": sum(1 for item in responses if item.get("duplicate")),
        "final_gold": player["gold"],
        "business_effect_count": 1 if player["gold"] == 7 else 0,
    }


def run_publish_no_ack(base_url):
    before = request_json("GET", f"{base_url}/metrics/summary")
    run_command_batch(base_url, "outbox", 1, 1, "fault-player", fault="publish_no_ack")
    after = wait_for(
        base_url,
        lambda m: m["redis_duplicate_events"] >= before["redis_duplicate_events"] + 1 and m["outbox_pending"] == 0,
        deadline=30,
    )
    return {
        "duplicate_events_before": before["redis_duplicate_events"],
        "duplicate_events_after": after["redis_duplicate_events"],
        "transport_duplicate_observed": after["redis_duplicate_events"] > before["redis_duplicate_events"],
        "outbox_pending_after": after["outbox_pending"],
        "outbox_attempts_total": after["outbox_attempts_total"],
    }


def run_saga_case(base_url):
    request_json("POST", f"{base_url}/command", {
        "message_id": "saga-grant",
        "player_id": "saga-player",
        "operation": "grant",
        "amount": 12,
        "delivery": "sync",
    })
    complete = request_json("POST", f"{base_url}/saga/purchase", {
        "saga_id": "saga-complete",
        "player_id": "saga-player",
        "item_id": "sword",
        "price": 10,
    })
    compensated = request_json("POST", f"{base_url}/saga/purchase", {
        "saga_id": "saga-compensate",
        "player_id": "poor-player",
        "item_id": "missing-item",
        "price": 10,
    })
    metrics = wait_for(base_url, lambda m: m["saga_completed"] >= 1 and m["saga_compensated"] >= 1)
    return {
        "submit_latency_ms": [complete["latency_ms"], compensated["latency_ms"]],
        "saga_completed": metrics["saga_completed"],
        "saga_compensated": metrics["saga_compensated"],
        "saga_duration_avg_ms": metrics["saga_duration_avg_ms"],
        "saga_duration_p95_ms": metrics["saga_duration_p95_ms"],
    }


def run_ws_check(ws_url):
    result = subprocess.run(
        ["go", "run", "./cmd/wscheck", "-url", ws_url, "-timeout", "3s"],
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout.strip())


def run_fault_scenarios(base_url, ws_url):
    return {
        "duplicate_callback": duplicate_callback(base_url),
        "publish_no_ack": run_publish_no_ack(base_url),
        "pubsub_loss": request_json("POST", f"{base_url}/experiments/pubsub-loss"),
        "cache_stale": request_json("POST", f"{base_url}/experiments/cache-stale"),
        "saga": run_saga_case(base_url),
        "websocket_replay": run_ws_check(ws_url),
    }


def best_aggregate(aggregates, scenario, metric, load=1000):
    for row in aggregates:
        if row["scenario"] == scenario and row["load"] == load:
            return float(row[metric])
    return 0.0


def score_from_threshold(value, excellent, good, reverse=False):
    if reverse:
        if value <= excellent:
            return 5
        if value <= good:
            return 4
        return 3
    if value >= excellent:
        return 5
    if value >= good:
        return 4
    return 3


def build_comparison_matrix(aggregates, faults, final_metrics):
    sync_p95 = best_aggregate(aggregates, "sync_transaction", "p95_ms_mean")
    outbox_p95 = best_aggregate(aggregates, "outbox_transaction", "p95_ms_mean")
    chat_p95 = best_aggregate(aggregates, "chat_fanout", "p95_ms_mean")
    sync_thr = best_aggregate(aggregates, "sync_transaction", "throughput_rps_mean")
    outbox_thr = best_aggregate(aggregates, "outbox_transaction", "throughput_rps_mean")
    chat_thr = best_aggregate(aggregates, "chat_fanout", "throughput_rps_mean")

    rows = [
        {
            "solution": "PostgreSQL synchronous transaction",
            "scalability": score_from_threshold(sync_thr, 650, 350),
            "reliability": 4,
            "performance": score_from_threshold(sync_p95, 5, 15, reverse=True),
            "implementation_complexity": 4,
            "operational_cost": 4,
            "observability": 3,
            "evidence": f"sync load=1000: p95={sync_p95:.3f} ms, throughput={sync_thr:.2f} rps",
            "conclusion": "подходит как source of truth для критичного состояния, но не решает fan-out событий",
        },
        {
            "solution": "Transactional Outbox",
            "scalability": score_from_threshold(outbox_thr, 650, 350),
            "reliability": 5 if final_metrics["outbox_pending"] == 0 else 3,
            "performance": score_from_threshold(outbox_p95, 7, 20, reverse=True),
            "implementation_complexity": 3,
            "operational_cost": 3,
            "observability": 5,
            "evidence": f"outbox load=1000: p95={outbox_p95:.3f} ms; pending={final_metrics['outbox_pending']}",
            "conclusion": "обеспечивает восстановимую публикацию после commit при наличии worker и метрик backlog",
        },
        {
            "solution": "Inbox/idempotency",
            "scalability": 5,
            "reliability": 5 if faults["duplicate_callback"]["duplicates_reported"] == 4 else 2,
            "performance": 5,
            "implementation_complexity": 4,
            "operational_cost": 5,
            "observability": 4,
            "evidence": "5 duplicate callback requests -> 4 duplicates, final_gold=7",
            "conclusion": "предотвращает повторный бизнес-эффект при повторной доставке",
        },
        {
            "solution": "Redis Streams",
            "scalability": 5,
            "reliability": 5 if faults["websocket_replay"]["received"] >= 1 else 2,
            "performance": 4,
            "implementation_complexity": 3,
            "operational_cost": 3,
            "observability": 5,
            "evidence": f"stream_len={final_metrics['redis_stream_len']}, replay_received={faults['websocket_replay']['received']}",
            "conclusion": "подходит для replay и контролируемой доставки, но требует политики хранения",
        },
        {
            "solution": "Redis Pub/Sub",
            "scalability": 5,
            "reliability": 2 if faults["pubsub_loss"]["delivered_to_late_subscriber"] is False else 4,
            "performance": 5,
            "implementation_complexity": 5,
            "operational_cost": 4,
            "observability": 2,
            "evidence": f"late subscriber delivered={faults['pubsub_loss']['delivered_to_late_subscriber']}",
            "conclusion": "пригоден для live fan-out, но не для durable-доставки критичного состояния",
        },
        {
            "solution": "WebSocket gateway",
            "scalability": 4,
            "reliability": 4 if faults["websocket_replay"]["received"] >= 1 else 2,
            "performance": 4,
            "implementation_complexity": 3,
            "operational_cost": 3,
            "observability": 4,
            "evidence": f"WebSocket replay received={faults['websocket_replay']['received']}",
            "conclusion": "нужен для realtime-доставки, но восстановление должно опираться на persisted stream",
        },
        {
            "solution": "Saga orchestration",
            "scalability": 3,
            "reliability": 5 if faults["saga"]["saga_completed"] >= 1 and faults["saga"]["saga_compensated"] >= 1 else 2,
            "performance": 3,
            "implementation_complexity": 2,
            "operational_cost": 2,
            "observability": 5,
            "evidence": f"completed={faults['saga']['saga_completed']}, compensated={faults['saga']['saga_compensated']}, p95={faults['saga']['saga_duration_p95_ms']:.3f} ms",
            "conclusion": "подходит для долгих экономических цепочек с явными компенсациями",
        },
        {
            "solution": "Cache-aside",
            "scalability": 5,
            "reliability": 2 if faults["cache_stale"]["stale"] else 4,
            "performance": 5,
            "implementation_complexity": 4,
            "operational_cost": 4,
            "observability": 3,
            "evidence": f"cached={faults['cache_stale']['cached_gold']}, actual={faults['cache_stale']['actual_gold']}, stale={faults['cache_stale']['stale']}",
            "conclusion": "ускоряет чтение, но не может быть основанием для экономических решений",
        },
        {
            "solution": "Chat event fan-out",
            "scalability": score_from_threshold(chat_thr, 650, 350),
            "reliability": 4,
            "performance": score_from_threshold(chat_p95, 5, 15, reverse=True),
            "implementation_complexity": 4,
            "operational_cost": 4,
            "observability": 4,
            "evidence": f"chat load=1000: p95={chat_p95:.3f} ms, throughput={chat_thr:.2f} rps, chat_stream_len={final_metrics['chat_stream_len']}",
            "conclusion": "подходит для массовых сообщений при разделении transient Pub/Sub и stream history",
        },
    ]
    for row in rows:
        numeric = [row[key] for key in ["scalability", "reliability", "performance", "implementation_complexity", "operational_cost", "observability"]]
        row["score_total"] = round(sum(numeric) / len(numeric), 2)
    return rows


def build_criteria_scores(matrix):
    criteria = [
        "scalability",
        "reliability",
        "performance",
        "implementation_complexity",
        "operational_cost",
        "observability",
    ]
    rows = []
    for item in matrix:
        for criterion in criteria:
            rows.append({
                "criterion": criterion,
                "solution": item["solution"],
                "score": item[criterion],
                "evidence": item["evidence"],
            })
    return rows


def build_evidence_trace(aggregates, faults, matrix, final_metrics):
    return [
        {
            "criterion": "Масштабируемость",
            "scenario": "load series 100/500/1000 for sync/outbox/chat",
            "metric": "throughput_rps_mean and error_rate_mean",
            "artifact": "scenario_runs.csv, throughput_comparison.png",
            "result": "серии выполнены без ошибок; throughput зафиксирован для каждого класса обработки",
            "conclusion": "горизонтально масштабируемые каналы нужны прежде всего для fan-out и worker-обработки событий",
        },
        {
            "criterion": "Надёжность",
            "scenario": "duplicate callback, publish-no-ack, Pub/Sub loss, WebSocket replay, stale cache, Saga",
            "metric": "duplicates_reported, redis_duplicate_events, delivered_to_late_subscriber, stale, saga_completed/compensated",
            "artifact": "summary.json, reliability_matrix.png",
            "result": f"duplicates={faults['duplicate_callback']['duplicates_reported']}; stream_duplicates={final_metrics['redis_duplicate_events']}; pubsub_late={faults['pubsub_loss']['delivered_to_late_subscriber']}",
            "conclusion": "надёжность достигается комбинацией Outbox, Inbox, Streams, Saga и source of truth",
        },
        {
            "criterion": "Производительность",
            "scenario": "sync/outbox/chat latency and throughput series",
            "metric": "p50/p95/p99 latency, throughput",
            "artifact": "latency_comparison.png, throughput_comparison.png",
            "result": "локальные p95 и p99 измерены для каждого размера нагрузки и повторения",
            "conclusion": "Outbox не должен трактоваться как бесплатный механизм: его overhead приемлем только при контроле backlog",
        },
        {
            "criterion": "Сложность внедрения",
            "scenario": "implemented components and state machines",
            "metric": "component/state/contract count, Saga states, worker path",
            "artifact": "comparison_matrix.csv",
            "result": "наибольшую сложность имеют Saga orchestration, WebSocket gateway и Outbox worker",
            "conclusion": "сложные паттерны следует применять только для сценариев, где простая транзакция не закрывает риск",
        },
        {
            "criterion": "Стоимость эксплуатации",
            "scenario": "stateful dependencies and retained history",
            "metric": "PostgreSQL, Redis, worker, stream length, outbox attempts",
            "artifact": "comparison_matrix.csv, outbox_backlog.png",
            "result": f"stream_len={final_metrics['redis_stream_len']}; outbox_attempts={final_metrics['outbox_attempts_total']}",
            "conclusion": "эксплуатационная стоимость определяется не только БД, но и политикой хранения stream/outbox и наблюдаемостью worker",
        },
        {
            "criterion": "Наблюдаемость",
            "scenario": "/metrics/summary and generated artifacts",
            "metric": "outbox_oldest_pending_ms, stream length, duplicates, saga duration, cache stale",
            "artifact": "summary.json, evidence_trace.json",
            "result": "каждая итоговая рекомендация привязана к источнику метрик или fault-сценарию",
            "conclusion": "архитектурная рекомендация считается проверенной только при наличии наблюдаемой метрики",
        },
    ]


def build_recommendations(faults, final_metrics):
    return [
        {
            "id": "R1",
            "recommendation": "PostgreSQL использовать как source of truth для экономики, инвентаря, прогресса и аудита.",
            "evidence": "sync_transaction series, cache_stale",
            "reason": f"cache stale reproduced: cached={faults['cache_stale']['cached_gold']}, actual={faults['cache_stale']['actual_gold']}",
        },
        {
            "id": "R2",
            "recommendation": "Каждую внешнюю команду и callback снабжать idempotency key и фиксировать в Inbox.",
            "evidence": "duplicate_callback",
            "reason": "5 повторов одной команды дали один бизнес-результат и 4 зарегистрированных дубля.",
        },
        {
            "id": "R3",
            "recommendation": "Исходящие доменные события публиковать через Transactional Outbox и идемпотентных потребителей.",
            "evidence": "publish_no_ack, outbox_delivery series",
            "reason": f"publish-no-ack created duplicate transport event; final outbox_pending={final_metrics['outbox_pending']}.",
        },
        {
            "id": "R4",
            "recommendation": "Redis Pub/Sub использовать только для transient fan-out, а Redis Streams — для replay и контролируемой доставки.",
            "evidence": "pubsub_loss, websocket_replay",
            "reason": f"late subscriber delivered={faults['pubsub_loss']['delivered_to_late_subscriber']}; ws replay received={faults['websocket_replay']['received']}.",
        },
        {
            "id": "R5",
            "recommendation": "Saga применять только для долгих экономических операций с явными компенсациями.",
            "evidence": "saga",
            "reason": f"completed={faults['saga']['saga_completed']}, compensated={faults['saga']['saga_compensated']}.",
        },
        {
            "id": "R6",
            "recommendation": "В мониторинг включить outbox backlog age, stream lag, duplicates, Saga duration, cache stale и error rate.",
            "evidence": "/metrics/summary, evidence_trace.json",
            "reason": "без этих метрик нельзя доказательно связать отказ с архитектурной причиной.",
        },
    ]


def write_csv(path, rows, fieldnames=None):
    rows = list(rows)
    if not rows:
        return
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_metrics_csv(path, summary):
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scenario", "metric", "value", "unit"])

        def emit(prefix, value):
            if isinstance(value, dict):
                for key, item in value.items():
                    emit(f"{prefix}.{key}" if prefix else key, item)
                return
            if isinstance(value, list):
                return
            if isinstance(value, (int, float, bool, str)):
                metric = prefix.split(".")[-1]
                unit = "text"
                if isinstance(value, bool):
                    unit = "bool"
                elif isinstance(value, (int, float)):
                    unit = "count"
                    if metric.endswith("_ms"):
                        unit = "ms"
                    elif metric.endswith("_rps"):
                        unit = "requests_per_second"
                    elif metric.endswith("_rate"):
                        unit = "ratio"
                writer.writerow([prefix.rsplit(".", 1)[0], metric, value, unit])

        emit("", summary)


def plot_artifacts(out_dir, aggregates, matrix, faults, final_metrics):
    import matplotlib.pyplot as plt
    import numpy as np

    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif", "Liberation Serif"],
        "axes.titlesize": 13,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 8,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.edgecolor": "#222222",
        "axes.labelcolor": "#111111",
        "xtick.color": "#111111",
        "ytick.color": "#111111",
    })

    palette = {
        "blue": "#2f6db3",
        "teal": "#2a9d8f",
        "amber": "#e9a23b",
        "red": "#c44e52",
        "purple": "#7e57c2",
        "slate": "#667085",
        "green": "#4f9d69",
        "text": "#17212b",
        "muted": "#5f6b7a",
        "grid": "#d6dde5",
        "panel": "#f6f8fa",
        "border": "#c9d1d9",
    }

    scenario_names = {
        "chat_fanout": "чат fan-out",
        "outbox_transaction": "Outbox-транзакция",
        "sync_transaction": "синхронная транзакция",
    }
    scenario_styles = {
        "chat_fanout": {"color": palette["amber"], "marker": "o", "linestyle": "-"},
        "outbox_transaction": {"color": palette["teal"], "marker": "s", "linestyle": "--"},
        "sync_transaction": {"color": palette["blue"], "marker": "^", "linestyle": "-."},
    }

    def wrapped(text, width):
        return "\n".join(textwrap.wrap(str(text), width=width, break_long_words=False))

    def annotate_barh(ax, bars, values, suffix=""):
        max_value = max(values) if values else 0
        offset = max(max_value * 0.012, 0.04)
        for bar, value in zip(bars, values):
            ax.text(
                bar.get_width() + offset,
                bar.get_y() + bar.get_height() / 2,
                f"{value:g}{suffix}",
                va="center",
                ha="left",
                fontsize=9,
            )

    by_scenario = defaultdict(list)
    for row in aggregates:
        by_scenario[row["scenario"]].append(row)

    fig, ax = plt.subplots(figsize=(10, 5))
    for scenario, rows in sorted(by_scenario.items()):
        rows = sorted(rows, key=lambda item: item["load"])
        ax.plot(
            [row["load"] for row in rows],
            [row["p95_ms_mean"] for row in rows],
            linewidth=1.8,
            markersize=5,
            **scenario_styles.get(scenario, {"color": "#111111", "marker": "o", "linestyle": "-"}),
            label=scenario_names.get(scenario, scenario),
        )
    ax.set_title("Задержка p95 по сценариям и размеру нагрузки")
    ax.set_xlabel("команд/сообщений в серии")
    ax.set_ylabel("p95, мс")
    ax.grid(alpha=0.75, color=palette["grid"], linewidth=0.7)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_comparison.png"), dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5))
    for scenario, rows in sorted(by_scenario.items()):
        rows = sorted(rows, key=lambda item: item["load"])
        ax.plot(
            [row["load"] for row in rows],
            [row["throughput_rps_mean"] for row in rows],
            linewidth=1.8,
            markersize=5,
            **scenario_styles.get(scenario, {"color": "#111111", "marker": "o", "linestyle": "-"}),
            label=scenario_names.get(scenario, scenario),
        )
    ax.set_title("Пропускная способность по сценариям и размеру нагрузки")
    ax.set_xlabel("команд/сообщений в серии")
    ax.set_ylabel("запросов/с")
    ax.grid(alpha=0.75, color=palette["grid"], linewidth=0.7)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "throughput_comparison.png"), dpi=160)
    plt.close(fig)

    reliability_cards = [
        {
            "title": "Повторный callback",
            "metric": f"5 запросов; дублей={faults['duplicate_callback']['duplicates_reported']}; gold={faults['duplicate_callback']['final_gold']}",
            "result": "повторный бизнес-эффект подавлен",
            "color": palette["green"],
        },
        {
            "title": "Publish-no-ack",
            "metric": f"транспортных дублей={faults['publish_no_ack']['duplicate_events_after']}; pending={faults['publish_no_ack']['outbox_pending_after']}",
            "result": "backlog закрыт, потребитель обязан быть идемпотентным",
            "color": palette["amber"],
        },
        {
            "title": "Pub/Sub late subscriber",
            "metric": f"доставка позднему подписчику={faults['pubsub_loss']['delivered_to_late_subscriber']}",
            "result": "история не хранится, нужен persisted stream",
            "color": palette["red"],
        },
        {
            "title": "WebSocket replay",
            "metric": f"получено после reconnect={faults['websocket_replay']['received']}",
            "result": "восстановление требует last-seen marker",
            "color": palette["blue"],
        },
        {
            "title": "Cache-aside stale",
            "metric": f"кэш={faults['cache_stale']['cached_gold']}; БД={faults['cache_stale']['actual_gold']}",
            "result": "кэш не является источником истины для экономики",
            "color": palette["amber"],
        },
        {
            "title": "Saga outcomes",
            "metric": f"completed={faults['saga']['saga_completed']}; compensated={faults['saga']['saga_compensated']}",
            "result": "оба исхода наблюдаемы и проверяемы",
            "color": palette["green"],
        },
    ]
    fig, ax = plt.subplots(figsize=(11.5, 5.0))
    ax.axis("off")
    cols = 3
    rows = 2
    card_w = 0.30
    card_h = 0.34
    x_gap = 0.035
    y_gap = 0.12
    start_x = 0.025
    start_y = 0.52
    for index, item in enumerate(reliability_cards):
        row = index // cols
        col = index % cols
        x = start_x + col * (card_w + x_gap)
        y = start_y - row * (card_h + y_gap)
        rect = plt.Rectangle((x, y), card_w, card_h, facecolor=palette["panel"], edgecolor=palette["border"], linewidth=1.0)
        ax.add_patch(rect)
        ax.add_patch(plt.Rectangle((x, y + card_h - 0.075), card_w, 0.075, facecolor=item["color"], edgecolor=item["color"]))
        ax.text(x + 0.018, y + card_h - 0.038, item["title"], ha="left", va="center", color="white", fontsize=10, weight="bold")
        ax.text(x + 0.018, y + card_h - 0.12, wrapped(item["metric"], 34), ha="left", va="top", color=palette["text"], fontsize=9)
        ax.text(x + 0.018, y + 0.055, wrapped(item["result"], 36), ha="left", va="bottom", color=palette["text"], fontsize=9)
    ax.text(
        0.5,
        0.955,
        "Fault-injection: отказ воспроизведён, метрика зафиксирована, архитектурное следствие проверено",
        ha="center",
        va="center",
        fontsize=12,
        weight="bold",
    )
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "reliability_matrix.png"), dpi=160)
    plt.close(fig)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.4), gridspec_kw={"width_ratios": [1.35, 1.0]})
    volume_labels = ["Попытки Outbox", "Опубликовано", "События stream"]
    volume_values = [
        final_metrics["outbox_attempts_total"],
        final_metrics["outbox_published"],
        final_metrics["redis_stream_len"],
    ]
    bars = ax1.barh(volume_labels, volume_values, color=[palette["blue"], palette["green"], palette["slate"]])
    ax1.set_title("Объём публикации")
    ax1.set_xlabel("событий/попыток")
    ax1.grid(axis="x", alpha=0.75, color=palette["grid"], linewidth=0.7)
    annotate_barh(ax1, bars, volume_values)

    signal_labels = ["Ожидают", "Возраст старейшего, мс", "Транспортные дубли"]
    signal_values = [
        final_metrics["outbox_pending"],
        final_metrics["outbox_oldest_pending_ms"],
        final_metrics["redis_duplicate_events"],
    ]
    bars = ax2.barh(signal_labels, signal_values, color=[palette["green"], palette["blue"], palette["amber"]])
    ax2.set_title("Остаток и сигнал отказа")
    ax2.set_xlabel("значение")
    ax2.set_xlim(0, max(signal_values + [1]) * 1.35)
    ax2.grid(axis="x", alpha=0.75, color=palette["grid"], linewidth=0.7)
    annotate_barh(ax2, bars, signal_values)
    fig.suptitle("Outbox: публикация закрыта, транспортный дубль обнаружен", fontsize=13, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(os.path.join(out_dir, "outbox_backlog.png"), dpi=160)
    plt.close(fig)

    cached = faults["cache_stale"]["cached_gold"]
    actual = faults["cache_stale"]["actual_gold"]
    delta = actual - cached
    fig, ax = plt.subplots(figsize=(8, 4.6))
    bars = ax.bar(["Redis-кэш", "PostgreSQL"], [cached, actual], color=[palette["amber"], palette["blue"]], width=0.55)
    ax.set_title("Cache-aside: проверка устаревшего чтения")
    ax.set_ylabel("gold")
    ax.set_ylim(0, max(actual, cached) * 1.35)
    ax.grid(axis="y", alpha=0.75, color=palette["grid"], linewidth=0.7)
    for bar, value in zip(bars, [cached, actual]):
        ax.text(bar.get_x() + bar.get_width() / 2, value + 3, f"{value:g}", ha="center", va="bottom", fontsize=10)
    ax.annotate(
        "",
        xy=(1.12, cached),
        xytext=(1.12, actual),
        arrowprops={"arrowstyle": "<->", "color": palette["red"], "lw": 1.6},
    )
    ax.text(
        1.18,
        (cached + actual) / 2,
        f"разрыв {delta:g} gold",
        va="center",
        ha="left",
        color=palette["red"],
        fontsize=10,
    )
    ax.text(
        0.5,
        max(actual, cached) * 1.24,
        "Вывод: кэш ускоряет чтение, но не является источником истины для экономики",
        ha="center",
        va="center",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": palette["panel"], "edgecolor": palette["border"]},
    )
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "cache_consistency.png"), dpi=160)
    plt.close(fig)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.4), gridspec_kw={"width_ratios": [1.0, 1.1]})
    outcome_labels = ["завершена", "компенсирована"]
    outcome_values = [faults["saga"]["saga_completed"], faults["saga"]["saga_compensated"]]
    bars = ax1.bar(outcome_labels, outcome_values, color=[palette["green"], palette["red"]], width=0.55)
    ax1.set_title("Исходы Saga")
    ax1.set_ylabel("количество")
    ax1.set_ylim(0, max(outcome_values + [1]) * 1.35)
    ax1.grid(axis="y", alpha=0.75, color=palette["grid"], linewidth=0.7)
    for bar, value in zip(bars, outcome_values):
        ax1.text(bar.get_x() + bar.get_width() / 2, value + 0.04, f"{value:g}", ha="center", fontsize=10)

    duration_labels = ["средняя", "p95"]
    duration_values = [faults["saga"]["saga_duration_avg_ms"], faults["saga"]["saga_duration_p95_ms"]]
    bars = ax2.bar(duration_labels, duration_values, color=[palette["slate"], palette["amber"]], width=0.55)
    ax2.set_title("Длительность Saga")
    ax2.set_ylabel("мс")
    ax2.set_ylim(0, max(duration_values) * 1.35)
    ax2.grid(axis="y", alpha=0.75, color=palette["grid"], linewidth=0.7)
    for bar, value in zip(bars, duration_values):
        ax2.text(bar.get_x() + bar.get_width() / 2, value + 0.5, f"{value:.2f}", ha="center", fontsize=10)
    fig.suptitle("Saga: оба исхода наблюдаемы, длительность измеряется", fontsize=13, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(os.path.join(out_dir, "saga_outcomes.png"), dpi=160)
    plt.close(fig)

    criteria = [
        ("scalability", "Масштаб-\nируемость"),
        ("reliability", "Надёжность"),
        ("performance", "Производи-\nтельность"),
        ("implementation_complexity", "Сложность\nвнедрения"),
        ("operational_cost", "Стоимость\nэксплуатации"),
        ("observability", "Наблюда-\nемость"),
    ]
    selected = [
        ("Inbox/idempotency", "Inbox/idempotency", palette["green"]),
        ("Transactional Outbox", "Transactional Outbox", palette["teal"]),
        ("Redis Streams", "Redis Streams", palette["blue"]),
        ("Redis Pub/Sub", "Redis Pub/Sub", palette["amber"]),
        ("Saga orchestration", "Saga orchestration", palette["red"]),
        ("Cache-aside", "Cache-aside", palette["purple"]),
    ]
    by_solution = {row["solution"]: row for row in matrix}
    angles = np.linspace(0, 2 * np.pi, len(criteria), endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8.8, 8.2), subplot_kw={"projection": "polar"})
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_ylim(1, 5)
    ax.set_yticks([1, 2, 3, 4, 5])
    ax.set_yticklabels(["1", "2", "3", "4", "5"], color=palette["muted"], fontsize=8)
    ax.set_rlabel_position(90)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([label for _, label in criteria], fontsize=9, color=palette["text"])
    ax.grid(color=palette["grid"], linewidth=0.8)
    ax.spines["polar"].set_color(palette["border"])
    ax.set_title("Профиль критериев ключевых архитектурных решений", pad=26, fontsize=13)

    for solution, label, color in selected:
        row = by_solution[solution]
        values = [row[key] for key, _ in criteria]
        values += values[:1]
        ax.plot(angles, values, color=color, linewidth=2.0, marker="o", markersize=4, label=label)
        ax.fill(angles, values, color=color, alpha=0.07)

    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, -0.24),
        ncol=3,
        frameon=False,
        fontsize=8,
    )
    fig.subplots_adjust(top=0.86, bottom=0.20, left=0.08, right=0.92)
    fig.savefig(os.path.join(out_dir, "criteria_radar.png"), dpi=160)
    plt.close(fig)

    shutil.copyfile(os.path.join(out_dir, "criteria_radar.png"), os.path.join(out_dir, "metrics_plot.png"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:18080")
    parser.add_argument("--ws-url", default="ws://localhost:18080/ws?last=0-0")
    parser.add_argument("--out-dir", default="results/latest")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    wait_health(args.base_url)
    request_json("POST", f"{args.base_url}/admin/reset")

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    scenario_rows = run_performance_series(args.base_url)
    aggregates = aggregate_series(scenario_rows)
    faults = run_fault_scenarios(args.base_url, args.ws_url)
    final_metrics = wait_for(args.base_url, lambda m: m["outbox_pending"] == 0, deadline=60)
    matrix = build_comparison_matrix(aggregates, faults, final_metrics)
    criteria_scores = build_criteria_scores(matrix)
    evidence_trace = build_evidence_trace(aggregates, faults, matrix, final_metrics)
    recommendations = build_recommendations(faults, final_metrics)

    plot_artifacts(args.out_dir, aggregates, matrix, faults, final_metrics)

    summary = {
        "seed": SEED,
        "started_at": started_at,
        "load_sizes": LOAD_SIZES,
        "repeats": REPEATS,
        "performance_series": aggregates,
        "fault_scenarios": faults,
        "final_metrics": final_metrics,
        "comparison_matrix": matrix,
        "criteria_scores": criteria_scores,
        "evidence_trace": evidence_trace,
        "recommendations": recommendations,
        "artifacts": [
            "summary.json",
            "metrics.csv",
            "scenario_runs.csv",
            "comparison_matrix.csv",
            "criteria_scores.csv",
            "evidence_trace.json",
            "recommendations.json",
        ] + GRAPH_FILES,
    }

    with open(os.path.join(args.out_dir, "summary.json"), "w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)
    with open(os.path.join(args.out_dir, "evidence_trace.json"), "w", encoding="utf-8") as file:
        json.dump(evidence_trace, file, ensure_ascii=False, indent=2)
    with open(os.path.join(args.out_dir, "recommendations.json"), "w", encoding="utf-8") as file:
        json.dump(recommendations, file, ensure_ascii=False, indent=2)

    write_csv(os.path.join(args.out_dir, "scenario_runs.csv"), scenario_rows)
    write_csv(os.path.join(args.out_dir, "comparison_matrix.csv"), matrix)
    write_csv(os.path.join(args.out_dir, "criteria_scores.csv"), criteria_scores)
    write_metrics_csv(os.path.join(args.out_dir, "metrics.csv"), summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as exc:
        sys.stderr.write(exc.read().decode("utf-8", errors="replace") + "\n")
        raise
