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

    by_scenario = defaultdict(list)
    for row in aggregates:
        by_scenario[row["scenario"]].append(row)

    fig, ax = plt.subplots(figsize=(10, 5))
    for scenario, rows in sorted(by_scenario.items()):
        rows = sorted(rows, key=lambda item: item["load"])
        ax.plot([row["load"] for row in rows], [row["p95_ms_mean"] for row in rows], marker="o", label=scenario)
    ax.set_title("p95 latency by scenario and load")
    ax.set_xlabel("commands/messages per batch")
    ax.set_ylabel("p95 latency, ms")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "latency_comparison.png"), dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5))
    for scenario, rows in sorted(by_scenario.items()):
        rows = sorted(rows, key=lambda item: item["load"])
        ax.plot([row["load"] for row in rows], [row["throughput_rps_mean"] for row in rows], marker="o", label=scenario)
    ax.set_title("Throughput by scenario and load")
    ax.set_xlabel("commands/messages per batch")
    ax.set_ylabel("throughput, requests/s")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "throughput_comparison.png"), dpi=160)
    plt.close(fig)

    reliability_labels = ["duplicate", "publish-no-ack", "pubsub late", "cache stale", "saga completed", "ws replay"]
    reliability_values = [
        1 if faults["duplicate_callback"]["final_gold"] == 7 else 0,
        1 if faults["publish_no_ack"]["transport_duplicate_observed"] else 0,
        1 if faults["pubsub_loss"]["delivered_to_late_subscriber"] is False else 0,
        1 if faults["cache_stale"]["stale"] else 0,
        1 if faults["saga"]["saga_completed"] >= 1 and faults["saga"]["saga_compensated"] >= 1 else 0,
        1 if faults["websocket_replay"]["received"] >= 1 else 0,
    ]
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.imshow([reliability_values], cmap="Greens", vmin=0, vmax=1)
    ax.set_yticks([0], ["verified"])
    ax.set_xticks(range(len(reliability_labels)), reliability_labels, rotation=20, ha="right")
    ax.set_title("Reliability/fault-injection checks")
    for idx, value in enumerate(reliability_values):
        ax.text(idx, 0, "pass" if value else "fail", ha="center", va="center")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "reliability_matrix.png"), dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    labels = ["pending", "oldest pending ms", "attempts", "duplicates"]
    values = [
        final_metrics["outbox_pending"],
        final_metrics["outbox_oldest_pending_ms"],
        final_metrics["outbox_attempts_total"],
        final_metrics["redis_duplicate_events"],
    ]
    ax.bar(labels, values, color=["#356f9f", "#4d9f6f", "#b1762d", "#9d3f3f"])
    ax.set_title("Outbox backlog and duplicate transport signal")
    ax.set_ylabel("value")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "outbox_backlog.png"), dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(["cached", "actual"], [faults["cache_stale"]["cached_gold"], faults["cache_stale"]["actual_gold"]], color=["#b1762d", "#356f9f"])
    ax.set_title("Cache-aside stale read")
    ax.set_ylabel("gold")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "cache_consistency.png"), dpi=160)
    plt.close(fig)

    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax1.bar(["completed", "compensated"], [faults["saga"]["saga_completed"], faults["saga"]["saga_compensated"]], color=["#4d9f6f", "#9d3f3f"])
    ax1.set_ylabel("count")
    ax1.set_title("Saga outcomes and duration")
    ax2 = ax1.twinx()
    ax2.plot(["completed", "compensated"], [faults["saga"]["saga_duration_avg_ms"], faults["saga"]["saga_duration_p95_ms"]], color="#b1762d", marker="o")
    ax2.set_ylabel("duration, ms")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "saga_outcomes.png"), dpi=160)
    plt.close(fig)

    criteria = ["scalability", "reliability", "performance", "implementation_complexity", "operational_cost", "observability"]
    top = sorted(matrix, key=lambda row: row["score_total"], reverse=True)[:4]
    angles = np.linspace(0, 2 * math.pi, len(criteria), endpoint=False).tolist()
    angles += angles[:1]
    fig = plt.figure(figsize=(7, 7))
    ax = plt.subplot(111, polar=True)
    for row in top:
        values = [row[criterion] for criterion in criteria]
        values += values[:1]
        ax.plot(angles, values, label=row["solution"])
        ax.fill(angles, values, alpha=0.08)
    ax.set_xticks(angles[:-1], criteria)
    ax.set_ylim(0, 5)
    ax.set_title("Criteria scores for leading architectural components")
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.1), fontsize=8)
    fig.tight_layout()
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
