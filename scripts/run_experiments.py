#!/usr/bin/env python3
import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request


def request_json(method, url, payload=None, timeout=5):
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
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"backend is not healthy: {last_error}")


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    rank = (p / 100.0) * (len(values) - 1)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    frac = rank - low
    return values[low] * (1 - frac) + values[high] * frac


def run_command_batch(base_url, delivery, count, player_id, amount=1, fault=None):
    latencies = []
    started = time.time()
    for i in range(count):
        payload = {
            "message_id": f"{delivery}-{player_id}-{i}",
            "player_id": player_id,
            "operation": "grant",
            "amount": amount,
            "delivery": delivery,
        }
        if fault and i == 0:
            payload["fault"] = fault
        response = request_json("POST", f"{base_url}/command", payload)
        latencies.append(float(response["latency_ms"]))
    elapsed = max(time.time() - started, 0.001)
    return {
        "count": count,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "throughput_rps": count / elapsed,
    }


def wait_for(base_url, predicate, deadline=20):
    end = time.time() + deadline
    last = None
    while time.time() < end:
        last = request_json("GET", f"{base_url}/metrics/summary")
        if predicate(last):
            return last
        time.sleep(0.25)
    return last


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
    }


def run_saga_case(base_url):
    request_json("POST", f"{base_url}/command", {
        "message_id": "saga-grant",
        "player_id": "saga-player",
        "operation": "grant",
        "amount": 12,
        "delivery": "sync",
    })
    request_json("POST", f"{base_url}/saga/purchase", {
        "saga_id": "saga-complete",
        "player_id": "saga-player",
        "item_id": "sword",
        "price": 10,
    })
    request_json("POST", f"{base_url}/saga/purchase", {
        "saga_id": "saga-compensate",
        "player_id": "poor-player",
        "item_id": "missing-item",
        "price": 10,
    })
    return wait_for(base_url, lambda m: m["saga_completed"] >= 1 and m["saga_compensated"] >= 1)


def run_ws_check(ws_url):
    result = subprocess.run(
        ["go", "run", "./cmd/wscheck", "-url", ws_url, "-timeout", "2s"],
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout.strip())


def write_plot(out_dir, summary):
    labels = [
        "sync p95",
        "outbox p95",
        "duplicates",
        "stream dup",
        "ws replay",
        "saga ok",
    ]
    values = [
        summary["sync_transaction"]["p95_ms"],
        summary["outbox_delivery"]["p95_ms"],
        summary["duplicate_callback"]["duplicates_reported"],
        summary["final_metrics"]["redis_duplicate_events"],
        summary["websocket_replay"]["received"],
        summary["final_metrics"]["saga_completed"],
    ]
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(labels, values, color=["#356f9f", "#4d9f6f", "#b1762d", "#9d3f3f", "#7057a3", "#4c7f3f"])
        ax.set_ylabel("value")
        ax.set_title("MMORPG architecture research stand metrics")
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "metrics_plot.png"), dpi=160)
        plt.close(fig)
        return "metrics_plot.png"
    except Exception:
        svg_path = os.path.join(out_dir, "metrics_plot.svg")
        max_value = max(values) or 1
        bars = []
        for idx, (label, value) in enumerate(zip(labels, values)):
            height = int((value / max_value) * 220)
            x = 30 + idx * 95
            y = 260 - height
            bars.append(f'<rect x="{x}" y="{y}" width="48" height="{height}" fill="#356f9f"/>')
            bars.append(f'<text x="{x}" y="285" font-size="10" transform="rotate(25 {x},285)">{label}</text>')
        svg = '<svg xmlns="http://www.w3.org/2000/svg" width="680" height="340">' + "".join(bars) + "</svg>"
        with open(svg_path, "w", encoding="utf-8") as file:
            file.write(svg)
        return "metrics_plot.svg"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:18080")
    parser.add_argument("--ws-url", default="ws://localhost:18080/ws?last=0-0")
    parser.add_argument("--out-dir", default="results/latest")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    wait_health(args.base_url)
    request_json("POST", f"{args.base_url}/admin/reset")

    summary = {
        "seed": 4116,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    summary["sync_transaction"] = run_command_batch(args.base_url, "sync", 80, "sync-player")
    summary["outbox_delivery"] = run_command_batch(args.base_url, "outbox", 80, "outbox-player")
    wait_for(args.base_url, lambda m: m["redis_unique_events"] >= 80)
    summary["duplicate_callback"] = duplicate_callback(args.base_url)
    run_command_batch(args.base_url, "outbox", 1, "fault-player", fault="publish_no_ack")
    wait_for(args.base_url, lambda m: m["redis_duplicate_events"] >= 1 and m["outbox_pending"] == 0)
    summary["pubsub_loss"] = request_json("POST", f"{args.base_url}/experiments/pubsub-loss")
    summary["cache_stale"] = request_json("POST", f"{args.base_url}/experiments/cache-stale")
    summary["saga"] = run_saga_case(args.base_url)
    wait_for(args.base_url, lambda m: m["redis_unique_events"] >= 84)
    summary["websocket_replay"] = run_ws_check(args.ws_url)
    summary["final_metrics"] = request_json("GET", f"{args.base_url}/metrics/summary")
    summary["plot"] = write_plot(args.out_dir, summary)

    csv_path = os.path.join(args.out_dir, "metrics.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scenario", "metric", "value", "unit"])
        for scenario, data in summary.items():
            if not isinstance(data, dict):
                continue
            for metric, value in data.items():
                if isinstance(value, (int, float, bool)):
                    unit = "bool" if isinstance(value, bool) else "count"
                    if metric.endswith("_ms"):
                        unit = "ms"
                    elif metric.endswith("_rps"):
                        unit = "requests_per_second"
                    writer.writerow([scenario, metric, value, unit])

    json_path = os.path.join(args.out_dir, "summary.json")
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as exc:
        sys.stderr.write(exc.read().decode("utf-8", errors="replace") + "\n")
        raise
