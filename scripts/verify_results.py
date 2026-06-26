#!/usr/bin/env python3
import csv
import json
import os
import sys


REQUIRED_ARTIFACTS = [
    "summary.json",
    "metrics.csv",
    "scenario_runs.csv",
    "comparison_matrix.csv",
    "criteria_scores.csv",
    "evidence_trace.json",
    "recommendations.json",
    "latency_comparison.png",
    "throughput_comparison.png",
    "reliability_matrix.png",
    "outbox_backlog.png",
    "cache_consistency.png",
    "saga_outcomes.png",
    "criteria_radar.png",
    "metrics_plot.png",
]


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def main(path):
    with open(path, encoding="utf-8") as file:
        data = json.load(file)

    base_dir = os.path.dirname(path)
    for artifact in REQUIRED_ARTIFACTS:
        full_path = os.path.join(base_dir, artifact)
        require(os.path.exists(full_path), f"missing artifact: {artifact}")
        require(os.path.getsize(full_path) > 0, f"empty artifact: {artifact}")

    require(data["seed"] == 4116, "unexpected seed")
    require(data["load_sizes"] == [100, 500, 1000], "unexpected load sizes")
    require(data["repeats"] == 5, "unexpected repeat count")
    require(len(data["performance_series"]) == 9, "aggregate performance matrix must contain 9 rows")
    require(len(data["comparison_matrix"]) >= 8, "comparison matrix is incomplete")
    require(len(data["evidence_trace"]) >= 6, "proof trace is incomplete")
    require(len(data["recommendations"]) >= 6, "recommendations are incomplete")

    runs = read_csv(os.path.join(base_dir, "scenario_runs.csv"))
    require(len(runs) == 45, "scenario_runs.csv must contain 45 experimental batches")
    require(all(float(row["error_rate"]) == 0 for row in runs), "load series contains failed requests")
    require(all(float(row["p95_ms"]) > 0 for row in runs), "latency was not recorded for every batch")

    faults = data["fault_scenarios"]
    require(faults["duplicate_callback"]["final_gold"] == 7, "idempotent callback check failed")
    require(faults["duplicate_callback"]["duplicates_reported"] == 4, "duplicate count mismatch")
    require(faults["publish_no_ack"]["transport_duplicate_observed"] is True, "publish-no-ack duplicate was not observed")
    require(faults["publish_no_ack"]["outbox_pending_after"] == 0, "outbox remained pending after publish-no-ack")
    require(faults["pubsub_loss"]["delivered_to_late_subscriber"] is False, "Pub/Sub loss scenario failed")
    require(faults["cache_stale"]["stale"] is True, "cache staleness scenario failed")
    require(faults["saga"]["saga_completed"] >= 1, "completed saga was not observed")
    require(faults["saga"]["saga_compensated"] >= 1, "compensated saga was not observed")
    require(faults["websocket_replay"]["received"] >= 1, "WebSocket replay check failed")

    final_metrics = data["final_metrics"]
    require(final_metrics["outbox_pending"] == 0, "outbox has pending events after experiment")
    require(final_metrics["outbox_published"] > 0, "outbox did not publish events")
    require(final_metrics["inbox_count"] > 0, "inbox metrics were not collected")
    require(final_metrics["chat_messages"] >= 8000, "chat load series did not run fully")
    require(final_metrics["chat_stream_len"] == final_metrics["chat_messages"], "chat stream length mismatch")
    require(final_metrics["redis_duplicate_events"] >= 1, "transport duplicate metric missing")
    require(final_metrics["saga_duration_p95_ms"] > 0, "saga duration was not recorded")

    criteria = read_csv(os.path.join(base_dir, "criteria_scores.csv"))
    seen_criteria = {row["criterion"] for row in criteria}
    require(seen_criteria == {
        "scalability",
        "reliability",
        "performance",
        "implementation_complexity",
        "operational_cost",
        "observability",
    }, "criteria scores do not cover all criteria")

    print("ok: experiment results, artifacts, proof trace and invariants verified")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify_results.py results/latest/summary.json")
    main(sys.argv[1])
