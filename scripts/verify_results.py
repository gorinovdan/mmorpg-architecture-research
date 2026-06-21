#!/usr/bin/env python3
import json
import sys


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def main(path):
    with open(path, encoding="utf-8") as file:
        data = json.load(file)

    require(data["sync_transaction"]["count"] == 80, "sync batch did not run")
    require(data["outbox_delivery"]["count"] == 80, "outbox batch did not run")
    require(data["sync_transaction"]["p95_ms"] > 0, "sync latency was not recorded")
    require(data["outbox_delivery"]["p95_ms"] > 0, "outbox latency was not recorded")
    require(data["duplicate_callback"]["final_gold"] == 7, "idempotent callback check failed")
    require(data["duplicate_callback"]["duplicates_reported"] == 4, "duplicate count mismatch")
    require(data["final_metrics"]["redis_duplicate_events"] >= 1, "outbox publish-no-ack duplicate was not observed")
    require(data["pubsub_loss"]["delivered_to_late_subscriber"] is False, "Pub/Sub loss scenario failed")
    require(data["cache_stale"]["stale"] is True, "stale cache scenario failed")
    require(data["final_metrics"]["saga_completed"] >= 1, "completed saga was not observed")
    require(data["final_metrics"]["saga_compensated"] >= 1, "compensated saga was not observed")
    require(data["websocket_replay"]["received"] >= 1, "WebSocket replay check failed")
    require(data["final_metrics"]["outbox_pending"] == 0, "outbox has pending events after experiment")
    print("ok: experiment results verified")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify_results.py results/latest/summary.json")
    main(sys.argv[1])
