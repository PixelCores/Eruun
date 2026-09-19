"""Submit a fixed-rate batch of synthetic Harbor evaluation Jobs."""

import argparse
import copy
import http.client
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def submit_job(url: str, token: str, workspace_id: str, body: dict, scheduled_at: float) -> dict:
    name = body["name"]
    started_at = time.time()
    record = {"name": name, "scheduledAt": scheduled_at, "startedAt": started_at,
              "httpStatus": None, "businessCode": None, "taskId": None, "accepted": False}
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}",
                 "X-Eruun-Workspace-ID": workspace_id,
                 "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            record["httpStatus"] = response.status
            envelope = json.load(response)
    except urllib.error.HTTPError as error:
        record["httpStatus"] = error.code
        try:
            envelope = json.load(error)
        except (ValueError, OSError):
            envelope = {}
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as error:
        envelope = {}
        record["errorType"] = type(error).__name__
    except ValueError:
        envelope = {}
        record["errorType"] = "InvalidJSON"

    if isinstance(envelope, dict):
        record["businessCode"] = envelope.get("code")
        data = envelope.get("data")
        if isinstance(data, dict):
            record["taskId"] = data.get("taskId")
    record["accepted"] = (
        record["httpStatus"] == 202
        and record["businessCode"] == 0
        and isinstance(record["taskId"], str)
        and bool(record["taskId"])
    )
    record["completedAt"] = time.time()
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True, help="Eruun API base URL")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--output", required=True, type=Path, help="new JSONL file")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=20, help="scheduled requests per second")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--template", type=Path, default=Path(__file__).with_name("evaluation.json"))
    args = parser.parse_args()
    if args.count < 1 or args.rate <= 0 or args.workers < 1:
        parser.error("count, rate and workers must be positive")
    token = os.environ.get("ERUUN_TOKEN")
    if not token:
        parser.error("set ERUUN_TOKEN in the environment")
    template = json.loads(args.template.read_text())
    if template.get("type") != "job" or template.get("traits", {}).get("evaluation", {}).get("agent") != "oracle":
        parser.error("template must submit type=job with traits.evaluation.agent=oracle")

    url = args.api_url.rstrip("/") + "/api/v1/jobs"
    run_id = secrets.token_hex(6)
    accepted = 0
    lock = threading.Lock()
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        def worker(body: dict, scheduled_at: float) -> None:
            nonlocal accepted
            record = submit_job(url, token, args.workspace_id, body, scheduled_at)
            with lock:
                output.write(json.dumps(record, sort_keys=True) + "\n")
                output.flush()
                accepted += int(record["accepted"])

        epoch = time.time()
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = []
            for index in range(args.count):
                scheduled = started + index / args.rate
                time.sleep(max(0, scheduled - time.monotonic()))
                body = copy.deepcopy(template)
                body["name"] = f"harbor-load-{run_id}-{index:05d}"
                body["traits"]["evaluation"]["taskPackageId"] = args.dataset_id
                futures.append(pool.submit(worker, body, epoch + index / args.rate))
            for future in futures:
                future.result()

    print(f"runId={run_id} accepted={accepted}/{args.count} output={args.output}")
    if accepted != args.count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
