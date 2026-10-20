"""Exercise scaffold: CLI I/O works; the assessed feature is intentionally absent."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import typing


# --- DOMAIN CLASSES ---

class Job:
    TERMINAL_STATES = {"succeeded", "failed", "cancelled"}
    VALID_STATES = {"queued", "running", "retry_wait"} | TERMINAL_STATES

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.state = "queued"
        self.attempts = 0
        self.next_start_at: int | None = 0

    def is_terminal(self) -> bool:
        return self.state in self.TERMINAL_STATES

    def process_event(self, event_type: str, at: int, max_attempts: int, backoff_seconds: int) -> typing.Tuple[bool, str]:
        """Processes an event and returns (success_boolean, reason_string)."""
        if self.is_terminal():
            return False, f"job {self.job_id} is already in terminal state {self.state}"

        if event_type == "start":
            if self.state not in {"queued", "retry_wait"}:
                return False, f"cannot start from {self.state}"
            if self.next_start_at is not None and at < self.next_start_at:
                return False, f"cannot start before next_start_at ({self.next_start_at})"
            
            self.state = "running"
            self.attempts += 1
            self.next_start_at = None
            return True, "started"

        elif event_type == "success":
            if self.state != "running":
                return False, f"cannot succeed from {self.state}"
            self.state = "succeeded"
            self.next_start_at = None
            return True, "succeeded"

        elif event_type == "fail":
            if self.state != "running":
                return False, f"cannot fail from {self.state}"
            
            if self.attempts < max_attempts:
                self.state = "retry_wait"
                self.next_start_at = at + backoff_seconds
                return True, "retrying"
            else:
                self.state = "failed"
                self.next_start_at = None
                return True, "failed (max attempts reached)"

        elif event_type == "cancel":
            self.state = "cancelled"
            self.next_start_at = None
            return True, "cancelled"

        return False, f"unknown event type {event_type}"

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "job_id": self.job_id,
            "state": self.state,
            "attempts": self.attempts,
            "next_start_at": self.next_start_at
        }


class Reducer:
    VALID_EVENT_TYPES = {"start", "success", "fail", "cancel"}

    def __init__(self, policy: dict[str, typing.Any], jobs_data: list[dict[str, typing.Any]]):
        self._validate_policy(policy)
        self.max_attempts = policy["max_attempts"]
        self.backoff_seconds = policy["backoff_seconds"]
        
        self.jobs: dict[str, Job] = {}
        for j in jobs_data:
            if not isinstance(j, dict):
                raise ValueError("each job must be an object")
            job_id = j.get("job_id")
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("Invalid job_id in jobs definition")
            if job_id in self.jobs:
                raise ValueError(f"duplicate job_id {job_id}")
            self.jobs[job_id] = Job(job_id)            

        self.watermark = 0
        self.seen_event_ids: set[str] = set()
        
        self.audit: list[dict[str, typing.Any]] = []
        self.accepted_events = 0
        self.rejected_events = 0

    def _validate_policy(self, policy: dict[str, typing.Any]) -> None:
        if not isinstance(policy, dict):
            raise ValueError("policy must be an object")
        
        ma = policy.get("max_attempts")
        bs = policy.get("backoff_seconds")
        
        # Booleans are subclasses of int in Python, must explicitly reject them
        if type(ma) is bool or type(bs) is bool:
            raise ValueError("policy values cannot be booleans")
        if not isinstance(ma, int) or not isinstance(bs, int):
            raise ValueError("policy values must be integers")
        if ma <= 0:
            raise ValueError("max_attempts must be positive")
        if bs < 0:
            raise ValueError("backoff_seconds must be nonnegative")

    def log_audit(self, row: int, event_id: str | None, accepted: bool, reason: str, job_state: str | None = None) -> None:
        entry = {"row": row, "event_id": event_id, "accepted": accepted, "reason": reason}
        if job_state is not None:
            entry["job_state"] = job_state
        self.audit.append(entry)
        if accepted:
            self.accepted_events += 1
        else:
            self.rejected_events += 1

    def process_events(self, events: list) -> None:
        for row, event in enumerate(events, start=1):
            if not isinstance(event, dict):
                self.log_audit(row, None, False, "event must be an object")
                continue

            event_id = event.get("event_id")
            job_id = event.get("job_id")
            at = event.get("at")
            ev_type = event.get("type")

            # Reserve the event ID on first occurrence, even if the event is rejected later
            id_ok = isinstance(event_id, str) and event_id != ""
            duplicate = id_ok and event_id in self.seen_event_ids
            if id_ok:
                self.seen_event_ids.add(event_id)

            # Watermark applies to every event with a valid time, whatever else is wrong
            time_ok = type(at) is int and at >= 0
            below = time_ok and at < self.watermark
            if time_ok and not below:
                self.watermark = at

            job = self.jobs.get(job_id) if isinstance(job_id, str) else None
            state = job.state if job else None
            audit_id = event_id if id_ok else None

            def reject(reason: str) -> None:
                self.log_audit(row, audit_id, False, reason, state)

            if not id_ok:
                reject("missing or invalid event_id")
            elif duplicate:
                reject("duplicate event_id")
            elif not time_ok:
                reject("invalid time format")
            elif below:
                reject("time below watermark")
            elif not isinstance(job_id, str) or not job_id:
                reject("missing or invalid job_id")
            elif job is None:
                reject(f"unknown job_id {job_id}")
            elif not isinstance(ev_type, str) or ev_type not in self.VALID_EVENT_TYPES:
                reject(f"unknown event type {ev_type!r}")
            else:
                accepted, reason = job.process_event(ev_type, at, self.max_attempts, self.backoff_seconds)
                self.log_audit(row, event_id, accepted, reason, job.state)

    def generate_output(self) -> dict[str, typing.Any]:
        sorted_jobs = [self.jobs[jid].to_dict() for jid in sorted(self.jobs.keys())]
        return {
            "jobs": sorted_jobs,
            "audit": self.audit,
            "summary": {
                "accepted_events": self.accepted_events,
                "rejected_events": self.rejected_events
            }
        }

# --- MAIN CLI INTEGRATION ---

def reduce_jobs(payload: dict[str, typing.Any], arguments: argparse.Namespace) -> dict[str, typing.Any]:
    if "policy" not in payload or "jobs" not in payload or "events" not in payload:
        raise ValueError("Missing required top-level keys in payload")
    if not isinstance(payload["events"], list):
        raise ValueError("events must be an array")

    reducer = Reducer(payload["policy"], payload["jobs"])
    reducer.process_events(payload["events"])
    
    return reducer.generate_output()

def main() -> int:
    parser = argparse.ArgumentParser(description="Replay job events without running scans or sleeping.")
    parser.add_argument("--input", default="inputs/primary.json", help="UTF-8 fixture JSON")
    arguments = parser.parse_args()
    try:
        payload = json.loads(pathlib.Path(arguments.input).read_text(encoding="utf-8"))
        if isinstance(payload, dict) is False:
            raise ValueError("input must be a JSON object")
        result = reduce_jobs(payload, arguments)
        print(json.dumps(result, indent=2, sort_keys=True))
    except NotImplementedError as error:
        print(str(error), file=sys.stderr)
        return 3
    except (OSError, UnicodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
