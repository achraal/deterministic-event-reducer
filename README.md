# Scan Job Retry State Machine

A deterministic event reducer for a fictional scan scheduler. It replays job events (`start`, `success`, `fail`, `cancel`) against a retry policy and a virtual clock. It never launches scans, contacts services or sleeps. Requires Python 3.10+ (tested on 3.13.1), standard library only.

## 1. Run guide

From the `candidate/` directory:

```sh
python3 app.py --input inputs/primary.json     # expect exit 0, JSON on stdout
python3 app.py --input inputs/secondary.json   # expect exit 0, JSON on stdout
python3 app.py --input inputs/malformed-input.txt   # expect exit 2, "error: ..." on stderr, no stdout
python3 -m unittest discover -s tests -p '*_test.py' -v   # Ran 22 tests in ... OK
```

(On Windows without a `python3` alias, use `python`.)

Exit codes: `0` success (even if some events were rejected), `2` file or top-level/definition error (unreadable file, malformed JSON, invalid policy, invalid or duplicate jobs, `events` not an array).

### Representative output: `inputs/primary.json`

Final jobs: `j1` failed (2 attempts), `j2` succeeded (1), `j3` cancelled (0). Summary: 7 accepted, 6 rejected.

| Row / event | Result | Why |
|---|---|---|
| 1 / e1, 2 / e2 | accepted | j1 and j2 start at t=0 |
| 3 / e3 | accepted, `retry_wait` | j1 fails at t=1 with 1 of 2 attempts used, so `next_start_at = 1 + 5 = 6` |
| 4 / e4 | rejected: `cannot start before next_start_at (6)` | early retry: t=2 < 6 |
| 5 / e5 | accepted | j2 succeeds |
| 6 / e6 | rejected: already in terminal state succeeded | terminal immutability (cancel after success) |
| 7 / e7 | accepted | **exact retry boundary**: start at t=6 equals `next_start_at` |
| 8 / e8 | accepted, `failed` | 2nd attempt failed, `attempts == max_attempts` |
| 9 / e9 | rejected: already in terminal state failed | success after failure |
| 10 / e10 | rejected: `time below watermark` | t=4 < watermark 8 |
| 11 / e11 | accepted | j3 cancelled from `queued` (0 attempts) |
| 12 / e12 | rejected: `unknown job_id missing` | unknown job |
| 13 / e13 | rejected: `unknown event type 'dance'` | invalid type |

### Representative output: `inputs/secondary.json`

Job `x` (max 3 attempts, backoff 2) ends `succeeded` with 2 attempts. Rows 1-4 (`a`, `b`, `c`, `d`) are accepted: `b` fails at t=1 so `next_start_at = 3`, and `c` starts at exactly t=3. Row 5 (`e`, a `fail` after success) is rejected as terminal. Summary: 4 accepted, 1 rejected. This shows the reducer is reusable with a different policy and job set.

### Malformed input

`inputs/malformed-input.txt` (truncated JSON despite the `.txt` extension) gives exit 2 and `error: Expecting ',' delimiter: line 2 column 1 (char 34)` on stderr, with no traceback and nothing on stdout.

## 2. What is tested

`tests/app_test.py` (the original help/file-error smoke checks are kept; the placeholder assertion was replaced by feature assertions):

- **Primary acceptance**: final states, attempts and 7/6 summary, plus exact audit reasons for rows 4, 6, 10, 12 and 13.
- **Secondary**: exact job output and 4/1 summary (not only the exit code).
- **Retry boundary**: `start` exactly at `next_start_at` is accepted (fail at 15 + backoff 5, start at 20); one second earlier is rejected.
- **Terminal immutability**, **equal timestamps allowed**, **fail at max attempts becomes `failed` with `next_start_at = null`**.
- **Watermark**: out-of-order rejected; a duplicate-ID event still advances it.
- **Event IDs**: duplicates rejected; an ID is reserved even when its first event was rejected.
- **Unknown job**: rejected without losing earlier work.
- **Validation (exit 2)**: boolean/zero/negative/float/missing policy values, duplicate/empty/non-object jobs, `jobs` not a list.
- **Malformed events**: non-object event, `at: true`, negative `at`, unhashable `type`, missing `event_id` are diagnosed in the audit, never a crash.
- **Determinism**: the same input yields identical output.

## 3. Decision and work log

**Priorities.** I implemented the core contract first and kept two classes: `Job` (state transitions) and `Reducer` (stream processing, watermark, event-ID reservation, audit). Deferred: streaming/JSONL output, schema checks for extra fields, any logging framework. The goal was a clear, tested core.

**Obstacle.** The watermark rule: it must advance for every event with a valid integer time, even one rejected for another reason, but never go backwards. I solved it by computing "ID reserved" and "watermark advanced" *before* choosing a rejection reason, then picking one reason by fixed precedence (see Assumptions).

**Validation / refinement.**
1. My first version rejected duplicate or invalid `event_id`s with an early `continue` *before* the time check, so such events never advanced the watermark. I found this by re-reading and found in a review of my code against the contract. I added `test_watermark_advancesOnDuplicateEventId` (I fixed this and added test_watermark_advancesOnDuplicateEventId to lock the behaviour in.), and moved the ID reservation and watermark update ahead of all rejections. Primary is unchanged at 7/6.
2. Robustness: events such as `42` or `"type": []` crashed with a traceback. They are now diagnosed per row. While adding the stricter jobs validation, I caught a mistake of my own: the new loop had been pasted inside the old `for` loop, so `"jobs": null` crashed with a `TypeError`. I added `test_jobs_nonList_exits2ViaCli` and fixed it, so these now exit 2 with a readable message.
3. The audit now includes `job_state` whenever the job is known, including rejected rows (for example row 10 / e10 shows `failed`).

**Assumptions** (the contract left these open):
- Rejection precedence: event_id invalid, duplicate event_id, invalid time, below watermark, invalid/unknown job_id, unknown event type, then the state rule. Only the first applicable reason is reported. For example e10 (row 10) is a `start` on a failed job, but it is reported as `time below watermark`.
- An event with an invalid or missing `event_id` gets `event_id: null` in the audit; a non-object event gets `event_id: null` and does not touch the watermark.
- Event IDs and the watermark are only touched by object events with a valid string ID or valid integer time respectively. Booleans are never integers.
- `job_state` is omitted from an audit row only when the job is unknown or missing.

**Known limits.** One reason per rejected event. Extra unknown fields in events or jobs are ignored. The full output is built in memory. Reasons are human-readable strings, not stable machine codes.

**Next step.** Stable reason codes (for example `TIME_BELOW_WATERMARK`) next to the text, and optional JSONL output for very large streams.

## 4. AI-use note

I used an AI assistant to scaffold the `Job`/`Reducer` classes, to draft unit tests, and to review my code against the contract. **Corrected output:** the AI-drafted `process_events` rejected duplicate event IDs with an early `continue`, which skipped the watermark update that the contract requires. I checked it against the contract line ("advance the watermark even if another rule rejects the event"), wrote a test (`test_watermark_advancesOnDuplicateEventId`: duplicate `at: 50` followed by `at: 20` must give `time below watermark`) and changed the code until it passed. **Checked:** I also ran the unit tests and both fixtures after each change, and compared primary output to the required 7 accepted / 6 rejected.

## 5. Walkthrough (text version)

**Core workflow.** `main()` reads the JSON file; `reduce_jobs` checks top-level keys; `Reducer` validates the policy and jobs (exit 2 on error), then processes events in supplied order. For each event it reserves the event ID, updates the watermark if the time is valid, picks the first applicable rejection reason or lets `Job.process_event` apply the transition, and appends an audit row. Output is `jobs` (sorted by `job_id`), `audit` and `summary`.

**Edge case.** Primary row 10 (e10, `start` for j1 at t=4): j1 is already `failed`, but the watermark is 8, so the event is rejected as `time below watermark`, and j1 stays untouched. Row 7 (e7) shows the opposite boundary: `start` at t=6 is accepted because it equals `next_start_at`.

**Execution evidence.** unit test run `Ran 22 tests in 0.763s OK`; primary summary `7 accepted / 6 rejected`; secondary job `x` `succeeded` with 2 attempts; malformed input exit code `2` (`error: Expecting ',' delimiter: line 2 column 1 (char 34)`).
