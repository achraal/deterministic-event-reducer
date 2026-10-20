from __future__ import annotations

import json
import subprocess
import sys
import unittest
import os
import tempfile

# Import des classes pour tester directement la logique interne
from app import Reducer, reduce_jobs

def _run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "app.py", *arguments], capture_output=True, text=True, check=False)

class CLIInfrastructureTests(unittest.TestCase):
    def testCli_whenHelpRequested_returnsUsage(self) -> None:
        result = _run_cli("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("--input", result.stdout)

    def testCli_whenInputMissing_reportsReadableError(self) -> None:
        result = _run_cli("--input", "inputs/does-not-exist.json")
        self.assertEqual(result.returncode, 2)
        self.assertIn("error:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def testCli_whenJsonMalformed_reportsReadableError(self) -> None:
        result = _run_cli("--input", "inputs/malformed-input.txt")
        self.assertEqual(result.returncode, 2)
        self.assertIn("error:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


class FeatureAcceptanceTests(unittest.TestCase):
    def testCli_withPrimaryFixture_matchesCriteria(self) -> None:
        """
        Vérifie les critères exacts du README pour le primary.json :
        j1 failed à 2 tentatives, j2 succeeded à 1, j3 cancelled à 0,
        avec 7 événements acceptés et 6 rejetés.
        """
        result = _run_cli("--input", "inputs/primary.json")
        self.assertEqual(result.returncode, 0, f"Expected success, got stderr: {result.stderr}")
        
        output = json.loads(result.stdout)
        jobs = {j["job_id"]: j for j in output["jobs"]}
        
        self.assertEqual(jobs["j1"]["state"], "failed")
        self.assertEqual(jobs["j1"]["attempts"], 2)
        
        self.assertEqual(jobs["j2"]["state"], "succeeded")
        self.assertEqual(jobs["j2"]["attempts"], 1)
        
        self.assertEqual(jobs["j3"]["state"], "cancelled")
        self.assertEqual(jobs["j3"]["attempts"], 0)
        
        self.assertEqual(output["summary"]["accepted_events"], 7)
        self.assertEqual(output["summary"]["rejected_events"], 6)

    def testCli_withPrimaryFixture_auditRowsAreExact(self) -> None:
        out = json.loads(_run_cli("--input", "inputs/primary.json").stdout)
        a = {r["row"]: r for r in out["audit"]}
        self.assertIn("cannot start before next_start_at (6)", a[4]["reason"])   # e4
        self.assertIn("terminal state succeeded", a[6]["reason"])                # e6
        self.assertEqual(a[10]["reason"], "time below watermark")               # e10
        self.assertEqual(a[10]["job_state"], "failed")
        self.assertEqual(a[12]["reason"], "unknown job_id missing")             # e12
        self.assertIn("unknown event type", a[13]["reason"])                    # e13

    def testCli_withSecondaryFixture_reusesFeature(self) -> None:
        r = _run_cli("--input", "inputs/secondary.json")
        self.assertEqual(r.returncode, 0, r.stderr)
        out = json.loads(r.stdout)
        self.assertEqual(out["jobs"], [{"job_id": "x", "state": "succeeded", "attempts": 2, "next_start_at": None}])
        self.assertEqual(out["summary"], {"accepted_events": 4, "rejected_events": 1})  # row 5 'e' rejected

    def testCli_withInvalidPolicy_exits2(self) -> None:
        bad = {"policy": {"max_attempts": True, "backoff_seconds": 1}, "jobs": [], "events": []}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(bad, f)
        try:
            r = _run_cli("--input", f.name)
        finally:
            os.unlink(f.name)
        self.assertEqual(r.returncode, 2)
        self.assertIn("error:", r.stderr)
        self.assertEqual(r.stdout, "")

    def testReduce_isDeterministic(self) -> None:
        with open("inputs/primary.json", encoding="utf-8") as f:
            payload = json.load(f)
        self.assertEqual(reduce_jobs(payload, None), reduce_jobs(payload, None))


class ValidationTests(unittest.TestCase):
    def test_policy_rejectsBadValues(self) -> None:
        for p in [{"max_attempts": True, "backoff_seconds": 1},
                  {"max_attempts": 0, "backoff_seconds": 1},
                  {"max_attempts": 1, "backoff_seconds": -1},
                  {"max_attempts": 1, "backoff_seconds": 1.5},
                  {"max_attempts": 1}]:
            with self.subTest(p=p), self.assertRaises(ValueError):
                Reducer(p, [])

    def test_jobs_rejectDuplicatesAndBadEntries(self) -> None:
        pol = {"max_attempts": 1, "backoff_seconds": 0}
        for jobs in [[{"job_id": "a"}, {"job_id": "a"}], [{"job_id": ""}], [42], "x"]:
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                Reducer(pol, jobs)

    def test_malformedEvents_areDiagnosedNotCrashed(self) -> None:
        r = Reducer({"max_attempts": 2, "backoff_seconds": 5}, [{"job_id": "A"}])
        r.process_events([
            42,                                                              # row 1
            {"event_id": "a", "at": True, "job_id": "A", "type": "start"},   # row 2
            {"event_id": "b", "at": -1, "job_id": "A", "type": "start"},     # row 3
            {"event_id": "c", "at": 1, "job_id": "A", "type": []},           # row 4
            {"at": 1, "job_id": "A", "type": "start"},                       # row 5
            {"event_id": "d", "at": 2, "job_id": "A", "type": "start"},      # row 6 valid
        ])
        out = r.generate_output()
        self.assertEqual([x["accepted"] for x in out["audit"]], [False]*5 + [True])
        self.assertEqual(out["summary"], {"accepted_events": 1, "rejected_events": 5})

    def testCli_withSecondaryFixture_succeeds(self) -> None:
        """S'assure que le second fichier de test passe sans erreur."""
        result = _run_cli("--input", "inputs/secondary.json")
        self.assertEqual(result.returncode, 0, f"Expected success, got stderr: {result.stderr}")

class DomainLogicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = {"max_attempts": 2, "backoff_seconds": 5}
        self.jobs_data = [{"job_id": "job_A"}]

    def test_boundary_earlyRetry_isRejected(self) -> None:
        """Cas limite (Boundary case) : Un start avant le next_start_at doit être rejeté."""
        reducer = Reducer(self.policy, self.jobs_data)
        events = [
            {"event_id": "1", "at": 10, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 15, "job_id": "job_A", "type": "fail"}, # fail déclenche retry_wait, next_start_at = 15+5=20
            {"event_id": "3", "at": 18, "job_id": "job_A", "type": "start"} # at=18 est inférieur à 20 -> rejet
        ]
        reducer.process_events(events)
        out = reducer.generate_output()
        self.assertEqual(out["audit"][2]["accepted"], False)
        self.assertIn("cannot start before next_start_at", out["audit"][2]["reason"])

    def test_refinement_outOfOrderTime_isRejected(self) -> None:
        """Cas de raffinement : Le watermark temporel interdit à un événement de remonter le temps."""
        reducer = Reducer(self.policy, self.jobs_data)
        events = [
            {"event_id": "1", "at": 10, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 5, "job_id": "job_A", "type": "cancel"} # at=5 alors que watermark=10
        ]
        reducer.process_events(events)
        out = reducer.generate_output()
        self.assertEqual(out["audit"][1]["accepted"], False)
        self.assertIn("time below watermark", out["audit"][1]["reason"])

    def test_boundary_terminalImmutability_isRespected(self) -> None:
        """Test de l'immuabilité : On ne peut pas modifier un job déjà annulé ou terminé."""
        reducer = Reducer(self.policy, self.jobs_data)
        events = [
            {"event_id": "1", "at": 10, "job_id": "job_A", "type": "cancel"},
            {"event_id": "2", "at": 11, "job_id": "job_A", "type": "start"} # tente de démarrer un job annulé
        ]
        reducer.process_events(events)
        out = reducer.generate_output()
        self.assertEqual(out["jobs"][0]["state"], "cancelled")
        self.assertEqual(out["audit"][1]["accepted"], False)

    def test_duplicate_eventIds_areRejected(self) -> None:
        """Les doublons d'ID d'événements doivent être identifiés et bloqués."""
        reducer = Reducer(self.policy, self.jobs_data)
        events = [
            {"event_id": "dup_1", "at": 10, "job_id": "job_A", "type": "start"},
            {"event_id": "dup_1", "at": 11, "job_id": "job_A", "type": "success"} # Même ID
        ]
        reducer.process_events(events)
        out = reducer.generate_output()
        self.assertEqual(out["audit"][1]["accepted"], False)
        self.assertIn("duplicate event_id", out["audit"][1]["reason"])

    def test_boundary_startExactlyAtNextStartAt_isAccepted(self) -> None:
        r = Reducer(self.policy, self.jobs_data)
        r.process_events([
            {"event_id": "1", "at": 10, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 15, "job_id": "job_A", "type": "fail"},
            {"event_id": "3", "at": 20, "job_id": "job_A", "type": "start"},
        ])
        out = r.generate_output()
        self.assertTrue(out["audit"][2]["accepted"])
        self.assertEqual(out["jobs"][0]["state"], "running")
        self.assertIsNone(out["jobs"][0]["next_start_at"])

    def test_watermark_advancesOnDuplicateEventId(self) -> None:
        r = Reducer(self.policy, self.jobs_data)
        r.process_events([
            {"event_id": "1", "at": 10, "job_id": "job_A", "type": "start"},
            {"event_id": "1", "at": 50, "job_id": "job_A", "type": "fail"},  # duplicate, still advances
            {"event_id": "2", "at": 20, "job_id": "job_A", "type": "fail"},
        ])
        out = r.generate_output()
        self.assertEqual(out["audit"][1]["reason"], "duplicate event_id")
        self.assertEqual(out["audit"][2]["reason"], "time below watermark")

    def test_rejectedEvent_reservesEventId(self) -> None:
        r = Reducer(self.policy, self.jobs_data)
        r.process_events([
            {"event_id": "x", "at": 1, "job_id": "nope", "type": "start"},   # rejected: unknown job
            {"event_id": "x", "at": 2, "job_id": "job_A", "type": "start"},  # ID already reserved
        ])
        self.assertEqual(r.generate_output()["audit"][1]["reason"], "duplicate event_id")

    def test_equalTimestamps_areAllowed(self) -> None:
        r = Reducer(self.policy, self.jobs_data)
        r.process_events([
            {"event_id": "1", "at": 5, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 5, "job_id": "job_A", "type": "success"},
        ])
        self.assertEqual(r.generate_output()["jobs"][0]["state"], "succeeded")

    def test_unknownJob_isRejectedWithoutLosingEarlierWork(self) -> None:
        r = Reducer(self.policy, self.jobs_data)
        r.process_events([
            {"event_id": "1", "at": 1, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 2, "job_id": "ghost", "type": "start"},
        ])
        out = r.generate_output()
        self.assertEqual(out["audit"][1]["reason"], "unknown job_id ghost")
        self.assertEqual(out["jobs"][0]["state"], "running")

    def test_failAtMaxAttempts_isTerminalFailed(self) -> None:
        r = Reducer({"max_attempts": 1, "backoff_seconds": 5}, self.jobs_data)
        r.process_events([
            {"event_id": "1", "at": 1, "job_id": "job_A", "type": "start"},
            {"event_id": "2", "at": 2, "job_id": "job_A", "type": "fail"},
        ])
        j = r.generate_output()["jobs"][0]
        self.assertEqual((j["state"], j["next_start_at"]), ("failed", None))

if __name__ == "__main__":
    unittest.main()