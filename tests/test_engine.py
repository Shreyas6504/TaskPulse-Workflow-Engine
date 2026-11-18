"""
Unit Tests for TaskPulse Engine
"""

import sys
import os
import time
import unittest

# Ensure engine module can be imported from root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from engine import TaskEngine

class TestTaskEngine(unittest.TestCase):
    def setUp(self):
        self.engine = TaskEngine(num_workers=2)

    def test_successful_job_execution(self):
        self.engine.register_handler("add", lambda p: p["a"] + p["b"])
        job_id = self.engine.submit_job("Add Numbers", "add", {"a": 10, "b": 25}, priority=1)
        
        time.sleep(0.3)
        jobs = {j["id"]: j for j in self.engine.get_all_jobs()}
        
        self.assertIn(job_id, jobs)
        self.assertEqual(jobs[job_id]["status"], "SUCCESS")
        self.assertEqual(jobs[job_id]["result"], 35)

    def test_priority_ordering(self):
        execution_order = []
        def handler(p):
            execution_order.append(p["name"])
            return True

        self.engine.register_handler("track", handler)
        self.engine.submit_job("Low", "track", {"name": "Low"}, priority=4)
        self.engine.submit_job("Critical", "track", {"name": "Critical"}, priority=1)
        
        time.sleep(0.4)
        self.assertIn("Critical", execution_order)

    def test_dead_letter_queue_on_persistent_failure(self):
        def failing_handler(p):
            raise ValueError("Deliberate failure")

        self.engine.register_handler("fail", failing_handler)
        job_id = self.engine.submit_job("Failing Job", "fail", {}, priority=1, max_retries=1)

        time.sleep(2.0)
        jobs = {j["id"]: j for j in self.engine.get_all_jobs()}
        self.assertEqual(jobs[job_id]["status"], "DEAD_LETTER")
        self.assertGreaterEqual(self.engine.get_stats()["dead_letter_count"], 1)

if __name__ == "__main__":
    unittest.main()
