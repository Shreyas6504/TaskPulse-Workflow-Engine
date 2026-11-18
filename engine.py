"""
TaskPulse — Asynchronous Task Orchestration & Distributed Worker Queue Engine
Author: Shreyas (https://github.com/Shreyas6504)
Description: A high-throughput, concurrent background worker queue in Python with priority scheduling,
exponential backoff retry policies, and dead-letter queue (DLQ) resilience.
"""

import time
import uuid
import heapq
import random
import logging
import threading
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")

@dataclass(order=True)
class Job:
    # Priority sorting: Lower number = higher priority (1: Critical, 2: High, 3: Medium, 4: Low)
    priority: int
    created_at: float = field(compare=True)
    id: str = field(compare=False)
    name: str = field(compare=False)
    handler: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    status: str = field(default="PENDING", compare=False) # PENDING, RUNNING, SUCCESS, RETRYING, DEAD_LETTER
    max_retries: int = field(default=3, compare=False)
    current_retry: int = field(default=0, compare=False)
    last_error: Optional[str] = field(default=None, compare=False)
    result: Optional[Any] = field(default=None, compare=False)
    execution_time_ms: float = field(default=0.0, compare=False)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "handler": self.handler,
            "priority": self.priority,
            "status": self.status,
            "current_retry": self.current_retry,
            "max_retries": self.max_retries,
            "last_error": self.last_error,
            "result": self.result,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "created_at": datetime.fromtimestamp(self.created_at).strftime("%Y-%m-%d %H:%M:%S")
        }

class TaskEngine:
    """Core Task Queue Engine managing worker threads and fault-tolerant job dispatch."""
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self._heap: List[Job] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._jobs: Dict[str, Job] = {}
        self._handlers: Dict[str, Callable] = {}
        self._dead_letter_queue: List[Job] = []
        self._is_running = True
        self._workers: List[threading.Thread] = []

        # Metrics
        self.total_processed = 0
        self.total_success = 0
        self.total_failed = 0
        self.total_retried = 0

        # Boot worker thread pool
        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop, name=f"Worker-{i+1}", daemon=True)
            t.start()
            self._workers.append(t)

    def register_handler(self, name: str, func: Callable):
        """Register a callable handler function."""
        self._handlers[name] = func

    def submit_job(self, name: str, handler: str, payload: Dict[str, Any] = None, priority: int = 3, max_retries: int = 3) -> str:
        """Enqueue a new job with priority ordering."""
        job_id = str(uuid.uuid4())[:8]
        job = Job(
            priority=priority,
            created_at=time.time(),
            id=job_id,
            name=name,
            handler=handler,
            payload=payload or {},
            max_retries=max_retries
        )

        with self._lock:
            self._jobs[job_id] = job
            heapq.heappush(self._heap, job)
            self._not_empty.notify()

        logging.info(f"Enqueued Job [{job_id}] '{name}' with Priority {priority}")
        return job_id

    def _worker_loop(self):
        """Worker thread lifecycle."""
        while self._is_running:
            job: Optional[Job] = None
            with self._not_empty:
                while not self._heap and self._is_running:
                    self._not_empty.wait()
                if not self._is_running:
                    break
                job = heapq.heappop(self._heap)
                job.status = "RUNNING"

            if not job:
                continue

            self._execute_job(job)

    def _execute_job(self, job: Job):
        """Executes a single job with timing and retry backoff mechanics."""
        start_time = time.time()
        handler_fn = self._handlers.get(job.handler)

        if not handler_fn:
            job.status = "DEAD_LETTER"
            job.last_error = f"Handler '{job.handler}' not registered"
            self._dead_letter_queue.append(job)
            with self._lock:
                self.total_failed += 1
            return

        try:
            res = handler_fn(job.payload)
            job.result = res
            job.status = "SUCCESS"
            job.execution_time_ms = (time.time() - start_time) * 1000
            with self._lock:
                self.total_processed += 1
                self.total_success += 1
            logging.info(f"Job [{job.id}] completed successfully in {job.execution_time_ms:.1f}ms")

        except Exception as e:
            job.last_error = str(e)
            job.execution_time_ms = (time.time() - start_time) * 1000

            if job.current_retry < job.max_retries:
                job.current_retry += 1
                job.status = "RETRYING"
                # Exponential backoff formula with random jitter: (2^retry) * 0.5s + jitter
                backoff = (2 ** job.current_retry) * 0.4 + random.uniform(0.1, 0.3)
                logging.warning(f"Job [{job.id}] failed: {e}. Scheduling retry {job.current_retry}/{job.max_retries} in {backoff:.2f}s")
                
                with self._lock:
                    self.total_retried += 1

                # Schedule retry in separate timer
                timer = threading.Timer(backoff, self._re_enqueue, args=[job])
                timer.daemon = True
                timer.start()
            else:
                job.status = "DEAD_LETTER"
                with self._lock:
                    self.total_failed += 1
                    self.total_processed += 1
                    self._dead_letter_queue.append(job)
                logging.error(f"Job [{job.id}] exceeded max retries. Moved to Dead-Letter Queue (DLQ).")

    def _re_enqueue(self, job: Job):
        with self._lock:
            heapq.heappush(self._heap, job)
            self._not_empty.notify()

    def get_all_jobs(self) -> List[Dict]:
        with self._lock:
            return [j.to_dict() for j in sorted(self._jobs.values(), key=lambda x: x.created_at, reverse=True)]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_workers": self.num_workers,
                "queued_jobs": len(self._heap),
                "total_processed": self.total_processed,
                "total_success": self.total_success,
                "total_failed": self.total_failed,
                "total_retried": self.total_retried,
                "dead_letter_count": len(self._dead_letter_queue)
            }
