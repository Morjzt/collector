"""
Docstring for utils.engine


- PII scrubbing (legal reasons)
- Data Quality Metrics: Counting nulls, invalid formats, anomalies or successful "healings" (corrections).

- CPU tracking
- Memory tracking
- Execution tracking / Wall time (ms)
- Record counts I/O
- Error rates - Tracks how often a stage fails (Unit/atomic tracking.) - Requires modelling.


- Simple alerting
- Batch metadata
- Correlation ID - To trace one specific order through all stages

Main Metrics:

- Counter 
- Gauges
- Histograms
"""

#Imports.

import logging
import time
import json
import os
import psutil
import re
import sys
from logging.handlers import RotatingFileHandler
from contextvars import ContextVar
import collections
from typing import Dict, Callable, Any

# <------ Performance Tracking ------>

class InvariantDeque:
    """
    The 'sliding window' enforces a memory invariant where
    the last N samples are kept for a rolling average

    In other words: The CPU doesn't instantly spike on call (which is misleading).
    """
    def __init__(self, maxlen: int = 50):
        self._buffer = collections.deque(maxlen=maxlen)

    def add(self, value: float):
        self._buffer.append(value)

    def get_average(self) -> float:
        return sum(self._buffer) / len(self._buffer) if self._buffer else 0.0

class CPUMonitor:
    """
    Tracks the CPU time in seconds that the exact process used.
    """
    def __init__(self, process: psutil.Process):
        self.process = process

    def get_thread_time(self) -> float:  
        """
        thread_time() measures actual CPU effort (work done).
        While clock() measures elapsed real-world time (waiting included). 
        The CPU only counts time when it is actively processing the program(code). 
        """
        return time.thread_time()

class MemoryTracking: 
    # Tracks physical RAM usage (RSS) in megabytes 
    def __init__(self, process: psutil.Process):
        self.process = process
        self.peak = 0.0

    def get_mb(self) -> float:
        # Corrected: bytes to megabytes conversion
        mb = self.process.memory_info().rss / (1024 * 1024)
        if mb > self.peak:
            self.peak = mb
        return mb
    

# <------ Telemetry Monolith ------>

class TelemetryMonolith:
    def __init__(self, logger: logging.Logger):
        self._proc = psutil.Process(os.getpid())
        self.cpu = CPUMonitor(self._proc)
        self.ram = MemoryTracking(self._proc)
        self.logger = logger

        self.latencies = collections.defaultdict(InvariantDeque)
        self.counts = collections.Counter()

    def record(self, stage: str, wall_time: float, cpu_delta: float):
        # Captures snapshot of a specific stage.
        self.latencies[stage].add(wall_time)
        self.counts[f"{stage}_cpu_total"] += cpu_delta
        self.counts[f"{stage}_hits"] += 1

    def report(self):
        #Outputs the telemetry state via the configured logger.
        stats = {
            "peak_ram_MB": round(self.ram.peak, 2),
            "stages": {
                stage: {
                    "avg_ms": round(deque.get_average(), 4),
                    "total_runs": self.counts[f"{stage}_hits"]
                } for stage, deque in self.latencies.items()
            }
        }
        self.logger.info("TELEMETRY_REPORT", extra={"metrics": stats})

#< ------ Custom Exceptions ------>

class CircutBreakerError(Exception):
    pass


# <------ Engine Implementation ------>

class CollectorEngine:
    def __init__(self, name: str, log_file: str = "collector.log"):
        self.name = name
        self._job_context: ContextVar[dict] = ContextVar(f"{name}_context", default={})
        
        #Initialize logger so that other components can use it.
        self.logger = self._setup_logger(name, log_file)
        self.telemetry = TelemetryMonolith(self.logger)

    def _scrub_pii(self, text: str) -> str:
        # Pattern based PII scrubbing, for legal reasons.

        # For day 1 - Simple email 
        email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        return re.sub(email_pattern, "[EMAIL_REDACTED]", text)

    def _get_formatter(self):
        engine_ref = self
        class EngineJsonFormatter(logging.Formatter):
            def format(self, record):
                ctx = engine_ref._job_context.get()
                log_record = {
                    "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
                    "lvl": record.levelname,
                    "engine": engine_ref.name,
                    "msg": engine_ref._scrub_pii(record.getMessage()),
                    "ctx": ctx
                }
                if hasattr(record, "metrics"):
                    log_record["metrics"] = record.metrics
                if record.exc_info:
                    log_record["exc"] = self.formatException(record.exc_info)
                return json.dumps(log_record)
        return EngineJsonFormatter()

    def _setup_logger(self, name, log_file): 
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.DEBUG)
            formatter = self._get_formatter()
            
            # Rotating File Handler: max 50MB per file
            fh = RotatingFileHandler(log_file, maxBytes=50_000_000, backupCount=10)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            
            # Stream Handler for console output
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger

    def wrap_stage(self, stage_name: str, func: Callable, *args, **kwargs):
        """
        Instead of a complex decorator, it's better to start with a simple wrapper.
        This makes it easier to 'see' the snapshots happening.
        """

        payload = args[0] if args else None

        """
        The classic invariant( within this system is that everything 
        must be atomic. Including bundles (which have multiple products {components})
        Instead of dumping the bundle (huge mistake) - Formalize it then fallback.
        """

        if isinstance(payload, list):
            # Record the rejection in Telemetry before falling back
            self.telemetry.counts[f"{stage_name}_circuit_broken"] += 1
            self.logger.warning(
                f"CIRCUIT BREAKER: Stage '{stage_name}' rejected a bundle. Falling back.",
                extra={"payload_type": "list", "size": len(payload)}
            )
            # Raise exception to trigger immediate fallback in the calling code
            raise CircutBreakerError(f"Bundle detected in {stage_name}. Engine not configured for batches.")


        # 1. Start Snapshot
        t0 = time.perf_counter_ns()
        cpu0 = self.telemetry.cpu.get_thread_time()
        
        try:
           # 2. Actual work
            result = func(*args, **kwargs)
            return result
        finally:
            # 3. End Snapshot (Even if it fails, the attempt must be recorded.)
            t1 = time.perf_counter_ns()
            cpu1 = self.telemetry.cpu.get_thread_time()
            
            # 4. Math & Record
            wall_ms = (t1 - t0) / 1_000_000
            cpu_diff = cpu1 - cpu0
            self.telemetry.record(stage_name, wall_ms, cpu_diff)
            
            # Update RAM tracker.
            self.telemetry.ram.get_mb()

if __name__ == "__main__":
    engine = CollectorEngine(name="NexusPrimary")
    def clean_data(data: str):
        time.sleep(0.02)
        return data.strip()

    try:
        engine.wrap_stage("scrubber", clean_data, "  user@example.com  ")
        
        engine.wrap_stage("scrubber", clean_data, ["item1", "item2"])
        
    except CircutBreakerError as e:
        engine.logger.error(f"Handled expected break: {e}")

    engine.telemetry.report()


"""

Quick notes:

- Ram tracking is global not per stage
- circut logic breaker is mixed with telemetry (it should be spread, too vague):
    > Bundle rejection policy outside
    > Telemetry must be the observer not the policy maker. 
- ContexctVar doesn't have invariants enforced yet. Still based on assumption
- Per stage is too abstract, but conceptual.

"""