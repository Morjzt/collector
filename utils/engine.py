"""
Docstring for utils.engine


- PII scrubbing (legal reasons) - KLAAR
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


quick note:

Data is HASHED, not the platform itself!
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
    

# <------ Stage Configuration ------>


class StageConfig:
    """
    Partition keys and configuration for each stage.
    Each stage gets its own isolated config.
    """
    
    # Stage identifiers (partition keys)
    FETCH = "fetch"
    SANITIZE = "sanitize"
    VALIDATE = "validate"
    CORRECT = "correct"
    STORE = "store"
    
    #Stage specific configs
    configs = {
        FETCH: {
            "timeout": 30,
            "retry_limit": 3,
            "payload_size": 50,
            "payload_depth": 50,
            "batch_size": 10,
            "rate_limit": 2.0  
        },
        SANITIZE: {
            "encoding": "utf-8",
            "trim_whitespace": True
        },
        VALIDATE: {
            "required_fields": ["sku", "price", "inventory"],
            "rules": [] 
        },
        CORRECT: {
            "confidence_deduction": {
                "missing_sku": 20,
                "null_price": 15,
                "invalid_inventory": 10
            }
        },
        STORE: {
            "batch_size": 100
        }
    }
    
    @classmethod
    def get_config(cls, stage_key: str) -> Dict:
        if stage_key not in cls.configs:
            raise ValueError(f"Unknown stage: {stage_key}")
        return cls.configs[stage_key].copy()


# <------ Circuit Breaker  ------>

class CircuitBreakerError(Exception):
    pass

class CircuitBreakerPolicy:
    """
    Enforces stage-specific policies.
    Separated from telemetry (observer vs policy maker).

    Ideally, the observability engine should not enforce decisions.
    However, due to constraints, it has to be enforced.

    This only executes at the surface level, no deep logic here.
    """
    
    def __init__(self):
        self.breaks = collections.Counter()
        self.policies = {

            # Stage-specific policies (configurable)

            StageConfig.FETCH: self._check_fetch,
            StageConfig.SANITIZE: self._check_sanitize,
            StageConfig.VALIDATE: self._check_validate,
        }
    
    def check(self, stage_key: str, payload: Any):
        
        # Checks if payload violates stage policy.
        # Raises CircuitBreakerError if violation found.
        
        if stage_key in self.policies:
            self.policies[stage_key](stage_key, payload)
    
    def _check_fetch(self, stage_key: str, payload: Any):
        #Fetch stage should receive platform name
        if not isinstance(payload, str):
            self.breaks[stage_key] += 1
            raise CircuitBreakerError(
                f"{stage_key}: Expected platform string, got {type(payload).__name__}"
            )
    
    def _check_sanitize(self, stage_key: str, payload: Any):
        # Sanitize stage should receive single record (dict)
        if isinstance(payload, list):
            self.breaks[stage_key] += 1
            raise CircuitBreakerError(
                f"{stage_key}: Bundle detected. Expected an atomic record, got {len(payload)} items."
            )
        if not isinstance(payload, dict):
            self.breaks[stage_key] += 1
            raise CircuitBreakerError(
                f"{stage_key}: Expected dict, got {type(payload).__name__}"
            )
    
    def _check_validate(self, stage_key: str, payload: Any):
        # Validate stage should receive a sanitized dict
        if not isinstance(payload, dict):
            self.breaks[stage_key] += 1
            raise CircuitBreakerError(
                f"{stage_key}: Expected dict, got {type(payload).__name__}"
            )


# <------ Stage Executor ------>

class StageExecutor:
    """
    Executes stages with fault isolation.
    Each stage runs independently - one failure doesn't crash the others.
    """

    
    def __init__(self, engine):
        self.engine = engine
        self.telemetry = engine.telemetry
        self.circuit_breaker = CircuitBreakerPolicy()
        self.results: Dict[str, Any] = {}  
        self.errors: Dict[str, Dict[str, str]] = {}   
    
    def run_stage(self, stage_key: str, func: Callable, payload: Any, **kwargs) -> Any:
        """
        Run a single stage with fault isolation.
        
        Returns:
            Result if successful
            None if failed (error stored in self.errors)
        """

        try:
            self.circuit_breaker.check(stage_key, payload)
            
            # Record Input count
            in_count = len(payload) if hasattr(payload, '__len__') else 1

            result = self.engine.wrap_stage(
                stage_name=stage_key,
                func=func,
                payload=payload, # Fixed: passing payload as positional arg
                **kwargs
            )
            
            # Record Output count
            out_count = len(result) if hasattr(result, '__len__') else (1 if result is not None else 0)
            
            self.results[stage_key] = result
            self.telemetry.record_io(stage_key, in_count, out_count)
            self.telemetry.counts[f"{stage_key}_success"] += 1
            
            return result
            
        except CircuitBreakerError as e:
            # Policy violation - expected failure
            self.errors[stage_key] = {
                "type": "CircuitBreakerError",
                "message": str(e)
            }
            self.engine.logger.warning(
                f"Stage {stage_key} blocked by circuit breaker",
                extra={"error": str(e)}
            )
            return None
            
        except Exception as e:
            # Unexpected failure
            self.errors[stage_key] = {
                "type": type(e).__name__,
                "message": str(e)
            }
            self.telemetry.counts[f"{stage_key}_failure"] += 1
            self.engine.logger.error(
                f"Stage {stage_key} failed",
                extra={"error": str(e)},
                exc_info=True
            )
            return None
    
    def run_pipeline(self, stages: list, initial_payload: Any) -> Dict:
        """
        Run multiple stages in sequence with fault isolation.
        If one stage fails, log it and continue with next stage.
        
        Args:
            stages: List of (stage_key, function) tuples
            initial_payload: Starting data
        
        Returns:
            Dict with results and errors per stage
        """
        payload = initial_payload
        
        for stage_key, func in stages:
            self.engine.logger.info(f"Running stage: {stage_key}")
            
            result = self.run_stage(stage_key, func, payload)
            
            if result is None:
                self.engine.logger.warning(
                    f"Stage {stage_key} failed, stopping pipeline"
                )
                break 
            
            payload = result
        
        return {
            "results": self.results,
            "errors": self.errors,
            "final_payload": payload
        }
    

# <------ Telemetry Monolith ------>

class TelemetryMonolith:
    def __init__(self, logger: logging.Logger):
        self._proc = psutil.Process(os.getpid())
        self.cpu = CPUMonitor(self._proc)
        self.ram = MemoryTracking(self._proc)
        self.logger = logger
        self.latencies = collections.defaultdict(InvariantDeque)
        self.ram_deltas = collections.defaultdict(list)  
        self.counts = collections.Counter()
        self.io_metrics = collections.defaultdict(lambda: {"in": 0, "out": 0})
        self.cpu_pcts = collections.defaultdict(InvariantDeque)

    def record_io(self, stage: str, count_in: int, count_out: int):
      # Captures record counts I/O for batch metadata 
        self.io_metrics[stage]["in"] += count_in
        self.io_metrics[stage]["out"] += count_out

    def record(self, stage: str, wall_ms: float, cpu_diff: float, ram_delta: float):
        self.latencies[stage].add(wall_ms)
        self.ram_deltas[stage].append(ram_delta)
        self.counts[f"{stage}_hits"] += 1
        
        cpu_pct = (cpu_diff / (wall_ms / 1000.0)) * 100 if wall_ms > 0 else 0
        self.cpu_pcts[stage].add(cpu_pct)

    def report(self):
        stats = {
            "peak_ram_MB": round(self.ram.peak, 2),
            "stages": {
                stage: {
                    "avg_wall_ms": round(deque.get_average(), 4),
                    "avg_cpu_ms": round((self.counts[f"{stage}_cpu_total"] * 1000) / self.counts[f"{stage}_hits"], 4) if self.counts[f"{stage}_hits"] > 0 else 0,
                    "total_runs": self.counts[f"{stage}_hits"],
                    "records_in": self.io_metrics[stage]["in"],
                    "records_out": self.io_metrics[stage]["out"],
                    "yield_ratio": round(self.io_metrics[stage]["out"] / self.io_metrics[stage]["in"], 2) if self.io_metrics[stage]["in"] > 0 else 0,
                    "avg_ram_delta_MB": round(sum(self.ram_deltas[stage]) / len(self.ram_deltas[stage]), 2) if self.ram_deltas[stage] else 0.0,
                    "success_count": self.counts.get(f"{stage}_success", 0),
                    "failure_count": self.counts.get(f"{stage}_failure", 0)
                } for stage, deque in self.latencies.items()
            }
        }
        
        self.logger.info("=" * 60)
        self.logger.info("TELEMETRY REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Peak RAM: {stats['peak_ram_MB']} MB")
        self.logger.info("")
        
        for stage_name, metrics in stats['stages'].items():
            self.logger.info(f"Stage: {stage_name}")
            self.logger.info(f"  Avg Wall Time: {metrics['avg_wall_ms']:.4f} ms")
            # ADDED THIS LOG LINE:
            self.logger.info(f"  Avg CPU Time:  {metrics['avg_cpu_ms']:.4f} ms") 
            self.logger.info(f"  Avg RAM Delta: {metrics['avg_ram_delta_MB']:.2f} MB")
            self.logger.info(f"  Yield Ratio:   {metrics['yield_ratio']}")
            self.logger.info(f"  Total Runs: {metrics['total_runs']} (S: {metrics['success_count']} / F: {metrics['failure_count']})")
            self.logger.info("")
        
        self.logger.info("=" * 60)

# <------ Engine Implementation ------>

class CollectorEngine:
    def __init__(self, name: str, log_file: str = "collector.log"):
        self.name = name
        self._job_context: ContextVar[dict] = ContextVar(f"{name}_context", default={})
        
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

    def wrap_stage(self, stage_name: str, func: Callable, payload: Any, *args, **kwargs):
        # 1. Start Snapshot
        ram0 = self.telemetry.ram.get_mb()
        t0 = time.perf_counter_ns()
        cpu0 = self.telemetry.cpu.get_thread_time()
        
        try:
            # 2. Actual work - Passing payload as positional argument
            result = func(payload, *args, **kwargs)
            return result
        finally:
            # 3. End Snapshot (Even if it fails, the attempt must be recorded.)
            t1 = time.perf_counter_ns()
            cpu1 = self.telemetry.cpu.get_thread_time()
            ram1 = self.telemetry.ram.get_mb()
            
            # 4. record everything
            wall_ms = (t1 - t0) / 1_000_000
            cpu_diff = cpu1 - cpu0
            ram_diff = ram1 - ram0
            
            self.telemetry.record(stage_name, wall_ms, cpu_diff, ram_diff)


# The "Hell Dataset" - Designed to trigger every Circuit Breaker and Telemetry Gauge
hell_dataset = [
    # 1. THE BUNDLE BOMB (Circuit Breaker Test)
    # Objective: Trigger the rejection of non-atomic batches.
    ["item_1", {"id": 2}, [3, 4, 5]], 

    # 2. THE MEMORY HOG (RAM Delta Test)
    # Objective: Force a massive delta between ram0 and ram1 to test your Gauge.
    {"id": "MEM_SPIKE", "data": bytearray(1024 * 1024 * 50)}, # 50MB allocation

    # 3. THE PII OBFUSCATOR (Scrubber Test)
    # Objective: Test if your regex catches emails buried in nested structures.
    {
        "user": "legit_user", 
        "meta": {"contact": "hidden_leak@malicious.com", "logs": "found user: ceo@company.org"}
    },

    # 4. THE TYPE ANARCHIST (Data Quality/Yield Test)
    # Objective: Send types that break standard dict.get() logic.
    {None: "null_key", "price": "THIRTY_DOLLARS", "inventory": float('nan')},

    # 5. THE RECURSIVE TRAP (CPU/Wall Time Test)
    # Objective: If your logic tries to deep-copy or serialize this, it may hang.
    (lambda: {
        "infinite": "loop"
    })(), # Passing a raw callable or a self-referencing object

    # 6. THE HASHED GHOST (Empty Yield Test)
    # Objective: A record that exists but has 0 usable content (Yield 1 -> 0).
    {} 
]


if __name__ == "__main__":

    engine = CollectorEngine("StressTester")
    executor = StageExecutor(engine)

    # Use a dummy function that just passes data through to see where the engine breaks
    def pass_through(data): return data

    print("STAGING HELL DATASET...")
    for i, pill in enumerate(hell_dataset):
        print(f"Testing Pill #{i}...")
        try:
            # We use SANITIZE to trigger the dict-only policy
            executor.run_stage(StageConfig.SANITIZE, pass_through, pill)
        except Exception as e:
            # The engine should catch these, log them, and keep moving
            continue

    # Final verification of your Gauges
    engine.telemetry.report()