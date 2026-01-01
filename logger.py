import logging
import sys
import time
import uuid
import json
import hashlib
import re
import os
from logging.handlers import RotatingFileHandler
from contextvars import ContextVar
from typing import Optional, Callable, Any, Dict

try:
    import psutil
except ImportError:
    psutil = None

class PipelineEngine:
    def __init__(self, name: str, log_file: str = "pipeline.log"):
        self.name = name
        self._job_context: ContextVar[dict] = ContextVar(f"{name}_context", default={})
        self.logger = self._setup_logger(name, log_file)  
        self.metric_hooks: list[Callable[[str, dict], None]] = []
        self.alert_hooks: list[Callable[[str, dict], None]] = []
        self.logger = self._setup_logger(name, log_file)

    def _setup_logger(self, name, log_file):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        
        if not logger.handlers:
            formatter = self._get_formatter()
            
            fh = RotatingFileHandler(log_file, maxBytes=50_000_000, backupCount=10)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger

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
                if "perf" in ctx:
                    log_record["perf"] = ctx["perf"]
                
                if record.exc_info:
                    log_record["exc"] = self.formatException(record.exc_info)
                return json.dumps(log_record)
        return EngineJsonFormatter()

    def _scrub_pii(self, message: str) -> str:
        message = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[REDACTED_EMAIL]', message)
        message = re.sub(r'\b\d{10,12}\b', '[REDACTED_PHONE]', message)
        return message

    def _hash_payload(self, payload: Any) -> str:
        p_str = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(p_str.encode()).hexdigest()

    def _get_process_cpu(self):
        if psutil:
            return psutil.Process(os.getpid()).cpu_percent()
        return 0.0

    def add_metric_hook(self, fn: Callable): self.metric_hooks.append(fn)
    def add_alert_hook(self, fn: Callable): self.alert_hooks.append(fn)

    def _trigger(self, hooks, name, payload):
        for hook in hooks:
            try: hook(name, payload)
            except: pass

    def monitor(self, platform: str, stage: str):
        def decorator(func):
            def wrapper(*args, **kwargs):
                payload = args[0] if args else kwargs.get("payload", {})
                p_hash = self._hash_payload(payload)
                job_id = str(uuid.uuid4())
            
                self._job_context.set({
                    "job_id": job_id,
                    "platform": platform,
                    "stage": stage,
                    "p_hash": p_hash[:12]
                })
                
                self.logger.info(f"Pipeline processing started for {platform}")
                self._trigger(self.metric_hooks, "job_started", {"job_id": job_id})
                start_wall = time.perf_counter()
                start_cpu = time.process_time()
                
                try:
                    result = func(*args, **kwargs)
                    
                    wall_ms = int((time.perf_counter() - start_wall) * 1000)
                    cpu_ms = int((time.process_time() - start_cpu) * 1000)
                
                    ctx = self._job_context.get()
                    ctx["perf"] = {
                        "wall_ms": wall_ms,
                        "cpu_ms": cpu_ms,
                        "cpu_util": self._get_process_cpu()
                    }
                    self._job_context.set(ctx)

                    self.logger.info(f"Pipeline stage {stage} completed")
                    self._trigger(self.metric_hooks, "job_performance", ctx["perf"])
                    
                    return result

                except Exception as e:
                    self.logger.error(f"Critical failure in {stage}: {str(e)}")
                    self._trigger(self.alert_hooks, "job_failure", {"error": str(e)})
                    raise
                finally:
                    self._job_context.set({})
            return wrapper
        return decorator

#Sample test
if __name__ == "__main__":
    engine = PipelineEngine(name="ShipScan_Core", log_file="test_run.log")

    def my_metrics(name, data):
        print(f"[METRIC RECORDED]: {name} -> {data}")
    
    engine.add_metric_hook(my_metrics)

    @engine.monitor(platform="shopify", stage="bundle_deconstructor")
    def heavy_processing_task(payload: dict):
        for _ in range(1000000):
            _ = 100 * 100
        time.sleep(0.2)
        return {"status": "success"}


#testing
    print("\n--- Running Success Test ---")
    test_payload = {
        "order_id": "999", 
        "customer_email": "founder@shipscan.io", 
        "items": ["bundle_A", "bundle_B"]
    }
    
    engine.logger.info("Initializing test sequence...")
    heavy_processing_task(payload=test_payload)
    print("Test complete. Check console output above and test_run.log for JSON logs.")