import logging
import sys
import time
import json
import re
from logging.handlers import RotatingFileHandler
from contextvars import ContextVar
from typing import Callable, Any
from cpu_tracking import CPUMonitor, InvariantDeque

class CollectorEngine:
    def __init__(self, name: str, log_file: str = "collector.log"):
        self.name = name
        self._job_context: ContextVar[dict] = ContextVar(f"{name}_context", default={})
        self.cpu_tracker = CPUMonitor(window_size=15)
        self.metric_hooks: list[Callable] = []
        self.alert_hooks: list[Callable] = [] 
        
        self.logger = self._setup_logger(name, log_file)

    def _setup_logger(self, name, log_file):
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.setLevel(logging.DEBUG)
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
                if record.exc_info:
                    log_record["exc"] = self.formatException(record.exc_info)
                return json.dumps(log_record)
        return EngineJsonFormatter()

    def _scrub_pii(self, data: Any) -> Any: #Will scrub (names, addresses, credit card patterns) later.
        text = str(data)
        text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[REDACTED_EMAIL]', text)
        text = re.sub(r'\b\d{10,12}\b', '[REDACTED_PHONE]', text)
        return text

    def add_metric_hook(self, fn: Callable): self.metric_hooks.append(fn)
    
    def monitor(self, platform: str, stage: str):
        def decorator(func):
            def wrapper(job_id: str, p_hash: str, payload: dict, *args, **kwargs):
                self._job_context.set({
                    "job_id": job_id,
                    "platform": platform,
                    "stage": stage,
                    "p_hash": p_hash
                })
            
                self.logger.info(f"Collector stage {stage} started")
            
                start_wall = time.perf_counter()
            
                try:
                    result = func(payload, *args, **kwargs)
                    wall_ms = int((time.perf_counter() - start_wall) * 1000)
                    cpu_avg = self.cpu_tracker.update()
                    ctx = self._job_context.get()
                    ctx["perf"] = {
                        "wall_ms": wall_ms,
                        "cpu_avg": cpu_avg
                    }
                    ctx["output"] = self._scrub_pii(result) 
                
                    self.logger.info(f"Collector stage {stage} completed", extra={"ctx": ctx})
                    
                    for hook in self.metric_hooks:
                        hook(f"{stage}_perf", ctx["perf"])
                    
                    return result 
                finally:
                    self._job_context.set({}) 
            return wrapper
        return decorator

if __name__ == "__main__":
    engine = CollectorEngine(name="Collector_Core")
    
    @engine.monitor(platform="shopify", stage="deconstructor") #Error handling for later.
    def process_bundle(data):
        sum(i*i for i in range(10**6)) 
        return {"status": "success", "email": "test@user.com", "skus": ["SKU1", "SKU2"]}

    for i in range(3):
        process_bundle(job_id=f"id-{i}", p_hash="hash-x", payload={"items": []})