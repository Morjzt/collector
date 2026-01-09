import time
import uuid
import random
import string
import json
from utils.engine import CollectorEngine

def generate_perfect_payload():
    """Generates a predictable, structured, and clean dataset (Zero Entropy)."""
    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": int(time.time()),
        "status": "COMPLETED",
        "customer": {
            "name": "John Doe",
            "email": "john.doe@example.com",
            "country_code": "US"
        },
        "items": [
            {
                "sku": "ITEM-001",
                "quantity": 1,
                "price": 10.00,
                "currency": "USD"
            }
        ],
        "metadata": {
            "source": "web_store",
            "version": "1.0"
        }
    }

def run_stress_test(iterations=int, mode="PERFECT"):
    engine = CollectorEngine(name="brain-baseline", log_file="baseline_stress.log")
    
    def on_metric(name, data):
        print(f"[{mode} TEST] {name} | Iter: {data.get('iteration')} | Wall: {data['wall_ms']}ms | CPU Avg: {data['cpu_avg']}%")

    engine.add_metric_hook(on_metric)

    @engine.monitor(platform="chaos_lab", stage="data_processing")
    def process_data(payload, iteration):
        # Even with clean data, we keep the logic consistent for a fair test
        serialized = json.dumps(payload)
        _ = [char for char in serialized if char in string.punctuation]
        return "Clean Logged"

    print(f"--- LAUNCHING {iterations} ITERATIONS OF {mode} DATA ---")
    
    # We use a separate log file for the perfect data to see disk behavior
    with open("perfect_dataset_dump.log", "a", encoding="utf-8") as f:
        for i in range(iterations):
            data = generate_perfect_payload()
            f.write(f"ITERATION {i} | DATA: {json.dumps(data)}\n")
            
            process_data(
                job_id=f"clean-{i}", 
                p_hash=f"h-{random.getrandbits(64)}", 
                payload=data,
                iteration=i 
            )

if __name__ == "__main__":
    # Run 10 iterations to see the baseline
    run_stress_test(100000, mode="PERFECT")