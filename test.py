import time
import uuid
import random
from engine import CollectorEngine

#Random payload generator - Stress test

def generate_massive_payload(item_count=150):
    return {
        "order_id": f"MSG-{uuid.uuid4().hex[:8].upper()}",
        "customer": {
            "name": "Heavy User",
            "email": f"user_{random.randint(1,100)}@shipscan.io",
            "phone": "5551234567"
        },
        "items": [
            {
                "sku": f"PROD-{i}",
                "price": random.uniform(10, 500),
                "note": f"Contact buyer at buyer_{i}@gmail.com" 
            } for i in range(item_count)
        ],
        "raw_data": "URGENT: Call 555-000-1111 for delivery instructions."
    }

def run_integrated_test():
    engine = CollectorEngine(name="stress-test", log_file="stress_test.log")
    
    def on_metric(name, data):
        print(f"   [METRIC] {name}: Wall: {data['wall_ms']}ms | CPU Avg: {data['cpu_avg']}%")

    engine.add_metric_hook(on_metric)


    @engine.monitor(platform="shopify", stage="bulk_processor")
    def process_bulk_job(payload):
        count = 0
        for item in payload['items']:
            _ = item['price'] * 1.05 
            count += 1
        time.sleep(0.05) 
        return f"Processed {count} items"

    print("--- Starting test ---")
    print("Generating and processing 20 massive payloads...")
    print("-" * 50)

    for i in range(20):
        massive_data = generate_massive_payload(200)
        
        job_id = f"job-{uuid.uuid4().hex[:6]}"
        p_hash = f"hash-{random.getrandbits(32)}"


        print(f"Running Job {i+1}/20 ({job_id})...")
        process_bulk_job(job_id=job_id, p_hash=p_hash, payload=massive_data)

    print("-" * 50)
    print("Test complete")
    print("Check 'stress_test.log' to verify PII redaction and Node-based CPU averaging.")

if __name__ == "__main__":
    run_integrated_test()