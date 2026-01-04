from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json

app = FastAPI()

# Data Contract matching Go's Struct
class JobContext(BaseModel):
    id: str
    source: str
    payload_hash: str
    raw_data: str

@app.post("/process")
async def process_signal(job: JobContext):
    print(f"\n[BRAIN] Received Job: {job.id}")
    print(f"[BRAIN] Hash: {job.payload_hash}")
    
    # "Thawing" the frozen Shopify data
    try:
        shopify_json = json.loads(job.raw_data)
        # Logic: Anomaly detection, DB storage, etc.
        return {"status": "success", "hash": job.payload_hash}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)