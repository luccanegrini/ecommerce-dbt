import json
import uuid
import random
from datetime import datetime, timezone
import time

import boto3

# ---------- CONFIG ----------
AWS_REGION = "us-east-2"  # mesma região do bucket
S3_BUCKET = "ecommerce-luccanegrini-001"
S3_PREFIX = "events"  # pasta base
BATCH_SIZE = 200      # quantos eventos por arquivo
# ----------------------------

s3 = boto3.client("s3", region_name=AWS_REGION)

EVENT_TYPES = ["purchase", "add_to_cart", "view_item"]


def generate_event() -> dict:
    now = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": now.isoformat(),   # compatível com TIMESTAMP_NTZ::ISO
        "user_id": f"user_{random.randint(1, 500)}",
        "session_id": f"session_{random.randint(1, 200)}",
        "payload": {
            "campaign_id": random.choice(["FB_1", "IG_1", "GC_1"]),
            "currency": "BRL",
            "items": [
                {
                    "product_id": f"prod_{random.randint(1, 100)}",
                    "price": round(random.uniform(10, 500), 2),
                    "qty": random.randint(1, 3),
                }
            ],
        },
    }


def send_batch_to_s3(batch_size: int = BATCH_SIZE):
    events = [generate_event() for _ in range(batch_size)]
    body = "\n".join(json.dumps(e) for e in events)

    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    ts_part = int(now.timestamp())

    key = f"{S3_PREFIX}/dt={date_part}/events_{ts_part}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
    )

    print(f"Enviados {batch_size} eventos para s3://{S3_BUCKET}/{key}")


if __name__ == "__main__":
    # se quiser ficar gerando pra sempre, descomenta o loop
    # while True:
    #     send_batch_to_s3()
    #     time.sleep(10)

    # por enquanto, só 1 batch
    send_batch_to_s3()
