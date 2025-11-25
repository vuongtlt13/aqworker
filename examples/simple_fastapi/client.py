"""
Simple client script to enqueue demo jobs against the FastAPI example.
"""

import asyncio
import random
from typing import Any, Dict

import httpx

BASE_URL = "http://localhost:8000"


async def enqueue_job(
    client: httpx.AsyncClient,
    endpoint: str,
    payload: Dict[str, Any],
) -> None:
    response = await client.post(f"{BASE_URL}{endpoint}", json=payload)
    if response.is_success:
        data = response.json()
        print(f"[Client] ✓ Enqueued job via {endpoint}: {data}")
    else:
        print(f"[Client] ✗ Failed to enqueue via {endpoint}: {response.text}")


async def main() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        job_counter = 0
        try:
            while True:
                job_counter += 1
                await enqueue_job(
                    client,
                    "/jobs/email",
                    {
                        "recipient": f"user{random.randint(1, 999)}@example.com",
                        "subject": f"Client email #{job_counter}",
                        "body": "This job was triggered by the sample client.",
                    },
                )

                await enqueue_job(
                    client,
                    "/jobs/notification",
                    {
                        "user_id": str(random.randint(1, 999)),
                        "message": f"Client notification #{job_counter}",
                        "type": "info",
                    },
                )

                await asyncio.sleep(0.2)
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n[Client] Stopping client...")


if __name__ == "__main__":
    asyncio.run(main())
