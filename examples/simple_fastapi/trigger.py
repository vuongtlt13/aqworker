import asyncio
import logging

from examples.simple_fastapi.job_service import job_service
from examples.simple_fastapi.worker import aq_worker

logger = logging.getLogger("aqworker")
logger.setLevel(logging.DEBUG)


async def main():
    print("Enqueueing performance index calculation job...")
    job = await aq_worker.job_service.enqueue_job(
        handler="email",
        queue_name="emails",
        data={
            "recipient": "test",
            "subject": "test",
            "body": "test",
        },
    )

    job_id = job.id
    print(f"✓ Job enqueued with ID: {job_id}\n")

    job = await job_service.get_job(job_id)
    print(job)

    ok = await job_service.complete_job(job=job, success=True)
    await asyncio.sleep(2)
    job = await job_service.get_job(job_id)

    print(job)


asyncio.run(main())
