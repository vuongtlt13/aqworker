"""
Simple FastAPI application with AQWorker integration.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from worker import aq_worker

# FastAPI app
app = FastAPI(
    title="Simple FastAPI with AQWorker",
    description="A simple FastAPI application with background job processing",
    version="1.0.0",
)


class EmailRequest(BaseModel):
    recipient: str
    subject: str
    body: str


class NotificationRequest(BaseModel):
    user_id: str
    message: str
    type: str = "info"


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "FastAPI Worker Example",
        "available_workers": aq_worker.get_available_workers(),
        "registered_handlers": list(aq_worker.handler_registry.snapshot().keys()),
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.post("/jobs/email")
async def enqueue_email_job(request: EmailRequest):
    """
    Enqueue an email job.

    Args:
        request: Email request with recipient, subject, and body

    Returns:
        Job information
    """
    try:
        job = await aq_worker.job_service.enqueue_job(
            queue_name="emails",
            handler="email",
            data={
                "recipient": request.recipient,
                "subject": request.subject,
                "body": request.body,
            },
        )
        return {
            "message": "Email job enqueued",
            "job_id": job.id,
            "queue": job.queue_name,
            "status": "pending",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/notification")
async def enqueue_notification_job(request: NotificationRequest):
    """
    Enqueue a notification job.

    Args:
        request: Notification request with user_id, message, and type

    Returns:
        Job information
    """
    try:
        job = await aq_worker.job_service.enqueue_job(
            queue_name="notifications",
            handler="notification",
            data={
                "user_id": request.user_id,
                "message": request.message,
                "type": request.type,
            },
        )
        return {
            "message": "Notification job enqueued",
            "job_id": job.id,
            "queue": job.queue_name,
            "status": "pending",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/queues/{queue_name}/stats")
async def get_queue_stats(queue_name: str):
    """
    Get statistics for a queue.

    Args:
        queue_name: Name of the queue

    Returns:
        Queue statistics
    """
    try:
        stats = await aq_worker.job_service.get_queue_stats([queue_name])
        return {"queue": queue_name, "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workers")
async def list_workers():
    """List all registered workers."""
    return {
        "workers": aq_worker.get_available_workers(),
        "definitions": [
            {"name": defn.name, "queues": list(defn.queue_names)}
            for defn in aq_worker.worker_registry.list_definitions()
        ],
    }


@app.get("/handlers")
async def list_handlers():
    """List all registered handlers."""
    handlers = aq_worker.handler_registry.snapshot()
    return {
        "handlers": [
            {"name": name, "class": f"{cls.__module__}.{cls.__name__}"}
            for name, cls in handlers.items()
        ]
    }
