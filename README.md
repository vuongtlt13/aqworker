# AQWorker - Async Queue Worker

## Overview

AQWorker is a Redis-based background job processing system for Python applications. It provides a simple, efficient way to handle asynchronous tasks with support for multiple worker types, queue isolation, and job status tracking. While it works seamlessly with FastAPI, it's a standalone package that can be used with any Python application.

## Features

- **Framework Agnostic**: Works with FastAPI, Django, Flask, or any Python application
- **Async/Await Support**: Built with async/await for modern Python applications
- **Redis-based**: Uses Redis for reliable job queue management
- **Worker Registry**: Centralized worker management with automatic discovery
- **Handler Registry**: Organized handler system for job processing
- **Handler Auto-Discovery**: Import handler packages automatically via `include_packages`
- **CLI Tools**: Command-line interface for managing workers and jobs
- **Queue Isolation**: Separate queues for different job types
- **Job Status Tracking**: Track job lifecycle (PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED)
- **Automatic Retries**: Built-in retry mechanism with configurable delays

## Architecture

```
┌─────────────┐    get_next_job()    ┌─────────────┐    dequeue()    ┌─────────────┐
│   Worker    │ ────────────────────►│ JobService  │ ──────────────► │  JobQueue   │
│             │                      │             │                 │             │
│             │ ◄────────────────────│             │ ◄────────────── │             │
└─────────────┘    complete_job()    └─────────────┘   complete_job()└─────────────┘

┌─────────────┐    enqueue_job()     ┌─────────────┐    enqueue()    ┌─────────────┐
│ Application │ ────────────────────►│   AQWorker  │ ──────────────► │  JobQueue   │
│ (FastAPI/   │                      │             │                 │             │
│  Django/    │                      │  - Worker   │                 │             │
│  Flask/     │                      │    Registry │                 │             │
│  Script)    │                      │  - Handler  │                 │             │
│             │                      │    Registry │                 │             │
└─────────────┘                      │  - Job      │                 └─────────────┘
                                     │    Service  │
                                     └─────────────┘
```

## Components

### 1. AQWorker (`core.py`)
- **Purpose**: Main orchestrator class that manages workers, handlers, and job service
- **Features**:
  - Worker and handler registration
  - Worker instance creation
  - Job service integration
  - Centralized configuration

### 2. WorkerRegistry (`worker/registry.py`)
- **Purpose**: Manages worker class registrations
- **Features**:
  - Register worker classes with names
  - List available workers
  - Get worker definitions

### 3. HandlerRegistry (`handler/registry.py`)
- **Purpose**: Manages handler class registrations
- **Features**:
  - Register handler classes
  - Lookup handlers by name
  - Snapshot of all registered handlers

### 4. JobService (`job/service.py`)
- **Purpose**: High-level service layer for job operations
- **Features**:
  - Enqueue jobs
  - Dequeue jobs for workers
  - Get job status
  - Queue statistics
  - Job lifecycle management

### 5. JobQueue (`job/queue.py`)
- **Purpose**: Low-level Redis operations for job management
- **Features**:
  - FIFO queue processing
  - Async/await support
  - Job status tracking
  - Redis operations: enqueue, dequeue, complete_job

### 6. BaseWorker (`worker/base.py`)
- **Purpose**: Base class for all workers
- **Features**:
  - Automatic job polling
  - Concurrent job processing
  - Error handling and retries
  - Health monitoring

### 7. Job & CronJob (`job/base.py`)
- **Purpose**: Base classes for job handlers
- **Job**: For one-time jobs
- **CronJob**: For scheduled/recurring jobs with cron expressions
- **Features**:
  - Async/sync handler support
  - Standardized job processing interface
  - Required `queue_name` attribute

## Installation

```bash
pip install aqworker
```

Or install from source:

```bash
git clone <repository>
cd aqworker
pip install -e .
```

## Quick Start

### 1. Define Handlers

**Method 1: Using Job class (for one-time jobs)**

```python
# handlers.py
from aqworker import Job

class EmailJob(Job):
    name = "email"
    queue_name = "emails"  # Required: specify which queue this job uses
    
    async def handle(self, data: dict) -> bool:
        recipient = data.get("recipient")
        subject = data.get("subject")
        body = data.get("body")
        
        # Send email logic here
        print(f"Sending email to {recipient}: {subject}")
        return True
```

**Method 2: Using CronJob class (for scheduled jobs)**

```python
from aqworker import CronJob

class DailyReportCronJob(CronJob):
    name = "daily_report"
    queue_name = "reports"  # Required: specify which queue this job uses
    
    @classmethod
    def cron(cls) -> str:
        """Cron expression: run daily at midnight."""
        return "0 0 * * *"
    
    async def handle(self, data: dict) -> bool:
        # Generate daily report
        print("Generating daily report...")
        return True
```

**Method 3: Using decorators**

```python
from aqworker import AQWorker

aq_worker = AQWorker()

# Job decorator (one-time jobs)
@aq_worker.job(name='send_email', queue_name='emails')  # queue_name is required
async def send_email(data: dict) -> bool:
    recipient = data.get("recipient")
    print(f"Sending email to {recipient}")
    return True

# CronJob decorator (scheduled jobs)
@aq_worker.cronjob(cron='0 0 * * *', name='daily_report', queue_name='reports')  # queue_name is required
async def daily_report(data: dict) -> bool:
    print("Generating daily report...")
    return True
```

**Important Notes:**
- `queue_name` is **required** for all handlers (Job, CronJob, or decorators)
- Each handler must specify which queue it uses
- If you override `queue_name` when enqueueing, you'll get a warning

### 2. Define Workers

```python
# workers.py
from aqworker import BaseWorker, WorkerConfig

class EmailWorker(BaseWorker):
    worker_name = "email"
    worker_config = WorkerConfig(
        queue_names=["emails"],
        max_concurrent_jobs=3,
        poll_interval=0.5,
    )
```

### 3. Initialize AQWorker

```python
# aq_worker.py
from aqworker import AQWorker
from aqworker.job.service import JobService
from workers import EmailWorker
from handlers import EmailHandler  # optional when using include_packages

# Create JobService
job_service = JobService()

# Initialize AQWorker and auto-import handlers package
aq_worker = AQWorker(include_packages=["handlers"])

# Register workers
aq_worker.register_worker(EmailWorker)
# Handlers inside "handlers" will be discovered automatically (Job, CronJob, or decorators)
# You can still register manually if desired:
# aq_worker.register_handler(EmailJob)

# Connect job service
aq_worker.listen(job_service)
```

#### Automatic handler discovery

If your handlers are spread across multiple modules, you can tell `AQWorker` to
import them automatically by passing `include_packages`. Every module inside
those packages will be imported once at startup, so any `@aq_worker.job` or
`@aq_worker.cronjob` decorators (or subclasses of `Job`/`CronJob`) run and register themselves:

```python
aq_worker = AQWorker(include_packages=[
    "my_project.workers",
    "examples.simple.aq_worker",
])
```

You can also call `aq_worker.autodiscover_handlers([...])` later if you need to
load additional packages dynamically. After discovery completes AQWorker logs the
full list of available handlers so you can confirm everything loaded correctly.

### 4. Enqueue Jobs

```python
# In your application
from worker import aq_worker

# Method 1: Using handler class (queue_name is taken from handler)
job = await aq_worker.job_service.enqueue_job(
    handler=EmailJob,  # queue_name is automatically taken from EmailJob.queue_name
    data={
        "recipient": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    }
)

# Method 2: Using handler name (queue_name must be provided)
job = await aq_worker.job_service.enqueue_job(
    handler="email",
    queue_name="emails",  # Required when handler is a string
    data={
        "recipient": "user@example.com",
        "subject": "Welcome!",
        "body": "Welcome to our service!"
    }
)

# Method 3: Override queue_name (will log a warning if different from handler's queue_name)
job = await aq_worker.job_service.enqueue_job(
    handler=EmailJob,
    queue_name="other_queue",  # Warning: overrides EmailJob.queue_name
    data={...}
)
```

### 5. Run Workers

**Using CLI:**

```bash
# Option 1: Pass file path
aqworker start email aq_worker.py

# Option 2: Use environment variable
export AQWORKER_FILE=aq_worker.py
aqworker start email
```

**Using Python:**

```python
import asyncio
from worker import aq_worker

async def main():
    worker = aq_worker.create_worker("email")
    await worker.run()

asyncio.run(main())
```

### 6. Run Cron Scheduler (Beat)

If you have CronJob handlers, you need to run the beat service to schedule them. The beat service automatically checks cron expressions and enqueues jobs when they match.

**Using CLI:**

```bash
# Start beat service
aqworker beat aq_worker.py

# With custom options
aqworker beat aq_worker.py --check-interval 0.1

# Using environment variable
export AQWORKER_FILE=aq_worker.py
aqworker beat
```

> ### 🔔 Cron expression primer  
> AQWorker supports both classic 5-field cron strings (`minute hour day month weekday`) and 6-field strings that include seconds. When we detect six fields we call `croniter(..., second_at_beginning=True)` so the **first slot is the seconds field**. Double-check your syntax:
>
> | Cadence               | Expression        | Meaning                                                   |
> |-----------------------|-------------------|-----------------------------------------------------------|
> | Every 10 seconds      | `*/10 * * * * *`  | 6-field format. `*/10` sits in the seconds field.         |
> | Every minute at 0 sec | `0 * * * * *`     | Fires once per minute exactly at the top of the minute.   |
> | Every 5 minutes       | `*/5 * * * *`     | Classic 5-field format on the minutes column.             |
> | Weekdays at 09:00     | `0 9 * * MON-FRI` | Standard cron with no seconds column.                     |
>
> **Caution:** If you accidentally supply six fields when you intended five, your job may run every few seconds instead of every few minutes. Always review the first token—when there are six tokens, it is the seconds slot.

**Using Python:**

```python
from aqworker import AQWorker, CronScheduler

aq_worker = AQWorker(include_packages=["handlers"])

# Create and start cron scheduler
cron_scheduler = CronScheduler(
    handler_registry=aq_worker.handler_registry,
    job_service=aq_worker.job_service,
    # check_interval defaults to 0.1 seconds (supports second-level cron)
)

await cron_scheduler.start()
```

**Important Notes:**
- Each CronJob must have `queue_name` defined (required)
- Beat service runs independently from workers
- Supports both 5-field (minute-level) and 6-field (second-level) cron expressions
- Default check interval is 0.1 seconds for better precision

## Usage Examples

### Standalone Python Application

```python
# main.py
import asyncio
from worker import aq_worker

async def main():
    # Enqueue jobs
    for i in range(10):
        job = await aq_worker.job_service.enqueue_job(
            handler="email",
            queue_name="emails",
            data={"recipient": f"user{i}@example.com", "subject": f"Email {i}"}
        )
        print(f"Enqueued job: {job.id}")
    
    # Start aq_worker
    worker = aq_worker.create_worker("email")
    await worker.run()

asyncio.run(main())
```

### FastAPI Integration

The repo ships with a complete FastAPI + AQWorker demo under `examples/simple_fastapi/`.
Key files:

- `worker.py` – configures `AQWorker`, registers the example workers, and wires up the
  shared `JobService`.
- `main.py` – FastAPI app that exposes `/jobs/email`, `/jobs/notification`, queue stats,
  and discovery endpoints.
- `client.py` – small CLI client that calls the HTTP endpoints so you can watch jobs
  flow through the queues.

Minimal excerpt of the worker wiring:

```python
# examples/simple_fastapi/worker.py
from aqworker import AQWorker
from workers import EmailWorker, NotificationWorker
from job_service import job_service

aq_worker = AQWorker(include_packages=["handlers"])
aq_worker.register_worker(EmailWorker)
aq_worker.register_worker(NotificationWorker)
aq_worker.listen(job_service)
```

To run the demo API and fire sample requests:

```bash
cd examples/simple_fastapi
uv run python main.py            # start FastAPI

# in another shell: send a job via the bundled client
uv run python client.py --recipient user@example.com --subject Test --body "Hello!"
```

### Django Integration

```python
# views.py
from django.http import JsonResponse
from worker import aq_worker
import asyncio

async def send_email_view(request):
    job = await aq_worker.job_service.enqueue_job(
        handler="email",
        queue_name="emails",
        data={
            "recipient": request.POST.get("recipient"),
            "subject": request.POST.get("subject"),
            "body": request.POST.get("body")
        }
    )
    return JsonResponse({"job_id": job.id})

# Use asyncio.run or Django's async support
```

## CLI Commands

AQWorker provides a CLI tool for managing workers and jobs:

### List Commands

```bash
# List available workers
aqworker list:aq_worker [file_path]

# List registered handlers
aqworker list:handlers [file_path]

# List queues
aqworker list:queue [file_path]
```

### Worker Commands

```bash
# Start a aq_worker
aqworker start <worker_name> [file_path]

# Get queue statistics
aqworker stats <queue_name> [file_path]
```

### Using Environment Variable

```bash
# Set AQWorker file path
export AQWORKER_FILE=aq_worker.py

# Use commands without file path
aqworker list:aq_worker
aqworker start email
aqworker stats emails
```

**Note**: The file path should point to a Python file containing an `AQWorker` instance. The CLI automatically finds the instance (looking for variables named `aq_worker`, `worker`, `aqworker`, or `aq`).

## Configuration

### Environment Variables

```bash
# Redis Configuration (used when creating JobService)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# AQWorker CLI
AQWORKER_FILE=aq_worker.py  # Path to file containing AQWorker instance
```

### Worker Configuration

```python
from aqworker import WorkerConfig

class MyWorker(BaseWorker):
    worker_name = "my_worker"
    worker_config = WorkerConfig(
        queue_names=["my_queue"],
        max_concurrent_jobs=5,      # Max jobs processed simultaneously
        poll_interval=0.5,          # Seconds between queue polls
        job_timeout=300,            # Job timeout in seconds
    )
```

## Job Lifecycle

```
1. ENQUEUE: Job created via JobService.enqueue_job()
2. QUEUE: Job added to Redis queue (aqw:{queue_name})
3. DEQUEUE: Worker gets job via JobService.get_next_job()
4. PROCESS: Job moved to processing queue, handler executed
5. COMPLETE: Job marked as completed/failed
6. CLEANUP: Old jobs cleaned up automatically
```

## Queue System

### Redis key prefixes

AQWorker keeps every Redis key namespaced with the `aqw` prefix so multiple apps can
share the same Redis without collisions. The most important keys are:

- `aqw:{queue_name}` – pending FIFO queue for a specific worker queue
- `aqw:processing`, `aqw:completed`, `aqw:failed` – global processing/completed/failed lists
- `aqw:job:{job_id}` – job status hash (timestamps, error state, etc.)
- `aqw:jl:{job_id}` – per-job lock key used to ensure only one worker processes the job

All public APIs use helpers from `aqworker.constants` (`get_queue_name`,
`get_job_status_key`, `get_job_lock_key`) so you rarely need to construct these strings
manually.

### Queue Structure

```
Redis Keys:
├── aqw:emails        # Email jobs (FIFO)
├── aqw:notifications # Notification jobs (FIFO)
├── aqw:processing    # Jobs currently being processed
├── aqw:completed     # Successfully completed jobs
└── aqw:failed        # Failed jobs
```

### Queue Isolation

- Each worker processes specific queue names
- Jobs are isolated by queue name
- No cross-queue interference
- Use different queues for different job types

### FIFO Processing

- Simple first-in-first-out queue processing
- Jobs processed in order of arrival
- All jobs in a queue are processed equally
- Use different queue names for different priority levels

## Advanced Usage

### Custom Job Data

```python
# Using handler class (queue_name from handler)
job = await aq_worker.job_service.enqueue_job(
    handler=EmailJob,  # queue_name automatically taken from EmailJob.queue_name
    data={
        "recipient": "user@example.com",
        "subject": "Welcome",
        "body": "Welcome!",
        "attachments": ["file1.pdf", "file2.jpg"],
        "priority": "high"
    },
    metadata={
        "source": "api",
        "user_id": "12345",
        "campaign_id": "summer2024"
    },
    max_retries=5,
    retry_delay=60
)

# Or using handler name (queue_name required)
job = await aq_worker.job_service.enqueue_job(
    handler="email",
    queue_name="emails",  # Required when handler is a string
    data={...},
    metadata={...}
)
```

### Multiple Workers

```python
# Register multiple workers
aq_worker.register_worker(EmailWorker)
aq_worker.register_worker(NotificationWorker)
aq_worker.register_worker(ReportWorker)

# Start specific aq_worker
worker = aq_worker.create_worker("email")
await worker.run()
```

### Handler with Async Support

```python
class AsyncJob(Job):
    name = "async_task"
    queue_name = "async_tasks"  # Required
    
    async def handle(self, data: dict) -> bool:
        # Async operations
        result = await some_async_operation(data)
        return result is not None
```

## Best Practices

### 1. Job Design
- Keep jobs small and focused
- Use descriptive handler names
- Include all necessary data in `data` parameter
- Use `metadata` for tracking/debugging information

### 2. Worker Design
- Implement proper error handling in handlers
- Use appropriate concurrency levels
- Monitor job processing time
- Log important events

### 3. Queue Management
- Use descriptive queue names
- Monitor queue depths
- Clean up old jobs regularly
- Balance load across workers

### 4. Handler Design
- **Required**: All handlers (Job, CronJob) must define `queue_name` attribute
- Make handlers idempotent when possible
- Handle errors gracefully
- Return `True` on success, `False` on failure
- Use async handlers for I/O operations
- Use `Job` for one-time jobs, `CronJob` for scheduled/recurring jobs

## Troubleshooting

### Common Issues

1. **Jobs not processing**
   - Check worker is running
   - Verify queue names match between enqueue and worker
   - Check Redis connection
   - Verify handler is registered

2. **High memory usage**
   - Clean up old jobs: `await job_service.cleanup_old_jobs(days=7)`
   - Reduce queue depths
   - Optimize job data size

3. **Slow processing**
   - Increase worker concurrency (`max_concurrent_jobs`)
   - Optimize handler logic
   - Check Redis performance
   - Consider multiple workers

### Debugging

```python
# Check queue statistics
stats = await aq_worker.job_service.get_queue_stats(["emails"])
print(f"Queue stats: {stats}")

# Get job status
job = await aq_worker.job_service.get_job(job_id)
print(f"Job status: {job.status}")

# List registered workers
workers = aq_worker.get_available_workers()
print(f"Available workers: {workers}")

# List registered handlers
handlers = aq_worker.handler_registry.snapshot()
print(f"Registered handlers: {list(handlers.keys())}")
```

## Examples

The `examples/` directory contains end-to-end demos:

### `examples/simple/`
- Pure Python example with a background thread enqueuing email jobs while a worker
  (created via `create_worker("email")`) processes them.
- Handlers live in `examples.simple.aq_worker.handlers` and are picked up through
  `AQWorker(include_packages=["examples.simple.aq_worker"])`, so you can keep job logic
  in separate modules without manual registration.
- Run `python -m examples.simple.main` to start the enqueue loop and worker concurrently
  or use the CLI (`aqworker start email examples/simple/worker.py`) in separate shells.

### `examples/simple_fastapi/`
- FastAPI service exposing REST endpoints for enqueuing jobs plus health/metadata
  routes (`/handlers`, `/workers`, `/jobs/queues/{queue}/stats`).
- Worker definition in `examples/simple_fastapi/worker.py` registers worker classes and
  auto-discovers handlers via `include_packages=["examples.simple_fastapi.handlers"]`.
- `examples/simple_fastapi/handlers.py` simulates real work with 0.5–2 s `asyncio.sleep`
  delays so you can observe concurrent processing.
- `examples/simple_fastapi/client.py` is an `httpx` script that continuously enqueues
  email + notification jobs every ~0.2 s—perfect for smoke-testing the API while
  watching worker logs scroll.

## Contributing

When adding new features:
1. Follow async/await patterns
2. Add proper error handling
3. Update documentation
4. Add tests
5. Consider performance impact

## License

This project is licensed under the terms specified in the LICENSE file.
