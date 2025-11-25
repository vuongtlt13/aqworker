# Simple AQWorker Example

This example demonstrates how to use AQWorker for background job processing without FastAPI.

## Structure

- `aq_worker/handlers.py` - Handler classes auto-discovered via `include_packages`
- `aq_worker/workers.py` - Worker classes that consume jobs from queues
- `aq_worker/job_service.py` - JobService instance
- `worker.py` - AQWorker initialization (registers workers + auto-loads handlers)
- `main.py` - Script that enqueues jobs and starts an email worker loop

## Setup

1. Install dependencies:
```bash
cd examples/simple
pip install -r requirements.txt
```

2. Make sure Redis is running:
```bash
redis-server
```

## Usage

### 1. Enqueue Jobs

Run the main script to enqueue some example jobs:

```bash
python -m examples.simple.main
```

This will enqueue email and notification jobs to their respective queues.

### 2. Run Workers

In separate terminals, run workers to process the jobs. You can use either the CLI or the Python script:

#### Using AQWorker CLI

**Option 1: Pass file path as argument**

```bash
# Terminal 1 - Email worker
aqworker start email examples/simple/worker.py

# Terminal 2 - Notification worker
aqworker start notification examples/simple/worker.py

# List available workers (loads AQWorker from worker.py)
aqworker list:aq_worker examples/simple/worker.py

# Get queue statistics
aqworker stats emails examples/simple/worker.py
```

**Option 2: Use environment variable**

```bash
# Set the AQWorker file path
export AQWORKER_FILE=examples/simple/worker.py

# Terminal 1 - Email Worker
aqworker start email

# Terminal 2 - Notification Worker
aqworker start notification

# List available workers
aqworker list:aq_worker

# Get queue statistics
aqworker stats emails
```

#### Using Python script

`examples/simple/main.py` already spins up an email worker (via `create_worker("email")`)
while a background thread enqueues jobs. Running the script is enough to see worker
processing in action.

## Example Output

When you run the main script:
```
============================================================
AQWorker Simple Example
============================================================

Enqueueing email jobs...
  ✓ Enqueued email job: abc123...
  ✓ Enqueued email job: def456...

Enqueueing notification jobs...
  ✓ Enqueued notification job: ghi789...
  ✓ Enqueued notification job: jkl012...

============================================================
Jobs enqueued successfully!
============================================================
```

When workers process jobs:
```
[EmailHandler] Sending email to user1@example.com
  Subject: Welcome!
  Body: Welcome to our service!

[NotificationHandler] Sending info notification to user 123
  Message: You have a new message
```

## Customization

You can modify:
- **Handlers** (`aq_worker/handlers.py`) - Add new `BaseHandler` subclasses; they are
  auto-registered thanks to `AQWorker(include_packages=["examples.simple.aq_worker"])`.
- **Workers** (`aq_worker/workers.py`) - Configure queue names, concurrency, poll intervals
- **Main script** (`main.py`) - Enqueue your own jobs with custom data

> Prefer decorator-based handlers? Keep using `@aq_worker.handler`—just make sure the
> module with the decorator is imported (e.g., include it in `include_packages` or import
> it explicitly) so the registration runs.

