# Simple AQWorker Example

This example demonstrates how to use AQWorker for background job processing without FastAPI,
using the new `Job`/`CronJob` handler types and the `queue_name`-first configuration.

## Structure

- `aq_worker/handlers.py` - `Job` classes auto-discovered via `include_packages`
- `aq_worker/workers.py` - Worker classes that consume jobs from queues
- `aq_worker/job_service.py` - JobService instance
- `worker.py` - AQWorker initialization (registers workers + auto-loads handlers)
- `main.py` - Script that enqueues jobs and starts an email worker loop

## Setup

1. Install dependencies (inside this example directory):
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

From the `examples/simple` directory, run the main script to enqueue some example jobs:

```bash
python -m examples.simple.main
```

This will enqueue email and notification jobs to their respective queues.

### 2. Run workers

Open additional terminals, `cd examples/simple` in each one, then run workers to process the jobs. You can use either the CLI or the Python script:

#### Using AQWorker CLI

**Option 1: Pass file path as argument**

```bash
# Terminal 1 - Email worker
aqworker start email examples/simple/worker.py

# Terminal 2 - Notification worker
aqworker start notification examples/simple/worker.py

# Terminal 3 - Cron worker (processes CronJob queue)
aqworker start cron examples/simple/worker.py

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

# Terminal 3 - Cron Worker
aqworker start cron

# List available workers
aqworker list:aq_worker

# Get queue statistics
aqworker stats emails
```

#### Using Python script

`examples/simple/main.py` already spins up an email worker (via `create_worker("email")`)
while a background thread enqueues jobs. Running the script is enough to see worker
processing in action. (Cron jobs are best run via the CLI instructions above.)

### 3. Run the cron scheduler

`aq_worker/handlers.py` now includes `CleanupCronJob`, a `CronJob` that enqueues a maintenance task every 10 seconds onto the `cron` queue (easy to observe in logs). To enqueue those jobs automatically, run the scheduler (beat) from yet another terminal:

```bash
cd examples/simple
aqworker beat worker.py --check-interval 0.5
```

This command loads the same AQWorker instance, watches all registered `CronJob`s, and enqueues them at their scheduled times. Ensure the `cron` worker from step 2 is running so those jobs are processed.

Prefer a Python script instead of the CLI? Use the included runner (which internally
invokes the same CLI helper):

```bash
cd examples/simple
python run_beat.py
```

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

### 3. (Optional) Run the cron scheduler

This simple example focuses on regular jobs, but you can add `CronJob` subclasses inside
`aq_worker/handlers.py` and run the scheduler via:

```bash
aqworker beat examples/simple/worker.py --check-interval 0.5
```

The scheduler requires every handler to define `queue_name` and a cron expression. You can
also embed `CronScheduler` inside your own process if you prefer not to use the CLI.

Prefer a Python script instead of the CLI? Use the included runner (wraps the same CLI helper):

```bash
cd examples/simple
python run_beat.py
```

> #### Cron expression primer
>
> The sample `CleanupCronJob` uses a 6-field cron expression to run every 10 seconds: `*/10 * * * * *`. AQWorker auto-detects six fields and calls `croniter(..., second_at_beginning=True)` so the **first field represents seconds**. Other examples you can try:
>
> | Cadence               | Expression        | Notes                                                     |
> |-----------------------|-------------------|-----------------------------------------------------------|
> | Every 10 seconds      | `*/10 * * * * *`  | Seconds field first.                                      |
> | Every minute at zero  | `0 * * * * *`     | Six-field format; runs once per minute.                   |
> | Every 5 minutes       | `*/5 * * * *`     | Standard 5-field cron (minutes).                          |
> | Daily at 02:30        | `0 30 2 * *`      | Classic cron (no seconds).                                |
>
> **Tip:** When you intend minute-level scheduling, stick to five fields. Accidentally supplying six fields will treat the first token as seconds and may dramatically increase the job frequency.

## Customization

You can modify:
- **Handlers** (`aq_worker/handlers.py`) - Add new `Job`/`CronJob` subclasses with `name`
  and `queue_name`. They are auto-registered because `AQWorker(include_packages=["aq_worker"])`
  imports the module on startup. Decorator lovers can use `@aq_worker.job(queue_name="...")`
  or `@aq_worker.cronjob(cron="*/5 * * * * *", queue_name="...")`; just ensure the module is
  imported so registration runs.
- **Workers** (`aq_worker/workers.py`) - Configure queue names, concurrency, poll intervals.
- **Main script** (`main.py`) - Enqueue your own jobs with custom payloads via
  `JobService.enqueue_job(handler_class_or_name, queue_name=..., data=...)`.

