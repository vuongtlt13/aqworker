# Simple FastAPI with AQWorker Example

A simple FastAPI application example with background job processing using AQWorker.

## Installation

Install the dependencies:

```bash
pip install -r requirements.txt
```

This includes `httpx`, which is used by the sample client script.

## Running the Application

### 1. Start the FastAPI server

First change into this example directory:

```bash
cd examples/simple_fastapi
```

Then run the application using uvicorn:

```bash
uvicorn main:app --reload
```

The API will be available at:
- API: http://localhost:8000
- Interactive API docs (Swagger UI): http://localhost:8000/docs
- Alternative API docs (ReDoc): http://localhost:8000/redoc

### 2. Start queue workers

In another terminal, also `cd examples/simple_fastapi`, then run workers to process `Job` handlers. You can use either the CLI or the Python runner:

#### Using AQWorker CLI

**Option 1: Pass file path as argument**

```bash
# Start email worker
aqworker start email examples/simple_fastapi/worker.py

# Start notification worker
aqworker start notification examples/simple_fastapi/worker.py

# Start cron worker (handles CronJob queue)
aqworker start cron examples/simple_fastapi/worker.py

# List available workers
aqworker list:aq_worker examples/simple_fastapi/worker.py

# Get queue statistics
aqworker stats emails examples/simple_fastapi/worker.py
```

**Option 2: Use environment variable**

```bash
# Set the AQWorker file path
export AQWORKER_FILE=examples/simple_fastapi/worker.py

# Now use commands without specifying the file path
aqworker start email
aqworker start notification
aqworker start cron
aqworker list:aq_worker
aqworker stats emails
```

#### Using Python script

```bash
# Run email worker
python worker_runner.py email

# Or run notification worker
python worker_runner.py notification

# (Cron worker is best run via the CLI so you can have separate processes.)
```

### 3. (Optional) Start the cron scheduler (beat)

Cron jobs (`CronJob` subclasses) require the `CronScheduler`. When you run the FastAPI app, the scheduler is started via the lifespan context (see `main.py`). If you want to run it separately—e.g., without the API—you can use the CLI:

```bash
# Using file argument
cd examples/simple_fastapi
aqworker beat examples/simple_fastapi/worker.py --check-interval 0.5

# Or rely on AQWORKER_FILE
export AQWORKER_FILE=examples/simple_fastapi/worker.py
aqworker beat

# Prefer Python instead of CLI? Use the helper script (wraps the same CLI logic)
python run_beat.py --check-interval 0.5
```

Each `CronJob` must define `queue_name` and a `cron()` expression (5- or 6-field cron). The scheduler defaults to a 0.1s check interval to support second-level expressions (e.g., `"*/10 * * * * *"`).

### Cron job demo

`handlers.py` defines two `CronJob` classes:

- `DailyReportCronJob` (`queue_name="cron"`, cron: `*/10 * * * * *`) generates reports every 10 seconds for demo purposes.
- `HealthCheckCronJob` (`queue_name="cron"`, cron: `*/10 * * * * *`) performs a health check every 10 seconds.

Once the `cron` worker (step 2) and the scheduler/beat (step 3) are running, you'll see log lines from these cron jobs even if no API calls are happening.

> ### Cron expression primer
>
> AQWorker automatically detects whether your cron expression has 5 fields (minute-level) or 6 fields (seconds-level). When it has six fields we call `croniter(..., second_at_beginning=True)` which means the **first token represents seconds**. Examples:
>
> | Cadence               | Expression        | Notes                                                     |
> |-----------------------|-------------------|-----------------------------------------------------------|
> | Every 10 seconds      | `*/10 * * * * *`  | Seconds field first. Runs at 0s, 10s, 20s, ...            |
> | Every minute at zero  | `0 * * * * *`     | Six-field variant of “top of every minute.”               |
> | Every 5 minutes       | `*/5 * * * *`     | Classic 5-field expression (minutes).                     |
> | Weekdays at 09:00     | `0 9 * * MON-FRI` | No seconds field; standard cron.                          |
>
> **Reminder:** make sure you really mean to use seconds when supplying six fields. A misplaced sixth token can make your cron job run far more frequently than intended.

### 4. Trigger jobs via the sample client

From a new terminal (also in `examples/simple_fastapi`):

With the FastAPI server running on `http://localhost:8000`, execute:

```bash
python client.py
```

This continuously enqueues email + notification jobs every ~0.2s until you press
`Ctrl+C`, letting you watch the workers drain the queues in real time.

## Endpoints

### API Endpoints

- `GET /` - Root endpoint with worker information
- `GET /health` - Health check endpoint
- `GET /workers` - List all registered workers
- `GET /handlers` - List all registered handlers
- `GET /jobs/queues/{queue_name}/stats` - Get queue statistics

### Job Endpoints

- `POST /jobs/email` - Enqueue an email job
  ```json
  {
    "recipient": "user@example.com",
    "subject": "Hello",
    "body": "This is a test email"
  }
  ```

- `POST /jobs/notification` - Enqueue a notification job
  ```json
  {
    "user_id": "123",
    "message": "You have a new message",
    "type": "info"
  }
  ```

## Example Usage

### 1. Enqueue an email job

```bash
curl -X POST "http://localhost:8000/jobs/email" \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "user@example.com",
    "subject": "Welcome",
    "body": "Welcome to our service!"
  }'
```

### 2. Enqueue a notification job

```bash
curl -X POST "http://localhost:8000/jobs/notification" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123",
    "message": "Your order has been shipped",
    "type": "success"
  }'
```

### 3. Check queue statistics

```bash
curl "http://localhost:8000/jobs/queues/emails/stats"
```

## Architecture

- **Jobs (`Job`)**: One-off handlers (e.g., `EmailJob`, `NotificationJob`). Each class must set `name` and `queue_name`.
- **Cron jobs (`CronJob`)**: Scheduled handlers (`DailyReportCronJob`, `HealthCheckCronJob`) that expose `cron()` expressions. Enqueued automatically by `CronScheduler` or the `aqworker beat` CLI.
- **AQWorker**: Manages the handler registry, worker registry, and the shared `JobService`.
- **Workers**: Background consumers for specific queue sets (`EmailWorker`, `NotificationWorker`, `CronWorker`).
- **JobService**: Talks to Redis for enqueue/dequeue/stats.

## Project Structure

```
simple_fastapi/
├── main.py              # FastAPI application with endpoints
├── workers.py           # Worker class definitions
├── handlers.py          # Handler class definitions (auto-discovered, simulate 0.5-2s work)
├── client.py            # Simple HTTPX client to enqueue demo jobs
├── worker.py            # AQWorker setup (registers workers + include_packages)
├── worker_runner.py     # Script to run workers
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

`worker.py` configures `AQWorker(include_packages=["examples.simple_fastapi.handlers"])`,
so every subclass of `Job`/`CronJob` in `handlers.py` is auto-registered as long as it sets
`name` and `queue_name`. Prefer decorators? Use `@aq_worker.job(queue_name="...")` or
`@aq_worker.cronjob(cron="*/5 * * * * *", queue_name="...")` and ensure the module gets
imported so registration runs.
