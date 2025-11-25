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

Run the application using uvicorn:

```bash
uvicorn main:app --reload
```

The API will be available at:
- API: http://localhost:8000
- Interactive API docs (Swagger UI): http://localhost:8000/docs
- Alternative API docs (ReDoc): http://localhost:8000/redoc

### 2. Start a worker

In a separate terminal, run a worker to process jobs. You can use either the CLI or the Python script:

#### Using AQWorker CLI

**Option 1: Pass file path as argument**

```bash
# Start email worker
aqworker start email examples/simple_fastapi/worker.py

# Start notification worker
aqworker start notification examples/simple_fastapi/worker.py

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
aqworker list:aq_worker
aqworker stats emails
```

#### Using Python script

```bash
# Run email worker
python worker_runner.py email

# Or run notification worker
python worker_runner.py notification
```

### 3. Trigger jobs via the sample client

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

- **AQWorker**: Main instance managing workers, handlers, and job service
- **Workers**: Background processes that consume jobs from queues
- **Handlers**: Business logic for processing specific job types
- **JobService**: Manages job queue operations (enqueue, dequeue, stats)

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
so any handler classes you add under `handlers.py` (or submodules) are registered
automatically. Decorator-based handlers remain supported—just import the module or add it
to `include_packages` so the decorator executes.
