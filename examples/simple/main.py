"""
Simple example demonstrating AQWorker job processing without FastAPI.

This example shows:
- A background thread continuously enqueues email jobs
- Main thread runs the email aq_worker to process jobs
"""

import asyncio
import signal
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import suppress

from worker import aq_worker

# Global flag to control the enqueue loop
running = True
job_counter = 0
main_loop = None


def enqueue_email_jobs():
    """Background thread function that continuously enqueues email jobs."""
    global running, job_counter, main_loop

    print("[Enqueue Thread] Started enqueueing email jobs...")

    while running:
        try:
            if main_loop is None or main_loop.is_closed():
                time.sleep(0.1)
                continue

            job_counter += 1

            # Use run_coroutine_threadsafe to run coroutine in main thread's event loop
            future = asyncio.run_coroutine_threadsafe(
                aq_worker.job_service.enqueue_job(
                    handler="email",
                    queue_name="emails",
                    data={
                        "recipient": f"user{job_counter}@example.com",
                        "subject": f"Email #{job_counter}",
                        "body": f"This is email number {job_counter}",
                    },
                ),
                main_loop,
            )

            # Wait for the result (with timeout)
            try:
                job = future.result(timeout=5)
                print(f"[Enqueue Thread] ✓ Enqueued email job #{job_counter}: {job.id}")
            except FutureTimeoutError:
                print(f"[Enqueue Thread] Timeout enqueueing job #{job_counter}")
            except Exception as e:
                print(f"[Enqueue Thread] Error enqueueing job #{job_counter}: {e}")

            # Wait 2 seconds before enqueueing next job
            time.sleep(0.2)

        except Exception as e:
            print(f"[Enqueue Thread] Error: {e}")
            time.sleep(0.1)

    print("[Enqueue Thread] Stopped enqueueing jobs.")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n[Main] Received interrupt signal, shutting down...")
    running = False


async def _run_worker(worker_name: str):
    """Helper to create and run a worker."""
    worker = aq_worker.create_worker(worker_name)
    print(f"[Main] Starting {worker_name} worker...")
    print(f"[Main] Queues: {worker.queue_names}")
    print()
    await worker.run()


async def main():
    """Main function that runs the email and cron workers."""
    global running, main_loop

    # Store the event loop for the background thread
    main_loop = asyncio.get_event_loop()

    print("=" * 60)
    print("AQWorker Simple Example")
    print("=" * 60)
    print()
    print("Starting email + cron workers and enqueue thread...")
    print("Press Ctrl+C to stop\n")

    # Start background thread for enqueueing jobs
    enqueue_thread = threading.Thread(target=enqueue_email_jobs, daemon=True)
    enqueue_thread.start()

    # Give the thread a moment to start
    await asyncio.sleep(0.5)

    # Create and run workers concurrently (email + cron)
    worker_tasks = []
    try:
        worker_tasks.append(asyncio.create_task(_run_worker("email")))
        worker_tasks.append(asyncio.create_task(_run_worker("cron")))

        # Wait for workers to finish (until interrupted)
        await asyncio.gather(*worker_tasks)

    except KeyboardInterrupt:
        print("\n[Main] Worker interrupted")
    except Exception as e:
        print(f"[Main] Error: {e}")
    finally:
        running = False
        print("[Main] Shutting down...")
        # Cancel worker tasks gracefully
        for task in worker_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Exiting...")
