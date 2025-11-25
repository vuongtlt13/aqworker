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
                    queue_name="emails",
                    handler="email",
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


async def main():
    """Main function that runs the email aq_worker."""
    global running, main_loop

    # Store the event loop for the background thread
    main_loop = asyncio.get_event_loop()

    print("=" * 60)
    print("AQWorker Simple Example")
    print("=" * 60)
    print()
    print("Starting email aq_worker and enqueue thread...")
    print("Press Ctrl+C to stop\n")

    # Start background thread for enqueueing jobs
    enqueue_thread = threading.Thread(target=enqueue_email_jobs, daemon=True)
    enqueue_thread.start()

    # Give the thread a moment to start
    await asyncio.sleep(0.5)

    # Create and run email aq_worker in main thread
    try:
        email_worker = aq_worker.create_worker("email")
        print(f"[Main] Starting email aq_worker...")
        print(f"[Main] Queues: {email_worker.queue_names}")
        print()

        # Run the aq_worker (this will block until interrupted)
        await email_worker.run()

    except KeyboardInterrupt:
        print("\n[Main] Worker interrupted")
    except Exception as e:
        print(f"[Main] Error: {e}")
    finally:
        running = False
        print("[Main] Shutting down...")


if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Exiting...")
