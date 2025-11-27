"""Example handler definitions for auto-discovery demo."""

import asyncio
import random
from datetime import datetime

from aqworker import CronJob, Job


class EmailJob(Job):
    name = "email"
    queue_name = "emails"

    async def handle(self, data: dict) -> bool:
        """Send an email."""
        recipient = data.get("recipient")
        subject = data.get("subject", "No Subject")
        body = data.get("body", "")

        print(f"[EmailJob] Sending email to {recipient}")
        print(f"  Subject: {subject}")
        print(f"  Body: {body}")

        delay = random.uniform(1.0, 3.0)
        print(f"  Processing... (will take {delay:.2f}s)")
        await asyncio.sleep(delay)

        print(f"[EmailJob] ✓ Email sent to {recipient}")
        return True


class NotificationJob(Job):
    name = "notification"
    queue_name = "notifications"

    async def handle(self, data: dict) -> bool:
        """Send a notification."""
        user_id = data.get("user_id")
        message = data.get("message", "")
        notification_type = data.get("type", "info")

        print(
            f"[NotificationJob] Sending {notification_type} notification to user {user_id}"
        )
        print(f"  Message: {message}")

        return True


class CleanupCronJob(CronJob):
    """Cron job that runs every minute to clean up old jobs."""

    name = "cleanup_jobs"
    queue_name = "cron"

    @classmethod
    def cron(cls) -> str:
        """Run every 10 seconds for demo purposes."""
        return "*/10 * * * * *"

    async def handle(self, data: dict) -> bool:
        """Perform cleanup / health report task."""
        timestamp = datetime.utcnow().isoformat()
        print(f"[CleanupCronJob] Running scheduled cleanup at {timestamp}")
        # Simulate a quick maintenance task
        await asyncio.sleep(0.2)
        print("[CleanupCronJob] ✓ Cleanup completed")
        return True
