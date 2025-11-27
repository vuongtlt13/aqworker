"""
Example handlers for the simple FastAPI application.
"""

import asyncio
import random
from datetime import datetime

from aqworker import CronJob, Job


class EmailJob(Job):
    """Handler for sending emails."""

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

        delay = random.uniform(0.5, 2.0)
        print(f"  Processing... (will take {delay:.2f}s)")
        await asyncio.sleep(delay)

        print(f"[EmailJob] ✓ Email sent to {recipient}")
        return True


class NotificationJob(Job):
    """Handler for sending notifications."""

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

        delay = random.uniform(0.5, 2.0)
        print(f"  Processing... (will take {delay:.2f}s)")
        await asyncio.sleep(delay)

        print(f"[NotificationJob] ✓ Notification sent to user {user_id}")
        return True


class DailyReportCronJob(CronJob):
    """Cron job that runs daily at midnight to generate daily reports."""

    name = "daily_report"
    queue_name = "cron"

    @classmethod
    def cron(cls) -> str:
        """Run every 10 seconds for demo purposes."""
        return "*/10 * * * * *"

    async def handle(self, data: dict) -> bool:
        """Generate and send daily report."""
        print(f"[DailyReportCronJob] Generating daily report at {datetime.now()}")
        print("  Collecting statistics...")

        # Simulate report generation
        await asyncio.sleep(1.0)

        print("  ✓ Daily report generated successfully")
        return True


class HealthCheckCronJob(CronJob):
    """Cron job that runs every 5 minutes to check system health."""

    name = "health_check"
    queue_name = "cron"

    @classmethod
    def cron(cls) -> str:
        """Run every 10 seconds for demo purposes."""
        return "*/10 * * * * *"

    async def handle(self, data: dict) -> bool:
        """Perform health check."""
        print(f"[HealthCheckCronJob] Performing health check at {datetime.now()}")

        # Simulate health check
        await asyncio.sleep(0.5)

        print("  ✓ System health check completed")
        return True
