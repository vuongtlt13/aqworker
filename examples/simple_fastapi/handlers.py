"""
Example handlers for the simple FastAPI application.
"""

import asyncio
import random

from aqworker import BaseHandler


class EmailHandler(BaseHandler):
    """Handler for sending emails."""

    name = "email"

    async def handle(self, data: dict) -> bool:
        """Send an email."""
        recipient = data.get("recipient")
        subject = data.get("subject", "No Subject")
        body = data.get("body", "")

        print(f"[EmailHandler] Sending email to {recipient}")
        print(f"  Subject: {subject}")
        print(f"  Body: {body}")

        delay = random.uniform(0.5, 2.0)
        print(f"  Processing... (will take {delay:.2f}s)")
        await asyncio.sleep(delay)

        print(f"[EmailHandler] ✓ Email sent to {recipient}")
        return True


class NotificationHandler(BaseHandler):
    """Handler for sending notifications."""

    name = "notification"

    async def handle(self, data: dict) -> bool:
        """Send a notification."""
        user_id = data.get("user_id")
        message = data.get("message", "")
        notification_type = data.get("type", "info")

        print(
            f"[NotificationHandler] Sending {notification_type} notification to user {user_id}"
        )
        print(f"  Message: {message}")

        delay = random.uniform(0.5, 2.0)
        print(f"  Processing... (will take {delay:.2f}s)")
        await asyncio.sleep(delay)

        print(f"[NotificationHandler] ✓ Notification sent to user {user_id}")
        return True
