"""
Email service for RabiesResQ.

Sends email via Gmail SMTP when MAIL_USERNAME and MAIL_PASSWORD are set.
Otherwise falls back to printing to the console (development stub).
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send_email(to_email: str, subject: str, body: str) -> None:
    username = os.getenv("MAIL_USERNAME", "").strip()
    password = os.getenv("MAIL_PASSWORD", "").strip()

    if not username or not password:
        # Development stub: print to console when SMTP is not configured.
        print("=== RabiesResQ Email (DEV - no SMTP config) ===")
        print(f"To: {to_email}")
        print(f"Subject: {subject}")
        print(body)
        print("=== End Email ===")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = username
    msg["To"] = to_email
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(username, password)
            server.sendmail(username, to_email, msg.as_string())
    except Exception as e:
        logger.exception("Failed to send email to %s: %s", to_email, e)
        raise
