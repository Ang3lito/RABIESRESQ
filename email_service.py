"""
Email service stub.

RabiesResQ auth flow (per current requirements) does NOT use OTP or email verification.
This module exists only to avoid import errors if you later add notifications or emails.
"""


def send_email(to_email: str, subject: str, body: str) -> None:
    # Development-friendly stub: print emails to the server console.
    print("=== RabiesResQ Email (DEV) ===")
    print(f"To: {to_email}")
    print(f"Subject: {subject}")
    print(body)
    print("=== End Email ===")

