import os
import sqlite3

from flask import current_app, g


def get_db():
    if "db" not in g:
        db_path = current_app.config.get("DATABASE")
        if not db_path:
            os.makedirs(current_app.instance_path, exist_ok=True)
            db_path = os.path.join(current_app.instance_path, "rabiesresq.sqlite")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        g.db = conn
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

def init_app(app):
    app.teardown_appcontext(close_db)

    # Ensure password_reset_codes exists (for existing DBs before OTP flow).
    db_path = app.config.get("DATABASE")
    if not db_path:
        os.makedirs(app.instance_path, exist_ok=True)
        db_path = os.path.join(app.instance_path, "rabiesresq.sqlite")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("SELECT 1 FROM password_reset_codes LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS password_reset_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                is_used INTEGER NOT NULL DEFAULT 0 CHECK(is_used IN (0,1)),
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_password_reset_codes_email_expires ON password_reset_codes(email, expires_at)"
        )
        conn.commit()
    try:
        conn.execute("SELECT 1 FROM availability_slots LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS availability_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clinic_id INTEGER NOT NULL,
                slot_datetime TEXT NOT NULL,
                duration_minutes INTEGER NOT NULL DEFAULT 45,
                max_bookings INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(clinic_id, slot_datetime),
                FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE CASCADE
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_availability_slots_clinic_datetime ON availability_slots(clinic_id, slot_datetime)"
        )
        conn.commit()
    finally:
        conn.close()

