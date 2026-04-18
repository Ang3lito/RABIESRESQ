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
    # Ensure appointments.patient_hidden exists (for older DBs before "hide appointment" feature).
    try:
        conn.execute("SELECT patient_hidden FROM appointments LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE appointments ADD COLUMN patient_hidden INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        except sqlite3.OperationalError:
            # If appointments table itself doesn't exist yet, schema/init will create it later.
            pass
    try:
        # Ensure cases WHO category columns exist (for older DBs before WHO classifier).
        try:
            conn.execute("SELECT who_category_auto FROM cases LIMIT 1")
        except sqlite3.OperationalError:
            try:
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_auto TEXT")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_final TEXT")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_reasons_json TEXT")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_version TEXT")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_overridden_by_user_id INTEGER")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_overridden_at TEXT")
                conn.execute("ALTER TABLE cases ADD COLUMN who_category_override_reason TEXT")
                # Backfill from existing fields when available.
                conn.execute(
                    """
                    UPDATE cases
                    SET who_category_auto = COALESCE(NULLIF(TRIM(risk_level), ''), NULLIF(TRIM(category), '')),
                        who_category_final = COALESCE(NULLIF(TRIM(risk_level), ''), NULLIF(TRIM(category), ''))
                    WHERE (who_category_auto IS NULL OR TRIM(who_category_auto) = '')
                       OR (who_category_final IS NULL OR TRIM(who_category_final) = '')
                    """
                )
                conn.commit()
            except sqlite3.OperationalError:
                # If cases table itself doesn't exist yet, schema/init will create it later.
                pass

        # Ensure cases.animal_vaccination exists (for analytics/filtering parity with pre-screening form).
        _ensure_cases_animal_vaccination_column(conn)

        _ensure_clinics_operating_hours_column(conn)
        _ensure_user_session_logs_table(conn)
    finally:
        conn.close()


def _ensure_cases_animal_vaccination_column(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT animal_vaccination FROM cases LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE cases ADD COLUMN animal_vaccination TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass


def _ensure_clinics_operating_hours_column(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT operating_hours_json FROM clinics LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE clinics ADD COLUMN operating_hours_json TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass


def _ensure_user_session_logs_table(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT 1 FROM user_session_logs LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_session_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role_at_login TEXT NOT NULL,
                logged_in_at TEXT NOT NULL,
                logged_out_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_session_logs_user_id ON user_session_logs(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_session_logs_logged_in ON user_session_logs(logged_in_at DESC)"
        )
        conn.commit()

