import os
import secrets
import sqlite3
import string
from datetime import date, datetime, timedelta
import io
import json

import click
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for, make_response
from werkzeug.security import generate_password_hash

from auth import login_required, role_required
from db import get_db, init_app as init_db_app
from email_service import send_email


class SimplePagination:
    def __init__(self, items, page: int, per_page: int, total: int):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total
        self.pages = max((total + per_page - 1) // per_page, 1)
        if total == 0:
            self.first = 0
            self.last = 0
        else:
            self.first = (page - 1) * per_page + 1
            self.last = min(page * per_page, total)
        self.has_prev = page > 1
        self.has_next = page < self.pages
        self.prev_num = page - 1 if self.has_prev else None
        self.next_num = page + 1 if self.has_next else None

    def iter_pages(self):
        return range(1, self.pages + 1)


def _affected_area_tokens(affected_area: str | None) -> list[str]:
    """Split stored affected_area (comma/semicolon-separated) into trimmed tokens."""
    if not affected_area:
        return []
    parts: list[str] = []
    for chunk in affected_area.replace(";", ",").split(","):
        c = chunk.strip()
        if c:
            parts.append(c)
    return parts


def _age_from_iso_date(dob_str: str | None) -> int | None:
    """Full years from ISO date string (YYYY-MM-DD) to today, or None if invalid."""
    if not dob_str:
        return None
    try:
        d = date.fromisoformat(dob_str.strip()[:10])
    except ValueError:
        return None
    today = date.today()
    age = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
    return max(0, age)


def classify_pre_screening_risk(
    type_of_exposure: str | None,
    affected_area: str | None,
    wound_description: str | None,
    bleeding_type: str | None,
    animal_status: str | None,
    animal_vaccination: str | None,
    patient_prev_immunization: str | None,
) -> str:
    """
    Classify rabies exposure risk into Category I / II / III
    using simple rule-based logic aligned with common guidelines.
    """
    type_of_exposure = (type_of_exposure or "").strip()
    affected_area = (affected_area or "").strip()
    wound_description = (wound_description or "").strip()
    bleeding_type = (bleeding_type or "").strip()
    animal_status = (animal_status or "").strip()
    animal_vaccination = (animal_vaccination or "").strip()
    patient_prev_immunization = (patient_prev_immunization or "").strip()

    high_risk_exposures = {"Bite", "Contamination of Mucous Membrane"}
    high_risk_areas = {"Head/Face", "Neck"}
    severe_wounds = {"Punctured", "Lacerated", "Avulsed"}
    high_risk_animal_status = {"Sick", "Died", "Lost"}

    area_tokens = _affected_area_tokens(affected_area)
    has_high_risk_area = any(t in high_risk_areas for t in area_tokens)

    # Category III – clearly severe / high‑risk situations
    if (
        type_of_exposure in high_risk_exposures
        or has_high_risk_area
        or bleeding_type in {"Spontaneous", "Both spontaneous and induced"}
        or wound_description in severe_wounds
        or animal_status in high_risk_animal_status
    ):
        return "Category III"

    # Category II – scratches / minor wounds without clear high‑risk features
    if (
        type_of_exposure in {"Scratch", "Non-Bite"}
        or wound_description in {"Abrasion"}
        or bleeding_type == "Induced"
    ):
        return "Category II"

    # Fallback – minimal or uncertain exposure
    return "Category I"


def _count_completed_doses_in_course(course_rows: dict[int, dict]) -> int:
    """Count dose rows considered complete (date, vaccine type, given_by all set)."""
    n = 0
    for row in course_rows.values():
        dose_date = (row.get("dose_date") or "").strip()
        type_of_vaccine = (row.get("type_of_vaccine") or "").strip()
        given_by = (row.get("given_by") or "").strip()
        if dose_date and type_of_vaccine and given_by:
            n += 1
    return n


def _compute_vaccination_status_for_case(
    card_doses_by_type: dict[str, dict[int, dict]],
    risk_category_str: str | None,
) -> dict[str, object]:
    """
    Align Vaccination Status card with display course rules used by
    _build_vaccination_card_context_for_patient: booster rows first, else
    Category I -> pre-exposure, else post-exposure. If the protocol course has
    zero completed doses but another course has completions, use the course
    with the highest completed count (tie-break: booster > pre > post).
    """
    booster_rows = card_doses_by_type.get("booster") or {}
    category_lower = (risk_category_str or "").strip().lower()
    protocol_course: str
    if booster_rows:
        protocol_course = "booster"
    elif category_lower == "category i":
        protocol_course = "pre_exposure"
    else:
        protocol_course = "post_exposure"

    protocol_completed = _count_completed_doses_in_course(
        card_doses_by_type.get(protocol_course) or {}
    )

    course_order = ("booster", "pre_exposure", "post_exposure")
    counts = {c: _count_completed_doses_in_course(card_doses_by_type.get(c) or {}) for c in course_order}
    max_count = max(counts.values())

    display_course = protocol_course
    if protocol_completed == 0 and max_count > 0:
        for c in course_order:
            if counts[c] == max_count:
                display_course = c
                break

    if display_course == "booster":
        schedule_days = [0, 3]
        dose_type_label = "Booster Dose"
    elif display_course == "pre_exposure":
        schedule_days = [0, 7, 28]
        dose_type_label = "Pre-Exposure Dose"
    else:
        schedule_days = [0, 3, 7, 14, 28]
        dose_type_label = "Post-Exposure Dose"

    expected_doses = len(schedule_days)
    active_rows = card_doses_by_type.get(display_course) or {}
    doses_completed = _count_completed_doses_in_course(active_rows)
    progress_pct = (
        min(round((doses_completed / expected_doses) * 100), 100) if expected_doses else 0
    )

    day0_row = active_rows.get(0)
    day0_raw = ((day0_row or {}).get("dose_date") or "").strip() if day0_row else ""
    day0_date = None
    if day0_raw:
        try:
            day0_date = datetime.fromisoformat(day0_raw).date()
        except ValueError:
            day0_date = None

    next_appointment_display = None
    next_due_date = None
    for day in schedule_days:
        row = active_rows.get(day)
        dose_date_raw = ((row or {}).get("dose_date") or "").strip() if row else ""
        type_of_vaccine = ((row or {}).get("type_of_vaccine") or "").strip() if row else ""
        given_by = ((row or {}).get("given_by") or "").strip() if row else ""

        if dose_date_raw and type_of_vaccine and given_by:
            continue

        if dose_date_raw:
            try:
                next_due_date = datetime.fromisoformat(dose_date_raw).date()
            except ValueError:
                next_due_date = None
        elif day0_date and day > 0:
            next_due_date = day0_date + timedelta(days=day)

        if next_due_date:
            break

    if next_due_date:
        next_appointment_display = next_due_date.strftime("%B %d, %Y")

    return {
        "display_course": display_course,
        "dose_type_label": dose_type_label,
        "doses_completed": doses_completed,
        "expected_doses": expected_doses,
        "progress_pct": progress_pct,
        "next_appointment_display": next_appointment_display,
        "next_due_date": next_due_date,
    }


def _total_completed_doses_all_courses(
    card_doses_by_type: dict[str, dict[int, dict]],
) -> int:
    """Total completed doses across booster, pre-exposure, and post-exposure."""
    return sum(
        _count_completed_doses_in_course(card_doses_by_type.get(c) or {})
        for c in ("booster", "pre_exposure", "post_exposure")
    )


def _next_vaccination_due_date(
    card_doses_by_type: dict[str, dict[int, dict]],
    risk_category_str: str | None,
) -> date | None:
    """Next incomplete schedule due date for the resolved display course, if any."""
    return _compute_vaccination_status_for_case(
        card_doses_by_type, risk_category_str
    ).get("next_due_date")  # type: ignore[return-value]


def create_app():
    load_dotenv()

    app = Flask(__name__, instance_relative_config=True)

    secret_key = os.getenv("SECRET_KEY")
    if not secret_key:
        raise RuntimeError("SECRET_KEY is required. Set it in your environment or .env file.")
    app.config["SECRET_KEY"] = secret_key

    database = os.getenv("DATABASE")
    if database:
        app.config["DATABASE"] = database

    # DB teardown
    init_db_app(app)

    def _ensure_patient_onboarding_column():
        # Lightweight migration for existing DBs
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(patients)").fetchall()}
        if "onboarding_completed" not in cols:
            db.execute("ALTER TABLE patients ADD COLUMN onboarding_completed INTEGER NOT NULL DEFAULT 0")
            db.commit()

    def _migrate_patients_for_dependents():
        db = get_db()
        table_sql_row = db.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'patients'"
        ).fetchone()
        if table_sql_row is None or not table_sql_row["sql"]:
            return

        table_sql = table_sql_row["sql"].lower()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(patients)").fetchall()}
        has_relationship = "relationship_to_user" in cols
        has_user_unique = "user_id integer not null unique" in table_sql

        if not has_user_unique and has_relationship:
            return

        db.execute("PRAGMA foreign_keys = OFF")
        try:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS patients_new (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  first_name TEXT,
                  last_name TEXT,
                  phone_number TEXT,
                  address TEXT,
                  date_of_birth TEXT,
                  age INTEGER,
                  gender TEXT,
                  allergies TEXT,
                  pre_existing_conditions TEXT,
                  current_medications TEXT,
                  notification_settings TEXT,
                  relationship_to_user TEXT NOT NULL DEFAULT 'Self',
                  onboarding_completed INTEGER NOT NULL DEFAULT 0 CHECK(onboarding_completed IN (0,1)),
                  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )

            if has_relationship:
                db.execute(
                    """
                    INSERT INTO patients_new (
                      id, user_id, first_name, last_name, phone_number, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, relationship_to_user, onboarding_completed
                    )
                    SELECT
                      id, user_id, first_name, last_name, phone_number, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, COALESCE(relationship_to_user, 'Self'),
                      COALESCE(onboarding_completed, 0)
                    FROM patients
                    """
                )
            else:
                db.execute(
                    """
                    INSERT INTO patients_new (
                      id, user_id, first_name, last_name, phone_number, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, relationship_to_user, onboarding_completed
                    )
                    SELECT
                      id, user_id, first_name, last_name, phone_number, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, 'Self', COALESCE(onboarding_completed, 0)
                    FROM patients
                    """
                )

            db.execute("DROP TABLE patients")
            db.execute("ALTER TABLE patients_new RENAME TO patients")
            db.execute("CREATE INDEX IF NOT EXISTS idx_patients_user_id ON patients(user_id)")
            db.commit()
        finally:
            db.execute("PRAGMA foreign_keys = ON")

    def _ensure_appointments_patient_hidden_column():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(appointments)").fetchall()}
        if "patient_hidden" not in cols:
            db.execute(
                """
                ALTER TABLE appointments
                ADD COLUMN patient_hidden INTEGER NOT NULL DEFAULT 0
                CHECK(patient_hidden IN (0,1))
                """
            )
            db.commit()

    def _ensure_vaccination_card_tables():
        db = get_db()
        db.execute("""
            CREATE TABLE IF NOT EXISTS vaccination_card (
                case_id INTEGER PRIMARY KEY,
                anti_rabies TEXT,
                pvrv TEXT,
                pcec_batch TEXT,
                pcec_mfg_date TEXT,
                pcec_expiry TEXT,
                erig_hrig TEXT,
                tetanus_prophylaxis TEXT,
                tetanus_toxoid TEXT,
                ats TEXT,
                htig TEXT,
                remarks TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS vaccination_card_doses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                record_type TEXT NOT NULL CHECK(record_type IN ('pre_exposure','post_exposure','booster')),
                day_number INTEGER NOT NULL,
                dose_date TEXT,
                type_of_vaccine TEXT,
                dose TEXT,
                route_site TEXT,
                given_by TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE
            )
        """)
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_vaccination_card_doses_case_type ON vaccination_card_doses(case_id, record_type)"
        )
        db.commit()

    def _ensure_patient_notifications_table():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS patient_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                source_id INTEGER,
                message TEXT,
                created_at TEXT NOT NULL,
                is_read INTEGER NOT NULL DEFAULT 0 CHECK(is_read IN (0,1)),
                FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE
            )
            """
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_patient_notifications_patient_type_read ON patient_notifications(patient_id, type, is_read)"
        )
        db.commit()

    def _ensure_user_security_columns():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        if "must_change_password" not in cols:
            db.execute(
                """
                ALTER TABLE users
                ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0
                CHECK(must_change_password IN (0,1))
                """
            )
            db.commit()

    def _ensure_pending_emails_table():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                to_email TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'sent', 'failed')),
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_emails_status_created ON pending_emails(status, created_at)"
        )
        db.commit()

    with app.app_context():
        _ensure_patient_onboarding_column()
        _migrate_patients_for_dependents()
        _ensure_appointments_patient_hidden_column()
        _ensure_vaccination_card_tables()
        _ensure_patient_notifications_table()
        _ensure_user_security_columns()
        _ensure_pending_emails_table()

    # #region agent log helper
    def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict | None = None):
        try:
            payload = {
                "sessionId": "dd574b",
                "runId": run_id,
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data or {},
                "timestamp": int(datetime.now().timestamp() * 1000),
            }
            # Use explicit absolute path so debug file is created reliably
            log_path = r"c:\Users\angelo02\OneDrive\Desktop\RABIESRESQ\debug-dd574b.log"
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception:
            # Logging must never break app behavior
            pass
    # #endregion agent log helper

    def _get_primary_patient(user_id: int):
        db = get_db()
        return db.execute(
            """
            SELECT p.*, u.username, u.email
            FROM patients p
            JOIN users u ON u.id = p.user_id
            WHERE p.user_id = ?
            ORDER BY CASE
                WHEN LOWER(COALESCE(p.relationship_to_user, 'self')) = 'self' THEN 0
                ELSE 1
            END, p.id ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

    def _insert_patient_notification(
        patient_id: int, notif_type: str, source_id: int | None, message: str
    ) -> None:
        db = get_db()
        created_at = datetime.now().isoformat(timespec="seconds")
        db.execute(
            """
            INSERT INTO patient_notifications (patient_id, type, source_id, message, created_at, is_read)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (patient_id, notif_type, source_id, message, created_at),
        )

    def _build_unique_username(base_value: str) -> str:
        db = get_db()
        seed = "".join(ch for ch in (base_value or "").strip().lower() if ch.isalnum() or ch in {"_", "."})
        if not seed:
            seed = "patient"
        candidate = seed[:24]
        if not candidate:
            candidate = "patient"
        suffix = 0
        while True:
            username = candidate if suffix == 0 else f"{candidate[:18]}{suffix:04d}"
            exists = db.execute("SELECT 1 FROM users WHERE username = ? LIMIT 1", (username,)).fetchone()
            if not exists:
                return username
            suffix += 1

    def _generate_strong_password(length: int = 14) -> str:
        # Ensure password includes upper/lower/digit/symbol and is randomly shuffled.
        alphabet_lower = string.ascii_lowercase
        alphabet_upper = string.ascii_uppercase
        alphabet_digits = string.digits
        alphabet_symbols = "!@#$%^&*()-_=+"
        if length < 12:
            length = 12
        required = [
            secrets.choice(alphabet_lower),
            secrets.choice(alphabet_upper),
            secrets.choice(alphabet_digits),
            secrets.choice(alphabet_symbols),
        ]
        all_chars = alphabet_lower + alphabet_upper + alphabet_digits + alphabet_symbols
        required.extend(secrets.choice(all_chars) for _ in range(length - 4))
        secrets.SystemRandom().shuffle(required)
        return "".join(required)

    def _queue_pending_email(to_email: str, subject: str, body: str, last_error: str | None = None) -> None:
        db = get_db()
        db.execute(
            """
            INSERT INTO pending_emails (to_email, subject, body, retry_count, status, last_error, updated_at)
            VALUES (?, ?, ?, 0, 'pending', ?, CURRENT_TIMESTAMP)
            """,
            (to_email, subject, body, (last_error or "").strip()[:500] or None),
        )

    NO_SHOW_APPOINTMENT_NOTIFICATION_MSG = (
        "You missed a scheduled appointment. It was marked as no show because no vaccination record was updated."
    )

    def _insert_no_show_patient_notification_if_absent(
        patient_id: int, appointment_id: int
    ) -> None:
        db = get_db()
        exists = db.execute(
            """
            SELECT 1 FROM patient_notifications
            WHERE patient_id = ?
              AND type = 'appointment'
              AND source_id = ?
              AND COALESCE(message, '') = ?
            LIMIT 1
            """,
            (patient_id, appointment_id, NO_SHOW_APPOINTMENT_NOTIFICATION_MSG),
        ).fetchone()
        if exists:
            return
        _insert_patient_notification(
            patient_id=patient_id,
            notif_type="appointment",
            source_id=appointment_id,
            message=NO_SHOW_APPOINTMENT_NOTIFICATION_MSG,
        )

    def _get_patient_unread_counts(patient_user_id: int) -> dict[str, int]:
        """
        Return unread notification counts for the given user across ALL of their patients
        (self + dependents), grouped by type.
        """
        db = get_db()
        patient_rows = db.execute(
            """
            SELECT id
            FROM patients
            WHERE user_id = ?
            """,
            (patient_user_id,),
        ).fetchall()
        patient_ids = [row["id"] for row in patient_rows]
        if not patient_ids:
            return {"appointment": 0, "vaccination": 0}

        placeholders = ",".join(["?"] * len(patient_ids))
        rows = db.execute(
            f"""
            SELECT type, COUNT(*) AS n
            FROM patient_notifications
            WHERE patient_id IN ({placeholders})
              AND is_read = 0
            GROUP BY type
            """,
            patient_ids,
        ).fetchall()

        counts: dict[str, int] = {"appointment": 0, "vaccination": 0}
        for row in rows:
            notif_type = (row["type"] or "").strip()
            if notif_type in counts:
                counts[notif_type] = row["n"]
        return counts

    def _mark_patient_notifications_read(patient_user_id: int, notif_type: str) -> None:
        """
        Mark notifications of a given type as read for ALL patients under this user.
        """
        db = get_db()
        patient_rows = db.execute(
            """
            SELECT id
            FROM patients
            WHERE user_id = ?
            """,
            (patient_user_id,),
        ).fetchall()
        patient_ids = [row["id"] for row in patient_rows]
        if not patient_ids:
            return

        placeholders = ",".join(["?"] * len(patient_ids))
        params: list[object] = list(patient_ids) + [notif_type]

        db.execute(
            f"""
            UPDATE patient_notifications
            SET is_read = 1
            WHERE patient_id IN ({placeholders})
              AND type = ?
              AND is_read = 0
            """,
            params,
        )
        db.commit()

    def _notification_recipient_label(
        relationship: str | None, first_name: str | None, last_name: str | None
    ) -> str:
        rel = (relationship or "Self").strip()
        if rel.lower() == "self":
            return "For you"
        name = " ".join(
            p for p in [(first_name or "").strip(), (last_name or "").strip()] if p
        )
        return f"For {rel}: {name}" if name else f"For {rel}"

    def _get_unread_patient_notifications_for_user(
        patient_user_id: int, limit: int = 50
    ) -> tuple[list[dict], set[int], set[int]]:
        """
        Return unread notifications for all patients under this user, plus sets of
        appointment ids and case ids to highlight on the dashboard.
        """
        db = get_db()
        rows = db.execute(
            """
            SELECT
                pn.id,
                pn.type,
                pn.source_id,
                pn.message,
                pn.created_at,
                pn.is_read,
                pn.patient_id,
                p.relationship_to_user,
                p.first_name,
                p.last_name
            FROM patient_notifications pn
            JOIN patients p ON p.id = pn.patient_id
            WHERE p.user_id = ?
              AND pn.is_read = 0
            ORDER BY pn.created_at DESC
            LIMIT ?
            """,
            (patient_user_id, limit),
        ).fetchall()

        highlight_appointment_ids: set[int] = set()
        highlight_case_ids: set[int] = set()
        out: list[dict] = []
        for row in rows:
            r = dict(row)
            ntype = (r.get("type") or "").strip()
            sid = r.get("source_id")
            if ntype == "appointment" and sid is not None:
                highlight_appointment_ids.add(int(sid))
            elif ntype == "vaccination" and sid is not None:
                highlight_case_ids.add(int(sid))

            recipient_label = _notification_recipient_label(
                r.get("relationship_to_user"),
                r.get("first_name"),
                r.get("last_name"),
            )
            link_href = None
            if ntype == "appointment" and sid is not None:
                link_href = url_for("patient_appointment_view", appointment_id=int(sid))
            elif ntype == "vaccination" and sid is not None:
                ap_row = db.execute(
                    """
                    SELECT a.id
                    FROM appointments a
                    JOIN patients p ON p.id = a.patient_id
                    WHERE a.case_id = ? AND p.user_id = ?
                      AND COALESCE(a.patient_hidden, 0) = 0
                    ORDER BY datetime(a.appointment_datetime) DESC
                    LIMIT 1
                    """,
                    (int(sid), patient_user_id),
                ).fetchone()
                if ap_row:
                    link_href = url_for(
                        "patient_vaccination_card_view", appointment_id=ap_row["id"]
                    )
                else:
                    link_href = url_for("patient_vaccinations")

            out.append(
                {
                    "id": r["id"],
                    "type": ntype,
                    "source_id": sid,
                    "message": r.get("message") or "",
                    "created_at": r.get("created_at") or "",
                    "recipient_label": recipient_label,
                    "link_href": link_href,
                }
            )

        return out, highlight_appointment_ids, highlight_case_ids

    def _mark_appointment_notifications_read_for_appointment(
        patient_user_id: int, appointment_id: int
    ) -> None:
        """Mark unread appointment-type notifications for this appointment as read."""
        db = get_db()
        patient_rows = db.execute(
            "SELECT id FROM patients WHERE user_id = ?",
            (patient_user_id,),
        ).fetchall()
        patient_ids = [row["id"] for row in patient_rows]
        if not patient_ids:
            return
        placeholders = ",".join(["?"] * len(patient_ids))
        params: list[object] = list(patient_ids) + ["appointment", appointment_id]
        db.execute(
            f"""
            UPDATE patient_notifications
            SET is_read = 1
            WHERE patient_id IN ({placeholders})
              AND type = ?
              AND source_id = ?
              AND is_read = 0
            """,
            params,
        )
        db.commit()

    def _mark_vaccination_notifications_read_for_case(
        patient_user_id: int, case_id: int
    ) -> None:
        """Mark unread vaccination-type notifications for this case as read."""
        db = get_db()
        patient_rows = db.execute(
            "SELECT id FROM patients WHERE user_id = ?",
            (patient_user_id,),
        ).fetchall()
        patient_ids = [row["id"] for row in patient_rows]
        if not patient_ids:
            return
        placeholders = ",".join(["?"] * len(patient_ids))
        params: list[object] = list(patient_ids) + ["vaccination", case_id]
        db.execute(
            f"""
            UPDATE patient_notifications
            SET is_read = 1
            WHERE patient_id IN ({placeholders})
              AND type = ?
              AND source_id = ?
              AND is_read = 0
            """,
            params,
        )
        db.commit()

    def _get_staff_scheduled_appointments_count(clinic_id: int) -> int:
        """
        Count appointments that are visible in the staff Appointments page list.
        Currently, that list shows only pending/queued appointment requests.
        """
        db = get_db()
        row = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM appointments a
            WHERE a.clinic_id = ?
              AND LOWER(COALESCE(a.status, '')) IN ('pending', 'queued')
            """,
            (clinic_id,),
        ).fetchone()
        return int(row["n"] or 0)

    @app.context_processor
    def _inject_staff_scheduled_appointments_count():
        try:
            if session.get("role") != "clinic_personnel":
                return {}
            user_id = session.get("user_id")
            if not user_id:
                return {}
            db = get_db()
            staff = db.execute(
                "SELECT clinic_id FROM clinic_personnel WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if staff is None:
                return {}
            return {
                "scheduled_appointments_count": _get_staff_scheduled_appointments_count(
                    staff["clinic_id"]
                )
            }
        except Exception:
            # Never break rendering due to badge computation
            return {}

    def _run_case_status_maintenance(clinic_id: int):
        db = get_db()

        case_rows = db.execute(
            """
            SELECT id, risk_level, category, COALESCE(case_status, 'Pending') AS case_status
            FROM cases
            WHERE clinic_id = ?
            """,
            (clinic_id,),
        ).fetchall()

        case_updates = 0
        to_completed = 0
        to_pending = 0
        to_no_show = 0

        for case_row in case_rows:
            case_id = case_row["id"]
            risk_str = case_row["risk_level"] or case_row["category"] or ""

            doses_rows = db.execute(
                """
                SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                FROM vaccination_card_doses
                WHERE case_id = ?
                ORDER BY record_type, day_number
                """,
                (case_id,),
            ).fetchall()
            card_doses_by_type: dict[str, dict[int, dict]] = {
                "pre_exposure": {},
                "post_exposure": {},
                "booster": {},
            }
            for row in doses_rows:
                r = row["record_type"]
                d = row["day_number"]
                if r in card_doses_by_type:
                    card_doses_by_type[r][d] = dict(row)

            total_completed = _total_completed_doses_all_courses(card_doses_by_type)
            has_vaccination_update = total_completed > 0

            status_metrics = _compute_vaccination_status_for_case(card_doses_by_type, risk_str)
            vc_exp = int(status_metrics["expected_doses"] or 0)
            vc_done = int(status_metrics["doses_completed"] or 0)
            next_due = status_metrics.get("next_due_date")
            if next_due is not None and not isinstance(next_due, date):
                next_due = None

            has_overdue_active_appointment = db.execute(
                """
                SELECT 1
                FROM appointments
                WHERE case_id = ?
                  AND clinic_id = ?
                  AND LOWER(COALESCE(status, '')) NOT IN ('removed', 'cancelled', 'canceled')
                  AND datetime(appointment_datetime) < datetime('now', 'localtime', '-2 hours')
                ORDER BY datetime(appointment_datetime) DESC, id DESC
                LIMIT 1
                """,
                (case_id, clinic_id),
            ).fetchone() is not None

            today = date.today()
            vacc_schedule_overdue = next_due is not None and next_due < today
            no_show_eligible = (
                not has_vaccination_update
                and has_overdue_active_appointment
                and (next_due is None or vacc_schedule_overdue)
            )

            current_status = (case_row["case_status"] or "Pending").strip().lower()

            # Keep explicit/manual completion sticky.
            if current_status == "completed":
                desired_status = "Completed"
            elif vc_exp and vc_done >= vc_exp:
                desired_status = "Completed"
            elif no_show_eligible:
                desired_status = "No Show"
            else:
                # If progress exists but next schedule is missing, keep Pending so staff can schedule.
                desired_status = "Pending"
            if current_status != desired_status.lower():
                db.execute(
                    """
                    UPDATE cases
                    SET case_status = ?
                    WHERE id = ? AND clinic_id = ?
                    """,
                    (desired_status, case_id, clinic_id),
                )
                case_updates += 1

            if desired_status == "Completed":
                to_completed += 1
                db.execute(
                    """
                    UPDATE appointments
                    SET status = 'Completed'
                    WHERE id = (
                        SELECT id
                        FROM appointments
                        WHERE case_id = ? AND clinic_id = ?
                        ORDER BY datetime(appointment_datetime) DESC, id DESC
                        LIMIT 1
                    )
                    """,
                    (case_id, clinic_id),
                )
            elif desired_status == "No Show":
                to_no_show += 1
                no_show_rows = db.execute(
                    """
                    SELECT a.id, a.patient_id
                    FROM appointments a
                    WHERE a.case_id = ?
                      AND a.clinic_id = ?
                      AND LOWER(COALESCE(a.status, '')) NOT IN (
                          'removed', 'cancelled', 'canceled', 'no show', 'missed'
                      )
                      AND datetime(a.appointment_datetime) < datetime('now', 'localtime', '-2 hours')
                    """,
                    (case_id, clinic_id),
                ).fetchall()
                db.execute(
                    """
                    UPDATE appointments
                    SET status = 'No Show'
                    WHERE case_id = ?
                      AND clinic_id = ?
                      AND LOWER(COALESCE(status, '')) NOT IN (
                          'removed', 'cancelled', 'canceled', 'no show', 'missed'
                      )
                      AND datetime(appointment_datetime) < datetime('now', 'localtime', '-2 hours')
                    """,
                    (case_id, clinic_id),
                )
                for ap_row in no_show_rows:
                    _insert_no_show_patient_notification_if_absent(
                        int(ap_row["patient_id"]), int(ap_row["id"])
                    )
            else:
                to_pending += 1

        db.commit()
        return {
            "updated_cases": case_updates,
            "to_completed": to_completed,
            "to_pending": to_pending,
            "to_no_show": to_no_show,
        }

    # Auth blueprint
    from auth import bp as auth_bp

    app.register_blueprint(auth_bp)

    @app.get("/")
    def index():
        if not session.get("user_id"):
            return redirect(url_for("auth.login"))

        role = session.get("role")
        if role == "patient":
            if not session.get("patient_onboarding_done"):
                return redirect(url_for("patient_onboarding"))
            return redirect(url_for("patient_dashboard"))
        if role == "clinic_personnel":
            return redirect(url_for("staff_dashboard"))
        if role == "system_admin":
            return redirect(url_for("admin_dashboard"))

        session.clear()
        flash("Account role is invalid, contact admin.", "error")
        return redirect(url_for("auth.login"))

    @app.get("/patient/onboarding")
    @role_required("patient")
    def patient_onboarding():
        if session.get("patient_onboarding_done"):
            return redirect(url_for("patient_dashboard"))
        return render_template("patient_onboarding.html")

    @app.post("/patient/onboarding/complete")
    @role_required("patient")
    def patient_onboarding_complete():
        db = get_db()
        db.execute(
            """
            UPDATE patients
            SET onboarding_completed = 1
            WHERE user_id = ?
              AND LOWER(COALESCE(relationship_to_user, 'self')) = 'self'
            """,
            (session["user_id"],),
        )
        db.commit()
        session["patient_onboarding_done"] = True
        return redirect(url_for("patient_dashboard"))

    @app.get("/patient/dashboard")
    @role_required("patient")
    def patient_dashboard():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))
        db = get_db()
        clinic_rows = db.execute(
            """
            SELECT DISTINCT a.clinic_id
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE p.user_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
            """,
            (session["user_id"],),
        ).fetchall()
        for cr in clinic_rows:
            _run_case_status_maintenance(int(cr["clinic_id"]))

        unread_counts = _get_patient_unread_counts(session["user_id"])
        (
            dashboard_notifications,
            highlight_appointment_ids,
            highlight_case_ids,
        ) = _get_unread_patient_notifications_for_user(session["user_id"])
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        # Fetch cases for the primary self patient (for now)
        cases = db.execute(
            """
            SELECT c.*, psd.wound_description, psd.bleeding_type, psd.local_treatment,
                   psd.patient_prev_immunization, psd.prev_vaccine_date, psd.tetanus_date,
                   psd.hrtig_immunization
            FROM cases c
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE c.patient_id = ?
            ORDER BY c.created_at DESC
            """,
            (patient["id"],),
        ).fetchall()

        # Fetch appointments for all patients under this user (self + dependents)
        all_appointments_rows = db.execute(
            """
            SELECT
                a.*,
                c.type_of_exposure,
                c.exposure_date,
                c.risk_level,
                c.category AS case_category,
                p.first_name AS victim_first_name,
                p.last_name AS victim_last_name,
                p.relationship_to_user AS victim_relationship
            FROM appointments a
            JOIN cases c ON c.id = a.case_id
            JOIN patients p ON p.id = a.patient_id
            WHERE p.user_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
            ORDER BY a.appointment_datetime DESC
            """,
            (session["user_id"],),
        ).fetchall()

        # Optional status filter for dashboard chips
        status_filter = (request.args.get("status") or "").strip().lower()

        def _bucket_status(row: sqlite3.Row) -> str:
            status_value = (row["status"] or "").strip().lower()
            if status_value in ("cancelled", "canceled", "removed"):
                return "canceled"
            if status_value in ("completed",):
                return "completed"
            if status_value in ("no show", "missed"):
                return "missed"
            if status_value in ("pending", "queued"):
                return "pending"
            # Treat all other active / future-like statuses as "scheduled"
            return "scheduled"

        if status_filter in ("pending", "completed", "canceled", "scheduled", "missed"):
            filtered_rows = [
                row for row in all_appointments_rows if _bucket_status(row) == status_filter
            ]
        else:
            filtered_rows = list(all_appointments_rows)

        # Build per-account appointment sequence numbers (all patients, all statuses, non-hidden)
        sorted_for_sequence = sorted(
            all_appointments_rows,
            key=lambda r: (r["appointment_datetime"] or ""),
        )
        appointment_number_map: dict[int, int] = {}
        seq = 0
        for row in sorted_for_sequence:
            seq += 1
            appointment_number_map[row["id"]] = seq

        # Enrich appointments with display_time, display_date, display_type, display_dosage_label, appointment_number
        vaccination_cache: dict[int, dict] = {}

        def _compute_vaccination_summary(
            case_id: int, risk_level: str | None, case_category: str | None
        ) -> dict:
            if case_id in vaccination_cache:
                return vaccination_cache[case_id]

            vc_row = db.execute(
                "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
            ).fetchone()
            vaccination_card = dict(vc_row) if vc_row else {}

            doses_rows = db.execute(
                """
                SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                FROM vaccination_card_doses
                WHERE case_id = ?
                ORDER BY record_type, day_number
                """,
                (case_id,),
            ).fetchall()

            card_doses_by_type: dict[str, dict[int, dict]] = {
                "pre_exposure": {},
                "post_exposure": {},
                "booster": {},
            }
            for row in doses_rows:
                r = row["record_type"]
                d = row["day_number"]
                if r in card_doses_by_type:
                    card_doses_by_type[r][d] = dict(row)

            category_value = (risk_level or case_category or "").strip().lower()
            active_record_type = "pre_exposure" if category_value == "category i" else "post_exposure"

            booster_rows = card_doses_by_type.get("booster", {})
            # For dashboard display, prefer booster label when booster data exists
            if booster_rows:
                display_course = "booster"
                display_type = "Booster Vaccination"
            elif active_record_type == "pre_exposure":
                display_course = "pre_exposure"
                display_type = "Pre-Exposure Vaccination"
            else:
                display_course = "post_exposure"
                display_type = "Post-Exposure Vaccination"

            if display_course == "pre_exposure":
                schedule_days = [0, 7, 28]
            elif display_course == "post_exposure":
                schedule_days = [0, 3, 7, 14, 28]
            else:  # booster
                schedule_days = [0, 3]

            active_rows = card_doses_by_type.get(display_course, {})

            # Count completed doses in the chosen course
            doses_completed = 0
            for row in active_rows.values():
                dose_date = (row.get("dose_date") or "").strip()
                type_of_vaccine = (row.get("type_of_vaccine") or "").strip()
                given_by = (row.get("given_by") or "").strip()
                if dose_date and type_of_vaccine and given_by:
                    doses_completed += 1

            # Compute next due date from schedule (similar to staff view logic)
            day0_row = active_rows.get(0)
            day0_raw = ((day0_row or {}).get("dose_date") or "").strip() if day0_row else ""
            day0_date = None
            if day0_raw:
                try:
                    day0_date = datetime.fromisoformat(day0_raw).date()
                except ValueError:
                    day0_date = None

            next_due_date = None
            for day in schedule_days:
                row = active_rows.get(day)
                dose_date_raw = ((row or {}).get("dose_date") or "").strip() if row else ""
                type_of_vaccine = ((row or {}).get("type_of_vaccine") or "").strip() if row else ""
                given_by = ((row or {}).get("given_by") or "").strip() if row else ""

                # Completed dose rows do not count as next due.
                if dose_date_raw and type_of_vaccine and given_by:
                    continue

                if dose_date_raw:
                    try:
                        next_due_date = datetime.fromisoformat(dose_date_raw).date()
                    except ValueError:
                        next_due_date = None
                elif day0_date and day > 0:
                    next_due_date = day0_date + timedelta(days=day)

                if next_due_date:
                    break

            vaccination_cache[case_id] = {
                "vaccination_card": vaccination_card,
                "display_type": display_type,
                "doses_completed": doses_completed,
                "next_due_date": next_due_date,
                "has_vaccination_data": bool(doses_rows),
            }
            return vaccination_cache[case_id]

        def _ordinal(n: int) -> str:
            if n <= 0:
                return ""
            if 10 <= (n % 100) <= 20:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
            return f"{n}{suffix}"

        enriched_appointments: list[dict] = []
        for row in filtered_rows:
            appt = dict(row)

            # Human-friendly appointment number (per-patient count across all statuses)
            appt["appointment_number"] = appointment_number_map.get(appt["id"], appt["id"])

            # Time from the slot / appointment datetime
            raw_dt = (appt.get("appointment_datetime") or "").strip()
            display_time = ""
            fallback_date = ""
            if raw_dt:
                try:
                    dt_val = datetime.fromisoformat(raw_dt)
                    display_time = dt_val.strftime("%I:%M %p")
                    fallback_date = dt_val.strftime("%b %d, %Y")
                except ValueError:
                    display_time = raw_dt
                    fallback_date = raw_dt

            # Vaccination-derived type, dosage, and date
            vacc_summary = _compute_vaccination_summary(
                appt["case_id"], appt.get("risk_level"), appt.get("case_category")
            )
            display_type = vacc_summary["display_type"]
            doses_completed = vacc_summary["doses_completed"]
            next_due_date = vacc_summary["next_due_date"]

            display_dosage_label = ""
            if doses_completed > 0:
                display_dosage_label = f"{_ordinal(doses_completed)} dosage"

            # For 2nd vaccination dose and higher, show clinic hours instead of a specific slot time.
            if doses_completed >= 2:
                display_time = "8:00 AM-5:00 PM"

            if next_due_date:
                display_date = next_due_date.strftime("%b %d, %Y")
            else:
                display_date = fallback_date or raw_dt or "N/A"

            appt["display_time"] = display_time
            appt["display_date"] = display_date
            appt["display_type"] = display_type
            appt["display_dosage_label"] = display_dosage_label

            cid = appt.get("case_id")
            appt["notification_highlight"] = (
                appt["id"] in highlight_appointment_ids
                or (cid is not None and cid in highlight_case_ids)
            )

            enriched_appointments.append(appt)

        clinics = db.execute("SELECT id, name FROM clinics ORDER BY name").fetchall()

        has_any_appointments = len(all_appointments_rows) > 0

        return render_template(
            "patient_dashboard.html",
            patient=patient,
            cases=cases,
            appointments=enriched_appointments,
            clinics=clinics,
            has_any_appointments=has_any_appointments,
            selected_status=status_filter if status_filter in ("pending", "completed", "canceled", "scheduled", "missed") else "",
            active_page="dashboard",
            unread_appointments_count=unread_counts.get("appointment", 0),
            unread_vaccinations_count=unread_counts.get("vaccination", 0),
            dashboard_notifications=dashboard_notifications,
        )

    @app.get("/patient/profile")
    @role_required("patient")
    def patient_profile():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        return render_template("patient_profile.html", patient=patient, active_page="profile")

    @app.get("/patient/help")
    @role_required("patient")
    def patient_help():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))
        return render_template("patient_help.html", active_page="help")

    @app.get("/patient/vaccinations")
    @role_required("patient")
    def patient_vaccinations():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()
        _, _, vaccination_highlight_case_ids = _get_unread_patient_notifications_for_user(
            session["user_id"]
        )
        unread_counts = _get_patient_unread_counts(session["user_id"])
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        all_appointments_rows = db.execute(
            """
            SELECT
                a.*,
                c.type_of_exposure,
                c.affected_area,
                COALESCE(c.risk_level, c.category, 'N/A') AS risk_level,
                psd.wound_description,
                psd.bleeding_type,
                p.first_name AS victim_first_name,
                p.last_name AS victim_last_name,
                p.relationship_to_user AS victim_relationship
            FROM appointments a
            JOIN cases c ON c.id = a.case_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            JOIN patients p ON p.id = a.patient_id
            WHERE p.user_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
            ORDER BY a.appointment_datetime DESC
            """,
            (session["user_id"],),
        ).fetchall()

        _debug_log(
            run_id="initial",
            hypothesis_id="H1",
            location="app.py:patient_vaccinations",
            message="Loaded appointments for vaccinations view",
            data={"count": len(all_appointments_rows)},
        )

        # Build appointment numbers per account (same logic as dashboard)
        sorted_for_sequence = sorted(
            all_appointments_rows,
            key=lambda r: (r["appointment_datetime"] or ""),
        )
        appointment_number_map: dict[int, int] = {}
        seq = 0
        for row in sorted_for_sequence:
            seq += 1
            appointment_number_map[row["id"]] = seq

        vaccination_items: list[dict] = []
        for row in all_appointments_rows:
            appt = dict(row)

            _debug_log(
                run_id="initial",
                hypothesis_id="H2",
                location="app.py:patient_vaccinations",
                message="Processing appointment row for vaccination card",
                data={
                    "appointment_id": appt.get("id"),
                    "case_id": appt.get("case_id"),
                    "status": appt.get("status"),
                    "risk_level": appt.get("risk_level"),
                },
            )

            # Vaccination card + doses for this case
            case_id = appt["case_id"]
            vc_row = db.execute(
                "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
            ).fetchone()
            vaccination_card = dict(vc_row) if vc_row else {}

            doses_rows = db.execute(
                """
                SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                FROM vaccination_card_doses
                WHERE case_id = ?
                ORDER BY record_type, day_number
                """,
                (case_id,),
            ).fetchall()

            _debug_log(
                run_id="initial",
                hypothesis_id="H3",
                location="app.py:patient_vaccinations",
                message="Loaded vaccination card and doses for case",
                data={
                    "case_id": case_id,
                    "has_card": bool(vaccination_card),
                    "dose_count": len(doses_rows),
                },
            )

            if not doses_rows and not vaccination_card:
                continue

            card_doses_by_type: dict[str, dict[int, dict]] = {
                "pre_exposure": {},
                "post_exposure": {},
                "booster": {},
            }
            for drow in doses_rows:
                rtype = drow["record_type"]
                dnum = drow["day_number"]
                if rtype in card_doses_by_type:
                    card_doses_by_type[rtype][dnum] = dict(drow)

            category_value = (appt.get("risk_level") or appt.get("case_category") or "").strip()
            category_lower = category_value.lower()
            active_record_type = "pre_exposure" if category_lower == "category i" else "post_exposure"

            booster_rows = card_doses_by_type.get("booster", {})
            if booster_rows:
                display_course = "booster"
                course_label = "Booster Vaccination"
            elif active_record_type == "pre_exposure":
                display_course = "pre_exposure"
                course_label = "Pre-Exposure Vaccination"
            else:
                display_course = "post_exposure"
                course_label = "Post-Exposure Vaccination"

            if display_course == "pre_exposure":
                schedule_days = [0, 7, 28]
            elif display_course == "post_exposure":
                schedule_days = [0, 3, 7, 14, 28]
            else:
                schedule_days = [0, 3]

            active_rows = card_doses_by_type.get(display_course, {})
            course_rows = []
            for day in schedule_days:
                row_data = active_rows.get(day, {}) or {}
                row_copy = {
                    "day_number": day,
                    "dose_date": (row_data.get("dose_date") or "").strip() or None,
                    "type_of_vaccine": (row_data.get("type_of_vaccine") or "").strip() or None,
                    "dose": (row_data.get("dose") or "").strip() or None,
                    "route_site": (row_data.get("route_site") or "").strip() or None,
                    "given_by": (row_data.get("given_by") or "").strip() or None,
                }
                course_rows.append(row_copy)

            expected_doses = len(schedule_days)
            doses_completed = 0
            for row_data in active_rows.values():
                dose_date = (row_data.get("dose_date") or "").strip()
                type_of_vaccine = (row_data.get("type_of_vaccine") or "").strip()
                given_by = (row_data.get("given_by") or "").strip()
                if dose_date and type_of_vaccine and given_by:
                    doses_completed += 1

            victim_relationship = (appt.get("victim_relationship") or "Self").strip()
            if victim_relationship.lower() == "self":
                victim_label = "Self"
            else:
                victim_label = f"{victim_relationship} - {(appt.get('victim_first_name') or '').strip()} {(appt.get('victim_last_name') or '').strip()}".strip()

            vaccination_items.append(
                {
                    "appointment_id": appt["id"],
                    "appointment_number": appointment_number_map.get(appt["id"], appt["id"]),
                    "case_id": case_id,
                    "patient_name": (appt.get("victim_first_name") or patient["first_name"] or patient["username"]),
                    "victim_label": victim_label,
                    "appt_date_display": appt.get("appointment_datetime") or "",
                    "category_value": category_value,
                    "type_of_exposure": appt.get("type_of_exposure"),
                    "affected_area": appt.get("affected_area"),
                    "bleeding_type": appt.get("bleeding_type"),
                    "vaccination_card": vaccination_card,
                    "course_label": course_label,
                    "course_rows": course_rows,
                    "dose_type_label": course_label,
                    "expected_doses": expected_doses,
                    "doses_completed": doses_completed,
                }
            )

        return render_template(
            "patient_vaccinations.html",
            vaccination_items=vaccination_items,
            vaccination_highlight_case_ids=vaccination_highlight_case_ids,
            active_page="vaccinations",
            unread_appointments_count=unread_counts.get("appointment", 0),
            unread_vaccinations_count=unread_counts.get("vaccination", 0),
        )

    @app.post("/patient/appointments/<int:appointment_id>/cancel")
    @role_required("patient")
    def patient_cancel_appointment(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()

        # Ensure the appointment belongs to any patient under this user (self or dependents)
        appt = db.execute(
            """
            SELECT a.id, a.status
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        if appt["status"] == "Cancelled":
            flash("Appointment is already cancelled.", "info")
            return redirect(url_for("patient_dashboard"))

        db.execute(
            """
            UPDATE appointments
            SET status = ?
            WHERE id = ?
            """,
            ("Cancelled", appointment_id),
        )
        db.commit()
        flash("Appointment cancelled.", "success")
        return redirect(url_for("patient_dashboard"))

    @app.post("/patient/appointments/<int:appointment_id>/hide")
    @role_required("patient")
    def patient_hide_appointment(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()

        # Ensure the appointment belongs to any patient under this user (self or dependents)
        appt = db.execute(
            """
            SELECT a.id
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        db.execute(
            """
            UPDATE appointments
            SET patient_hidden = 1
            WHERE id = ?
            """,
            (appointment_id,),
        )
        db.commit()

        flash("Appointment removed from your list.", "success")
        return redirect(url_for("patient_dashboard"))

    @app.get("/patient/appointments/<int:appointment_id>")
    @role_required("patient")
    def patient_appointment_view(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()
        appt = db.execute(
            """
            SELECT
              a.*,
              p.first_name AS victim_first_name,
              p.last_name AS victim_last_name,
              p.relationship_to_user AS victim_relationship,
              p.phone_number,
              p.address,
              c.id AS case_id,
              c.type_of_exposure,
              c.exposure_date,
              c.affected_area,
              COALESCE(c.risk_level, c.category, 'N/A') AS risk_level,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            JOIN cases c ON c.id = a.case_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        _mark_appointment_notifications_read_for_appointment(
            session["user_id"], appointment_id
        )

        # Compute human-friendly appointment number for this patient (all non-hidden appointments up to this one)
        count_row = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE p.user_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
              AND datetime(a.appointment_datetime) <= datetime(?)
            """,
            (session["user_id"], appt["appointment_datetime"]),
        ).fetchone()
        appointment_number = count_row["n"] if count_row else 1

        victim_name = " ".join(
            part
            for part in [
                (appt["victim_first_name"] or "").strip(),
                (appt["victim_last_name"] or "").strip(),
            ]
            if part
        ) or "Unknown"

        appt_datetime_display = appt["appointment_datetime"] or ""
        appt_date_display = ""
        appt_time_display = ""
        if appt["appointment_datetime"]:
            try:
                dt = datetime.fromisoformat(appt["appointment_datetime"])
                appt_datetime_display = dt.strftime("%b %d, %Y @ %I:%M %p")
                appt_date_display = dt.strftime("%Y-%m-%d")
                appt_time_display = dt.strftime("%H:%M")
            except ValueError:
                pass

        status_value = (appt["status"] or "").strip()
        status_lower = status_value.lower()
        can_edit = status_lower in ("pending", "queued", "no show")

        # Vaccination card data (shared with staff case view, read-only for patients)
        case_id = appt["case_id"]
        vc_row = db.execute(
            "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
        ).fetchone()
        vaccination_card = dict(vc_row) if vc_row else {}

        vaccination_card_doses_rows = db.execute(
            """
            SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
            FROM vaccination_card_doses
            WHERE case_id = ?
            ORDER BY record_type, day_number
            """,
            (case_id,),
        ).fetchall()
        has_vaccination_card_data = vc_row is not None or len(vaccination_card_doses_rows) > 0
        card_doses_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}
        for row in vaccination_card_doses_rows:
            r = row["record_type"]
            d = row["day_number"]
            if r in card_doses_by_type:
                card_doses_by_type[r][d] = dict(row)

        status_metrics = _compute_vaccination_status_for_case(
            card_doses_by_type, appt["risk_level"]
        )
        active_record_type = status_metrics["display_course"]
        dose_type_label = status_metrics["dose_type_label"]
        doses_completed = status_metrics["doses_completed"]
        expected_doses = status_metrics["expected_doses"]
        progress_pct = status_metrics["progress_pct"]
        next_appointment_display = status_metrics["next_appointment_display"]

        return render_template(
            "patient_appointment_view.html",
            appointment=appt,
            appointment_number=appointment_number,
            victim_name=victim_name,
            appt_datetime_display=appt_datetime_display,
            appt_date_display=appt_date_display,
            appt_time_display=appt_time_display,
            can_edit=can_edit,
            vaccination_card=vaccination_card,
            card_doses_by_type=card_doses_by_type,
            active_record_type=active_record_type,
            dose_type_label=dose_type_label,
            doses_completed=doses_completed,
            expected_doses=expected_doses,
            progress_pct=progress_pct,
            next_appointment_display=next_appointment_display,
            has_vaccination_card_data=has_vaccination_card_data,
            active_page="dashboard",
        )

    def _build_vaccination_card_context_for_patient(appointment_id: int, user_id: int) -> dict | None:
        """
        Shared helper to build vaccination card context for patient-facing
        vaccination views (HTML and PDF). Ensures the appointment belongs
        to the current user.
        """
        db = get_db()
        appt = db.execute(
            """
            SELECT
                a.*,
                c.type_of_exposure,
                c.affected_area,
                COALESCE(c.risk_level, c.category, 'N/A') AS risk_level,
                psd.wound_description,
                psd.bleeding_type,
                p.first_name AS victim_first_name,
                p.last_name AS victim_last_name,
                p.relationship_to_user AS victim_relationship,
                p.phone_number AS victim_phone_number,
                p.address AS victim_address,
                p.date_of_birth AS victim_date_of_birth,
                p.age AS victim_age,
                p.gender AS victim_gender
            FROM appointments a
            JOIN cases c ON c.id = a.case_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, user_id),
        ).fetchone()

        if appt is None:
            return None

        appt = dict(appt)

        appointment_number = appt.get("id")

        appt_datetime_display = None
        raw_dt = (appt.get("appointment_datetime") or "").strip()
        if raw_dt:
            try:
                dt = datetime.fromisoformat(raw_dt)
                appt_datetime_display = dt.strftime("%b %d, %Y @ %I:%M %p")
            except ValueError:
                appt_datetime_display = raw_dt

        case_id = appt["case_id"]
        db = get_db()
        vc_row = db.execute(
            "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
        ).fetchone()
        vaccination_card = dict(vc_row) if vc_row else {}

        vaccination_card_doses_rows = db.execute(
            """
            SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
            FROM vaccination_card_doses
            WHERE case_id = ?
            ORDER BY record_type, day_number
            """,
            (case_id,),
        ).fetchall()
        card_doses_by_type: dict[str, dict[int, dict]] = {
            "pre_exposure": {},
            "post_exposure": {},
            "booster": {},
        }
        for row in vaccination_card_doses_rows:
            r = row["record_type"]
            d = row["day_number"]
            if r in card_doses_by_type:
                card_doses_by_type[r][d] = dict(row)

        category_value = (appt.get("risk_level") or appt.get("case_category") or "").strip()
        category_lower = category_value.lower()
        active_record_type = "pre_exposure" if category_lower == "category i" else "post_exposure"

        booster_rows = card_doses_by_type.get("booster", {})
        if booster_rows:
            display_course = "booster"
            course_label = "Booster Vaccination"
        elif active_record_type == "pre_exposure":
            display_course = "pre_exposure"
            course_label = "Pre-Exposure Vaccination"
        else:
            display_course = "post_exposure"
            course_label = "Post-Exposure Vaccination"

        if display_course == "pre_exposure":
            schedule_days = [0, 7, 28]
        elif display_course == "post_exposure":
            schedule_days = [0, 3, 7, 14, 28]
        else:
            schedule_days = [0, 3]

        active_rows = card_doses_by_type.get(display_course, {})
        course_rows = []
        for day in schedule_days:
            row_data = active_rows.get(day, {}) or {}
            row_copy = {
                "day_number": day,
                "dose_date": (row_data.get("dose_date") or "").strip() or None,
                "type_of_vaccine": (row_data.get("type_of_vaccine") or "").strip() or None,
                "dose": (row_data.get("dose") or "").strip() or None,
                "route_site": (row_data.get("route_site") or "").strip() or None,
                "given_by": (row_data.get("given_by") or "").strip() or None,
            }
            course_rows.append(row_copy)

        expected_doses = len(schedule_days)
        doses_completed = 0
        for row_data in active_rows.values():
            dose_date = (row_data.get("dose_date") or "").strip()
            type_of_vaccine = (row_data.get("type_of_vaccine") or "").strip()
            given_by = (row_data.get("given_by") or "").strip()
            if dose_date and type_of_vaccine and given_by:
                doses_completed += 1

        victim_relationship = (appt.get("victim_relationship") or "Self").strip()
        if victim_relationship.lower() == "self":
            victim_label = "Self"
        else:
            victim_label = victim_relationship

        victim_full_name = " ".join(
            p for p in [
                (appt.get("victim_first_name") or "").strip(),
                (appt.get("victim_last_name") or "").strip(),
            ] if p
        ) or "—"
        victim_birthday_raw = (appt.get("victim_date_of_birth") or "").strip()
        victim_birthday = victim_birthday_raw
        if victim_birthday_raw:
            try:
                d = datetime.fromisoformat(victim_birthday_raw)
                victim_birthday = d.strftime("%b %d, %Y")
            except ValueError:
                pass

        return {
            "appointment_id": appointment_id,
            "appointment_number": appointment_number,
            "case_id": case_id,
            "patient_name": victim_full_name,
            "victim_label": victim_label,
            "victim_full_name": victim_full_name,
            "victim_age": appt.get("victim_age"),
            "victim_gender": (appt.get("victim_gender") or "").strip() or "—",
            "victim_birthday": victim_birthday or "—",
            "victim_contact_number": (appt.get("victim_phone_number") or "").strip() or "—",
            "victim_address": (appt.get("victim_address") or "").strip() or "—",
            "appt_datetime_display": appt_datetime_display,
            "category_value": category_value,
            "type_of_exposure": appt.get("type_of_exposure"),
            "affected_area": appt.get("affected_area"),
            "bleeding_type": appt.get("bleeding_type"),
            "wound_description": appt.get("wound_description"),
            "vaccination_card": vaccination_card,
            "course_label": course_label,
            "course_rows": course_rows,
            "expected_doses": expected_doses,
            "doses_completed": doses_completed,
        }

    @app.get("/patient/vaccination-card/<int:appointment_id>")
    @role_required("patient")
    def patient_vaccination_card_view(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        context = _build_vaccination_card_context_for_patient(
            appointment_id=appointment_id, user_id=session["user_id"]
        )
        if context is None:
            flash("Vaccination card not found for this appointment.", "error")
            return redirect(url_for("patient_vaccinations"))

        _mark_vaccination_notifications_read_for_case(
            session["user_id"], int(context["case_id"])
        )

        return render_template(
            "patient_vaccination_card_view.html",
            active_page="vaccinations",
            **context,
        )

    @app.get("/patient/vaccination-card/<int:appointment_id>/download")
    @role_required("patient")
    def patient_vaccination_card_pdf(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable. Please contact the clinic.", "error")
            return redirect(url_for("patient_vaccination_card_view", appointment_id=appointment_id))

        context = _build_vaccination_card_context_for_patient(
            appointment_id=appointment_id, user_id=session["user_id"]
        )
        if context is None:
            flash("Vaccination card not found for this appointment.", "error")
            return redirect(url_for("patient_vaccinations"))

        html = render_template("vaccination_card_pdf.html", **context)
        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again or contact the clinic.", "error")
            return redirect(url_for("patient_vaccination_card_view", appointment_id=appointment_id))

        pdf_data = pdf_io.getvalue()
        if not pdf_data:
            flash("PDF generation produced an empty file. Please contact the clinic.", "error")
            return redirect(url_for("patient_vaccination_card_view", appointment_id=appointment_id))

        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        filename = f"vaccination_card_appt_{appointment_id}.pdf"
        response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    @app.get("/staff/cases/<int:case_id>/record.pdf")
    @role_required("clinic_personnel", "system_admin")
    def staff_case_record_pdf(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable. Please contact the clinic.", "error")
            return redirect(url_for("view_patient_case", case_id=case_id))

        context = _build_staff_case_context(case_id=case_id, staff_user_id=session["user_id"])
        if context is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        if context["case"] is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        html = render_template("staff_case_record_pdf.html", **{k: v for k, v in context.items() if k != "db"})
        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again or contact the clinic.", "error")
            return redirect(url_for("view_patient_case", case_id=case_id))

        pdf_data = pdf_io.getvalue()
        if not pdf_data:
            flash("PDF generation produced an empty file. Please contact the clinic.", "error")
            return redirect(url_for("view_patient_case", case_id=case_id))

        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        filename = f"case_{case_id}_record.pdf"
        response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    @app.get("/patient/appointments/<int:appointment_id>/edit")
    @role_required("patient")
    def patient_appointment_edit(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()
        appt = db.execute(
            """
            SELECT a.*
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        status_value = (appt["status"] or "").strip()
        status_lower = status_value.lower()
        if status_lower not in ("pending", "queued", "no show"):
            flash("This appointment can no longer be rescheduled.", "info")
            return redirect(url_for("patient_appointment_view", appointment_id=appointment_id))

        current_display = appt["appointment_datetime"] or ""
        if appt["appointment_datetime"]:
            try:
                dt = datetime.fromisoformat(appt["appointment_datetime"])
                current_display = dt.strftime("%b %d, %Y @ %I:%M %p")
            except ValueError:
                pass

        # Fetch available future slots for this clinic
        now_iso = datetime.now().isoformat()
        rows = db.execute(
            """
            SELECT s.id, s.slot_datetime, s.max_bookings,
                   (SELECT COUNT(*) FROM appointments a2
                    WHERE a2.clinic_id = s.clinic_id
                      AND a2.appointment_datetime = s.slot_datetime
                      AND a2.id != ?
                      AND LOWER(COALESCE(a2.status, '')) != 'cancelled') AS booking_count
            FROM availability_slots s
            WHERE s.clinic_id = ?
              AND s.is_active = 1
              AND s.slot_datetime > ?
            ORDER BY s.slot_datetime ASC
            """,
            (appointment_id, appt["clinic_id"], now_iso),
        ).fetchall()

        available_slots = []
        for row in rows:
            if (row["booking_count"] or 0) >= (row["max_bookings"] or 1):
                continue
            dt_str = row["slot_datetime"] or ""
            display = dt_str
            if dt_str:
                try:
                    display = datetime.fromisoformat(dt_str).strftime("%b %d, %Y @ %I:%M %p")
                except ValueError:
                    pass
            available_slots.append(
                {
                    "id": row["id"],
                    "display_datetime": display,
                }
            )

        return render_template(
            "patient_appointment_edit.html",
            appointment=appt,
            current_datetime_display=current_display,
            available_slots=available_slots,
            active_page="dashboard",
        )

    @app.post("/patient/appointments/<int:appointment_id>/edit")
    @role_required("patient")
    def patient_appointment_edit_post(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()
        appt = db.execute(
            """
            SELECT a.*
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        status_value = (appt["status"] or "").strip()
        status_lower = status_value.lower()
        if status_lower not in ("pending", "queued", "no show"):
            flash("This appointment can no longer be rescheduled.", "info")
            return redirect(url_for("patient_appointment_view", appointment_id=appointment_id))

        slot_id_raw = (request.form.get("appointment_slot_id") or "").strip()
        if not slot_id_raw:
            flash("Please select a new time.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        try:
            slot_id = int(slot_id_raw)
        except ValueError:
            flash("Invalid slot selection.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        slot_row = db.execute(
            """
            SELECT id, slot_datetime, max_bookings
            FROM availability_slots
            WHERE id = ? AND clinic_id = ? AND is_active = 1
            """,
            (slot_id, appt["clinic_id"]),
        ).fetchone()

        if not slot_row:
            flash("Selected slot is no longer available.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        slot_datetime = slot_row["slot_datetime"]
        if not slot_datetime:
            flash("Selected slot is invalid.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        if slot_datetime <= datetime.now().isoformat():
            flash("The selected slot is in the past. Please choose another date and time.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        # Check capacity excluding this appointment itself
        existing_count = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM appointments
            WHERE clinic_id = ?
              AND appointment_datetime = ?
              AND id != ?
              AND LOWER(COALESCE(status, '')) != 'cancelled'
            """,
            (appt["clinic_id"], slot_datetime, appointment_id),
        ).fetchone()["n"]
        max_bookings = slot_row["max_bookings"] or 1
        if existing_count >= max_bookings:
            flash("This time slot is no longer available. Please choose another.", "error")
            return redirect(url_for("patient_appointment_edit", appointment_id=appointment_id))

        db.execute(
            """
            UPDATE appointments
            SET appointment_datetime = ?,
                status = ?
            WHERE id = ?
            """,
            (slot_datetime, "Rescheduled", appointment_id),
        )
        db.commit()

        flash("Appointment rescheduled.", "success")
        return redirect(url_for("patient_appointment_view", appointment_id=appointment_id))

    @app.get("/patient/availability")
    @role_required("patient", "clinic_personnel")
    def patient_availability():
        """Return available slots for a clinic (and optional date).

        Used by the patient pre-screening / reschedule flows and by staff when
        rescheduling an appointment. Staff are always restricted to their own clinic.
        """
        db = get_db()

        if session.get("role") == "clinic_personnel":
            # Staff: always use their own clinic_id, ignore query param
            staff_row = db.execute(
                "SELECT clinic_id FROM clinic_personnel WHERE user_id = ?",
                (session["user_id"],),
            ).fetchone()
            if staff_row is None:
                return jsonify([])
            clinic_id = staff_row["clinic_id"]
        else:
            clinic_id = request.args.get("clinic_id", "").strip()
            if not clinic_id:
                row = db.execute("SELECT id FROM clinics LIMIT 1").fetchone()
                if not row:
                    return jsonify([])
                clinic_id = row["id"]
            else:
                try:
                    clinic_id = int(clinic_id)
                except ValueError:
                    return jsonify([])

        date_param = request.args.get("date", "").strip()
        from_param = request.args.get("from", "").strip()
        to_param = request.args.get("to", "").strip()
        now_iso = datetime.now().isoformat()

        if date_param:
            from_date = to_date = date_param
        elif from_param and to_param:
            from_date, to_date = from_param, to_param
        else:
            from_date = datetime.now().date().isoformat()
            to_date = (datetime.now().date() + timedelta(days=60)).isoformat()

        rows = db.execute(
            """
            SELECT s.id, s.slot_datetime, s.max_bookings,
                   (SELECT COUNT(*) FROM appointments a
                    WHERE a.clinic_id = s.clinic_id
                      AND a.appointment_datetime = s.slot_datetime
                      AND LOWER(COALESCE(a.status, '')) != 'cancelled') AS booking_count
            FROM availability_slots s
            WHERE s.clinic_id = ?
              AND s.is_active = 1
              AND DATE(s.slot_datetime) >= ?
              AND DATE(s.slot_datetime) <= ?
              AND s.slot_datetime > ?
            ORDER BY s.slot_datetime ASC
            """,
            (clinic_id, from_date, to_date, now_iso),
        ).fetchall()

        out = []
        for row in rows:
            if (row["booking_count"] or 0) >= (row["max_bookings"] or 1):
                continue
            dt_str = row["slot_datetime"] or ""
            time_display = dt_str
            if dt_str:
                try:
                    time_display = datetime.fromisoformat(dt_str).strftime("%I:%M %p")
                except ValueError:
                    pass
            out.append({
                "id": row["id"],
                "slot_datetime": dt_str,
                "time_display": time_display,
            })
        return jsonify(out)

    @app.post("/patient/pre-screening/submit")
    @role_required("patient")
    def pre_screening_submit():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))
        
        db = get_db()
        
        # Get patient record
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        # Get or create default clinic (use first clinic or create default)
        clinic = db.execute("SELECT id FROM clinics LIMIT 1").fetchone()
        if not clinic:
            db.execute("INSERT INTO clinics (name, address) VALUES (?, ?)", ("Default Clinic", None))
            db.commit()
            clinic = db.execute("SELECT id FROM clinics LIMIT 1").fetchone()
        clinic_id = clinic["id"]

        # Get form data
        form_type = request.form.get("form_type", "case")
        appointment_slot_id_raw = request.form.get("appointment_slot_id", "").strip()
        appointment_datetime_form = request.form.get("appointment_datetime", "").strip()
        form_clinic_id = request.form.get("clinic_id", "").strip()
        if form_type == "appointment" and form_clinic_id:
            try:
                fid = int(form_clinic_id)
                row = db.execute("SELECT id FROM clinics WHERE id = ?", (fid,)).fetchone()
                if row:
                    clinic_id = row["id"]
            except ValueError:
                pass
        type_of_exposure = request.form.get("type_of_exposure", "").strip()
        exposure_date = request.form.get("exposure_date", "").strip()
        exposure_time = request.form.get("exposure_time", "").strip()
        wound_description = request.form.get("wound_description", "").strip()
        spontaneous_bleeding = request.form.get("spontaneous_bleeding", "").strip()
        induced_bleeding = request.form.get("induced_bleeding", "").strip()
        patient_prev_immunization = request.form.get("patient_prev_immunization", "").strip()
        prev_vaccine_date = request.form.get("prev_vaccine_date", "").strip() or None
        animal_type = request.form.get("animal_type", "").strip()
        other_animal = request.form.get("other_animal", "").strip()
        animal_status = request.form.get("animal_status", "").strip()
        animal_vaccination = request.form.get("animal_vaccination", "").strip()
        local_treatment = request.form.get("local_treatment", "").strip()
        other_treatment = request.form.get("other_treatment", "").strip()
        place_of_exposure = request.form.get("place_of_exposure", "").strip()
        place_of_exposure_other = request.form.get("place_of_exposure_other", "").strip()
        affected_area_values = [
            a.strip() for a in request.form.getlist("affected_area") if a.strip()
        ]
        affected_area_other = request.form.get("affected_area_other", "").strip()
        tetanus_immunization = request.form.get("tetanus_immunization", "").strip()
        tetanus_date = request.form.get("tetanus_date", "").strip() or None
        hrtig_immunization = request.form.get("hrtig_immunization", "").strip()
        hrtig_date = request.form.get("hrtig_date", "").strip() or None
        
        # Victim info (update patient if provided)
        victim_first_name = request.form.get("victim_first_name", "").strip()
        victim_last_name = request.form.get("victim_last_name", "").strip()
        victim_middle_initial = request.form.get("victim_middle_initial", "").strip()
        date_of_birth = request.form.get("date_of_birth", "").strip() or None
        gender = request.form.get("gender", "").strip() or None
        age = request.form.get("age", "").strip()
        barangay = request.form.get("barangay", "").strip()
        victim_address = request.form.get("victim_address", "").strip()
        contact_number = request.form.get("contact_number", "").strip()
        email_address = request.form.get("email_address", "").strip().lower()
        relationship_to_user = (request.form.get("relationship_to_user", "Self") or "Self").strip()

        # Build combined address: Barangay, Street (or just barangay / just street)
        combined_address = None
        if barangay and victim_address:
            combined_address = f"{barangay}, {victim_address}"
        elif barangay:
            combined_address = barangay
        elif victim_address:
            combined_address = victim_address

        first_name = victim_first_name or None
        last_name = victim_last_name or None

        # Validation
        errors = []
        if not type_of_exposure:
            errors.append("Type of exposure is required.")
        if not exposure_date:
            errors.append("Exposure date is required.")
        else:
            try:
                exp_d = date.fromisoformat(exposure_date.strip()[:10])
            except ValueError:
                errors.append("Exposure date is invalid.")
            else:
                if exp_d > date.today():
                    errors.append("Exposure date cannot be in the future.")
        if not animal_type:
            errors.append("Type of animal is required.")
        if not animal_status:
            errors.append("Animal status is required.")
        if not local_treatment:
            errors.append("Local wound treatment is required.")
        if not place_of_exposure:
            errors.append("Place of exposure is required.")
        if place_of_exposure == "Other" and not place_of_exposure_other:
            errors.append("Please specify the other place of exposure.")
        if not affected_area_values:
            errors.append("Select at least one affected area.")
        has_other_area = "Other" in affected_area_values
        if has_other_area and not affected_area_other:
            errors.append("Please specify the other affected area.")
        if not tetanus_immunization:
            errors.append("Tetanus immunization status is required.")
        if not hrtig_immunization:
            errors.append("Human tetanus immunoglobulin status is required.")
        if hrtig_immunization == "Yes" and not hrtig_date:
            errors.append("HRIG date is required when Human Tetanus Immunoglobulin is Yes.")
        if not date_of_birth:
            errors.append("Birthday is required.")
        if not gender:
            errors.append("Gender is required.")

        if errors:
            for error in errors:
                flash(error, "error")
            return redirect(url_for("patient_dashboard"))

        target_patient_id = patient["id"]
        has_victim_info = bool(first_name or last_name or date_of_birth or gender or age or barangay or combined_address or contact_number or email_address)
        if has_victim_info:
            computed_age = _age_from_iso_date(date_of_birth)
            if computed_age is not None:
                parsed_age = computed_age
            else:
                parsed_age = patient["age"]
                if age:
                    try:
                        parsed_age = int(age)
                    except ValueError:
                        flash("Age must be a number.", "error")
                        return redirect(url_for("patient_dashboard"))

            if relationship_to_user.lower() == "self":
                # Update only the primary self record, never dependent rows.
                new_first_name = first_name if first_name else patient["first_name"]
                new_last_name = last_name if last_name else patient["last_name"]
                new_date_of_birth = date_of_birth if date_of_birth else patient.get("date_of_birth")
                new_gender = gender if gender else patient.get("gender")
                new_address = combined_address if combined_address else patient["address"]
                new_phone = contact_number if contact_number else patient["phone_number"]

                db.execute(
                    """
                    UPDATE patients
                    SET first_name = ?,
                        last_name = ?,
                        date_of_birth = ?,
                        gender = ?,
                        age = ?,
                        address = ?,
                        phone_number = ?,
                        relationship_to_user = ?
                    WHERE id = ?
                    """,
                    (new_first_name, new_last_name, new_date_of_birth, new_gender, parsed_age, new_address, new_phone, "Self", patient["id"]),
                )

                if email_address:
                    db.execute("UPDATE users SET email = ? WHERE id = ?", (email_address, session["user_id"]))
            else:
                db.execute(
                    """
                    INSERT INTO patients (
                        user_id, first_name, last_name, phone_number, address, date_of_birth, gender, age,
                        relationship_to_user, onboarding_completed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session["user_id"],
                        first_name,
                        last_name,
                        contact_number or None,
                        combined_address or None,
                        date_of_birth,
                        gender,
                        parsed_age,
                        relationship_to_user,
                        1,
                    ),
                )
                target_patient_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


        # Build animal_detail (combine animal_type and other_animal if applicable)
        animal_detail = animal_type
        if other_animal and animal_type == "Others":
            animal_detail = f"{animal_type}: {other_animal}"

        # Build place_of_exposure (combine with other text if applicable)
        final_place_of_exposure = place_of_exposure
        if place_of_exposure == "Other" and place_of_exposure_other:
            final_place_of_exposure = f"Other: {place_of_exposure_other}"

        # Build affected_area: comma-separated canonical areas; Other becomes "Other: …"
        canonical_area_parts: list[str] = []
        for av in affected_area_values:
            if av == "Other":
                continue
            canonical_area_parts.append(av)
        if has_other_area and affected_area_other:
            canonical_area_parts.append(f"Other: {affected_area_other}")
        final_affected_area = ", ".join(canonical_area_parts)

        # Build local_treatment (combine with other_treatment if applicable)
        final_local_treatment = local_treatment
        if other_treatment and local_treatment == "Others":
            final_local_treatment = f"{local_treatment}: {other_treatment}"

        # Build bleeding_type
        if spontaneous_bleeding == "Yes" and induced_bleeding == "Yes":
            bleeding_type = "Both spontaneous and induced"
        elif spontaneous_bleeding == "Yes":
            bleeding_type = "Spontaneous"
        elif induced_bleeding == "Yes":
            bleeding_type = "Induced"
        else:
            bleeding_type = "None"

        # Determine risk_level using rule-based helper
        risk_level = classify_pre_screening_risk(
            type_of_exposure=type_of_exposure,
            affected_area=final_affected_area,
            wound_description=wound_description,
            bleeding_type=bleeding_type,
            animal_status=animal_status,
            animal_vaccination=animal_vaccination,
            patient_prev_immunization=patient_prev_immunization,
        )

        # Insert into cases table
        try:
            case_cur = db.execute(
                """
                INSERT INTO cases (
                    patient_id, clinic_id, exposure_date, exposure_time,
                    place_of_exposure, affected_area,
                    type_of_exposure, animal_detail, animal_condition,
                    risk_level, case_status, tetanus_prophylaxis_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_patient_id,
                    clinic_id,
                    exposure_date,
                    exposure_time or None,
                    final_place_of_exposure,
                    final_affected_area,
                    type_of_exposure,
                    animal_detail,
                    animal_status,
                    risk_level,
                    "Queued" if form_type == "appointment" else "Active",
                    tetanus_immunization,
                ),
            )
            case_id = case_cur.lastrowid

            # Insert into pre_screening_details table
            db.execute(
                """
                INSERT INTO pre_screening_details (
                    case_id, wound_description, bleeding_type, local_treatment,
                    patient_prev_immunization, prev_vaccine_date, tetanus_date,
                    hrtig_immunization, hrtig_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_id,
                    wound_description or None,
                    bleeding_type,
                    final_local_treatment or None,
                    patient_prev_immunization or None,
                    prev_vaccine_date,
                    tetanus_date,
                    1 if hrtig_immunization == "Yes" else 0,
                    hrtig_date if hrtig_immunization == "Yes" else None,
                ),
            )

            # Create appointment if form_type is "appointment" (use chosen slot)
            if form_type == "appointment":
                slot_datetime = None
                slot_row = None
                if appointment_slot_id_raw:
                    try:
                        slot_row = db.execute(
                            """
                            SELECT id, slot_datetime, max_bookings
                            FROM availability_slots
                            WHERE id = ? AND clinic_id = ? AND is_active = 1
                            """,
                            (int(appointment_slot_id_raw), clinic_id),
                        ).fetchone()
                        if slot_row:
                            slot_datetime = slot_row["slot_datetime"]
                    except ValueError:
                        pass
                if not slot_datetime and appointment_datetime_form:
                    slot_row = db.execute(
                        """
                        SELECT id, slot_datetime, max_bookings
                        FROM availability_slots
                        WHERE clinic_id = ? AND slot_datetime = ? AND is_active = 1
                        """,
                        (clinic_id, appointment_datetime_form),
                    ).fetchone()
                    if slot_row:
                        slot_datetime = slot_row["slot_datetime"]

                if not slot_datetime:
                    db.rollback()
                    flash("Invalid or unavailable appointment slot. Please choose another date and time.", "error")
                    return redirect(url_for("patient_dashboard"))

                if slot_datetime <= datetime.now().isoformat():
                    db.rollback()
                    flash("The selected slot is in the past. Please choose another date and time.", "error")
                    return redirect(url_for("patient_dashboard"))

                existing_count = db.execute(
                    """
                    SELECT COUNT(*) AS n FROM appointments
                    WHERE clinic_id = ? AND appointment_datetime = ?
                    AND LOWER(COALESCE(status, '')) != 'cancelled'
                    """,
                    (clinic_id, slot_datetime),
                ).fetchone()["n"]
                max_bookings = (slot_row["max_bookings"] or 1) if slot_row else 1
                if existing_count >= max_bookings:
                    db.rollback()
                    flash("This time slot is no longer available. Please choose another.", "error")
                    return redirect(url_for("patient_dashboard"))

                cursor = db.execute(
                    """
                    INSERT INTO appointments (
                        patient_id, clinic_id, appointment_datetime,
                        status, type, case_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_patient_id,
                        clinic_id,
                        slot_datetime,
                        "Pending",
                        "Pre-screening",
                        case_id,
                    ),
                )

                appointment_id = cursor.lastrowid
                _insert_patient_notification(
                    patient_id=target_patient_id,
                    notif_type="appointment",
                    source_id=appointment_id,
                    message="New pre-screening appointment requested.",
                )

            db.commit()
            flash("Pre-screening form submitted successfully.", "success")
        except Exception as e:
            db.rollback()
            flash(f"Error submitting form: {str(e)}", "error")

        return redirect(url_for("patient_dashboard"))

    @app.post("/patient/profile")
    @role_required("patient")
    def patient_profile_update():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))
        
        db = get_db()
        
        # Fetch current patient record
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        # Get form data
        first_name = request.form.get("first_name", "").strip()
        last_name = request.form.get("last_name", "").strip()
        date_of_birth = request.form.get("date_of_birth", "").strip()
        gender = request.form.get("gender", "").strip()
        address = request.form.get("address", "").strip()
        phone_number = request.form.get("phone_number", "").strip()
        email = request.form.get("email", "").strip().lower()
        allergies = request.form.get("allergies", "").strip()
        pre_existing_conditions = request.form.get("pre_existing_conditions", "").strip()
        current_medications = request.form.get("current_medications", "").strip()
        
        # Password change fields (optional)
        new_password = request.form.get("new_password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        # Validation
        errors = []
        
        if not email:
            errors.append("Email is required.")
        elif "@" not in email:
            errors.append("Email must be valid.")
        
        if new_password:
            if len(new_password) < 8:
                errors.append("Password must be at least 8 characters.")
            elif new_password != confirm_password:
                errors.append("Passwords do not match.")
        
        if errors:
            for error in errors:
                flash(error, "error")
            return render_template("patient_profile.html", patient=patient, active_page="profile")

        # Update patients table
        db.execute(
            """
            UPDATE patients
            SET first_name = ?,
                last_name = ?,
                date_of_birth = ?,
                gender = ?,
                address = ?,
                phone_number = ?,
                allergies = ?,
                pre_existing_conditions = ?,
                current_medications = ?
            WHERE id = ?
            """,
            (
                first_name if first_name else None,
                last_name if last_name else None,
                date_of_birth if date_of_birth else None,
                gender if gender else None,
                address if address else None,
                phone_number if phone_number else None,
                allergies if allergies else None,
                pre_existing_conditions if pre_existing_conditions else None,
                current_medications if current_medications else None,
                patient["id"],
            ),
        )

        # Update users.email
        db.execute(
            """
            UPDATE users
            SET email = ?
            WHERE id = ?
            """,
            (email, session["user_id"]),
        )

        # Update password if provided
        if new_password:
            password_hash = generate_password_hash(new_password)
            db.execute(
                """
                UPDATE users
                SET password_hash = ?
                WHERE id = ?
                """,
                (password_hash, session["user_id"]),
            )

        db.commit()
        flash("Profile updated successfully.", "success")
        return redirect(url_for("patient_profile"))

    @app.get("/staff/dashboard")
    @role_required("clinic_personnel", "system_admin")
    def staff_dashboard():
        # Staff dashboard is for clinic_personnel only; system_admin users get redirected.
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email, c.name AS clinic_name
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            JOIN clinics c ON c.id = cp.clinic_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        _run_case_status_maintenance(staff["clinic_id"])

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)
        welcome_name = (
            f"{(staff['title'] or '').strip()} {(staff['last_name'] or '').strip()}".strip()
            or (staff["first_name"] or "").strip()
            or staff["username"]
        )

        current_date = datetime.now().strftime("%A, %d %B %Y")
        clinic_id = staff["clinic_id"]
        _run_case_status_maintenance(clinic_id)

        total_patients = db.execute(
            """
            SELECT COUNT(DISTINCT c.patient_id) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            """,
            (clinic_id,),
        ).fetchone()["total"]

        vaccinations_today = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM vaccination_records vr
            JOIN cases c ON c.id = vr.case_id
            WHERE c.clinic_id = ?
              AND DATE(vr.date_administered) = DATE('now', 'localtime')
            """,
            (clinic_id,),
        ).fetchone()["total"]

        ongoing_cases = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
              AND LOWER(COALESCE(c.case_status, 'pending')) = 'pending'
            """,
            (clinic_id,),
        ).fetchone()["total"]

        high_risk_cases = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
              AND LOWER(c.risk_level) IN ('category iii', 'high', 'high-risk', 'high risk')
            """,
            (clinic_id,),
        ).fetchone()["total"]

        monthly_appt_stats = db.execute(
            """
            SELECT
              SUM(CASE WHEN LOWER(a.status) = 'completed' THEN 1 ELSE 0 END) AS completed_count,
              COUNT(*) AS total_count
            FROM appointments a
            WHERE a.clinic_id = ?
              AND DATE(a.appointment_datetime) >= DATE('now', 'start of month', 'localtime')
            """,
            (clinic_id,),
        ).fetchone()
        completed_count = monthly_appt_stats["completed_count"] or 0
        total_count = monthly_appt_stats["total_count"] or 0
        vaccination_completion_pct = round((completed_count / total_count) * 100) if total_count else 0

        appointment_status = db.execute(
            """
            SELECT
              (
                SELECT COUNT(*)
                FROM cases c
                WHERE c.clinic_id = ?
                  AND LOWER(COALESCE(c.case_status, '')) = 'no show'
              ) AS missed,
              SUM(CASE WHEN LOWER(status) = 'rescheduled' THEN 1 ELSE 0 END) AS rescheduled
            FROM appointments
            WHERE clinic_id = ?
            """,
            (clinic_id, clinic_id),
        ).fetchone()
        missed_count = appointment_status["missed"] or 0
        rescheduled_count = appointment_status["rescheduled"] or 0

        bite_type_rows = db.execute(
            """
            SELECT
              CASE
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%' THEN 'Dog'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%' THEN 'Cat'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'bat%' THEN 'Bat'
                ELSE 'Other'
              END AS bite_type,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            GROUP BY bite_type
            """,
            (clinic_id,),
        ).fetchall()
        total_bite_cases = sum(row["total"] for row in bite_type_rows)
        bite_map = {"Dog": 0, "Cat": 0, "Bat": 0, "Other": 0}
        for row in bite_type_rows:
            bite_map[row["bite_type"]] = row["total"]
        common_bite_types = []
        for label in ["Dog", "Cat", "Bat", "Other"]:
            count = bite_map[label]
            pct = round((count / total_bite_cases) * 100) if total_bite_cases else 0
            common_bite_types.append({"label": label, "percent": pct})

        todays_appointments_rows = db.execute(
            """
            SELECT
              a.appointment_datetime,
              a.status,
              a.type,
              p.first_name,
              p.last_name,
              u.username
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            JOIN users u ON u.id = p.user_id
            WHERE a.clinic_id = ?
              AND DATE(a.appointment_datetime) = DATE('now', 'localtime')
            ORDER BY a.appointment_datetime ASC
            LIMIT 12
            """,
            (clinic_id,),
        ).fetchall()
        todays_appointments = []
        for row in todays_appointments_rows:
            first_name = (row["first_name"] or "").strip()
            last_name = (row["last_name"] or "").strip()
            patient_name = " ".join(part for part in [first_name, last_name] if part) or row["username"]
            owner_name = row["username"]
            appt_time = datetime.fromisoformat(row["appointment_datetime"]).strftime("%I:%M %p")
            todays_appointments.append(
                {
                    "time": appt_time,
                    "patient_name": patient_name,
                    "owner_name": owner_name,
                    "reason": row["type"],
                    "status": row["status"],
                }
            )

        return render_template(
            "staff_dashboard.html",
            staff=staff,
            staff_display_name=staff_display_name,
            welcome_name=welcome_name,
            current_date=current_date,
            total_patients=total_patients,
            vaccinations_today=vaccinations_today,
            ongoing_cases=ongoing_cases,
            high_risk_cases=high_risk_cases,
            vaccination_completion_pct=vaccination_completion_pct,
            missed_count=missed_count,
            rescheduled_count=rescheduled_count,
            common_bite_types=common_bite_types,
            todays_appointments=todays_appointments,
        )

    @app.get("/staff/profile")
    @role_required("clinic_personnel", "system_admin")
    def staff_profile():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email, c.name AS clinic_name
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            JOIN clinics c ON c.id = cp.clinic_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            fn = (staff["first_name"] or "").strip()
            ln = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, fn, ln] if part)
        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Profile", "href": None},
        ]
        return render_template(
            "staff_profile.html",
            staff=staff,
            staff_display_name=staff_display_name,
            breadcrumbs=breadcrumbs,
            active_page="profile",
        )

    @app.post("/staff/profile")
    @role_required("clinic_personnel", "system_admin")
    def staff_profile_update():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        username = (request.form.get("username") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        phone_number = (request.form.get("phone_number") or "").strip()
        specialty = (request.form.get("specialty") or "").strip()

        errors: list[str] = []
        if not username:
            errors.append("Username is required.")
        if not email:
            errors.append("Email is required.")
        elif "@" not in email:
            errors.append("Email must be valid.")

        # Check for username/email uniqueness (excluding current user)
        existing = db.execute(
            """
            SELECT id
            FROM users
            WHERE id != ? AND (LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?))
            LIMIT 1
            """,
            (session["user_id"], username, email),
        ).fetchone()
        if existing:
            errors.append("Username or email is already in use.")

        if errors:
            for msg in errors:
                flash(msg, "error")
            staff_display_name = staff["username"]
            if staff["first_name"] or staff["last_name"]:
                title = (staff["title"] or "").strip()
                fn = (staff["first_name"] or "").strip()
                ln = (staff["last_name"] or "").strip()
                staff_display_name = " ".join(part for part in [title, fn, ln] if part)
            breadcrumbs = [
                {"label": "Home", "href": url_for("staff_dashboard")},
                {"label": "Profile", "href": None},
            ]
            return render_template(
                "staff_profile.html",
                staff=staff,
                staff_display_name=staff_display_name,
                breadcrumbs=breadcrumbs,
                active_page="profile",
            )

        db.execute(
            """
            UPDATE users
            SET username = ?, email = ?
            WHERE id = ?
            """,
            (username, email, session["user_id"]),
        )

        db.execute(
            """
            UPDATE clinic_personnel
            SET first_name = ?,
                last_name = ?,
                phone_number = ?,
                specialty = ?
            WHERE user_id = ?
            """,
            (first_name or None, last_name or None, phone_number or None, specialty or None, session["user_id"]),
        )

        db.commit()

        # Keep session username/email in sync
        session["username"] = username
        session["email"] = email

        flash("Profile updated successfully.", "success")
        return redirect(url_for("staff_profile"))

    @app.route("/staff/patients/new-account", methods=["GET", "POST"])
    @role_required("clinic_personnel", "system_admin")
    def staff_new_patient_account():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        form_data = {
            "first_name": "",
            "last_name": "",
            "email": "",
            "date_of_birth": "",
            "gender": "",
            "age": "",
            "phone_number": "",
            "address": "",
            "exposure_date": "",
            "type_of_exposure": "",
            "animal_detail": "",
            "risk_level": "",
            "wound_description": "",
            "bleeding_type": "",
            "local_treatment": "",
            "patient_prev_immunization": "",
            "prev_vaccine_date": "",
            "tetanus_date": "",
            "hrtig_immunization": "",
        }
        vaccination_card = {}
        card_doses_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}

        if request.method == "POST":
            for key in form_data:
                form_data[key] = (request.form.get(key) or "").strip()

            def _v(name: str) -> str:
                return (request.form.get(name) or "").strip()

            vaccination_card = {
                "anti_rabies": _v("vc_anti_rabies"),
                "pvrv": _v("vc_pvrv"),
                "pcec_batch": _v("vc_pcec_batch"),
                "pcec_mfg_date": _v("vc_pcec_mfg_date"),
                "pcec_expiry": _v("vc_pcec_expiry"),
                "erig_hrig": _v("vc_erig_hrig"),
                "tetanus_prophylaxis": _v("vc_tetanus_prophylaxis"),
                "tetanus_toxoid": _v("vc_tetanus_toxoid"),
                "ats": _v("vc_ats"),
                "htig": _v("vc_htig"),
                "remarks": _v("vc_remarks"),
            }
            for record_type, prefix, days in [
                ("pre_exposure", "vc_pre", [0, 7, 28]),
                ("post_exposure", "vc_post", [0, 3, 7, 14, 28]),
                ("booster", "vc_booster", [0, 3]),
            ]:
                for day in days:
                    card_doses_by_type[record_type][day] = {
                        "dose_date": _v(f"{prefix}_{day}_date"),
                        "type_of_vaccine": _v(f"{prefix}_{day}_type"),
                        "dose": _v(f"{prefix}_{day}_dose"),
                        "route_site": _v(f"{prefix}_{day}_route_site"),
                        "given_by": _v(f"{prefix}_{day}_given_by"),
                    }

            email = (form_data["email"] or "").lower()
            errors = []
            if not email or "@" not in email or "." not in email.split("@")[-1]:
                errors.append("A valid patient email is required.")
            else:
                existing_email = db.execute("SELECT 1 FROM users WHERE email = ? LIMIT 1", (email,)).fetchone()
                if existing_email:
                    errors.append("That email is already used by another account.")
            if not form_data["first_name"] and not form_data["last_name"]:
                errors.append("Patient first name or last name is required.")
            if not form_data["exposure_date"]:
                errors.append("Exposure date is required.")
            if not form_data["type_of_exposure"]:
                errors.append("Type of exposure is required.")
            if not form_data["animal_detail"]:
                errors.append("Animal detail is required.")
            if not form_data["risk_level"]:
                errors.append("Category / risk level is required.")
            if errors:
                for err in errors:
                    flash(err, "error")
            else:
                risk_level = form_data["risk_level"]
                if risk_level.lower() in {"category 1", "category i", "1", "i"}:
                    risk_level = "Category I"
                elif risk_level.lower() in {"category 2", "category ii", "2", "ii"}:
                    risk_level = "Category II"
                elif risk_level.lower() in {"category 3", "category iii", "3", "iii"}:
                    risk_level = "Category III"

                first_name = form_data["first_name"] or None
                last_name = form_data["last_name"] or None
                dob = form_data["date_of_birth"] or None
                gender = form_data["gender"] or None
                phone_number = form_data["phone_number"] or None
                address = form_data["address"] or None
                age_value = None
                if dob:
                    age_value = _age_from_iso_date(dob)
                if age_value is None and form_data["age"]:
                    try:
                        age_value = int(form_data["age"])
                    except ValueError:
                        flash("Age must be a number.", "error")
                        age_value = None

                username_seed = email.split("@", 1)[0]
                if first_name or last_name:
                    username_seed = ".".join(part for part in [(first_name or "").lower(), (last_name or "").lower()] if part)
                username = _build_unique_username(username_seed)
                generated_password = _generate_strong_password(14)
                password_hash = generate_password_hash(generated_password)
                subject = "RabiesResQ Patient Account Credentials"
                body = (
                    "Hello,\n\n"
                    "A clinic personnel created your RabiesResQ patient account.\n\n"
                    f"Username: {username}\n"
                    f"Email: {email}\n"
                    f"Temporary password: {generated_password}\n\n"
                    "For security, you will be required to change this password at first login.\n"
                    "Please keep this information private."
                )

                try:
                    cur = db.execute(
                        """
                        INSERT INTO users (username, email, password_hash, role, must_change_password)
                        VALUES (?, ?, ?, 'patient', 1)
                        """,
                        (username, email, password_hash),
                    )
                    user_id = cur.lastrowid
                    db.execute(
                        """
                        INSERT INTO patients (
                          user_id, first_name, last_name, age, phone_number, address, date_of_birth,
                          gender, relationship_to_user, onboarding_completed
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Self', 1)
                        """,
                        (user_id, first_name, last_name, age_value, phone_number, address, dob, gender),
                    )
                    patient_id = db.execute(
                        """
                        SELECT id FROM patients
                        WHERE user_id = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (user_id,),
                    ).fetchone()["id"]

                    cur_case = db.execute(
                        """
                        INSERT INTO cases (
                          patient_id, clinic_id, exposure_date, type_of_exposure, animal_detail,
                          risk_level, category, case_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Pending')
                        """,
                        (
                            patient_id,
                            staff["clinic_id"],
                            form_data["exposure_date"],
                            form_data["type_of_exposure"],
                            form_data["animal_detail"],
                            risk_level,
                            risk_level,
                        ),
                    )
                    case_id = cur_case.lastrowid

                    hrtig_value = None
                    if form_data["hrtig_immunization"] in {"0", "1"}:
                        hrtig_value = int(form_data["hrtig_immunization"])
                    db.execute(
                        """
                        INSERT INTO pre_screening_details (
                          case_id, wound_description, bleeding_type, local_treatment,
                          patient_prev_immunization, prev_vaccine_date, tetanus_date, hrtig_immunization
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            form_data["wound_description"] or None,
                            form_data["bleeding_type"] or None,
                            form_data["local_treatment"] or None,
                            form_data["patient_prev_immunization"] or None,
                            form_data["prev_vaccine_date"] or None,
                            form_data["tetanus_date"] or None,
                            hrtig_value,
                        ),
                    )

                    def _normalize_iso_date_input(raw_value: str) -> str:
                        value = (raw_value or "").strip()
                        if not value:
                            return ""
                        try:
                            return datetime.fromisoformat(value).date().isoformat()
                        except ValueError:
                            return ""

                    vc_pcec_mfg_date = _normalize_iso_date_input(_v("vc_pcec_mfg_date"))
                    vc_pcec_expiry = _normalize_iso_date_input(_v("vc_pcec_expiry"))
                    today_iso = datetime.now().date().isoformat()
                    if vc_pcec_expiry and vc_pcec_expiry < today_iso:
                        db.rollback()
                        flash("Expiry date cannot be earlier than today.", "error")
                        return redirect(url_for("staff_new_patient_account"))

                    db.execute(
                        """
                        INSERT INTO vaccination_card (
                            case_id, anti_rabies, pvrv, pcec_batch, pcec_mfg_date, pcec_expiry,
                            erig_hrig, tetanus_prophylaxis, tetanus_toxoid, ats, htig, remarks
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            _v("vc_anti_rabies"),
                            _v("vc_pvrv"),
                            _v("vc_pcec_batch"),
                            vc_pcec_mfg_date,
                            vc_pcec_expiry,
                            _v("vc_erig_hrig"),
                            _v("vc_tetanus_prophylaxis"),
                            _v("vc_tetanus_toxoid"),
                            _v("vc_ats"),
                            _v("vc_htig"),
                            _v("vc_remarks"),
                        ),
                    )

                    for record_type, prefix, days in [
                        ("pre_exposure", "vc_pre", [0, 7, 28]),
                        ("post_exposure", "vc_post", [0, 3, 7, 14, 28]),
                        ("booster", "vc_booster", [0, 3]),
                    ]:
                        for day in days:
                            dose_date = _v(f"{prefix}_{day}_date")
                            type_of_vaccine = _v(f"{prefix}_{day}_type")
                            dose = _v(f"{prefix}_{day}_dose")
                            route_site = _v(f"{prefix}_{day}_route_site")
                            given_by = _v(f"{prefix}_{day}_given_by")
                            if any([dose_date, type_of_vaccine, dose, route_site, given_by]):
                                db.execute(
                                    """
                                    INSERT INTO vaccination_card_doses (
                                        case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        case_id,
                                        record_type,
                                        day,
                                        dose_date or None,
                                        type_of_vaccine or None,
                                        dose or None,
                                        route_site or None,
                                        given_by or None,
                                    ),
                                )

                    try:
                        send_email(to_email=email, subject=subject, body=body)
                    except Exception as email_err:
                        _queue_pending_email(to_email=email, subject=subject, body=body, last_error=str(email_err))
                        flash(
                            "Patient and case created, but email delivery failed. Credentials were queued for retry.",
                            "warning",
                        )
                    else:
                        flash("Patient and case created. Credentials were sent to the provided email.", "success")

                    db.commit()
                    return redirect(url_for("view_patient_case", case_id=case_id))
                except Exception:
                    db.rollback()
                    flash("Failed to create new patient record. Please try again.", "error")

        personnel_rows = db.execute(
            """
            SELECT cp.title, cp.first_name, cp.last_name, u.username
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.clinic_id = ?
            ORDER BY cp.title, cp.first_name, cp.last_name, u.username
            """,
            (staff["clinic_id"],),
        ).fetchall()
        personnel_options = []
        seen_personnel = set()
        for row in personnel_rows:
            title = (row["title"] or "").strip()
            first_name = (row["first_name"] or "").strip()
            last_name = (row["last_name"] or "").strip()
            username = (row["username"] or "").strip()
            display_name = " ".join(part for part in [title, first_name, last_name] if part) or username
            if display_name and display_name not in seen_personnel:
                seen_personnel.add(display_name)
                personnel_options.append(display_name)
        suggested_dates_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": "New Patient", "href": None},
        ]
        return render_template(
            "staff_new_patient.html",
            staff=staff,
            staff_display_name=staff_display_name,
            form=form_data,
            vaccination_card=vaccination_card,
            card_doses_by_type=card_doses_by_type,
            personnel_options=personnel_options,
            suggested_dates_by_type=suggested_dates_by_type,
            expiry_min_date=datetime.now().date().isoformat(),
            breadcrumbs=breadcrumbs,
            active_page="cases",
        )

    @app.route("/staff/cases/new", methods=["GET", "POST"])
    @role_required("clinic_personnel", "system_admin")
    def staff_create_case_record():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        form_data = {
            "first_name": "",
            "last_name": "",
            "age": "",
            "phone_number": "",
            "address": "",
            "exposure_date": "",
            "type_of_exposure": "",
            "animal_detail": "",
            "risk_level": "",
            "wound_description": "",
            "bleeding_type": "",
            "local_treatment": "",
            "patient_prev_immunization": "",
            "prev_vaccine_date": "",
            "tetanus_date": "",
            "hrtig_immunization": "",
        }
        vaccination_card = {}
        card_doses_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}

        if request.method == "POST":
            for key in form_data:
                form_data[key] = (request.form.get(key) or "").strip()
            def _v(name: str) -> str:
                return (request.form.get(name) or "").strip()

            vaccination_card = {
                "anti_rabies": _v("vc_anti_rabies"),
                "pvrv": _v("vc_pvrv"),
                "pcec_batch": _v("vc_pcec_batch"),
                "pcec_mfg_date": _v("vc_pcec_mfg_date"),
                "pcec_expiry": _v("vc_pcec_expiry"),
                "erig_hrig": _v("vc_erig_hrig"),
                "tetanus_prophylaxis": _v("vc_tetanus_prophylaxis"),
                "tetanus_toxoid": _v("vc_tetanus_toxoid"),
                "ats": _v("vc_ats"),
                "htig": _v("vc_htig"),
                "remarks": _v("vc_remarks"),
            }
            for record_type, prefix, days in [
                ("pre_exposure", "vc_pre", [0, 7, 28]),
                ("post_exposure", "vc_post", [0, 3, 7, 14, 28]),
                ("booster", "vc_booster", [0, 3]),
            ]:
                for day in days:
                    card_doses_by_type[record_type][day] = {
                        "dose_date": _v(f"{prefix}_{day}_date"),
                        "type_of_vaccine": _v(f"{prefix}_{day}_type"),
                        "dose": _v(f"{prefix}_{day}_dose"),
                        "route_site": _v(f"{prefix}_{day}_route_site"),
                        "given_by": _v(f"{prefix}_{day}_given_by"),
                    }

            errors = []
            if not form_data["first_name"] and not form_data["last_name"]:
                errors.append("Patient first name or last name is required.")
            if not form_data["exposure_date"]:
                errors.append("Exposure date is required.")
            if not form_data["type_of_exposure"]:
                errors.append("Type of exposure is required.")
            if not form_data["animal_detail"]:
                errors.append("Animal detail is required.")
            if not form_data["risk_level"]:
                errors.append("Category / risk level is required.")
            if errors:
                for err in errors:
                    flash(err, "error")
            else:
                risk_level = form_data["risk_level"]
                if risk_level.lower() in {"category 1", "category i", "1", "i"}:
                    risk_level = "Category I"
                elif risk_level.lower() in {"category 2", "category ii", "2", "ii"}:
                    risk_level = "Category II"
                elif risk_level.lower() in {"category 3", "category iii", "3", "iii"}:
                    risk_level = "Category III"

                try:
                    age_value = None
                    if form_data["age"]:
                        try:
                            age_value = int(form_data["age"])
                        except ValueError:
                            flash("Age must be a number.", "error")
                            return redirect(url_for("staff_create_case_record"))

                    db.execute(
                        """
                        INSERT INTO patients (
                          user_id, first_name, last_name, age, phone_number, address, relationship_to_user, onboarding_completed
                        ) VALUES (?, ?, ?, ?, ?, ?, 'Walk-in', 1)
                        """,
                        (
                            session["user_id"],
                            form_data["first_name"] or None,
                            form_data["last_name"] or None,
                            age_value,
                            form_data["phone_number"] or None,
                            form_data["address"] or None,
                        ),
                    )
                    patient_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    cur = db.execute(
                        """
                        INSERT INTO cases (
                          patient_id, clinic_id, exposure_date, type_of_exposure, animal_detail,
                          risk_level, category, case_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Pending')
                        """,
                        (
                            patient_id,
                            staff["clinic_id"],
                            form_data["exposure_date"],
                            form_data["type_of_exposure"],
                            form_data["animal_detail"],
                            risk_level,
                            risk_level,
                        ),
                    )
                    case_id = cur.lastrowid
                    hrtig_value = None
                    if form_data["hrtig_immunization"] in {"0", "1"}:
                        hrtig_value = int(form_data["hrtig_immunization"])
                    db.execute(
                        """
                        INSERT INTO pre_screening_details (
                          case_id, wound_description, bleeding_type, local_treatment,
                          patient_prev_immunization, prev_vaccine_date, tetanus_date, hrtig_immunization
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            form_data["wound_description"] or None,
                            form_data["bleeding_type"] or None,
                            form_data["local_treatment"] or None,
                            form_data["patient_prev_immunization"] or None,
                            form_data["prev_vaccine_date"] or None,
                            form_data["tetanus_date"] or None,
                            hrtig_value,
                        ),
                    )

                    def _normalize_iso_date_input(raw_value: str) -> str:
                        value = (raw_value or "").strip()
                        if not value:
                            return ""
                        try:
                            return datetime.fromisoformat(value).date().isoformat()
                        except ValueError:
                            return ""

                    vc_pcec_mfg_date = _normalize_iso_date_input(_v("vc_pcec_mfg_date"))
                    vc_pcec_expiry = _normalize_iso_date_input(_v("vc_pcec_expiry"))
                    today_iso = datetime.now().date().isoformat()
                    if vc_pcec_expiry and vc_pcec_expiry < today_iso:
                        flash("Expiry date cannot be earlier than today.", "error")
                        return redirect(url_for("staff_create_case_record"))

                    db.execute(
                        """
                        INSERT INTO vaccination_card (
                            case_id, anti_rabies, pvrv, pcec_batch, pcec_mfg_date, pcec_expiry,
                            erig_hrig, tetanus_prophylaxis, tetanus_toxoid, ats, htig, remarks
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            _v("vc_anti_rabies"),
                            _v("vc_pvrv"),
                            _v("vc_pcec_batch"),
                            vc_pcec_mfg_date,
                            vc_pcec_expiry,
                            _v("vc_erig_hrig"),
                            _v("vc_tetanus_prophylaxis"),
                            _v("vc_tetanus_toxoid"),
                            _v("vc_ats"),
                            _v("vc_htig"),
                            _v("vc_remarks"),
                        ),
                    )

                    for record_type, prefix, days in [
                        ("pre_exposure", "vc_pre", [0, 7, 28]),
                        ("post_exposure", "vc_post", [0, 3, 7, 14, 28]),
                        ("booster", "vc_booster", [0, 3]),
                    ]:
                        for day in days:
                            dose_date = _v(f"{prefix}_{day}_date")
                            type_of_vaccine = _v(f"{prefix}_{day}_type")
                            dose = _v(f"{prefix}_{day}_dose")
                            route_site = _v(f"{prefix}_{day}_route_site")
                            given_by = _v(f"{prefix}_{day}_given_by")
                            if any([dose_date, type_of_vaccine, dose, route_site, given_by]):
                                db.execute(
                                    """
                                    INSERT INTO vaccination_card_doses (
                                        case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        case_id,
                                        record_type,
                                        day,
                                        dose_date or None,
                                        type_of_vaccine or None,
                                        dose or None,
                                        route_site or None,
                                        given_by or None,
                                    ),
                                )

                    db.commit()
                    flash("Case record created successfully.", "success")
                    return redirect(url_for("view_patient_case", case_id=case_id))
                except Exception:
                    db.rollback()
                    flash("Failed to create case record. Please try again.", "error")

        personnel_rows = db.execute(
            """
            SELECT cp.title, cp.first_name, cp.last_name, u.username
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.clinic_id = ?
            ORDER BY cp.title, cp.first_name, cp.last_name, u.username
            """,
            (staff["clinic_id"],),
        ).fetchall()
        personnel_options = []
        seen_personnel = set()
        for row in personnel_rows:
            title = (row["title"] or "").strip()
            first_name = (row["first_name"] or "").strip()
            last_name = (row["last_name"] or "").strip()
            username = (row["username"] or "").strip()
            display_name = " ".join(part for part in [title, first_name, last_name] if part) or username
            if display_name and display_name not in seen_personnel:
                seen_personnel.add(display_name)
                personnel_options.append(display_name)
        suggested_dates_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": "Add Record", "href": None},
        ]
        return render_template(
            "staff_case_create.html",
            staff=staff,
            staff_display_name=staff_display_name,
            form=form_data,
            vaccination_card=vaccination_card,
            card_doses_by_type=card_doses_by_type,
            personnel_options=personnel_options,
            suggested_dates_by_type=suggested_dates_by_type,
            expiry_min_date=datetime.now().date().isoformat(),
            breadcrumbs=breadcrumbs,
            active_page="cases",
        )

    @app.get("/staff/patients")
    @role_required("clinic_personnel", "system_admin")
    def staff_patients():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email, c.name AS clinic_name
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            JOIN clinics c ON c.id = cp.clinic_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)
        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        search = (request.args.get("search") or "").strip()
        category = (request.args.get("category") or "all").strip().lower()
        if category not in {"all", "category i", "category ii", "category iii"}:
            category = "all"
        case_status = (request.args.get("status") or "all").strip().lower()
        if case_status not in {"all", "pending", "completed", "no show"}:
            case_status = "all"

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = 1 if page < 1 else page
        per_page = 10

        where_clauses = [
            "c.clinic_id = ?",
            "LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')",
        ]
        params: list[object] = [staff["clinic_id"]]

        if category != "all":
            where_clauses.append("LOWER(COALESCE(c.risk_level, c.category, '')) = ?")
            params.append(category)
        if case_status != "all":
            where_clauses.append("LOWER(COALESCE(c.case_status, 'pending')) = ?")
            params.append(case_status)

        if search:
            search_like = f"%{search.lower()}%"
            search_parts = [
                "LOWER(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')) LIKE ?",
                "LOWER(COALESCE(u.username, '')) LIKE ?",
            ]
            search_params: list[object] = [search_like, search_like]

            case_id_search = search.lower().removeprefix("c-")
            if case_id_search.isdigit():
                search_parts.append("c.id = ?")
                search_params.append(int(case_id_search))

            where_clauses.append("(" + " OR ".join(search_parts) + ")")
            params.extend(search_params)

        where_sql = " AND ".join(where_clauses)

        count_sql = (
            """
            SELECT COUNT(*) AS total
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            WHERE
            """
            + where_sql
        )
        total = db.execute(count_sql, params).fetchone()["total"]

        pages = max((total + per_page - 1) // per_page, 1)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        cases_query_sql = (
            """
            SELECT
                c.id AS case_id,
                COALESCE(
                    NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                    u.username,
                    'Unknown'
                ) AS patient_name,
                c.exposure_date,
                COALESCE(c.risk_level, c.category, 'N/A') AS category,
                COALESCE(c.case_status, 'Pending') AS case_status,
                COALESCE(
                    (
                        SELECT a.appointment_datetime
                        FROM appointments a
                        WHERE a.case_id = c.id
                          AND LOWER(COALESCE(a.status, '')) NOT IN ('removed', 'cancelled', 'canceled')
                        ORDER BY datetime(a.appointment_datetime) ASC, a.id ASC
                        LIMIT 1
                    ),
                    'N/A'
                ) AS initial_schedule
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            WHERE
            """
            + where_sql
            + """
            ORDER BY datetime(c.created_at) DESC, c.id DESC
            LIMIT ? OFFSET ?
            """
        )
        cases_rows = db.execute(cases_query_sql, [*params, per_page, offset]).fetchall()

        case_items = []
        for row in cases_rows:
            schedule_display = row["initial_schedule"] if row["initial_schedule"] else "N/A"
            if row["initial_schedule"] and row["initial_schedule"] != "N/A":
                try:
                    schedule_display = datetime.fromisoformat(row["initial_schedule"]).strftime("%b %d, %Y @ %I:%M %p")
                except ValueError:
                    schedule_display = row["initial_schedule"]

            category_value = (row["category"] or "").strip().lower()
            active_record_type = "pre_exposure" if category_value == "category i" else "post_exposure"
            expected_doses = 3 if active_record_type == "pre_exposure" else 5
            schedule_days = [0, 7, 28] if active_record_type == "pre_exposure" else [0, 3, 7, 14, 28]

            dose_rows = db.execute(
                """
                SELECT day_number, dose_date, type_of_vaccine, given_by
                FROM vaccination_card_doses
                WHERE case_id = ?
                  AND record_type = ?
                ORDER BY day_number ASC
                """,
                (row["case_id"], active_record_type),
            ).fetchall()
            active_rows = {dose["day_number"]: dict(dose) for dose in dose_rows}

            doses_completed = 0
            for dose in active_rows.values():
                dose_date = (dose.get("dose_date") or "").strip()
                type_of_vaccine = (dose.get("type_of_vaccine") or "").strip()
                given_by = (dose.get("given_by") or "").strip()
                if dose_date and type_of_vaccine and given_by:
                    doses_completed += 1

            schedule_or_next_dose = schedule_display
            if 0 < doses_completed < expected_doses:
                day0_row = active_rows.get(0)
                day0_raw = ((day0_row or {}).get("dose_date") or "").strip() if day0_row else ""
                day0_date = None
                if day0_raw:
                    try:
                        day0_date = datetime.fromisoformat(day0_raw).date()
                    except ValueError:
                        day0_date = None

                next_due_date = None
                for day in schedule_days:
                    dose = active_rows.get(day)
                    dose_date_raw = ((dose or {}).get("dose_date") or "").strip() if dose else ""
                    type_of_vaccine = ((dose or {}).get("type_of_vaccine") or "").strip() if dose else ""
                    given_by = ((dose or {}).get("given_by") or "").strip() if dose else ""
                    if dose_date_raw and type_of_vaccine and given_by:
                        continue
                    if dose_date_raw:
                        try:
                            next_due_date = datetime.fromisoformat(dose_date_raw).date()
                        except ValueError:
                            next_due_date = None
                    elif day0_date and day > 0:
                        next_due_date = day0_date + timedelta(days=day)
                    if next_due_date:
                        break

                if next_due_date:
                    schedule_or_next_dose = next_due_date.strftime("%b %d, %Y")

            if doses_completed >= expected_doses:
                schedule_or_next_dose = "N/A"

            case_items.append(
                {
                    "id": row["case_id"],
                    "case_code": f"C-000{row['case_id']}",
                    "patient_name": row["patient_name"],
                    "exposure_date": row["exposure_date"] or "N/A",
                    "category": row["category"],
                    "case_status": row["case_status"],
                    "schedule_next_dose": schedule_or_next_dose,
                }
            )

        cases = SimplePagination(case_items, page=page, per_page=per_page, total=total)

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": None},
        ]


        return render_template(
            "staff_patients.html",
            staff=staff,
            staff_display_name=staff_display_name,
            cases=cases,
            selected_category=category,
            selected_status=case_status,
            search=search,
            breadcrumbs=breadcrumbs,
            active_page="cases",
        )

    @app.get("/staff/appointments")
    @role_required("clinic_personnel", "system_admin")
    def staff_appointments():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)
        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        search = (request.args.get("search") or "").strip().lower()
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = 1 if page < 1 else page
        per_page = 10

        where_clauses = [
            "a.clinic_id = ?",
            "LOWER(COALESCE(a.status, '')) IN ('pending', 'queued')",
        ]
        params: list[object] = [staff["clinic_id"]]

        if search:
            where_clauses.append(
                """
                (
                    LOWER(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')) LIKE ?
                    OR LOWER(COALESCE(u.username, '')) LIKE ?
                    OR CAST(a.id AS TEXT) LIKE ?
                )
                """
            )
            search_like = f"%{search}%"
            params.extend([search_like, search_like, search_like])

        where_sql = " AND ".join(where_clauses)

        total = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            JOIN cases c ON c.id = a.case_id
            WHERE
            """
            + where_sql,
            params,
        ).fetchone()["total"]

        pages = max((total + per_page - 1) // per_page, 1)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        rows = db.execute(
            """
            SELECT
              a.id,
              a.appointment_datetime,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown'
              ) AS patient_name,
              COALESCE(c.risk_level, c.category, 'N/A') AS category
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            JOIN cases c ON c.id = a.case_id
            WHERE
            """
            + where_sql
            + """
            ORDER BY datetime(a.appointment_datetime) ASC, a.id ASC
            LIMIT ? OFFSET ?
            """,
            [*params, per_page, offset],
        ).fetchall()

        items = []
        for row in rows:
            appt_datetime = row["appointment_datetime"] or ""
            display_datetime = appt_datetime
            if appt_datetime:
                try:
                    display_datetime = datetime.fromisoformat(appt_datetime).strftime("%b %d, %Y @ %I:%M %p")
                except ValueError:
                    display_datetime = appt_datetime
            items.append(
                {
                    "id": row["id"],
                    "appointment_code": f"APT-{row['id']}",
                    "patient_name": row["patient_name"],
                    "appointment_datetime": display_datetime,
                    "category": row["category"],
                }
            )

        appointments = SimplePagination(items, page=page, per_page=per_page, total=total)
        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Appointments", "href": None},
        ]


        return render_template(
            "staff_appointments.html",
            staff=staff,
            staff_display_name=staff_display_name,
            appointments=appointments,
            search=search,
            breadcrumbs=breadcrumbs,
            active_page="appointments",
        )

    @app.get("/staff/vaccinations")
    @role_required("clinic_personnel", "system_admin")
    def staff_vaccinations():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        vaccine_type = (request.args.get("vaccine_type") or "").strip()
        dose_query = (request.args.get("dose_query") or "").strip()
        date_from = (request.args.get("date_from") or "").strip()
        date_to = (request.args.get("date_to") or "").strip()
        administered_by = (request.args.get("administered_by") or "").strip()
        sort_by = (request.args.get("sort_by") or "date").strip().lower()
        sort_dir = (request.args.get("sort_dir") or "desc").strip().lower()
        if sort_dir not in {"asc", "desc"}:
            sort_dir = "desc"
        if sort_by not in {"date", "vaccine_type", "dose", "administered_by", "patient"}:
            sort_by = "date"

        def _normalize_iso_date(raw_value: str) -> str:
            value = (raw_value or "").strip()
            if not value:
                return ""
            try:
                return datetime.fromisoformat(value).date().isoformat()
            except ValueError:
                return ""

        date_from = _normalize_iso_date(date_from)
        date_to = _normalize_iso_date(date_to)
        if date_from and date_to and date_from > date_to:
            flash("Date range is invalid. 'From' date must be on or before 'To' date.", "error")
            date_from = ""
            date_to = ""

        # Default view: vaccinations from the current week (latest to oldest).
        if not date_from and not date_to:
            today = datetime.now().date()
            date_to = today.isoformat()
            date_from = (today - timedelta(days=6)).isoformat()

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = 1 if page < 1 else page
        per_page = 10

        base_records_params: list[object] = [staff["clinic_id"]]
        base_card_params: list[object] = [staff["clinic_id"]]
        date_filters_records = ""
        date_filters_card = ""
        if date_from:
            date_filters_records += " AND DATE(vr.date_administered) >= DATE(?)"
            date_filters_card += " AND DATE(vcd.dose_date) >= DATE(?)"
            base_records_params.append(date_from)
            base_card_params.append(date_from)
        if date_to:
            date_filters_records += " AND DATE(vr.date_administered) <= DATE(?)"
            date_filters_card += " AND DATE(vcd.dose_date) <= DATE(?)"
            base_records_params.append(date_to)
            base_card_params.append(date_to)

        records_rows = db.execute(
            """
            SELECT
              vr.id,
              vr.case_id,
              vr.vaccine_type,
              vr.dose_number,
              vr.dose_amount,
              vr.date_administered,
              COALESCE(
                NULLIF(TRIM(COALESCE(cp.title, '') || ' ' || COALESCE(cp.first_name, '') || ' ' || COALESCE(cp.last_name, '')), ''),
                au.username,
                'Unknown Staff'
              ) AS administered_by_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                pu.username,
                'Unknown Patient'
              ) AS patient_name
            FROM vaccination_records vr
            JOIN cases c ON c.id = vr.case_id
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users pu ON pu.id = p.user_id
            LEFT JOIN clinic_personnel cp ON cp.id = vr.administered_by_personnel_id
            LEFT JOIN users au ON au.id = cp.user_id
            WHERE c.clinic_id = ?
            """
            + date_filters_records,
            base_records_params,
        ).fetchall()

        card_rows = db.execute(
            """
            SELECT
              vcd.id,
              vcd.case_id,
              vcd.type_of_vaccine AS vaccine_type,
              CAST(vcd.day_number AS TEXT) AS dose_number,
              vcd.dose AS dose_amount,
              vcd.dose_date AS date_administered,
              TRIM(COALESCE(vcd.given_by, '')) AS administered_by_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                pu.username,
                'Unknown Patient'
              ) AS patient_name
            FROM vaccination_card_doses vcd
            JOIN cases c ON c.id = vcd.case_id
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users pu ON pu.id = p.user_id
            WHERE c.clinic_id = ?
              AND TRIM(COALESCE(vcd.dose_date, '')) <> ''
              AND TRIM(COALESCE(vcd.type_of_vaccine, '')) <> ''
              AND TRIM(COALESCE(vcd.given_by, '')) <> ''
            """
            + date_filters_card,
            base_card_params,
        ).fetchall()

        normalized_rows = []
        seen_keys = set()

        def _safe_date(raw_value: str) -> str:
            try:
                return datetime.fromisoformat((raw_value or "").strip()).date().isoformat()
            except ValueError:
                return ""

        for source, rows in (("records", records_rows), ("card", card_rows)):
            for row in rows:
                date_iso = _safe_date(row["date_administered"] or "")
                vaccine_value = (row["vaccine_type"] or "").strip()
                dose_number = (row["dose_number"] or "").strip()
                dose_amount = (row["dose_amount"] or "").strip()
                administered_name = (row["administered_by_name"] or "").strip()
                dedupe_key = (
                    row["case_id"],
                    date_iso,
                    vaccine_value.lower(),
                    dose_number.lower(),
                    dose_amount.lower(),
                    administered_name.lower(),
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)

                normalized_rows.append(
                    {
                        "id": row["id"],
                        "source": source,
                        "case_id": row["case_id"],
                        "case_code": f"C-000{row['case_id']}",
                        "patient_name": row["patient_name"] or "Unknown Patient",
                        "vaccine_type": vaccine_value or "N/A",
                        "dose_number": dose_number,
                        "dose_amount": dose_amount,
                        "date_iso": date_iso,
                        "administered_by_name": administered_name or "Unknown Staff",
                    }
                )

        vaccine_type_l = vaccine_type.lower()
        dose_query_l = dose_query.lower()
        administered_by_l = administered_by.lower()
        filtered_rows = []
        for row in normalized_rows:
            if vaccine_type_l and vaccine_type_l not in (row["vaccine_type"] or "").lower():
                continue
            if dose_query_l:
                dose_haystack = f"{row['dose_number']} {row['dose_amount']}".lower()
                if dose_query_l not in dose_haystack:
                    continue
            if date_from and row["date_iso"] and row["date_iso"] < date_from:
                continue
            if date_from and not row["date_iso"]:
                continue
            if date_to and row["date_iso"] and row["date_iso"] > date_to:
                continue
            if date_to and not row["date_iso"]:
                continue
            if administered_by_l and administered_by_l not in (row["administered_by_name"] or "").lower():
                continue
            filtered_rows.append(row)

        def _sort_key(row):
            if sort_by == "vaccine_type":
                return (row["vaccine_type"] or "").lower()
            if sort_by == "dose":
                return f"{row['dose_number']} {row['dose_amount']}".lower()
            if sort_by == "administered_by":
                return (row["administered_by_name"] or "").lower()
            if sort_by == "patient":
                return (row["patient_name"] or "").lower()
            return row["date_iso"] or ""

        filtered_rows.sort(key=_sort_key, reverse=(sort_dir == "desc"))

        total = len(filtered_rows)
        pages = max((total + per_page - 1) // per_page, 1)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page
        page_rows = filtered_rows[offset : offset + per_page]

        items = []
        for row in page_rows:
            date_display = row["date_iso"] or "N/A"
            if row["date_iso"]:
                try:
                    date_display = datetime.fromisoformat(row["date_iso"]).strftime("%b %d, %Y")
                except ValueError:
                    date_display = row["date_iso"]
            dose_display = row["dose_number"] or ""
            if row["dose_amount"]:
                dose_display = f"{dose_display} ({row['dose_amount']})" if dose_display else row["dose_amount"]
            items.append(
                {
                    "id": row["id"],
                    "case_id": row["case_id"],
                    "case_code": row["case_code"],
                    "patient_name": row["patient_name"],
                    "vaccine_type": row["vaccine_type"],
                    "dose_display": dose_display or "N/A",
                    "date_given": date_display,
                    "administered_by_name": row["administered_by_name"],
                }
            )

        vaccinations = SimplePagination(items, page=page, per_page=per_page, total=total)

        personnel_rows = db.execute(
            """
            SELECT cp.title, cp.first_name, cp.last_name, u.username
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.clinic_id = ?
            ORDER BY cp.title, cp.first_name, cp.last_name, u.username
            """,
            (staff["clinic_id"],),
        ).fetchall()
        administered_by_options = []
        seen_options = set()
        for row in personnel_rows:
            title = (row["title"] or "").strip()
            first_name = (row["first_name"] or "").strip()
            last_name = (row["last_name"] or "").strip()
            username = (row["username"] or "").strip()
            display_name = " ".join(part for part in [title, first_name, last_name] if part) or username
            if display_name and display_name not in seen_options:
                seen_options.add(display_name)
                administered_by_options.append(display_name)

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Vaccinations", "href": None},
        ]

        return render_template(
            "staff_vaccinations.html",
            staff=staff,
            staff_display_name=staff_display_name,
            vaccinations=vaccinations,
            vaccine_type=vaccine_type,
            dose_query=dose_query,
            date_from=date_from,
            date_to=date_to,
            administered_by=administered_by,
            administered_by_options=administered_by_options,
            sort_by=sort_by,
            sort_dir=sort_dir,
            breadcrumbs=breadcrumbs,
            active_page="vaccinations",
        )

    def _get_staff_and_clinic():
        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            return None, None
        return db, staff

    @app.get("/staff/appointments/availability")
    @role_required("clinic_personnel", "system_admin")
    def staff_availability():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))
        db, staff = _get_staff_and_clinic()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        filter_date = (request.args.get("filter_date") or "").strip()
        today = datetime.now().date().isoformat()
        if not filter_date:
            filter_date = today
        from_date = filter_date
        to_date = filter_date

        rows = db.execute(
            """
            SELECT s.id, s.slot_datetime, s.duration_minutes, s.max_bookings, s.is_active,
                   (SELECT COUNT(*) FROM appointments a
                    WHERE a.clinic_id = s.clinic_id
                      AND a.appointment_datetime = s.slot_datetime
                      AND LOWER(COALESCE(a.status, '')) != 'cancelled') AS booking_count
            FROM availability_slots s
            WHERE s.clinic_id = ?
              AND DATE(s.slot_datetime) >= ?
              AND DATE(s.slot_datetime) <= ?
            ORDER BY s.slot_datetime ASC
            """,
            (staff["clinic_id"], from_date, to_date),
        ).fetchall()

        slots = []
        for row in rows:
            dt_str = row["slot_datetime"] or ""
            display_datetime = dt_str
            if dt_str:
                try:
                    display_datetime = datetime.fromisoformat(dt_str).strftime("%b %d, %Y @ %I:%M %p")
                except ValueError:
                    pass
            slots.append({
                "id": row["id"],
                "slot_datetime": dt_str,
                "display_datetime": display_datetime,
                "duration_minutes": row["duration_minutes"],
                "max_bookings": row["max_bookings"],
                "is_active": bool(row["is_active"]),
                "booking_count": row["booking_count"] or 0,
                "is_taken": (row["booking_count"] or 0) >= (row["max_bookings"] or 1),
            })

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Appointments", "href": url_for("staff_appointments")},
            {"label": "Manage availability", "href": None},
        ]
        return render_template(
            "staff_availability.html",
            staff=staff,
            staff_display_name=staff_display_name,
            slots=slots,
            filter_date=filter_date,
            from_date=from_date,
            to_date=to_date,
            breadcrumbs=breadcrumbs,
            active_page="appointments",
        )

    @app.post("/staff/appointments/availability")
    @role_required("clinic_personnel", "system_admin")
    def staff_availability_post():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))
        db, staff = _get_staff_and_clinic()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        slot_date_from = (request.form.get("slot_date_from") or "").strip()
        slot_date_to = (request.form.get("slot_date_to") or "").strip()
        start_time = (request.form.get("start_time") or "08:00").strip()
        end_time = (request.form.get("end_time") or "17:00").strip()
        lunch_start = (request.form.get("lunch_start") or "12:00").strip()
        lunch_end = (request.form.get("lunch_end") or "13:00").strip()
        duration_minutes = request.form.get("duration_minutes", "45").strip() or "45"
        max_bookings = request.form.get("max_bookings", "1").strip() or "1"

        try:
            duration_minutes = int(duration_minutes)
            max_bookings = int(max_bookings)
        except ValueError:
            duration_minutes = 45
            max_bookings = 1
        if duration_minutes < 1:
            duration_minutes = 45
        if max_bookings < 1:
            max_bookings = 1

        if not slot_date_from or not slot_date_to:
            flash("Please select both From and To dates.", "error")
            return redirect(url_for("staff_availability"))

        try:
            date_from = datetime.strptime(slot_date_from, "%Y-%m-%d").date()
            date_to = datetime.strptime(slot_date_to, "%Y-%m-%d").date()
        except ValueError:
            flash("Invalid date format.", "error")
            return redirect(url_for("staff_availability"))

        if date_from > date_to:
            flash("From date must be before or equal to To date.", "error")
            return redirect(url_for("staff_availability"))

        def parse_time(t):
            try:
                return datetime.strptime(t, "%H:%M").time()
            except ValueError:
                return None

        st = parse_time(start_time)
        et = parse_time(end_time)
        ls = parse_time(lunch_start)
        le = parse_time(lunch_end)
        if st is None or et is None:
            flash("Invalid start or end time.", "error")
            return redirect(url_for("staff_availability"))
        if ls is None:
            ls = datetime.strptime("12:00", "%H:%M").time()
        if le is None:
            le = datetime.strptime("13:00", "%H:%M").time()

        created = 0
        current_date = date_from
        interval = timedelta(minutes=45)
        while current_date <= date_to:
            current = datetime.combine(current_date, st)
            end_dt = datetime.combine(current_date, et)
            lunch_start_dt = datetime.combine(current_date, ls)
            lunch_end_dt = datetime.combine(current_date, le)
            while current < end_dt:
                if current >= lunch_end_dt or current < lunch_start_dt:
                    slot_dt = current.isoformat()
                    try:
                        db.execute(
                            """
                            INSERT INTO availability_slots (clinic_id, slot_datetime, duration_minutes, max_bookings, is_active)
                            VALUES (?, ?, ?, ?, 1)
                            """,
                            (staff["clinic_id"], slot_dt, duration_minutes, max_bookings),
                        )
                        created += 1
                    except sqlite3.IntegrityError:
                        pass
                current += interval
            current_date += timedelta(days=1)

        db.commit()
        flash(f"Created {created} slot(s) from {slot_date_from} to {slot_date_to}.", "success")
        return redirect(url_for("staff_availability"))

    @app.post("/staff/appointments/availability/<int:slot_id>/delete")
    @role_required("clinic_personnel", "system_admin")
    def staff_availability_delete(slot_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))
        db, staff = _get_staff_and_clinic()
        if staff is None:
            return redirect(url_for("auth.login"))
        row = db.execute(
            "SELECT id FROM availability_slots WHERE id = ? AND clinic_id = ?",
            (slot_id, staff["clinic_id"]),
        ).fetchone()
        if row:
            db.execute("DELETE FROM availability_slots WHERE id = ? AND clinic_id = ?", (slot_id, staff["clinic_id"]))
            db.commit()
            flash("Slot deleted.", "success")
        else:
            flash("Slot not found.", "error")
        return redirect(url_for("staff_availability"))

    @app.post("/staff/appointments/availability/<int:slot_id>/deactivate")
    @role_required("clinic_personnel", "system_admin")
    def staff_availability_deactivate(slot_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))
        db, staff = _get_staff_and_clinic()
        if staff is None:
            return redirect(url_for("auth.login"))
        row = db.execute(
            "SELECT id FROM availability_slots WHERE id = ? AND clinic_id = ?",
            (slot_id, staff["clinic_id"]),
        ).fetchone()
        if row:
            db.execute(
                "UPDATE availability_slots SET is_active = 0 WHERE id = ? AND clinic_id = ?",
                (slot_id, staff["clinic_id"]),
            )
            db.commit()
            flash("Slot deactivated.", "success")
        else:
            flash("Slot not found.", "error")
        return redirect(url_for("staff_availability"))

    @app.get("/staff/appointments/<int:appointment_id>")
    @role_required("clinic_personnel", "system_admin")
    def view_appointment(appointment_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        appt = db.execute(
            """
            SELECT
              a.*,
              p.first_name,
              p.last_name,
              p.phone_number,
              p.address,
              u.email,
              c.id AS case_id,
              c.type_of_exposure,
              c.exposure_date,
              COALESCE(c.risk_level, c.category, 'N/A') AS category,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.prev_vaccine_date,
              psd.tetanus_date,
              psd.hrtig_immunization
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            JOIN cases c ON c.id = a.case_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE a.id = ?
              AND a.clinic_id = ?
            """,
            (appointment_id, staff["clinic_id"]),
        ).fetchone()
        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("staff_appointments"))

        patient_name = " ".join(part for part in [(appt["first_name"] or "").strip(), (appt["last_name"] or "").strip()] if part) or "Unknown"
        appt_date = ""
        appt_time = ""
        requested_schedule_display = None
        if appt["appointment_datetime"]:
            try:
                dt = datetime.fromisoformat(appt["appointment_datetime"])
                appt_date = dt.strftime("%Y-%m-%d")
                appt_time = dt.strftime("%H:%M")
                requested_schedule_display = dt.strftime("%B %d, %Y at %I:%M %p")
            except ValueError:
                requested_schedule_display = appt["appointment_datetime"]
        if not requested_schedule_display:
            requested_schedule_display = "Not set"

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Appointments", "href": url_for("staff_appointments")},
            {"label": f"#{appt['id']}", "href": None},
        ]

        return render_template(
            "staff_appointment_view.html",
            staff=staff,
            staff_display_name=staff_display_name,
            appointment=appt,
            patient_name=patient_name,
            appointment_date=appt_date,
            appointment_time=appt_time,
            requested_schedule_display=requested_schedule_display,
            breadcrumbs=breadcrumbs,
            active_page="appointments",
        )

    @app.post("/staff/appointments/<int:appointment_id>/approve")
    @role_required("clinic_personnel", "system_admin")
    def approve_appointment(appointment_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            "SELECT clinic_id FROM clinic_personnel WHERE user_id = ?",
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        appt = db.execute(
            "SELECT id, case_id, patient_id FROM appointments WHERE id = ? AND clinic_id = ?",
            (appointment_id, staff["clinic_id"]),
        ).fetchone()
        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("staff_appointments"))

        db.execute(
            "UPDATE appointments SET status = ? WHERE id = ? AND clinic_id = ?",
            ("Approved", appointment_id, staff["clinic_id"]),
        )
        db.execute(
            """
            UPDATE cases
            SET case_status = ?
            WHERE id = ?
              AND clinic_id = ?
              AND LOWER(COALESCE(case_status, 'pending')) IN ('pending', 'queued', 'scheduled')
            """,
            ("Pending", appt["case_id"], staff["clinic_id"]),
        )

        # Notify the patient (self or dependent) that the appointment was approved.
        _insert_patient_notification(
            patient_id=appt["patient_id"],
            notif_type="appointment",
            source_id=appointment_id,
            message="Your appointment has been approved by the clinic.",
        )
        db.commit()


        flash("Appointment approved.", "success")
        return redirect(url_for("staff_appointments"))

    @app.post("/staff/appointments/<int:appointment_id>/remove")
    @role_required("clinic_personnel", "system_admin")
    def remove_appointment(appointment_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            "SELECT clinic_id FROM clinic_personnel WHERE user_id = ?",
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        db.execute(
            "UPDATE appointments SET status = ? WHERE id = ? AND clinic_id = ?",
            ("Removed", appointment_id, staff["clinic_id"]),
        )
        db.commit()


        flash("Appointment request removed.", "success")
        return redirect(url_for("staff_appointments"))

    @app.post("/staff/appointments/<int:appointment_id>/edit")
    @role_required("clinic_personnel", "system_admin")
    def edit_appointment(appointment_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            "SELECT clinic_id FROM clinic_personnel WHERE user_id = ?",
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        appt = db.execute(
            """
            SELECT id, patient_id, clinic_id, status
            FROM appointments
            WHERE id = ? AND clinic_id = ?
            """,
            (appointment_id, staff["clinic_id"]),
        ).fetchone()
        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("staff_appointments"))

        slot_id_raw = (request.form.get("appointment_slot_id") or "").strip()
        if not slot_id_raw:
            flash("Please select a new time slot.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        try:
            slot_id = int(slot_id_raw)
        except ValueError:
            flash("Invalid slot selection.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        slot_row = db.execute(
            """
            SELECT id, slot_datetime, max_bookings
            FROM availability_slots
            WHERE id = ? AND clinic_id = ? AND is_active = 1
            """,
            (slot_id, appt["clinic_id"]),
        ).fetchone()

        if not slot_row:
            flash("Selected slot is no longer available.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        slot_datetime = slot_row["slot_datetime"]
        if not slot_datetime:
            flash("Selected slot is invalid.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        if slot_datetime <= datetime.now().isoformat():
            flash("The selected slot is in the past. Please choose another date and time.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        # Capacity check excluding this appointment itself
        existing_count = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM appointments
            WHERE clinic_id = ?
              AND appointment_datetime = ?
              AND id != ?
              AND LOWER(COALESCE(status, '')) != 'cancelled'
            """,
            (appt["clinic_id"], slot_datetime, appointment_id),
        ).fetchone()["n"]
        max_bookings = slot_row["max_bookings"] or 1
        if existing_count >= max_bookings:
            flash("This time slot is no longer available. Please choose another.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        db.execute(
            """
            UPDATE appointments
            SET appointment_datetime = ?,
                status = ?
            WHERE id = ? AND clinic_id = ?
            """,
            (slot_datetime, "Rescheduled", appointment_id, staff["clinic_id"]),
        )
        db.commit()

        flash("Appointment updated.", "success")
        return redirect(url_for("view_appointment", appointment_id=appointment_id))

    def _build_staff_case_context(case_id: int, staff_user_id: int) -> dict | None:
        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username, u.email
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (staff_user_id,),
        ).fetchone()

        if staff is None:
            return None

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        case_row = db.execute(
            """
            SELECT
              c.*,
              cl.name AS clinic_name,
              p.phone_number,
              u.email,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.prev_vaccine_date,
              psd.tetanus_date,
              psd.hrtig_immunization,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown Patient'
              ) AS patient_name
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            JOIN clinics cl ON cl.id = c.clinic_id
            WHERE c.id = ?
              AND c.clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()

        if case_row is None:
            return None

        dose_rows = db.execute(
            """
            SELECT
              vr.id,
              vr.dose_number,
              vr.date_administered,
              vr.next_dose_date,
              COALESCE(
                NULLIF(TRIM(COALESCE(cp.title, '') || ' ' || COALESCE(cp.first_name, '') || ' ' || COALESCE(cp.last_name, '')), ''),
                au.username,
                'Unknown Staff'
              ) AS administered_by_name
            FROM vaccination_records vr
            LEFT JOIN clinic_personnel cp ON cp.id = vr.administered_by_personnel_id
            LEFT JOIN users au ON au.id = cp.user_id
            WHERE vr.case_id = ?
            ORDER BY datetime(vr.date_administered) ASC, vr.id ASC
            """,
            (case_id,),
        ).fetchall()
        dose_records: list[dict] = []
        for row in dose_rows:
            date_administered_display = row["date_administered"] or "N/A"
            if row["date_administered"]:
                try:
                    date_administered_display = datetime.fromisoformat(row["date_administered"]).strftime("%B %d, %Y")
                except ValueError:
                    date_administered_display = row["date_administered"]
            dose_records.append(
                {
                    "id": row["id"],
                    "dose_number": row["dose_number"],
                    "date_administered": date_administered_display,
                    "next_dose_date": row["next_dose_date"],
                    "administered_by_name": row["administered_by_name"],
                }
            )

        next_appointment = db.execute(
            """
            SELECT appointment_datetime, status, type
            FROM appointments
            WHERE case_id = ?
              AND datetime(appointment_datetime) >= datetime('now', 'localtime')
            ORDER BY datetime(appointment_datetime) ASC, id ASC
            LIMIT 1
            """,
            (case_id,),
        ).fetchone()
        next_appointment_display = None
        if next_appointment and next_appointment["appointment_datetime"]:
            try:
                dt = datetime.fromisoformat(next_appointment["appointment_datetime"])
                next_appointment_display = dt.strftime("%B %d, %Y @ %I:%M %p")
            except ValueError:
                next_appointment_display = next_appointment["appointment_datetime"]

        notes_rows = db.execute(
            """
            SELECT
              cn.note_content,
              cn.created_at,
              COALESCE(
                NULLIF(TRIM(COALESCE(cp.title, '') || ' ' || COALESCE(cp.first_name, '') || ' ' || COALESCE(cp.last_name, '')), ''),
                u.username,
                'Unknown Author'
              ) AS author_name
            FROM case_notes cn
            LEFT JOIN users u ON u.id = cn.user_id
            LEFT JOIN clinic_personnel cp ON cp.user_id = u.id
            WHERE cn.case_id = ?
            ORDER BY datetime(cn.created_at) DESC, cn.id DESC
            """,
            (case_id,),
        ).fetchall()
        notes: list[dict] = []
        for row in notes_rows:
            created_at_display = row["created_at"] or ""
            if row["created_at"]:
                try:
                    created_at_display = datetime.fromisoformat(row["created_at"]).strftime("%b %d, %Y, %I:%M %p")
                except ValueError:
                    created_at_display = row["created_at"]
            notes.append(
                {
                    "note_content": row["note_content"],
                    "created_at": created_at_display,
                    "author_name": row["author_name"],
                }
            )

        vc_row = db.execute(
            "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
        ).fetchone()
        vaccination_card = dict(vc_row) if vc_row else {}
        vaccination_card_doses_rows = db.execute(
            """
            SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
            FROM vaccination_card_doses
            WHERE case_id = ?
            ORDER BY record_type, day_number
            """,
            (case_id,),
        ).fetchall()
        card_doses_by_type: dict[str, dict[int, dict]] = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}
        for row in vaccination_card_doses_rows:
            r = row["record_type"]
            d = row["day_number"]
            if r in card_doses_by_type:
                card_doses_by_type[r][d] = dict(row)

        category_value = (case_row["risk_level"] or case_row["category"] or "").strip().lower()
        active_record_type = "pre_exposure" if category_value == "category i" else "post_exposure"
        dose_type_label = (
            "Pre-Exposure Dose"
            if active_record_type == "pre_exposure"
            else "Post-Exposure Dose"
        )
        expected_doses = 3 if active_record_type == "pre_exposure" else 5
        doses_completed = 0
        for row in card_doses_by_type.get(active_record_type, {}).values():
            dose_date = (row.get("dose_date") or "").strip()
            type_of_vaccine = (row.get("type_of_vaccine") or "").strip()
            given_by = (row.get("given_by") or "").strip()
            if dose_date and type_of_vaccine and given_by:
                doses_completed += 1
        progress_pct = min(round((doses_completed / expected_doses) * 100), 100) if expected_doses else 0

        schedule_days = [0, 7, 28] if active_record_type == "pre_exposure" else [0, 3, 7, 14, 28]
        active_rows = card_doses_by_type.get(active_record_type, {})
        day0_row = active_rows.get(0)
        day0_raw = ((day0_row or {}).get("dose_date") or "").strip() if day0_row else ""
        day0_date = None
        if day0_raw:
            try:
                day0_date = datetime.fromisoformat(day0_raw).date()
            except ValueError:
                day0_date = None

        next_due_date = None
        for day in schedule_days:
            row = active_rows.get(day)
            dose_date_raw = ((row or {}).get("dose_date") or "").strip() if row else ""
            type_of_vaccine = ((row or {}).get("type_of_vaccine") or "").strip() if row else ""
            given_by = ((row or {}).get("given_by") or "").strip() if row else ""

            if dose_date_raw and type_of_vaccine and given_by:
                continue

            if dose_date_raw:
                try:
                    next_due_date = datetime.fromisoformat(dose_date_raw).date()
                except ValueError:
                    next_due_date = None
            elif day0_date and day > 0:
                next_due_date = day0_date + timedelta(days=day)

            if next_due_date:
                break

        if next_due_date:
            next_appointment_display = next_due_date.strftime("%B %d, %Y")

        if not dose_records:
            active_rows = card_doses_by_type.get(active_record_type, {})
            derived_dose_records: list[dict] = []

            def _ordinal(n: int) -> str:
                if 10 <= (n % 100) <= 20:
                    suffix = "th"
                else:
                    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
                return f"{n}{suffix}"

            for day in sorted(active_rows.keys()):
                row = active_rows[day]
                dose_date_raw = (row.get("dose_date") or "").strip()
                type_of_vaccine = (row.get("type_of_vaccine") or "").strip()
                given_by = (row.get("given_by") or "").strip()
                if not (dose_date_raw and type_of_vaccine and given_by):
                    continue
                try:
                    dose_date_display = datetime.fromisoformat(dose_date_raw).strftime("%B %d, %Y")
                except ValueError:
                    dose_date_display = dose_date_raw
                dose_index = len(derived_dose_records) + 1
                derived_dose_records.append(
                    {
                        "id": row.get("id"),
                        "dose_number": _ordinal(dose_index),
                        "date_administered": dose_date_display,
                        "next_dose_date": None,
                        "administered_by_name": given_by,
                    }
                )
            dose_records = derived_dose_records

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": case_row["patient_name"], "href": None},
        ]

        return {
            "db": db,
            "staff": staff,
            "staff_display_name": staff_display_name,
            "case": case_row,
            "dose_records": dose_records,
            "doses_completed": doses_completed,
            "expected_doses": expected_doses,
            "progress_pct": progress_pct,
            "next_appointment": next_appointment,
            "next_appointment_display": next_appointment_display,
            "notes": notes,
            "vaccination_card": vaccination_card,
            "card_doses_by_type": card_doses_by_type,
            "active_record_type": active_record_type,
            "dose_type_label": dose_type_label,
            "breadcrumbs": breadcrumbs,
        }

    @app.get("/staff/patients/<int:case_id>")
    @role_required("clinic_personnel", "system_admin")
    def view_patient_case(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        context = _build_staff_case_context(case_id=case_id, staff_user_id=session["user_id"])
        if context is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        if context["case"] is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        return render_template(
            "staff_patient_view.html",
            active_page="cases",
            **{k: v for k, v in context.items() if k != "db"},
        )

    @app.post("/staff/cases/<int:case_id>/notes")
    @role_required("clinic_personnel", "system_admin")
    def add_case_note(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT clinic_id
            FROM clinic_personnel
            WHERE user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        case_row = db.execute(
            """
            SELECT id
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        note_content = (request.form.get("note_content") or "").strip()
        if not note_content:
            flash("Note cannot be empty.", "error")
            return redirect(url_for("view_patient_case", case_id=case_id))
        if len(note_content) > 1000:
            flash("Note is too long. Maximum is 1000 characters.", "error")
            return redirect(url_for("view_patient_case", case_id=case_id))

        db.execute(
            """
            INSERT INTO case_notes (case_id, user_id, note_content)
            VALUES (?, ?, ?)
            """,
            (case_id, session["user_id"], note_content),
        )
        db.commit()

        flash("Note added.", "success")
        return redirect(url_for("view_patient_case", case_id=case_id))

    @app.post("/staff/cases/<int:case_id>/delete")
    @role_required("clinic_personnel", "system_admin")
    def delete_patient_case(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT clinic_id
            FROM clinic_personnel
            WHERE user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        case_row = db.execute(
            """
            SELECT id, case_status
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        db.execute(
            """
            UPDATE cases
            SET case_status = ?
            WHERE id = ? AND clinic_id = ?
            """,
            ("archived", case_id, staff["clinic_id"]),
        )
        db.commit()

        flash("Case removed successfully.", "success")
        return redirect(url_for("staff_patients"))

    @app.post("/staff/cases/<int:case_id>/complete")
    @role_required("clinic_personnel", "system_admin")
    def complete_patient_case(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT clinic_id
            FROM clinic_personnel
            WHERE user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        case_row = db.execute(
            """
            SELECT id
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        db.execute(
            """
            UPDATE cases
            SET case_status = 'Completed'
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        )
        db.execute(
            """
            UPDATE appointments
            SET status = 'Completed'
            WHERE id = (
                SELECT id
                FROM appointments
                WHERE case_id = ? AND clinic_id = ?
                ORDER BY datetime(appointment_datetime) DESC, id DESC
                LIMIT 1
            )
            """,
            (case_id, staff["clinic_id"]),
        )
        db.commit()


        flash("Case marked as completed.", "success")
        return redirect(url_for("view_patient_case", case_id=case_id))

    @app.route("/staff/cases/<int:case_id>/edit", methods=["GET", "POST"])
    @role_required("clinic_personnel", "system_admin")
    def edit_patient_case(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.*, u.username
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        case_patient = db.execute(
            """
            SELECT
              c.id AS case_id,
              c.clinic_id,
              c.exposure_date,
              c.type_of_exposure,
              c.animal_detail,
              c.category,
              c.risk_level,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.prev_vaccine_date,
              psd.tetanus_date,
              psd.hrtig_immunization,
              p.id AS patient_id,
              p.first_name,
              p.last_name,
              p.age,
              p.address,
              p.phone_number,
              u.email
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE c.id = ?
              AND c.clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_patient is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        if request.method == "POST":
            full_name = (request.form.get("full_name") or "").strip()
            age_raw = (request.form.get("age") or "").strip()
            address = (request.form.get("address") or "").strip()
            phone_number = (request.form.get("phone_number") or "").strip()
            email = (request.form.get("email") or "").strip().lower()
            exposure_date = (request.form.get("exposure_date") or "").strip()
            type_of_exposure = (request.form.get("type_of_exposure") or "").strip()
            animal_detail = (request.form.get("animal_detail") or "").strip()
            risk_level = (request.form.get("risk_level") or request.form.get("category") or "").strip()
            wound_description = (request.form.get("wound_description") or "").strip()
            bleeding_type = (request.form.get("bleeding_type") or "").strip()
            local_treatment = (request.form.get("local_treatment") or "").strip()
            patient_prev_immunization = (request.form.get("patient_prev_immunization") or "").strip()
            prev_vaccine_date = (request.form.get("prev_vaccine_date") or "").strip()
            tetanus_date = (request.form.get("tetanus_date") or "").strip()
            hrtig_raw = (request.form.get("hrtig_immunization") or "").strip()
            hrtig_immunization = None
            if hrtig_raw in {"0", "1"}:
                hrtig_immunization = int(hrtig_raw)

            first_name = None
            last_name = None
            if full_name:
                parts = full_name.split(" ", 1)
                first_name = parts[0]
                last_name = parts[1] if len(parts) > 1 else ""

            age = case_patient["age"]
            if age_raw:
                try:
                    age = int(age_raw)
                except ValueError:
                    flash("Age must be a number.", "error")
                    return redirect(url_for("edit_patient_case", case_id=case_id))

            db.execute(
                """
                UPDATE patients
                SET first_name = ?,
                    last_name = ?,
                    age = ?,
                    address = ?,
                    phone_number = ?
                WHERE id = ?
                """,
                (
                    first_name if first_name is not None else case_patient["first_name"],
                    last_name if last_name is not None else case_patient["last_name"],
                    age,
                    address if address else case_patient["address"],
                    phone_number if phone_number else case_patient["phone_number"],
                    case_patient["patient_id"],
                ),
            )

            db.execute(
                """
                UPDATE cases
                SET exposure_date = ?,
                    type_of_exposure = ?,
                    animal_detail = ?,
                    category = ?,
                    risk_level = ?
                WHERE id = ? AND clinic_id = ?
                """,
                (
                    exposure_date if exposure_date else case_patient["exposure_date"],
                    type_of_exposure if type_of_exposure else case_patient["type_of_exposure"],
                    animal_detail if animal_detail else case_patient["animal_detail"],
                    risk_level if risk_level else case_patient["category"],
                    risk_level if risk_level else case_patient["risk_level"],
                    case_id,
                    staff["clinic_id"],
                ),
            )

            if email:
                db.execute(
                    """
                    UPDATE users
                    SET email = ?
                    WHERE id = (SELECT user_id FROM patients WHERE id = ?)
                    """,
                    (email, case_patient["patient_id"]),
                )

            db.execute(
                """
                INSERT INTO pre_screening_details (
                    case_id,
                    wound_description,
                    bleeding_type,
                    local_treatment,
                    patient_prev_immunization,
                    prev_vaccine_date,
                    tetanus_date,
                    hrtig_immunization
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(case_id) DO UPDATE SET
                    wound_description = excluded.wound_description,
                    bleeding_type = excluded.bleeding_type,
                    local_treatment = excluded.local_treatment,
                    patient_prev_immunization = excluded.patient_prev_immunization,
                    prev_vaccine_date = excluded.prev_vaccine_date,
                    tetanus_date = excluded.tetanus_date,
                    hrtig_immunization = excluded.hrtig_immunization
                """,
                (
                    case_id,
                    wound_description or None,
                    bleeding_type or None,
                    local_treatment or None,
                    patient_prev_immunization or None,
                    prev_vaccine_date or None,
                    tetanus_date or None,
                    hrtig_immunization,
                ),
            )

            def _v(name):
                return (request.form.get(name) or "").strip()

            def _normalize_iso_date_input(raw_value: str) -> str:
                value = (raw_value or "").strip()
                if not value:
                    return ""
                try:
                    return datetime.fromisoformat(value).date().isoformat()
                except ValueError:
                    return ""

            vc_pcec_mfg_date = _normalize_iso_date_input(_v("vc_pcec_mfg_date"))
            vc_pcec_expiry = _normalize_iso_date_input(_v("vc_pcec_expiry"))
            today_iso = datetime.now().date().isoformat()
            if vc_pcec_expiry and vc_pcec_expiry < today_iso:
                flash("Expiry date cannot be earlier than today.", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))

            db.execute(
                """
                INSERT INTO vaccination_card (
                    case_id, anti_rabies, pvrv, pcec_batch, pcec_mfg_date, pcec_expiry,
                    erig_hrig, tetanus_prophylaxis, tetanus_toxoid, ats, htig, remarks
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(case_id) DO UPDATE SET
                    anti_rabies = excluded.anti_rabies,
                    pvrv = excluded.pvrv,
                    pcec_batch = excluded.pcec_batch,
                    pcec_mfg_date = excluded.pcec_mfg_date,
                    pcec_expiry = excluded.pcec_expiry,
                    erig_hrig = excluded.erig_hrig,
                    tetanus_prophylaxis = excluded.tetanus_prophylaxis,
                    tetanus_toxoid = excluded.tetanus_toxoid,
                    ats = excluded.ats,
                    htig = excluded.htig,
                    remarks = excluded.remarks
                """,
                (
                    case_id,
                    _v("vc_anti_rabies"),
                    _v("vc_pvrv"),
                    _v("vc_pcec_batch"),
                    vc_pcec_mfg_date,
                    vc_pcec_expiry,
                    _v("vc_erig_hrig"),
                    _v("vc_tetanus_prophylaxis"),
                    _v("vc_tetanus_toxoid"),
                    _v("vc_ats"),
                    _v("vc_htig"),
                    _v("vc_remarks"),
                ),
            )

            db.execute("DELETE FROM vaccination_card_doses WHERE case_id = ?", (case_id,))
            for record_type, prefix, days in [
                ("pre_exposure", "vc_pre", [0, 7, 28]),
                ("post_exposure", "vc_post", [0, 3, 7, 14, 28]),
                ("booster", "vc_booster", [0, 3]),
            ]:
                for day in days:
                    dose_date = _v(f"{prefix}_{day}_date")
                    type_of_vaccine = _v(f"{prefix}_{day}_type")
                    dose = _v(f"{prefix}_{day}_dose")
                    route_site = _v(f"{prefix}_{day}_route_site")
                    given_by = _v(f"{prefix}_{day}_given_by")
                    if any([dose_date, type_of_vaccine, dose, route_site, given_by]):
                        db.execute(
                            """
                            INSERT INTO vaccination_card_doses (
                                case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (case_id, record_type, day, dose_date or None, type_of_vaccine or None, dose or None, route_site or None, given_by or None),
                        )

            # Notify the patient that the vaccination record for this case was updated.
            _insert_patient_notification(
                patient_id=case_patient["patient_id"],
                notif_type="vaccination",
                source_id=case_id,
                message="Your vaccination record has been updated by the clinic.",
            )

            db.commit()

            flash("Case information updated.", "success")
            return redirect(url_for("view_patient_case", case_id=case_id))

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)

        patient_name = (
            " ".join(
                part for part in [(case_patient["first_name"] or "").strip(), (case_patient["last_name"] or "").strip()] if part
            )
            or "Unknown Patient"
        )

        vc_row = db.execute(
            "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
        ).fetchone()
        vaccination_card = dict(vc_row) if vc_row else {}
        for _date_field in ("pcec_mfg_date", "pcec_expiry"):
            raw_value = (vaccination_card.get(_date_field) or "").strip()
            if not raw_value:
                vaccination_card[_date_field] = ""
                continue
            try:
                vaccination_card[_date_field] = datetime.fromisoformat(raw_value).date().isoformat()
            except ValueError:
                vaccination_card[_date_field] = ""
        vaccination_card_doses_rows = db.execute(
            """
            SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
            FROM vaccination_card_doses
            WHERE case_id = ?
            ORDER BY record_type, day_number
            """,
            (case_id,),
        ).fetchall()
        card_doses_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}
        for row in vaccination_card_doses_rows:
            r = row["record_type"]
            d = row["day_number"]
            if r in card_doses_by_type:
                card_doses_by_type[r][d] = dict(row)

        personnel_rows = db.execute(
            """
            SELECT cp.title, cp.first_name, cp.last_name, u.username
            FROM clinic_personnel cp
            JOIN users u ON u.id = cp.user_id
            WHERE cp.clinic_id = ?
            ORDER BY cp.title, cp.first_name, cp.last_name, u.username
            """,
            (case_patient["clinic_id"],),
        ).fetchall()
        personnel_options = []
        seen_personnel = set()
        for row in personnel_rows:
            title = (row["title"] or "").strip()
            first_name = (row["first_name"] or "").strip()
            last_name = (row["last_name"] or "").strip()
            username = (row["username"] or "").strip()
            display_name = " ".join(part for part in [title, first_name, last_name] if part) or username
            if display_name and display_name not in seen_personnel:
                seen_personnel.add(display_name)
                personnel_options.append(display_name)

        suggested_dates_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}
        schedule_days = {
            "pre_exposure": [0, 7, 28],
            "post_exposure": [0, 3, 7, 14, 28],
            "booster": [0, 3],
        }
        for record_type, days in schedule_days.items():
            day0_row = card_doses_by_type.get(record_type, {}).get(0)
            day0_raw = (day0_row.get("dose_date") if day0_row else "") or ""
            if not day0_raw:
                continue
            try:
                day0_date = datetime.fromisoformat(day0_raw).date()
            except ValueError:
                continue
            for day in days:
                if day == 0:
                    continue
                existing_row = card_doses_by_type.get(record_type, {}).get(day)
                existing_date = (existing_row.get("dose_date") if existing_row else "") or ""
                if existing_date:
                    continue
                suggested_dates_by_type[record_type][day] = (day0_date + timedelta(days=day)).isoformat()

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": patient_name, "href": url_for("view_patient_case", case_id=case_id)},
            {"label": "Edit", "href": None},
        ]

        return render_template(
            "staff_patient_edit.html",
            staff=staff,
            staff_display_name=staff_display_name,
            case=case_patient,
            patient_name=patient_name,
            vaccination_card=vaccination_card,
            card_doses_by_type=card_doses_by_type,
            personnel_options=personnel_options,
            suggested_dates_by_type=suggested_dates_by_type,
            expiry_min_date=datetime.now().date().isoformat(),
            breadcrumbs=breadcrumbs,
            active_page="cases",
        )

    @app.get("/admin/dashboard")
    @role_required("system_admin")
    def admin_dashboard():
        db = get_db()
        admin = db.execute(
            """
            SELECT sa.*, u.username, u.email
            FROM system_admins sa
            JOIN users u ON u.id = sa.user_id
            WHERE sa.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        return render_template("admin_dashboard.html", admin=admin)

    # =========================
    # Admin-only account creation (CLI)
    # =========================

    @app.cli.command("create-clinic")
    @click.option("--name", required=True)
    @click.option("--address", default=None)
    def create_clinic_command(name, address):
        db = get_db()
        try:
            db.execute("INSERT INTO clinics (name, address) VALUES (?, ?)", (name, address))
            db.commit()
        except Exception as e:
            db.rollback()
            raise click.ClickException(f"Failed to create clinic: {e}")
        click.echo("Clinic created.")

    @app.cli.command("create-admin")
    @click.option("--username", required=True)
    @click.option("--email", required=True)
    @click.option("--password", required=True)
    @click.option("--employee-id", "employee_id", required=True)
    @click.option("--first-name", "first_name", default=None)
    @click.option("--last-name", "last_name", default=None)
    def create_admin_command(username, email, password, employee_id, first_name, last_name):
        email_norm = (email or "").strip().lower()
        username = (username or "").strip()

        if not username or not email_norm:
            raise click.ClickException("Username and email are required.")

        db = get_db()
        dup_user = db.execute(
            "SELECT 1 FROM users WHERE username = ? OR email = ? LIMIT 1",
            (username, email_norm),
        ).fetchone()
        if dup_user:
            raise click.ClickException("Username or email already exists.")

        dup_emp = db.execute(
            "SELECT 1 FROM system_admins WHERE employee_id = ? LIMIT 1",
            (employee_id,),
        ).fetchone()
        if dup_emp:
            raise click.ClickException("Employee ID already exists.")

        try:
            cur = db.execute(
                "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)",
                (username, email_norm, generate_password_hash(password), "system_admin"),
            )
            user_id = cur.lastrowid
            db.execute(
                "INSERT INTO system_admins (user_id, first_name, last_name, employee_id) VALUES (?, ?, ?, ?)",
                (user_id, first_name, last_name, employee_id),
            )
            db.commit()
        except Exception as e:
            db.rollback()
            raise click.ClickException(f"Failed to create admin: {e}")

        click.echo("Admin created.")

    @app.cli.command("create-staff")
    @click.option("--username", required=True)
    @click.option("--email", required=True)
    @click.option("--password", required=True)
    @click.option("--clinic-id", "clinic_id", required=True, type=int)
    @click.option("--employee-id", "employee_id", required=True)
    @click.option("--title", required=True, type=click.Choice(["Doctor", "Nurse"], case_sensitive=True))
    @click.option("--license-number", "license_number", default=None)
    @click.option("--first-name", "first_name", default=None)
    @click.option("--last-name", "last_name", default=None)
    def create_staff_command(
        username,
        email,
        password,
        clinic_id,
        employee_id,
        title,
        license_number,
        first_name,
        last_name,
    ):
        email_norm = (email or "").strip().lower()
        username = (username or "").strip()

        if not username or not email_norm:
            raise click.ClickException("Username and email are required.")

        db = get_db()
        clinic = db.execute("SELECT 1 FROM clinics WHERE id = ? LIMIT 1", (clinic_id,)).fetchone()
        if not clinic:
            raise click.ClickException("Clinic ID does not exist.")

        dup_user = db.execute(
            "SELECT 1 FROM users WHERE username = ? OR email = ? LIMIT 1",
            (username, email_norm),
        ).fetchone()
        if dup_user:
            raise click.ClickException("Username or email already exists.")

        dup_emp = db.execute(
            "SELECT 1 FROM clinic_personnel WHERE employee_id = ? LIMIT 1",
            (employee_id,),
        ).fetchone()
        if dup_emp:
            raise click.ClickException("Employee ID already exists.")

        if license_number:
            dup_lic = db.execute(
                "SELECT 1 FROM clinic_personnel WHERE license_number = ? LIMIT 1",
                (license_number,),
            ).fetchone()
            if dup_lic:
                raise click.ClickException("License number already exists.")

        try:
            cur = db.execute(
                "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)",
                (username, email_norm, generate_password_hash(password), "clinic_personnel"),
            )
            user_id = cur.lastrowid
            db.execute(
                """
                INSERT INTO clinic_personnel (
                  user_id, clinic_id, first_name, last_name, employee_id, license_number, title
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, clinic_id, first_name, last_name, employee_id, license_number, title),
            )
            db.commit()
        except Exception as e:
            db.rollback()
            raise click.ClickException(f"Failed to create staff: {e}")

        click.echo("Staff created.")

    @app.cli.command("retry-pending-emails")
    @click.option("--limit", default=50, show_default=True, type=int)
    def retry_pending_emails_command(limit: int):
        db = get_db()
        rows = db.execute(
            """
            SELECT id, to_email, subject, body, retry_count
            FROM pending_emails
            WHERE status IN ('pending', 'failed')
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()
        if not rows:
            click.echo("No pending emails.")
            return

        sent = 0
        failed = 0
        for row in rows:
            try:
                send_email(to_email=row["to_email"], subject=row["subject"], body=row["body"])
                db.execute(
                    """
                    UPDATE pending_emails
                    SET status = 'sent',
                        updated_at = CURRENT_TIMESTAMP,
                        last_error = NULL
                    WHERE id = ?
                    """,
                    (row["id"],),
                )
                sent += 1
            except Exception as e:
                db.execute(
                    """
                    UPDATE pending_emails
                    SET status = 'failed',
                        retry_count = ?,
                        updated_at = CURRENT_TIMESTAMP,
                        last_error = ?
                    WHERE id = ?
                    """,
                    ((row["retry_count"] or 0) + 1, str(e)[:500], row["id"]),
                )
                failed += 1
        db.commit()
        click.echo(f"Pending email retry complete. Sent: {sent}, Failed: {failed}")

    return app

