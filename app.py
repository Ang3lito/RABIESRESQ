import os
from datetime import datetime

import click
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import generate_password_hash

from auth import login_required, role_required
from db import get_db, init_app as init_db_app


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

    # Category III – clearly severe / high‑risk situations
    if (
        type_of_exposure in high_risk_exposures
        or affected_area in high_risk_areas
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

    with app.app_context():
        _ensure_patient_onboarding_column()
        _migrate_patients_for_dependents()

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

    def _run_case_status_maintenance(clinic_id: int):
        db = get_db()

        to_no_show = db.execute(
            """
            UPDATE cases
            SET case_status = 'No Show'
            WHERE clinic_id = ?
              AND LOWER(COALESCE(case_status, 'pending')) = 'pending'
              AND EXISTS (
                SELECT 1
                FROM appointments a
                WHERE a.case_id = cases.id
                  AND a.clinic_id = cases.clinic_id
                  AND LOWER(COALESCE(a.status, '')) IN ('approved', 'pending', 'queued')
                  AND datetime(a.appointment_datetime) <= datetime('now', 'localtime', '-10 hours')
              )
            """,
            (clinic_id,),
        ).rowcount

        if to_no_show:
            db.execute(
                """
                UPDATE appointments
                SET status = 'No Show'
                WHERE clinic_id = ?
                  AND LOWER(COALESCE(status, '')) IN ('approved', 'pending', 'queued')
                  AND datetime(appointment_datetime) <= datetime('now', 'localtime', '-10 hours')
                  AND case_id IN (
                    SELECT id
                    FROM cases
                    WHERE clinic_id = ?
                      AND LOWER(COALESCE(case_status, '')) = 'no show'
                  )
                """,
                (clinic_id, clinic_id),
            )

        archived_from_no_show = db.execute(
            """
            UPDATE cases
            SET case_status = 'archived'
            WHERE clinic_id = ?
              AND LOWER(COALESCE(case_status, '')) = 'no show'
              AND EXISTS (
                SELECT 1
                FROM appointments a
                WHERE a.case_id = cases.id
                  AND a.clinic_id = cases.clinic_id
                  AND datetime(a.appointment_datetime) <= datetime('now', 'localtime', '-24 hours')
              )
            """,
            (clinic_id,),
        ).rowcount

        if archived_from_no_show:
            db.execute(
                """
                UPDATE appointments
                SET status = 'Removed'
                WHERE clinic_id = ?
                  AND case_id IN (
                    SELECT id
                    FROM cases
                    WHERE clinic_id = ?
                      AND LOWER(COALESCE(case_status, '')) = 'archived'
                  )
                """,
                (clinic_id, clinic_id),
            )

        db.commit()
        return {"to_no_show": to_no_show or 0, "archived_from_no_show": archived_from_no_show or 0}

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
        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        # Fetch cases and appointments for this patient
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

        appointments = db.execute(
            """
            SELECT
                a.*,
                c.type_of_exposure,
                c.exposure_date,
                c.risk_level
            FROM appointments a
            JOIN cases c ON c.id = a.case_id
            WHERE a.patient_id = ?
            ORDER BY a.appointment_datetime DESC
            """,
            (patient["id"],),
        ).fetchall()

        return render_template("patient_dashboard.html", patient=patient, cases=cases, appointments=appointments, active_page="dashboard")

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

    @app.post("/patient/appointments/<int:appointment_id>/cancel")
    @role_required("patient")
    def patient_cancel_appointment(appointment_id: int):
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()

        patient = _get_primary_patient(session["user_id"])

        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        appt = db.execute(
            """
            SELECT id, status
            FROM appointments
            WHERE id = ? AND patient_id = ?
            """,
            (appointment_id, patient["id"]),
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
            WHERE id = ? AND patient_id = ?
            """,
            ("Cancelled", appointment_id, patient["id"]),
        )
        db.commit()
        flash("Appointment cancelled.", "success")
        return redirect(url_for("patient_dashboard"))

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
            # Create default clinic if none exists
            db.execute("INSERT INTO clinics (name, address) VALUES (?, ?)", ("Default Clinic", None))
            db.commit()
            clinic = db.execute("SELECT id FROM clinics LIMIT 1").fetchone()
        clinic_id = clinic["id"]

        # Get form data
        form_type = request.form.get("form_type", "case")
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
        affected_area = request.form.get("affected_area", "").strip()
        affected_area_other = request.form.get("affected_area_other", "").strip()
        tetanus_immunization = request.form.get("tetanus_immunization", "").strip()
        tetanus_date = request.form.get("tetanus_date", "").strip() or None
        hrtig_immunization = request.form.get("hrtig_immunization", "").strip()
        hrtig_date = request.form.get("hrtig_date", "").strip() or None
        
        # Victim info (update patient if provided)
        full_name = request.form.get("full_name", "").strip()
        age = request.form.get("age", "").strip()
        barangay = request.form.get("barangay", "").strip()
        contact_number = request.form.get("contact_number", "").strip()
        email_address = request.form.get("email_address", "").strip().lower()
        relationship_to_user = (request.form.get("relationship_to_user", "Self") or "Self").strip()

        # Validation
        errors = []
        if not type_of_exposure:
            errors.append("Type of exposure is required.")
        if not exposure_date:
            errors.append("Exposure date is required.")
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
        if not affected_area:
            errors.append("Affected area is required.")
        if affected_area == "Other" and not affected_area_other:
            errors.append("Please specify the other affected area.")
        if not tetanus_immunization:
            errors.append("Tetanus immunization status is required.")
        if not hrtig_immunization:
            errors.append("Human tetanus immunoglobulin status is required.")
        if hrtig_immunization == "Yes" and not hrtig_date:
            errors.append("HRIG date is required when Human Tetanus Immunoglobulin is Yes.")

        if errors:
            for error in errors:
                flash(error, "error")
            return redirect(url_for("patient_dashboard"))

        target_patient_id = patient["id"]
        if full_name or age or barangay or contact_number or email_address:
            name_parts = full_name.split(" ", 1) if full_name else ["", ""]
            first_name = name_parts[0] if len(name_parts) > 0 else None
            last_name = name_parts[1] if len(name_parts) > 1 else None

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
                new_address = barangay if barangay else patient["address"]
                new_phone = contact_number if contact_number else patient["phone_number"]

                db.execute(
                    """
                    UPDATE patients
                    SET first_name = ?,
                        last_name = ?,
                        age = ?,
                        address = ?,
                        phone_number = ?,
                        relationship_to_user = ?
                    WHERE id = ?
                    """,
                    (new_first_name, new_last_name, parsed_age, new_address, new_phone, "Self", patient["id"]),
                )

                if email_address:
                    db.execute("UPDATE users SET email = ? WHERE id = ?", (email_address, session["user_id"]))
            else:
                db.execute(
                    """
                    INSERT INTO patients (
                        user_id, first_name, last_name, phone_number, address, age,
                        relationship_to_user, onboarding_completed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session["user_id"],
                        first_name,
                        last_name,
                        contact_number or None,
                        barangay or None,
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

        # Build affected_area (combine with other text if applicable)
        final_affected_area = affected_area
        if affected_area == "Other" and affected_area_other:
            final_affected_area = f"Other: {affected_area_other}"

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
            affected_area=affected_area,
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

            # Create appointment if form_type is "appointment"
            if form_type == "appointment":
                from datetime import datetime, timedelta
                # Set appointment datetime to tomorrow at 9 AM as default
                appointment_datetime = (datetime.now() + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0).isoformat()
                
                db.execute(
                    """
                    INSERT INTO appointments (
                        patient_id, clinic_id, appointment_datetime,
                        status, type, case_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_patient_id,
                        clinic_id,
                        appointment_datetime,
                        "Pending",
                        "Pre-screening",
                        case_id,
                    ),
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

        staff_display_name = staff["username"]
        if staff["first_name"] or staff["last_name"]:
            title = (staff["title"] or "").strip()
            first_name = (staff["first_name"] or "").strip()
            last_name = (staff["last_name"] or "").strip()
            staff_display_name = " ".join(part for part in [title, first_name, last_name] if part)
        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        maintenance = _run_case_status_maintenance(staff["clinic_id"])
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
              AND (
                c.case_status IS NULL
                OR LOWER(c.case_status) NOT IN ('closed', 'resolved', 'completed', 'cancelled', 'archived')
              )
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
              SUM(CASE WHEN LOWER(status) = 'missed' THEN 1 ELSE 0 END) AS missed,
              SUM(CASE WHEN LOWER(status) = 'rescheduled' THEN 1 ELSE 0 END) AS rescheduled
            FROM appointments
            WHERE clinic_id = ?
            """,
            (clinic_id,),
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

        place_row = db.execute(
            """
            SELECT COALESCE(place_of_exposure, 'Unspecified') AS place_name, COUNT(*) AS total
            FROM cases
            WHERE clinic_id = ?
            GROUP BY place_name
            ORDER BY total DESC
            LIMIT 1
            """,
            (clinic_id,),
        ).fetchone()
        if place_row:
            demographic_summary = (
                f"Most cases were reported in {place_row['place_name']} "
                f"({place_row['total']} case{'s' if place_row['total'] != 1 else ''})."
            )
        else:
            demographic_summary = "No case location data available yet."

        busiest_day_row = db.execute(
            """
            SELECT STRFTIME('%w', appointment_datetime) AS day_num, COUNT(*) AS total
            FROM appointments
            WHERE clinic_id = ?
            GROUP BY day_num
            ORDER BY total DESC
            LIMIT 1
            """,
            (clinic_id,),
        ).fetchone()
        busiest_hour_row = db.execute(
            """
            SELECT STRFTIME('%H', appointment_datetime) AS hour_24, COUNT(*) AS total
            FROM appointments
            WHERE clinic_id = ?
            GROUP BY hour_24
            ORDER BY total DESC
            LIMIT 1
            """,
            (clinic_id,),
        ).fetchone()
        day_names = {
            "0": "Sunday",
            "1": "Monday",
            "2": "Tuesday",
            "3": "Wednesday",
            "4": "Thursday",
            "5": "Friday",
            "6": "Saturday",
        }
        if busiest_day_row and busiest_hour_row and busiest_hour_row["hour_24"] is not None:
            day_name = day_names.get(busiest_day_row["day_num"], "Unknown day")
            start_hour = int(busiest_hour_row["hour_24"])
            end_hour = (start_hour + 1) % 24
            peak_flow_summary = (
                f"Busiest day is {day_name}, with highest traffic around "
                f"{start_hour:02d}:00-{end_hour:02d}:00."
            )
        else:
            peak_flow_summary = "No appointment traffic data available yet."

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
            demographic_summary=demographic_summary,
            peak_flow_summary=peak_flow_summary,
            todays_appointments=todays_appointments,
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
                          AND LOWER(COALESCE(a.status, '')) NOT IN ('removed', 'cancelled')
                        ORDER BY datetime(a.appointment_datetime) ASC, a.id ASC
                        LIMIT 1
                    ),
                    'N/A'
                ) AS schedule,
                COALESCE(
                    (
                        SELECT vr.next_dose_date
                        FROM vaccination_records vr
                        WHERE vr.case_id = c.id
                        ORDER BY datetime(vr.date_administered) DESC, vr.id DESC
                        LIMIT 1
                    ),
                    'N/A'
                ) AS next_dose
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
            schedule_display = row["schedule"] if row["schedule"] else "N/A"
            if row["schedule"] and row["schedule"] != "N/A":
                try:
                    schedule_display = datetime.fromisoformat(row["schedule"]).strftime("%b %d, %Y @ %I:%M %p")
                except ValueError:
                    schedule_display = row["schedule"]
            case_items.append(
                {
                    "id": row["case_id"],
                    "case_code": f"C-000{row['case_id']}",
                    "patient_name": row["patient_name"],
                    "exposure_date": row["exposure_date"] or "N/A",
                    "category": row["category"],
                    "case_status": row["case_status"],
                    "schedule": schedule_display,
                    "next_dose": row["next_dose"] if row["next_dose"] else "N/A",
                }
            )

        cases = SimplePagination(case_items, page=page, per_page=per_page, total=total)

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": None},
        ]


        return render_template(
            "patients.html",
            staff=staff,
            staff_display_name=staff_display_name,
            cases=cases,
            selected_category=category,
            selected_status=case_status,
            search=search,
            breadcrumbs=breadcrumbs,
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
            "appointments.html",
            staff=staff,
            staff_display_name=staff_display_name,
            appointments=appointments,
            search=search,
            breadcrumbs=breadcrumbs,
        )

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
              COALESCE(c.risk_level, c.category, 'N/A') AS category,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment
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
        if appt["appointment_datetime"]:
            try:
                dt = datetime.fromisoformat(appt["appointment_datetime"])
                appt_date = dt.strftime("%Y-%m-%d")
                appt_time = dt.strftime("%H:%M")
            except ValueError:
                pass

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Appointments", "href": url_for("staff_appointments")},
            {"label": f"#{appt['id']}", "href": None},
        ]


        return render_template(
            "appointment_view.html",
            staff=staff,
            staff_display_name=staff_display_name,
            appointment=appt,
            patient_name=patient_name,
            appointment_date=appt_date,
            appointment_time=appt_time,
            breadcrumbs=breadcrumbs,
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
            "SELECT id, case_id FROM appointments WHERE id = ? AND clinic_id = ?",
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
            SELECT id, patient_id, clinic_id
            FROM appointments
            WHERE id = ? AND clinic_id = ?
            """,
            (appointment_id, staff["clinic_id"]),
        ).fetchone()
        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("staff_appointments"))

        patient_name = (request.form.get("patient_name") or "").strip()
        appointment_date = (request.form.get("appointment_date") or "").strip()
        appointment_time = (request.form.get("appointment_time") or "").strip()

        if not appointment_date or not appointment_time:
            flash("Appointment date and time are required.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        try:
            new_datetime = datetime.fromisoformat(f"{appointment_date}T{appointment_time}").isoformat()
        except ValueError:
            flash("Invalid date/time format.", "error")
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

        if patient_name:
            parts = patient_name.split(" ", 1)
            first_name = parts[0]
            last_name = parts[1] if len(parts) > 1 else ""
            db.execute(
                """
                UPDATE patients
                SET first_name = ?, last_name = ?
                WHERE id = ?
                """,
                (first_name, last_name, appt["patient_id"]),
            )

        db.execute(
            """
            UPDATE appointments
            SET appointment_datetime = ?
            WHERE id = ? AND clinic_id = ?
            """,
            (new_datetime, appointment_id, staff["clinic_id"]),
        )
        db.commit()


        flash("Appointment updated.", "success")
        return redirect(url_for("view_appointment", appointment_id=appointment_id))

    @app.get("/staff/patients/<int:case_id>")
    @role_required("clinic_personnel", "system_admin")
    def view_patient_case(case_id: int):
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

        case_row = db.execute(
            """
            SELECT
              c.*,
              cl.name AS clinic_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown Patient'
              ) AS patient_name
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            JOIN clinics cl ON cl.id = c.clinic_id
            WHERE c.id = ?
              AND c.clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()

        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

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
        dose_records = []
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

        expected_doses = 5
        doses_completed = len(dose_records)
        progress_pct = min(round((doses_completed / expected_doses) * 100), 100) if expected_doses else 0

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
        notes = []
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

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": case_row["patient_name"], "href": None},
        ]

        return render_template(
            "patient_view.html",
            staff=staff,
            staff_display_name=staff_display_name,
            case=case_row,
            dose_records=dose_records,
            doses_completed=doses_completed,
            expected_doses=expected_doses,
            progress_pct=progress_pct,
            next_appointment=next_appointment,
            next_appointment_display=next_appointment_display,
            notes=notes,
            breadcrumbs=breadcrumbs,
        )

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
        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Patients", "href": url_for("staff_patients")},
            {"label": patient_name, "href": url_for("view_patient_case", case_id=case_id)},
            {"label": "Edit", "href": None},
        ]

        return render_template(
            "patient_edit.html",
            staff=staff,
            staff_display_name=staff_display_name,
            case=case_patient,
            patient_name=patient_name,
            breadcrumbs=breadcrumbs,
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

    return app

