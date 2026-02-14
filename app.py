import os

import click
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import generate_password_hash

from auth import login_required, role_required
from db import get_db, init_app as init_db_app


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

    with app.app_context():
        _ensure_patient_onboarding_column()

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
            "UPDATE patients SET onboarding_completed = 1 WHERE user_id = ?",
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
        patient = db.execute(
            """
            SELECT p.*, u.username, u.email
            FROM patients p
            JOIN users u ON u.id = p.user_id
            WHERE p.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

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
        db = get_db()
        patient = db.execute(
            """
            SELECT p.*, u.username, u.email
            FROM patients p
            JOIN users u ON u.id = p.user_id
            WHERE p.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

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

        patient = db.execute(
            """
            SELECT id
            FROM patients
            WHERE user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

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
        patient = db.execute(
            """
            SELECT p.*, u.username, u.email
            FROM patients p
            JOIN users u ON u.id = p.user_id
            WHERE p.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

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

        # Update patient info if provided
        if full_name or age or barangay or contact_number or email_address:
            name_parts = full_name.split(" ", 1) if full_name else ["", ""]
            first_name = name_parts[0] if len(name_parts) > 0 else None
            last_name = name_parts[1] if len(name_parts) > 1 else None

            # Parameterized update (no dynamic SQL)
            new_first_name = first_name if first_name else patient["first_name"]
            new_last_name = last_name if last_name else patient["last_name"]
            new_address = barangay if barangay else patient["address"]
            new_phone = contact_number if contact_number else patient["phone_number"]

            new_age = patient["age"]
            if age:
                try:
                    new_age = int(age)
                except ValueError:
                    flash("Age must be a number.", "error")
                    return redirect(url_for("patient_dashboard"))

            db.execute(
                """
                UPDATE patients
                SET first_name = ?,
                    last_name = ?,
                    age = ?,
                    address = ?,
                    phone_number = ?
                WHERE user_id = ?
                """,
                (new_first_name, new_last_name, new_age, new_address, new_phone, session["user_id"]),
            )

            if email_address:
                db.execute("UPDATE users SET email = ? WHERE id = ?", (email_address, session["user_id"]))

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
                    risk_level, tetanus_prophylaxis_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    patient["id"],
                    clinic_id,
                    exposure_date,
                    exposure_time or None,
                    final_place_of_exposure,
                    final_affected_area,
                    type_of_exposure,
                    animal_detail,
                    animal_status,
                    risk_level,
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
                        patient["id"],
                        clinic_id,
                        appointment_datetime,
                        "Scheduled",
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
        patient = db.execute(
            """
            SELECT p.*, u.username, u.email
            FROM patients p
            JOIN users u ON u.id = p.user_id
            WHERE p.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()

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
            WHERE user_id = ?
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
                session["user_id"],
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

        return render_template("staff_dashboard.html", staff=staff)

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

