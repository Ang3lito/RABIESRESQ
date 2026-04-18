import functools
import logging
import secrets
from datetime import datetime, timedelta, timezone

from flask import (
    Blueprint,
    flash,
    g,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.security import check_password_hash, generate_password_hash

from db import get_db
from email_service import send_email


bp = Blueprint("auth", __name__)

_OTP_EXPIRY_MINUTES = 10
_RESET_TOKEN_SALT = "rabiesresq-password-reset-token"
_RESET_TOKEN_MAX_AGE_SECONDS = 15 * 60  # 15 minutes
_MAX_VERIFY_ATTEMPTS = 5

logger = logging.getLogger(__name__)


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _is_valid_email(email: str) -> bool:
    if not email or len(email) > 254:
        return False
    return "@" in email and "." in email.split("@")[-1]


def _generate_otp() -> str:
    return "".join(secrets.choice("0123456789") for _ in range(6))


def _reset_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"])


def _make_reset_token(email: str) -> str:
    return _reset_token_serializer().dumps(
        {"email": email}, salt=_RESET_TOKEN_SALT
    )


def _verify_reset_token(token: str) -> dict | None:
    try:
        data = _reset_token_serializer().loads(
            token, salt=_RESET_TOKEN_SALT, max_age=_RESET_TOKEN_MAX_AGE_SECONDS
        )
        if not isinstance(data, dict) or "email" not in data:
            return None
        return data
    except (SignatureExpired, BadSignature):
        return None


def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login"))
        return view(**kwargs)

    return wrapped_view


def role_required(*roles):
    def decorator(view):
        @functools.wraps(view)
        def wrapped_view(**kwargs):
            if not session.get("user_id"):
                return redirect(url_for("auth.login"))
            if session.get("role") not in roles:
                flash("You do not have access to that page.", "error")
                return redirect(url_for("index"))
            return view(**kwargs)

        return wrapped_view

    return decorator


@bp.before_app_request
def load_logged_in_user():
    user_id = session.get("user_id")
    if not user_id:
        g.user = None
        return

    db = get_db()
    g.user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if g.user is not None:
        ia = g.user["is_active"] if "is_active" in g.user.keys() else 1
        if int(ia or 0) == 0:
            session.clear()
            flash("Your account has been deactivated.", "error")
            return redirect(url_for("auth.login"))


@bp.before_app_request
def enforce_patient_password_change():
    user_id = session.get("user_id")
    if not user_id or session.get("role") != "patient":
        return None
    if g.get("user") is None:
        return None
    must_change = g.user["must_change_password"] if "must_change_password" in g.user.keys() else 0
    if not must_change:
        return None

    endpoint = request.endpoint or ""
    allowed_endpoints = {
        "auth.patient_force_password",
        "auth.patient_force_password_post",
        "auth.logout",
        "static",
    }
    if endpoint in allowed_endpoints:
        return None
    return redirect(url_for("auth.patient_force_password"))


@bp.before_app_request
def enforce_staff_password_change():
    user_id = session.get("user_id")
    if not user_id or session.get("role") != "clinic_personnel":
        return None
    if g.get("user") is None:
        return None
    must_change = g.user["must_change_password"] if "must_change_password" in g.user.keys() else 0
    if not must_change:
        return None

    endpoint = request.endpoint or ""
    allowed_endpoints = {
        "auth.staff_force_password",
        "auth.staff_force_password_post",
        "auth.logout",
        "static",
    }
    if endpoint in allowed_endpoints:
        return None
    return redirect(url_for("auth.staff_force_password"))


@bp.get("/login")
def login():
    return render_template("login.html")


@bp.post("/login")
def login_post():
    email = _normalize_email(request.form.get("email"))
    password = request.form.get("password") or ""

    if not email or not password:
        flash("Invalid credentials.", "error")
        return render_template("login.html", email=email)

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ? LIMIT 1", (email,)).fetchone()

    if user is None or not check_password_hash(user["password_hash"], password):
        flash("Invalid credentials.", "error")
        return render_template("login.html", email=email)

    ia = user["is_active"] if "is_active" in user.keys() else 1
    if int(ia or 0) == 0:
        flash("This account has been deactivated. Contact your administrator.", "error")
        return render_template("login.html", email=email)

    session.clear()
    session["user_id"] = user["id"]
    session["role"] = user["role"]
    session["username"] = user["username"]
    session["email"] = user["email"]

    logged_at = datetime.now().isoformat(timespec="seconds")
    try:
        cur = db.execute(
            """
            INSERT INTO user_session_logs (user_id, role_at_login, logged_in_at)
            VALUES (?, ?, ?)
            """,
            (user["id"], user["role"], logged_at),
        )
        db.commit()
        session["session_log_id"] = cur.lastrowid
    except Exception:
        db.rollback()

    if user["role"] == "patient":
        patient = db.execute(
            "SELECT onboarding_completed FROM patients WHERE user_id = ?",
            (user["id"],),
        ).fetchone()
        onboarding_done = bool(patient and patient["onboarding_completed"])
        session["patient_onboarding_done"] = onboarding_done
        return redirect(url_for("patient_dashboard" if onboarding_done else "patient_onboarding"))
    if user["role"] == "clinic_personnel":
        mc = int(user["must_change_password"] or 0) if "must_change_password" in user.keys() else 0
        if mc:
            return redirect(url_for("auth.staff_force_password"))
        return redirect(url_for("staff_dashboard"))
    if user["role"] == "system_admin":
        return redirect(url_for("admin_analytics", tab="overview", period="30d"))

    session.clear()
    flash("Account role is invalid, contact admin.", "error")
    return redirect(url_for("auth.login"))


@bp.get("/register")
def register():
    return render_template("register.html")


@bp.post("/register")
def register_post():
    username = (request.form.get("username") or "").strip()
    email = _normalize_email(request.form.get("email"))
    password = request.form.get("password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    # Patient fields (nullable)
    first_name = (request.form.get("first_name") or "").strip() or None
    last_name = (request.form.get("last_name") or "").strip() or None
    phone_number = (request.form.get("phone_number") or "").strip() or None
    address = (request.form.get("address") or "").strip() or None
    date_of_birth = (request.form.get("date_of_birth") or "").strip() or None
    age_raw = (request.form.get("age") or "").strip()
    gender = (request.form.get("gender") or "").strip() or None
    allergies = (request.form.get("allergies") or "").strip() or None
    pre_existing_conditions = (request.form.get("pre_existing_conditions") or "").strip() or None
    current_medications = (request.form.get("current_medications") or "").strip() or None
    notification_settings = (request.form.get("notification_settings") or "").strip() or None

    age = None
    if age_raw:
        try:
            age = int(age_raw)
        except ValueError:
            flash("Age must be a number.", "error")
            return render_template("register.html", form=request.form)

    if not username:
        flash("Username is required.", "error")
        return render_template("register.html", form=request.form)
    if not email:
        flash("Email is required.", "error")
        return render_template("register.html", form=request.form)
    if not password:
        flash("Password is required.", "error")
        return render_template("register.html", form=request.form)
    if password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("register.html", form=request.form)

    db = get_db()
    exists = db.execute(
        "SELECT 1 FROM users WHERE username = ? OR email = ? LIMIT 1",
        (username, email),
    ).fetchone()
    if exists:
        flash("Username or email is already in use.", "error")
        return render_template("register.html", form=request.form)

    try:
        cur = db.execute(
            "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)",
            (username, email, generate_password_hash(password), "patient"),
        )
        user_id = cur.lastrowid

        db.execute(
            """
            INSERT INTO patients (
              user_id, first_name, last_name, phone_number, address, date_of_birth, age, gender,
              allergies, pre_existing_conditions, current_medications, notification_settings
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                first_name,
                last_name,
                phone_number,
                address,
                date_of_birth,
                age,
                gender,
                allergies,
                pre_existing_conditions,
                current_medications,
                notification_settings,
            ),
        )
        db.commit()
    except Exception:
        db.rollback()
        flash("Registration failed. Please try again.", "error")
        return render_template("register.html", form=request.form)

    # Auto-login
    session.clear()
    session["user_id"] = user_id
    session["role"] = "patient"
    session["username"] = username
    session["email"] = email

    logged_at = datetime.now().isoformat(timespec="seconds")
    try:
        cur = db.execute(
            """
            INSERT INTO user_session_logs (user_id, role_at_login, logged_in_at)
            VALUES (?, ?, ?)
            """,
            (user_id, "patient", logged_at),
        )
        db.commit()
        session["session_log_id"] = cur.lastrowid
    except Exception:
        db.rollback()

    flash("Registration successful. Welcome!", "success")
    return redirect(url_for("patient_dashboard"))


@bp.get("/forgot-password")
def forgot_password():
    return render_template("forgot_password.html")


@bp.post("/forgot-password/request")
def forgot_password_request():
    email = _normalize_email(request.form.get("email"))
    if not email:
        flash("Email is required.", "error")
        return render_template("forgot_password.html", email=email)
    if not _is_valid_email(email):
        flash("Please enter a valid email address.", "error")
        return render_template("forgot_password.html", email=email)

    db = get_db()
    user = db.execute("SELECT id, email FROM users WHERE email = ? LIMIT 1", (email,)).fetchone()
    if user is not None:
        code = _generate_otp()
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=_OTP_EXPIRY_MINUTES)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        try:
            db.execute(
                "UPDATE password_reset_codes SET is_used = 1 WHERE email = ?",
                (email,),
            )
            db.execute(
                "INSERT INTO password_reset_codes (email, code, expires_at, is_used, attempts) VALUES (?, ?, ?, 0, 0)",
                (email, code, expires_at),
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.exception("Failed to store password reset code")
        else:
            send_email(
                to_email=email,
                subject="RabiesResQ Password Reset Code",
                body=f"Your RabiesResQ verification code is: {code}. Valid for {_OTP_EXPIRY_MINUTES} minutes. Do not share.",
            )
            logger.info("Password reset code sent for email (id redacted)")

    flash("If an account exists for that email, a verification code has been sent.", "success")
    return redirect(url_for("auth.forgot_password_verify", email=email))


@bp.get("/forgot-password/verify")
def forgot_password_verify():
    email = _normalize_email(request.args.get("email"))
    if not email:
        return redirect(url_for("auth.forgot_password"))
    return render_template("forgot_password_verify.html", email=email)


@bp.post("/forgot-password/verify")
def forgot_password_verify_post():
    email = _normalize_email(request.form.get("email"))
    code = (request.form.get("code") or "").strip()
    if not email:
        flash("Email is required.", "error")
        return redirect(url_for("auth.forgot_password_verify"))
    if not code or len(code) != 6 or not code.isdigit():
        flash("Please enter the 6-digit code.", "error")
        return render_template("forgot_password_verify.html", email=email)

    db = get_db()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = db.execute(
        "SELECT id, code, attempts FROM password_reset_codes WHERE email = ? AND expires_at > ? AND is_used = 0 ORDER BY created_at DESC LIMIT 1",
        (email, now),
    ).fetchone()

    if row is None:
        flash("No valid code found for this email. Please request a new code.", "error")
        return render_template("forgot_password_verify.html", email=email)

    if row["code"] != code:
        attempts = (row["attempts"] or 0) + 1
        db.execute(
            "UPDATE password_reset_codes SET attempts = ? WHERE id = ?",
            (attempts, row["id"]),
        )
        db.commit()
        if attempts >= _MAX_VERIFY_ATTEMPTS:
            db.execute("UPDATE password_reset_codes SET is_used = 1 WHERE id = ?", (row["id"],))
            db.commit()
            flash("Too many failed attempts. Please request a new code.", "error")
        else:
            flash("Invalid code. Please try again.", "error")
        return render_template("forgot_password_verify.html", email=email)

    db.execute("UPDATE password_reset_codes SET is_used = 1 WHERE id = ?", (row["id"],))
    db.commit()

    reset_token = _make_reset_token(email)
    return redirect(url_for("auth.forgot_password_reset", token=reset_token))


@bp.get("/forgot-password/reset")
def forgot_password_reset():
    token = request.args.get("token")
    if not token or not _verify_reset_token(token):
        flash("Reset link is invalid or expired. Please start over.", "error")
        return redirect(url_for("auth.forgot_password"))
    return render_template("reset_password.html", token=token)


@bp.post("/forgot-password/reset")
def forgot_password_reset_post():
    token = request.form.get("token") or request.args.get("token") or ""
    data = _verify_reset_token(token) if token else None
    if not data:
        flash("Reset link is invalid or expired. Please start over.", "error")
        return redirect(url_for("auth.forgot_password"))

    email = data.get("email")
    password = request.form.get("password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    if not password:
        flash("Password is required.", "error")
        return render_template("reset_password.html", token=token)
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return render_template("reset_password.html", token=token)
    if password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("reset_password.html", token=token)

    db = get_db()
    user = db.execute("SELECT id FROM users WHERE email = ? LIMIT 1", (email,)).fetchone()
    if user is None:
        flash("Unable to reset password. Please request a new code.", "error")
        return redirect(url_for("auth.forgot_password"))

    try:
        db.execute(
            "UPDATE users SET password_hash = ?, must_change_password = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (generate_password_hash(password), user["id"]),
        )
        db.execute("UPDATE password_reset_codes SET is_used = 1 WHERE email = ?", (email,))
        db.commit()
    except Exception:
        db.rollback()
        flash("Password reset failed. Please try again.", "error")
        return render_template("reset_password.html", token=token)

    flash("Password updated. You can now log in.", "success")
    return redirect(url_for("auth.login"))


@bp.get("/patient/force-password")
@login_required
def patient_force_password():
    if session.get("role") != "patient":
        return redirect(url_for("index"))
    return render_template("patient_force_password.html")


@bp.post("/patient/force-password")
@login_required
def patient_force_password_post():
    if session.get("role") != "patient":
        return redirect(url_for("index"))

    new_password = request.form.get("new_password", "").strip()
    confirm_password = request.form.get("confirm_password", "").strip()

    if len(new_password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return render_template("patient_force_password.html")
    if new_password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("patient_force_password.html")

    db = get_db()
    try:
        db.execute(
            """
            UPDATE users
            SET password_hash = ?, must_change_password = 0, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (generate_password_hash(new_password), session["user_id"]),
        )
        db.commit()
    except Exception:
        db.rollback()
        flash("Failed to update password. Please try again.", "error")
        return render_template("patient_force_password.html")

    flash("Password updated successfully.", "success")
    if session.get("patient_onboarding_done"):
        return redirect(url_for("patient_dashboard"))
    return redirect(url_for("patient_onboarding"))


@bp.get("/staff/force-password")
@login_required
def staff_force_password():
    if session.get("role") != "clinic_personnel":
        return redirect(url_for("index"))
    return render_template("staff_force_password.html")


@bp.post("/staff/force-password")
@login_required
def staff_force_password_post():
    if session.get("role") != "clinic_personnel":
        return redirect(url_for("index"))

    new_password = request.form.get("new_password", "").strip()
    confirm_password = request.form.get("confirm_password", "").strip()

    if len(new_password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return render_template("staff_force_password.html")
    if new_password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("staff_force_password.html")

    db = get_db()
    try:
        db.execute(
            """
            UPDATE users
            SET password_hash = ?, must_change_password = 0, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (generate_password_hash(new_password), session["user_id"]),
        )
        db.commit()
    except Exception:
        db.rollback()
        flash("Failed to update password. Please try again.", "error")
        return render_template("staff_force_password.html")

    flash("Password updated successfully.", "success")
    return redirect(url_for("staff_dashboard"))


@bp.get("/logout")
def logout():
    log_id = session.get("session_log_id")
    if log_id:
        db = get_db()
        try:
            db.execute(
                """
                UPDATE user_session_logs
                SET logged_out_at = ?
                WHERE id = ?
                """,
                (datetime.now().isoformat(timespec="seconds"), log_id),
            )
            db.commit()
        except Exception:
            db.rollback()
    session.clear()
    return redirect(url_for("auth.login"))

