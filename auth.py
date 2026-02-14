import functools

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

_RESET_SALT = "rabiesresq-password-reset"
_RESET_MAX_AGE_SECONDS = 60 * 60  # 1 hour


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _reset_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"])


def _make_reset_token(user_id: int, email: str | None) -> str:
    return _reset_serializer().dumps({"user_id": user_id, "email": email}, salt=_RESET_SALT)


def _verify_reset_token(token: str) -> dict | None:
    try:
        data = _reset_serializer().loads(token, salt=_RESET_SALT, max_age=_RESET_MAX_AGE_SECONDS)
        if not isinstance(data, dict):
            return None
        if "user_id" not in data:
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

    session.clear()
    session["user_id"] = user["id"]
    session["role"] = user["role"]
    session["username"] = user["username"]
    session["email"] = user["email"]

    if user["role"] == "patient":
        patient = db.execute(
            "SELECT onboarding_completed FROM patients WHERE user_id = ?",
            (user["id"],),
        ).fetchone()
        onboarding_done = bool(patient and patient["onboarding_completed"])
        session["patient_onboarding_done"] = onboarding_done
        return redirect(url_for("patient_dashboard" if onboarding_done else "patient_onboarding"))
    if user["role"] == "clinic_personnel":
        return redirect(url_for("staff_dashboard"))
    if user["role"] == "system_admin":
        return redirect(url_for("admin_dashboard"))

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

    flash("Registration successful. Welcome!", "success")
    return redirect(url_for("patient_dashboard"))


@bp.get("/forgot-password")
def forgot_password():
    return render_template("forgot_password.html")


@bp.post("/forgot-password")
def forgot_password_post():
    email = _normalize_email(request.form.get("email"))
    if not email:
        flash("Email is required.", "error")
        return render_template("forgot_password.html", email=email)

    # Always respond generically to avoid account enumeration.
    db = get_db()
    user = db.execute("SELECT id, email FROM users WHERE email = ? LIMIT 1", (email,)).fetchone()
    if user is not None:
        token = _make_reset_token(user["id"], user["email"])
        reset_url = url_for("auth.reset_password", token=token, _external=True)
        send_email(
            to_email=email,
            subject="RabiesResQ Password Reset",
            body=f"Use this link to reset your password (valid for 1 hour):\n\n{reset_url}\n",
        )

    flash("If an account exists for that email, a password reset link has been sent.", "success")
    return redirect(url_for("auth.login"))


@bp.get("/reset-password/<token>")
def reset_password(token: str):
    data = _verify_reset_token(token)
    if not data:
        flash("Reset link is invalid or expired. Please request a new one.", "error")
        return redirect(url_for("auth.forgot_password"))
    return render_template("reset_password.html", token=token)


@bp.post("/reset-password/<token>")
def reset_password_post(token: str):
    data = _verify_reset_token(token)
    if not data:
        flash("Reset link is invalid or expired. Please request a new one.", "error")
        return redirect(url_for("auth.forgot_password"))

    password = request.form.get("password") or ""
    confirm_password = request.form.get("confirm_password") or ""
    if not password:
        flash("Password is required.", "error")
        return render_template("reset_password.html", token=token)
    if password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("reset_password.html", token=token)

    user_id = int(data["user_id"])
    email = data.get("email")

    db = get_db()
    user = db.execute("SELECT id, email FROM users WHERE id = ? LIMIT 1", (user_id,)).fetchone()
    if user is None:
        flash("Unable to reset password. Please request a new link.", "error")
        return redirect(url_for("auth.forgot_password"))

    # If the token contained an email, require it to match.
    if email is not None and user["email"] != email:
        flash("Unable to reset password. Please request a new link.", "error")
        return redirect(url_for("auth.forgot_password"))

    try:
        db.execute(
            "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (generate_password_hash(password), user_id),
        )
        db.commit()
    except Exception:
        db.rollback()
        flash("Password reset failed. Please try again.", "error")
        return render_template("reset_password.html", token=token)

    flash("Password updated. You can now log in.", "success")
    return redirect(url_for("auth.login"))


@bp.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))

