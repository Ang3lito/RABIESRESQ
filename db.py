import os
import sqlite3

from flask import current_app, g

from case_ref import normalize_branch_code, validate_branch_code


def get_db():
    if "db" not in g:
        db_path = current_app.config.get("DATABASE")
        if not db_path:
            os.makedirs(current_app.instance_path, exist_ok=True)
            db_path = os.path.join(current_app.instance_path, "rabiesresq.sqlite")

        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
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
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row

    # Initialize schema if users table is missing
    try:
        conn.execute("SELECT 1 FROM users LIMIT 1")
    except sqlite3.OperationalError:
        schema_path = os.path.join(app.root_path, "schema.sql")
        if os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as f:
                conn.executescript(f.read())
            conn.commit()

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
        _ensure_default_clinic(conn)
        _run_multi_clinic_and_super_admin_migrations(conn)
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


def _ensure_default_clinic(conn: sqlite3.Connection) -> None:
    try:
        row = conn.execute("SELECT 1 FROM clinics LIMIT 1").fetchone()
        if not row:
            try:
                conn.execute(
                    """
                    INSERT INTO clinics (name, address, branch_code)
                    VALUES (?, ?, ?)
                    """,
                    ("RabiesResQ Clinic", "Cebu City, Philippines", "CLINIC1"),
                )
            except sqlite3.OperationalError:
                cur = conn.execute(
                    "INSERT INTO clinics (name, address) VALUES (?, ?)",
                    ("RabiesResQ Clinic", "Cebu City, Philippines"),
                )
                cid = cur.lastrowid
                try:
                    conn.execute(
                        """
                        UPDATE clinics
                        SET branch_code = ?
                        WHERE id = ?
                          AND (
                              branch_code IS NULL
                              OR TRIM(COALESCE(branch_code, '')) = ''
                          )
                        """,
                        (f"CLINIC{cid}", cid),
                    )
                except sqlite3.OperationalError:
                    pass
            conn.commit()
    except sqlite3.OperationalError:
        # Table might not exist yet; schema.sql will handle it
        pass


def _migrate_users_role_super_admin(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='users'"
    ).fetchone()
    sql = (row["sql"] or "") if row else ""
    if not sql or "super_admin" in sql:
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            CREATE TABLE users__role_fix (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              email TEXT UNIQUE,
              password_hash TEXT NOT NULL,
              must_change_password INTEGER NOT NULL DEFAULT 0 CHECK(must_change_password IN (0,1)),
              is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
              role TEXT NOT NULL CHECK(role IN ('patient','clinic_personnel','system_admin','super_admin')),
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute("INSERT INTO users__role_fix SELECT * FROM users")
        conn.execute("DROP TABLE users")
        conn.execute("ALTER TABLE users__role_fix RENAME TO users")
        conn.commit()
    except sqlite3.OperationalError:
        conn.rollback()
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _ensure_clinics_branch_code_column(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(clinics)")
    columns = [row["name"] for row in cursor.fetchall()]
    if "branch_code" not in columns:
        try:
            conn.execute("ALTER TABLE clinics ADD COLUMN branch_code TEXT")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_clinics_branch_code ON clinics(branch_code)")
            conn.commit()
        except sqlite3.OperationalError:
            return
    rows = conn.execute("SELECT id FROM clinics ORDER BY id ASC").fetchall()
    used: set[str] = set()
    for r in rows:
        cid = int(r["id"])
        existing = conn.execute(
            "SELECT branch_code FROM clinics WHERE id = ?", (cid,)
        ).fetchone()
        bc = (
            (existing["branch_code"] or "").strip()
            if existing and "branch_code" in existing.keys()
            else ""
        )
        if bc and validate_branch_code(bc):
            used.add(normalize_branch_code(bc))
            continue
        base = f"CLINIC{cid}"
        candidate = base
        n = 0
        while candidate in used or conn.execute(
            "SELECT 1 FROM clinics WHERE branch_code = ? AND id != ? LIMIT 1",
            (candidate, cid),
        ).fetchone():
            n += 1
            candidate = f"{base}_{n}"
        conn.execute(
            "UPDATE clinics SET branch_code = ? WHERE id = ?", (candidate, cid)
        )
        used.add(candidate)
    conn.commit()


def _ensure_system_admins_clinic_id(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT clinic_id FROM system_admins LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute(
                """
                ALTER TABLE system_admins
                ADD COLUMN clinic_id INTEGER REFERENCES clinics(id) ON DELETE RESTRICT
                """
            )
            conn.commit()
        except sqlite3.OperationalError:
            return
    fallback = conn.execute(
        "SELECT id FROM clinics ORDER BY id LIMIT 1"
    ).fetchone()
    if fallback:
        conn.execute(
            """
            UPDATE system_admins
            SET clinic_id = ?
            WHERE clinic_id IS NULL
            """,
            (int(fallback["id"]),),
        )
        conn.commit()


def _ensure_super_admins_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS super_admins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL UNIQUE,
          first_name TEXT,
          last_name TEXT,
          employee_id TEXT UNIQUE,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def _ensure_clinic_case_sequences_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clinic_case_sequences (
          clinic_id INTEGER PRIMARY KEY,
          next_seq INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (clinic_id) REFERENCES clinics(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def _ensure_cases_case_ref_column(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(cases)")
    columns = [row["name"] for row in cursor.fetchall()]
    if "case_ref" not in columns:
        try:
            conn.execute("ALTER TABLE cases ADD COLUMN case_ref TEXT")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cases_case_ref ON cases(case_ref)")
            conn.commit()
        except sqlite3.OperationalError:
            pass


def _ensure_case_access_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_access_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          accessor_user_id INTEGER NOT NULL,
          case_id INTEGER NOT NULL,
          accessed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (accessor_user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_case_access_audit_case ON case_access_audit(case_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_case_access_audit_user ON case_access_audit(accessor_user_id)"
    )
    conn.commit()


def _backfill_case_refs_and_sequences(conn: sqlite3.Connection) -> None:
    clinic_rows = conn.execute(
        "SELECT id, branch_code FROM clinics ORDER BY id ASC"
    ).fetchall()
    for clin in clinic_rows:
        bid = int(clin["id"])
        branch = normalize_branch_code(
            clin["branch_code"] if "branch_code" in clin.keys() else ""
        )
        if not branch:
            branch = f"CLINIC{bid}"
            conn.execute(
                "UPDATE clinics SET branch_code = ? WHERE id = ?",
                (branch, bid),
            )
        case_rows = conn.execute(
            """
            SELECT id FROM cases
            WHERE clinic_id = ?
              AND (case_ref IS NULL OR TRIM(case_ref) = '')
            ORDER BY id ASC
            """,
            (bid,),
        ).fetchall()
        max_seq = conn.execute(
            "SELECT COALESCE(next_seq, 0) AS n FROM clinic_case_sequences WHERE clinic_id = ?",
            (bid,),
        ).fetchone()
        seq = int(max_seq["n"] or 0) if max_seq else 0
        for cr in case_rows:
            seq += 1
            ref = f"{branch}-{seq:04d}"
            suffix = 0
            while conn.execute(
                "SELECT 1 FROM cases WHERE case_ref = ? AND id != ? LIMIT 1",
                (ref, int(cr["id"])),
            ).fetchone():
                suffix += 1
                ref = f"{branch}-{seq:04d}x{suffix}"
            conn.execute(
                "UPDATE cases SET case_ref = ? WHERE id = ?", (ref, int(cr["id"]))
            )
        conn.execute(
            """
            INSERT INTO clinic_case_sequences (clinic_id, next_seq)
            VALUES (?, ?)
            ON CONFLICT(clinic_id) DO UPDATE SET next_seq = excluded.next_seq
            """,
            (bid, seq),
        )
    conn.commit()


def _run_multi_clinic_and_super_admin_migrations(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("SELECT 1 FROM clinics LIMIT 1")
    except sqlite3.OperationalError:
        return
    _migrate_users_role_super_admin(conn)
    _ensure_clinics_branch_code_column(conn)
    _ensure_system_admins_clinic_id(conn)
    _ensure_super_admins_table(conn)
    _ensure_clinic_case_sequences_table(conn)
    _ensure_cases_case_ref_column(conn)
    _ensure_case_access_audit_table(conn)
    _backfill_case_refs_and_sequences(conn)

