import calendar
import csv
import logging
import os
import re
import secrets
import sqlite3
import string
from datetime import date, datetime, timedelta, timezone
import io
import json
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import click
from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    Response,
    session,
    url_for,
    make_response,
)
from werkzeug.security import generate_password_hash

from auth import login_required, role_required
from db import get_db, init_app as init_db_app
from email_service import send_email
from text_utils import normalize_name_case, normalize_optional
from who_rules import WHO_RULES_VERSION

logger = logging.getLogger(__name__)

try:
    PHILIPPINES_TZ = ZoneInfo("Asia/Manila")
except Exception:
    PHILIPPINES_TZ = timezone(timedelta(hours=8))  # Philippines has no DST; fixed offset fallback

# Cebu City barangays (same master list as pre-screening / staff filters).
CEBU_BARANGAY_NAMES: tuple[str, ...] = (
    "Adlaon",
    "Agsungot",
    "Apas",
    "Babag",
    "Bacayan",
    "Banilad",
    "Basak Pardo",
    "Basak San Nicolas",
    "Binaliw",
    "Bonbon",
    "Budlaan",
    "Buhisan",
    "Bulacao",
    "Buot",
    "Busay",
    "Calamba",
    "Cambinocot",
    "Capitol Site",
    "Carreta",
    "Cogon Pardo",
    "Cogon Ramos",
    "Day-as",
    "Duljo Fatima",
    "Ermita",
    "Guadalupe",
    "Guba",
    "Hipodromo",
    "Inayawan",
    "Kalubihan",
    "Kalunasan",
    "Kamagayan",
    "Kamputhaw",
    "Kasambagan",
    "Kinasang-an Pardo",
    "Labangon",
    "Lahug",
    "Lorega San Miguel",
    "Lusaran",
    "Luz",
    "Mabini",
    "Mabolo",
    "Malubog",
    "Mambaling",
    "Pahina Central",
    "Pahina San Nicolas",
    "Pamutan",
    "Pari-an",
    "Paril",
    "Pasil",
    "Pit-os",
    "Poblacion Pardo",
    "Pulangbato",
    "Pung-ol Sibugay",
    "Punta Princesa",
    "Quiot Pardo",
    "Sambag I",
    "Sambag II",
    "San Antonio",
    "San Jose",
    "San Nicolas Proper",
    "San Roque",
    "Santa Cruz",
    "Santo Niño",
    "Sapangdaku",
    "Sawang Calero",
    "Sinsin",
    "Sirao",
    "Suba",
    "Sudlon I",
    "Sudlon II",
    "T. Padilla",
    "Tabunan",
    "Tagba-o",
    "Talamban",
    "Taptap",
    "Tejero",
    "Tinago",
    "Tisa",
    "To-ong",
    "Zapatera",
)

_SQL_STAFF_CASE_NOT_REMOVED = "COALESCE(c.staff_removed, 0) = 0"


def _sql_patient_barangay_lowercase_like() -> str:
    """Lowercased resolved barangay (column or legacy first segment of address) for LIKE filters."""
    return """
        LOWER(
          COALESCE(
            NULLIF(TRIM(COALESCE(p.barangay, '')), ''),
            CASE
              WHEN INSTR(COALESCE(p.address, ''), ',') > 0
              THEN TRIM(SUBSTR(COALESCE(p.address, ''), 1, INSTR(COALESCE(p.address, ''), ',') - 1))
              ELSE TRIM(COALESCE(p.address, ''))
            END
          )
        ) LIKE ?
    """


def _canonical_barangay_if_known(segment: str) -> str | None:
    t = (segment or "").strip()
    if not t:
        return None
    tl = t.lower()
    for name in CEBU_BARANGAY_NAMES:
        if name.lower() == tl:
            return name
    return None


def _barangay_export_value(barangay_col: str | None, address_col: str | None) -> str:
    b = (barangay_col or "").strip()
    if b:
        return b
    a = (address_col or "").strip()
    if not a:
        return ""
    if "," in a:
        return a.split(",", 1)[0].strip()
    return a


def _backfill_patients_barangay_address(db) -> None:
    """One-time style backfill: split legacy combined address; optional whole-string barangay match."""
    rows = db.execute("SELECT id, address, barangay FROM patients").fetchall()
    for r in rows:
        pid = int(r["id"])
        raw_addr = (r["address"] or "").strip()
        existing_b = (r["barangay"] or "").strip()
        if existing_b or not raw_addr:
            continue
        if "," in raw_addr:
            first, rest = raw_addr.split(",", 1)
            b = (first or "").strip()
            street = (rest or "").strip()
            db.execute(
                "UPDATE patients SET barangay = ?, address = ? WHERE id = ?",
                (b or None, street or None, pid),
            )
        else:
            canon = _canonical_barangay_if_known(raw_addr)
            if canon:
                db.execute(
                    "UPDATE patients SET barangay = ?, address = ? WHERE id = ?",
                    (canon, None, pid),
                )
    db.commit()


def _now_philippines_local_iso() -> str:
    """Wall-clock time in the Philippines (UTC+8), naive ISO string for storage and display."""
    dt = datetime.now(PHILIPPINES_TZ)
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")


def _dashboard_appointment_sequence_key(row, compute_summary) -> tuple:
    """Earliest effective next-dose or slot date first (matches dashboard card dates)."""
    summ = compute_summary(row["case_id"], row["risk_level"], row["case_category"])
    nd = summ.get("next_due_date")
    if nd is not None:
        return (nd.toordinal(), row["id"])
    raw = (row["appointment_datetime"] or "").strip()
    if raw:
        try:
            dt = datetime.fromisoformat(raw)
            return (dt.date().toordinal(), dt.timestamp(), row["id"])
        except ValueError:
            pass
    return (999999999, 0, row["id"])


def _sort_patient_dashboard_appointments_by_display_date(rows: list, compute_summary) -> list:
    """Order like dashboard cards: next_due_date when set, else appointment_datetime; upcoming first."""
    today_ph = datetime.now(PHILIPPINES_TZ).date()
    now_ph = datetime.now(PHILIPPINES_TZ).replace(tzinfo=None)

    def sort_key(r) -> tuple:
        rid = r["id"]
        summ = compute_summary(r["case_id"], r["risk_level"], r["case_category"])
        nd = summ.get("next_due_date")
        if nd is not None:
            dt = datetime.combine(nd, datetime.min.time())
            if nd >= today_ph:
                return (0, dt.timestamp(), 0, rid)
            return (1, -dt.timestamp(), 0, rid)
        raw = (r["appointment_datetime"] or "").strip()
        if not raw:
            return (2, 0.0, "", rid)
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return (2, 0.0, raw, rid)
        if dt >= now_ph:
            return (0, dt.timestamp(), 0, rid)
        return (1, -dt.timestamp(), 0, rid)

    return sorted(rows, key=sort_key)


def _normalize_vaccination_card_date_fields(vaccination_card: dict) -> None:
    """Normalize ISO date strings on vaccination_card for HTML date inputs and display."""
    for _date_field in ("pcec_mfg_date", "pcec_expiry", "tetanus_mfg_date", "tetanus_expiry"):
        raw_value = (vaccination_card.get(_date_field) or "").strip()
        if not raw_value:
            vaccination_card[_date_field] = ""
            continue
        try:
            vaccination_card[_date_field] = datetime.fromisoformat(raw_value).date().isoformat()
        except ValueError:
            vaccination_card[_date_field] = ""


def _pvrv_pcec_value_from_form(raw: str) -> str:
    x = (raw or "").strip().upper()
    if x == "PVRV":
        return "PVRV"
    if x == "PCEC":
        return "PCEC"
    return ""


def _pvrv_pcec_prefill_from_db(vc: dict) -> str:
    """Value for vc_pvrv_pcec select from stored row (PVRV/PCEC or heuristic when PCEC data exists)."""
    pv = (vc.get("pvrv") or "").strip()
    ul = pv.upper()
    if ul == "PVRV":
        return "PVRV"
    if ul == "PCEC":
        return "PCEC"
    batch = (vc.get("pcec_batch") or "").strip()
    mfg = (vc.get("pcec_mfg_date") or "").strip()
    exp = (vc.get("pcec_expiry") or "").strip()
    if not pv and (batch or mfg or exp):
        return "PCEC"
    return ""


_AR_CELL_CULTURE_BRANDS = frozenset(
    {"Verorab", "Rabipur", "Speeda", "Abhayrab", "Vaxirab N", "ChiroRab"}
)


def _anti_rabies_vaccine_from_form(raw: str) -> tuple[str, str]:
    """Map unified Anti-Rabies select to (pvrv, erig_hrig) columns. Exactly one product."""
    x = (raw or "").strip()
    if not x:
        return ("", "")
    ul = x.upper()
    if ul == "PVRV":
        return ("PVRV", "")
    if ul == "PCEC":
        return ("PCEC", "")
    if ul == "ERIG":
        return ("", "ERIG")
    if ul == "HRIG":
        return ("", "HRIG")
    if x in _AR_CELL_CULTURE_BRANDS:
        return (x, "")
    return ("", "")


def _anti_rabies_type_label_from_form(raw: str) -> str:
    pv, er = _anti_rabies_vaccine_from_form(raw)
    return (pv or er or "").strip()


def _anti_rabies_vaccine_prefill_from_db(vc: dict) -> str:
    """Single select value from stored vaccination_card row (legacy codes or product name)."""
    pv = (vc.get("pvrv") or "").strip()
    eh = (vc.get("erig_hrig") or "").strip()
    pul = pv.upper()
    eul = eh.upper()
    if pul == "PVRV":
        return "PVRV"
    if pul == "PCEC":
        return "PCEC"
    if eul == "ERIG":
        return "ERIG"
    if eul == "HRIG":
        return "HRIG"
    if pv:
        return pv
    batch = (vc.get("pcec_batch") or "").strip()
    mfg = (vc.get("pcec_mfg_date") or "").strip()
    exp = (vc.get("pcec_expiry") or "").strip()
    if not eh and (batch or mfg or exp):
        return "PCEC"
    return ""


_DOSE_STANDARD_OPTIONS = frozenset({"0.1 mL", "0.5 mL", "1.0 mL"})


def _dose_value_from_form(sel: str, other: str) -> str:
    s = (sel or "").strip()
    if s == "Others":
        return (other or "").strip()
    return s


def _dose_sel_and_other_from_stored(dose: str | None) -> tuple[str, str]:
    d = (dose or "").strip()
    if d in _DOSE_STANDARD_OPTIONS:
        return (d, "")
    if not d:
        return ("", "")
    return ("Others", d)


# Pre- / post-exposure / booster dose rows: same calendar dose_date may exist in only one record_type (Pre > Post > Booster).
_VC_DOSE_SCHEDULES: tuple[tuple[str, str, tuple[int, ...]], ...] = (
    ("pre_exposure", "vc_pre", (0, 7, 28)),
    ("post_exposure", "vc_post", (0, 3, 7, 14, 28)),
    ("booster", "vc_booster", (0, 3)),
)


def _normalize_dose_date_key(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError:
        return ""


def _vaccination_dose_date_owners_from_getter(get_val: Callable[[str], str]) -> dict[str, str]:
    """Map YYYY-MM-DD -> record_type that owns that dose date (first seen: pre > post > booster)."""
    date_owner: dict[str, str] = {}
    for record_type, prefix, days in _VC_DOSE_SCHEDULES:
        for day in days:
            d = _normalize_dose_date_key(get_val(f"{prefix}_{day}_date"))
            if d and d not in date_owner:
                date_owner[d] = record_type
    return date_owner


def _vaccination_resolved_dose_date_iso(
    record_type: str,
    dose_date_raw: str,
    date_owners: dict[str, str],
) -> str | None:
    """Calendar dose date stored for this row: set only if this section owns that day (Pre > Post > Booster)."""
    dkey = _normalize_dose_date_key(dose_date_raw)
    if not dkey:
        return None
    if date_owners.get(dkey) == record_type:
        return dkey
    return None


def _vaccination_dose_row_should_insert(
    resolved_dose_date_iso: str | None,
    type_of_vaccine: str,
    dose: str,
    route_site: str,
    given_by: str,
) -> bool:
    return bool(any([resolved_dose_date_iso, type_of_vaccine, dose, route_site, given_by]))


def _vaccination_type_for_dose_row(
    type_raw: str,
    master_type: str,
    resolved_dose_date_iso: str | None,
) -> str:
    """Anti-rabies master selection fills type only for rows that have a resolved dose date."""
    if not resolved_dose_date_iso:
        return ""
    t = (type_raw or "").strip()
    return t or (master_type or "").strip()


def _vaccination_card_doses_apply_master_type_to_dated_rows(
    card_doses_by_type: dict[str, dict[int, dict]],
    master_type: str,
) -> None:
    """After dose_date fields are final, apply master vaccine label only where a dose date is set."""
    for record_type, _prefix, days in _VC_DOSE_SCHEDULES:
        for day in days:
            cell = (card_doses_by_type.get(record_type) or {}).get(day)
            if not cell:
                continue
            rd_key = _normalize_dose_date_key(cell.get("dose_date") or "")
            cell["type_of_vaccine"] = _vaccination_type_for_dose_row(
                cell.get("type_of_vaccine") or "",
                master_type,
                rd_key or None,
            )


def _vaccination_card_doses_apply_resolved_dates(
    card_doses_by_type: dict[str, dict[int, dict]],
    date_owners: dict[str, str],
) -> None:
    """Mutate sticky card_doses_by_type dose_date fields to match save-time exclusivity."""
    for record_type, _prefix, days in _VC_DOSE_SCHEDULES:
        for day in days:
            cell = (card_doses_by_type.get(record_type) or {}).get(day)
            if not cell:
                continue
            raw = cell.get("dose_date") or ""
            resolved = _vaccination_resolved_dose_date_iso(record_type, raw, date_owners)
            cell["dose_date"] = resolved or ""


_TETANUS_TOXOID_BRANDS = frozenset(
    {"Abhay-TOX", "T-Vac", "Tetavax", "Generic Tetanus Toxoid"}
)
_ATS_BRANDS = frozenset({"Antitet", "Sharjvax"})
_HTIG_BRANDS = frozenset({"Tetagam P", "Sero-Tet", "Generic HTIG"})


def _tetanus_triple_from_agent(raw: str) -> tuple[str, str, str]:
    x = (raw or "").strip()
    if not x:
        return ("", "", "")
    xl = x.lower()
    if xl == "tetanus toxoid":
        return ("Generic Tetanus Toxoid", "", "")
    if xl == "ats":
        return ("", "Antitet", "")
    if xl == "htig":
        return ("", "", "Generic HTIG")
    if x in _TETANUS_TOXOID_BRANDS:
        return (x, "", "")
    if x in _ATS_BRANDS:
        return ("", x, "")
    if x in _HTIG_BRANDS:
        return ("", "", x)
    return ("", "", "")


def _tetanus_agent_prefill_from_db(vc: dict) -> str:
    tt = (vc.get("tetanus_toxoid") or "").strip()
    ats = (vc.get("ats") or "").strip()
    htig = (vc.get("htig") or "").strip()
    if tt:
        if tt == "Tetanus Toxoid":
            return "Generic Tetanus Toxoid"
        return tt
    if ats:
        if ats == "ATS":
            return "Antitet"
        return ats
    if htig:
        if htig == "HTIG":
            return "Generic HTIG"
        return htig
    return ""


def _case_has_vaccination_record(db, case_id: int) -> bool:
    """True if the case has any vaccination record (doses or administered vaccination row)."""
    if db.execute(
        "SELECT 1 FROM vaccination_records WHERE case_id = ? LIMIT 1",
        (case_id,),
    ).fetchone():
        return True
    if db.execute(
        "SELECT 1 FROM vaccination_card_doses WHERE case_id = ? LIMIT 1",
        (case_id,),
    ).fetchone():
        return True
    return False


def _case_has_first_dose_recorded(db, case_id: int) -> bool:
    """True if at least one administered dose was recorded for the case."""
    if db.execute(
        "SELECT 1 FROM vaccination_records WHERE case_id = ? LIMIT 1",
        (case_id,),
    ).fetchone():
        return True
    if db.execute(
        """
        SELECT 1
        FROM vaccination_card_doses
        WHERE case_id = ?
          AND NULLIF(TRIM(COALESCE(dose_date, '')), '') IS NOT NULL
        LIMIT 1
        """,
        (case_id,),
    ).fetchone():
        return True
    return False


def _vaccination_card_has_visible_content(vaccination_card: dict | None) -> bool:
    """True if vaccination card has user-visible values (including remarks)."""
    if not vaccination_card:
        return False
    meaningful_fields = (
        "anti_rabies",
        "pvrv",
        "pcec_batch",
        "pcec_mfg_date",
        "pcec_expiry",
        "erig_hrig",
        "tetanus_prophylaxis",
        "tetanus_toxoid",
        "ats",
        "htig",
        "tetanus_batch",
        "tetanus_mfg_date",
        "tetanus_expiry",
        "remarks",
    )
    for field in meaningful_fields:
        val = vaccination_card.get(field)
        if val is not None and str(val).strip():
            return True
    return False


def _appointment_is_prescreening(db, appointment_row) -> bool:
    """True if this appointment should be treated as the pre-screening appointment."""
    appt_type = (appointment_row["type"] or "").strip().lower()
    if appt_type in ("pre-screening", "pre_screening", "prescreening"):
        return True

    # Fallback: earliest appointment for the case
    case_id = appointment_row["case_id"]
    if not case_id:
        return False
    first = db.execute(
        """
        SELECT id
        FROM appointments
        WHERE case_id = ?
        ORDER BY datetime(appointment_datetime) ASC, id ASC
        LIMIT 1
        """,
        (case_id,),
    ).fetchone()
    return first is not None and int(first["id"]) == int(appointment_row["id"])


def _patient_can_modify_appointment(db, appointment_row) -> bool:
    """True if patient is allowed to reschedule/cancel this appointment."""
    if not _appointment_is_prescreening(db, appointment_row):
        return False
    return not _case_has_first_dose_recorded(db, int(appointment_row["case_id"]))


def _walk_in_appointment_status_for_case(db, case_id: int) -> str:
    return "Scheduled" if _case_has_vaccination_record(db, case_id) else "Pending"


def _schedule_days_for_vaccination_record_type(record_type: str) -> list[int]:
    if record_type == "pre_exposure":
        return [0, 7, 28]
    if record_type == "post_exposure":
        return [0, 3, 7, 14, 28]
    if record_type == "booster":
        return [0, 3]
    return [0, 3, 7, 14, 28]


def _course_label_for_vaccination_record_type(record_type: str) -> str:
    return {
        "pre_exposure": "Pre-Exposure Vaccination",
        "post_exposure": "Post-Exposure Vaccination",
        "booster": "Booster Vaccination",
    }.get(record_type, record_type)


def _build_course_rows_from_active_map(
    active_rows: dict[int, dict], schedule_days: list[int]
) -> list[dict]:
    course_rows: list[dict] = []
    for day in schedule_days:
        row_data = active_rows.get(day, {}) or {}
        course_rows.append(
            {
                "day_number": day,
                "dose_date": (row_data.get("dose_date") or "").strip() or None,
                "type_of_vaccine": (row_data.get("type_of_vaccine") or "").strip() or None,
                "dose": (row_data.get("dose") or "").strip() or None,
                "route_site": (row_data.get("route_site") or "").strip() or None,
                "given_by": (row_data.get("given_by") or "").strip() or None,
            }
        )
    return course_rows


def _count_filled_card_doses(active_rows: dict[int, dict]) -> int:
    doses_completed = 0
    for row_data in active_rows.values():
        dose_date = (row_data.get("dose_date") or "").strip()
        type_of_vaccine = (row_data.get("type_of_vaccine") or "").strip()
        given_by = (row_data.get("given_by") or "").strip()
        if dose_date and type_of_vaccine and given_by:
            doses_completed += 1
    return doses_completed


def _dose_sections_for_patient_card(
    card_doses_by_type: dict[str, dict[int, dict]],
    category_value: str,
) -> list[dict]:
    """
    One Dose Record table per course type that has saved rows (so post-exposure
    doses are not hidden when the case category selects pre-exposure, etc.).
    If no rows exist in any course, returns a single section using the legacy
    category/booster rules and empty schedule slots.
    """
    order = ("pre_exposure", "post_exposure", "booster")
    sections: list[dict] = []
    for rt in order:
        active_rows = card_doses_by_type.get(rt) or {}
        if not active_rows:
            continue
        schedule_days = _schedule_days_for_vaccination_record_type(rt)
        course_rows = _build_course_rows_from_active_map(active_rows, schedule_days)
        sections.append(
            {
                "course_label": _course_label_for_vaccination_record_type(rt),
                "record_type": rt,
                "course_rows": course_rows,
                "expected_doses": len(schedule_days),
                "doses_completed": _count_filled_card_doses(active_rows),
            }
        )
    if sections:
        return sections

    category_lower = (category_value or "").strip().lower()
    active_record_type = "pre_exposure" if category_lower == "category i" else "post_exposure"
    booster_rows = card_doses_by_type.get("booster") or {}
    if booster_rows:
        display_course = "booster"
    elif active_record_type == "pre_exposure":
        display_course = "pre_exposure"
    else:
        display_course = "post_exposure"
    schedule_days = _schedule_days_for_vaccination_record_type(display_course)
    active_rows = card_doses_by_type.get(display_course) or {}
    course_rows = _build_course_rows_from_active_map(active_rows, schedule_days)
    return [
        {
            "course_label": _course_label_for_vaccination_record_type(display_course),
            "record_type": display_course,
            "course_rows": course_rows,
            "expected_doses": len(schedule_days),
            "doses_completed": _count_filled_card_doses(active_rows),
        }
    ]


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


def _admin_display_name(admin_row) -> str:
    if admin_row is None:
        return "Admin"
    fn = (admin_row["first_name"] or "").strip()
    ln = (admin_row["last_name"] or "").strip()
    if fn or ln:
        return " ".join(p for p in [fn, ln] if p)
    un = (admin_row["username"] or "").strip()
    return un or "Admin"


def _admin_initials(admin_row) -> str:
    if admin_row is None:
        return "A"
    fn = (admin_row["first_name"] or "").strip()
    ln = (admin_row["last_name"] or "").strip()
    if fn and ln:
        return (fn[0] + ln[0]).upper()
    if fn:
        return fn[0].upper()
    un = (admin_row["username"] or "A").strip()
    return (un[0] or "A").upper()


def _patient_display_name(patient_row) -> str:
    """Prefer full name; match dashboard greeting when only username is set."""
    if patient_row is None:
        return "Patient"
    fn = (patient_row["first_name"] or "").strip()
    ln = (patient_row["last_name"] or "").strip()
    if fn or ln:
        return " ".join(p for p in [fn, ln] if p)
    un = (patient_row["username"] or "").strip()
    return un or "Patient"


def _patient_display_name_from_session(patient_row, session_username: str | None) -> str:
    """Sidebar label: patient row if present, else logged-in username."""
    if patient_row is not None:
        return _patient_display_name(patient_row)
    u = (session_username or "").strip()
    return u or "Patient"


def _patient_initials(patient_row) -> str:
    if patient_row is None:
        return "P"
    fn = (patient_row["first_name"] or "").strip()
    ln = (patient_row["last_name"] or "").strip()
    if fn and ln:
        return (fn[0] + ln[0]).upper()
    if fn:
        return fn[0].upper()
    un = (patient_row["username"] or "P").strip()
    return (un[0] or "P").upper()


def _staff_initials(staff_row) -> str:
    if staff_row is None:
        return "V"
    fn = (staff_row["first_name"] or "").strip()
    ln = (staff_row["last_name"] or "").strip()
    if fn and ln:
        return (fn[0] + ln[0]).upper()
    if fn:
        return fn[0].upper()
    un = (staff_row["username"] or "V").strip()
    return (un[0] or "V").upper()


def _staff_display_name(staff_row) -> str:
    """Sidebar first line: given + family name only (title shown on second line)."""
    if staff_row is None:
        return "Staff"
    first_name = (staff_row["first_name"] or "").strip()
    last_name = (staff_row["last_name"] or "").strip()
    if first_name or last_name:
        return " ".join(part for part in [first_name, last_name] if part)
    un = (staff_row["username"] or "").strip()
    return un or "Staff"


def _staff_account_type_label(staff_row) -> str:
    """Human-readable clinic role: Doctor, Nurse, or fallback."""
    if staff_row is None:
        return "Clinic staff"
    t = (staff_row["title"] or "").strip()
    if t == "Doctor":
        return "Doctor"
    if t == "Nurse":
        return "Nurse"
    return "Clinic staff"


def _get_singleton_clinic_row(db):
    return db.execute(
        "SELECT id, name, address, operating_hours_json FROM clinics ORDER BY id LIMIT 1"
    ).fetchone()


def _session_log_role_label(role: str | None) -> str:
    r = (role or "").strip()
    if r == "patient":
        return "Patient"
    if r == "clinic_personnel":
        return "Clinic"
    if r == "system_admin":
        return "Admin"
    return r or "—"


def _format_session_timestamp(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return "—"
    try:
        return datetime.fromisoformat(str(raw)).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return str(raw).strip()


def _parse_local_slot_datetime(raw_value: str | None) -> datetime | None:
    """Parse stored slot datetime strings into local naive datetime for comparisons."""
    raw = (raw_value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("T", " "))
    except ValueError:
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(PHILIPPINES_TZ).replace(tzinfo=None)
    return dt


def _is_slot_in_past(raw_value: str | None) -> bool:
    slot_dt = _parse_local_slot_datetime(raw_value)
    if slot_dt is None:
        return True
    now_local = datetime.now(PHILIPPINES_TZ).replace(tzinfo=None)
    return slot_dt <= now_local


def _admin_fetch_user(db, user_id: int):
    return db.execute(
        """
        SELECT sa.*, u.username, u.email, u.must_change_password
        FROM system_admins sa
        JOIN users u ON u.id = sa.user_id
        WHERE sa.user_id = ?
        """,
        (user_id,),
    ).fetchone()


def _admin_user_manageable_in_clinic(db, clinic_id: int, target_user_id: int) -> bool:
    """True if the user is clinic staff at this clinic or a patient with a case at this clinic."""
    if (
        db.execute(
            """
            SELECT 1 FROM clinic_personnel
            WHERE user_id = ? AND clinic_id = ?
            LIMIT 1
            """,
            (target_user_id, clinic_id),
        ).fetchone()
        is not None
    ):
        return True
    return (
        db.execute(
            """
            SELECT 1 FROM patients p
            INNER JOIN cases c ON c.patient_id = p.id
            WHERE p.user_id = ? AND c.clinic_id = ?
            LIMIT 1
            """,
            (target_user_id, clinic_id),
        ).fetchone()
        is not None
    )


_ADMIN_PAGE_BADGE_KEYS = ("patients", "appointments", "reporting", "users", "session_logs")


_STAFF_PAGE_BADGE_KEYS = ("cases",)


def _staff_mark_page_seen(db, staff_user_id: int, page_key: str) -> None:
    if page_key not in _STAFF_PAGE_BADGE_KEYS:
        return
    db.execute(
        """
        INSERT INTO staff_page_last_seen (staff_user_id, page_key, last_seen_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(staff_user_id, page_key)
        DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (staff_user_id, page_key),
    )
    db.commit()


def _staff_nav_badge_counts(db, staff_user_id: int, clinic_id: int | None) -> dict[str, int]:
    counts = {key: 0 for key in _STAFF_PAGE_BADGE_KEYS}
    if clinic_id is None:
        return counts

    seen_rows = db.execute(
        """
        SELECT page_key, last_seen_at
        FROM staff_page_last_seen
        WHERE staff_user_id = ?
        """,
        (staff_user_id,),
    ).fetchall()
    last_seen_by_page = {
        (row["page_key"] or "").strip(): (row["last_seen_at"] or "").strip()
        for row in seen_rows
    }
    epoch = "1970-01-01 00:00:00"

    counts["cases"] = int(
        (
            db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM cases c
                WHERE c.clinic_id = ?
                  AND {_SQL_STAFF_CASE_NOT_REMOVED}
                  AND LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')
                  AND datetime(COALESCE(NULLIF(c.created_at, ''), '1970-01-01 00:00:00'))
                      > datetime(?)
                """,
                (clinic_id, last_seen_by_page.get("cases") or epoch),
            ).fetchone()["n"]
        )
        or 0
    )

    return counts


def _admin_mark_page_seen(db, admin_user_id: int, page_key: str) -> None:
    if page_key not in _ADMIN_PAGE_BADGE_KEYS:
        return
    # Use CURRENT_TIMESTAMP (UTC, no timezone suffix) — same as _staff_mark_page_seen —
    # so SQLite datetime() comparisons work correctly against cases.created_at.
    db.execute(
        """
        INSERT INTO admin_page_last_seen (admin_user_id, page_key, last_seen_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(admin_user_id, page_key)
        DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (admin_user_id, page_key),
    )
    db.commit()


def _admin_nav_badge_counts(db, admin_user_id: int, clinic_id: int | None) -> dict[str, int]:
    counts = {key: 0 for key in _ADMIN_PAGE_BADGE_KEYS}
    if clinic_id is None:
        return counts

    seen_rows = db.execute(
        """
        SELECT page_key, last_seen_at
        FROM admin_page_last_seen
        WHERE admin_user_id = ?
        """,
        (admin_user_id,),
    ).fetchall()
    last_seen_by_page = {
        (row["page_key"] or "").strip(): (row["last_seen_at"] or "").strip()
        for row in seen_rows
    }
    epoch = "1970-01-01 00:00:00"

    ls_patients = (last_seen_by_page.get("patients") or epoch).strip()
    counts["patients"] = int(
        (
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM cases c
                WHERE c.clinic_id = ?
                  AND LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('queued')
                  AND datetime(
                        REPLACE(
                            TRIM(COALESCE(NULLIF(c.created_at, ''), '1970-01-01 00:00:00')),
                            'T',
                            ' '
                        )
                      )
                      > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (clinic_id, ls_patients),
            ).fetchone()["n"]
        )
        or 0
    )

    ls_appointments = (last_seen_by_page.get("appointments") or epoch).strip()
    counts["appointments"] = int(
        (
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM appointments a
                WHERE a.clinic_id = ?
                  AND LOWER(COALESCE(a.type, '')) = 'pre-screening'
                  AND datetime(
                        REPLACE(
                            TRIM(COALESCE(NULLIF(a.created_at, ''), '1970-01-01 00:00:00')),
                            'T',
                            ' '
                        )
                      )
                      > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (clinic_id, ls_appointments),
            ).fetchone()["n"]
        )
        or 0
    )

    ls_reporting = (last_seen_by_page.get("reporting") or epoch).strip()
    counts["reporting"] = int(
        (
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM reports r
                WHERE r.clinic_id = ?
                  AND datetime(
                        REPLACE(
                            TRIM(COALESCE(NULLIF(r.generation_date, ''), '1970-01-01 00:00:00')),
                            'T',
                            ' '
                        )
                      ) > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (clinic_id, ls_reporting),
            ).fetchone()["n"]
        )
        or 0
    )

    ls_users = (last_seen_by_page.get("users") or epoch).strip()
    counts["users"] = int(
        (
            db.execute(
                """
                SELECT COUNT(DISTINCT u.id) AS n
                FROM users u
                WHERE (
                    EXISTS (
                        SELECT 1
                        FROM clinic_personnel cp
                        WHERE cp.user_id = u.id
                          AND cp.clinic_id = ?
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM patients p
                        INNER JOIN cases c ON c.patient_id = p.id
                        WHERE p.user_id = u.id
                          AND c.clinic_id = ?
                    )
                )
                AND datetime(
                      REPLACE(
                        TRIM(
                          COALESCE(
                            NULLIF(u.updated_at, ''),
                            NULLIF(u.created_at, ''),
                            '1970-01-01 00:00:00'
                          )
                        ),
                        'T',
                        ' '
                      )
                    ) > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (clinic_id, clinic_id, ls_users),
            ).fetchone()["n"]
        )
        or 0
    )

    ls_sess = (last_seen_by_page.get("session_logs") or epoch).strip()
    counts["session_logs"] = int(
        (
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM user_session_logs
                WHERE datetime(
                      REPLACE(
                        TRIM(COALESCE(NULLIF(logged_in_at, ''), '1970-01-01 00:00:00')),
                        'T',
                        ' '
                      )
                    )
                    > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (ls_sess,),
            ).fetchone()["n"]
        )
        or 0
    )

    return counts


def _admin_session_logs_notifications(db, admin_user_id: int) -> list[dict[str, object]]:
    """Login-activity alerts for the Session Logs page (same window as the sidebar badge)."""
    seen_rows = db.execute(
        """
        SELECT page_key, last_seen_at
        FROM admin_page_last_seen
        WHERE admin_user_id = ?
        """,
        (admin_user_id,),
    ).fetchall()
    last_seen_by_page = {
        (row["page_key"] or "").strip(): (row["last_seen_at"] or "").strip()
        for row in seen_rows
    }
    epoch = "1970-01-01 00:00:00"
    ls_sess = (last_seen_by_page.get("session_logs") or epoch).strip()
    n = int(
        (
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM user_session_logs
                WHERE datetime(
                      REPLACE(
                        TRIM(COALESCE(NULLIF(logged_in_at, ''), '1970-01-01 00:00:00')),
                        'T',
                        ' '
                      )
                    )
                    > datetime(REPLACE(TRIM(?), 'T', ' '))
                """,
                (ls_sess,),
            ).fetchone()["n"]
        )
        or 0
    )
    if n > 0:
        return [
            {
                "type": "session_logs",
                "page_key": "session_logs",
                "count": n,
                "message": "new login event(s) since you last viewed Session Logs.",
                "link_href": url_for("admin_session_logs", page=1),
                "recipient_label": None,
            }
        ]
    return [
        {
            "type": "session_logs",
            "page_key": "session_logs",
            "count": None,
            "message": "No new login events since you last viewed Session Logs.",
            "link_href": None,
            "recipient_label": None,
        }
    ]


def _get_admin_dashboard_notifications(db, clinic_id: int) -> list[dict[str, object]]:
    """Clinic-scoped summary alerts for Reporting overview (read-only)."""
    out: list[dict[str, object]] = []
    pending = (
        db.execute(
            """
            SELECT COUNT(*) AS n FROM cases
            WHERE clinic_id = ?
              AND LOWER(TRIM(COALESCE(case_status, 'pending'))) = 'pending'
            """,
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )
    # Removed 'case' notification from here to reduce redundancy; handled by global support banner.
    today_appts = (
        db.execute(
            """
            SELECT COUNT(*) AS n FROM appointments
            WHERE clinic_id = ?
              AND DATE(appointment_datetime) = DATE('now', 'localtime')
            """,
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )
    if today_appts:
        out.append(
            {
                "type": "appointment",
                "page_key": "appointments",
                "count": int(today_appts),
                "message": "appointment(s) scheduled for today.",
                "link_href": url_for("admin_appointments", date_filter="today", page=1),
                "recipient_label": None,
            }
        )

    recent_reports = (
        db.execute(
            """
            SELECT COUNT(*) AS n FROM reports r
            WHERE r.clinic_id = ?
              AND datetime(
                    COALESCE(NULLIF(r.generation_date, ''), '1970-01-01 00:00:00')
                  ) >= datetime('now', '-7 days')
            """,
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )
    if recent_reports:
        out.append(
            {
                "type": "report",
                "page_key": "reporting",
                "count": int(recent_reports),
                "message": "report(s) generated in the last 7 days.",
                "link_href": url_for("admin_analytics", tab="overview", period="30d"),
                "recipient_label": None,
            }
        )
    return out


def _admin_notifications_for_page(
    db, clinic_id: int | None, page_key: str
) -> list[dict[str, object]]:
    """Return only the notifications relevant to an admin page."""
    if clinic_id is None:
        return []
    notifs = _get_admin_dashboard_notifications(db, clinic_id)
    if page_key == "reporting":
        return notifs
    return [n for n in notifs if (n.get("page_key") or "") == page_key]


def _admin_case_vaccination_context(db, case_id: int, clinic_id: int) -> dict | None:
    """Read-only vaccination card + doses for an admin-scoped case."""
    case_row = db.execute(
        """
        SELECT
          c.id,
          c.risk_level,
          c.category,
          c.who_category_auto,
          c.who_category_final,
          c.type_of_exposure,
          COALESCE(TRIM(p.first_name || ' ' || p.last_name), u.username) AS patient_name
        FROM cases c
        JOIN patients p ON p.id = c.patient_id
        JOIN users u ON u.id = p.user_id
        WHERE c.id = ? AND c.clinic_id = ?
        """,
        (case_id, clinic_id),
    ).fetchone()
    if case_row is None:
        return None
    vc_row = db.execute("SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)).fetchone()
    vaccination_card = dict(vc_row) if vc_row else {}
    _normalize_vaccination_card_date_fields(vaccination_card)
    vaccination_card["form_vc_anti_rabies_vaccine"] = _anti_rabies_vaccine_prefill_from_db(vaccination_card)
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
        if r in card_doses_by_type and d is not None:
            card_doses_by_type[r][int(d)] = dict(row)
    return {
        "case": dict(case_row),
        "case_code": f"C-{case_id:05d}",
        "patient_name": (case_row["patient_name"] or "").strip() or "—",
        "vaccination_card": vaccination_card,
        "card_doses_by_type": card_doses_by_type,
    }


def _case_is_high_risk_sql() -> str:
    return "LOWER(COALESCE(c.risk_level, '')) IN ('category iii', 'high', 'high-risk', 'high risk')"


def _iter_months_ending_this_month(*, count: int = 7) -> list[tuple[int, int, str]]:
    """Return (year, month, short label) for the last `count` months ending at the current month."""
    today = date.today()
    y, m = today.year, today.month
    out: list[tuple[int, int, str]] = []
    for back in range(count - 1, -1, -1):
        mm = m - back
        yy = y
        while mm < 1:
            mm += 12
            yy -= 1
        while mm > 12:
            mm -= 12
            yy += 1
        label = date(yy, mm, 1).strftime("%b")
        out.append((yy, mm, label))
    return out


def _admin_year_dropdown_options() -> list[int]:
    """Years available for admin yearly filter (minimum 2025 through current calendar year)."""
    y = date.today().year
    end = max(y, 2025)
    return list(range(2025, end + 1))


def _admin_resolve_period_dates() -> tuple[str, str, str, int | None]:
    """period: 30d | yearly | custom; ISO date_from/date_to; yearly_year set when period is yearly."""
    period = (request.args.get("period") or "30d").strip().lower()
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()
    today = date.today()
    min_y = 2025
    max_y = today.year
    yearly_year: int | None = None

    if period == "yearly":
        raw_y = (request.args.get("year") or "").strip()
        try:
            y = int(raw_y)
        except ValueError:
            y = max_y
        y = max(min_y, min(max_y, y))
        yearly_year = y
        date_from = f"{y}-01-01"
        date_to = f"{y}-12-31"
    elif period == "custom" and date_from and date_to:
        pass
    else:
        period = "30d"
        date_from = (today - timedelta(days=30)).isoformat()
        date_to = today.isoformat()
    try:
        df_chk = date.fromisoformat(date_from)
        dt_chk = date.fromisoformat(date_to)
        if df_chk > dt_chk:
            date_from, date_to = date_to, date_from
    except ValueError:
        period = "30d"
        date_from = (today - timedelta(days=30)).isoformat()
        date_to = today.isoformat()
        yearly_year = None
    return period, date_from, date_to, yearly_year


def _count_completed_cases_in_period(
    db, clinic_id: int, date_from: str, date_to: str
) -> int:
    """Cases with completed status in the period, same definition as admin Cases completed filter.

    Matches LOWER(COALESCE(c.case_status,'')) = 'completed'. Date range uses COALESCE(created_at,
    exposure_date) like Bite Cases / Total Users on the admin dashboard.
    """
    row = db.execute(
        """
        SELECT COUNT(*) AS n
        FROM cases c
        WHERE c.clinic_id = ?
          AND LOWER(COALESCE(c.case_status, '')) = 'completed'
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
        """,
        (clinic_id, date_from, date_to),
    ).fetchone()
    return int(row["n"] or 0)


def _count_total_cases_in_period(db, clinic_id: int, date_from: str, date_to: str) -> int:
    """All cases with case date (created or exposure) in range; same denominator as Bite Cases."""
    row = db.execute(
        """
        SELECT COUNT(*) AS n
        FROM cases c
        WHERE c.clinic_id = ?
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
        """,
        (clinic_id, date_from, date_to),
    ).fetchone()
    return int(row["n"] or 0)


def _case_completion_pct(db, clinic_id: int, date_from: str, date_to: str) -> int:
    """Share of completed cases (Completed Cases definition) among all cases in the same date window."""
    total = _count_total_cases_in_period(db, clinic_id, date_from, date_to)
    if not total:
        return 0
    completed = _count_completed_cases_in_period(db, clinic_id, date_from, date_to)
    return round((completed / total) * 100)


def _count_ongoing_cases_in_period(db, clinic_id: int, date_from: str, date_to: str) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS n FROM cases c
        WHERE c.clinic_id = ?
          AND LOWER(COALESCE(c.case_status, 'pending')) = 'pending'
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
        """,
        (clinic_id, date_from, date_to),
    ).fetchone()
    return int(row["n"] or 0)


def _count_no_show_cases_in_period(db, clinic_id: int, date_from: str, date_to: str) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS n FROM cases c
        WHERE c.clinic_id = ?
          AND LOWER(TRIM(COALESCE(c.case_status, ''))) = 'no show'
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
        """,
        (clinic_id, date_from, date_to),
    ).fetchone()
    return int(row["n"] or 0)


def _count_appointments_in_period(db, clinic_id: int, date_from: str, date_to: str) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS n FROM appointments a
        WHERE a.clinic_id = ?
          AND DATE(a.appointment_datetime) >= DATE(?)
          AND DATE(a.appointment_datetime) <= DATE(?)
        """,
        (clinic_id, date_from, date_to),
    ).fetchone()
    return int(row["n"] or 0)


def _admin_month_keys_in_range(date_from: str, date_to: str) -> list[tuple[int, int, str]]:
    """Calendar months from date_from through date_to (inclusive), labels like 'Apr 2026'."""
    df = date.fromisoformat(date_from)
    dt_end = date.fromisoformat(date_to)
    month_keys: list[tuple[int, int, str]] = []
    cur = df.replace(day=1)
    end_m = dt_end.replace(day=1)
    while cur <= end_m:
        month_keys.append((cur.year, cur.month, cur.strftime("%b %Y")))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    if not month_keys:
        month_keys.append((df.year, df.month, df.strftime("%b %Y")))
    return month_keys


def _admin_reporting_overview_dict(db, clinic_id: int | None, date_from: str, date_to: str) -> dict:
    """KPIs and charts for the Reporting → Overview tab (former admin dashboard)."""
    total_users = 0
    bite_cases_period = 0
    ongoing_cases = 0
    completed_cases_period = 0
    vaccination_completion_pct = 0
    animal_type_rows: list[dict] = []
    monthly_labels: list[str] = []
    staff_visible_case_filter_sql = f"""
          AND {_SQL_STAFF_CASE_NOT_REMOVED}
          AND LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')
    """
    monthly_counts: list[int] = []

    if clinic_id is not None:
        total_users = (
            db.execute(
                f"""
                SELECT COUNT(DISTINCT c.patient_id) AS n FROM cases c
                WHERE c.clinic_id = ?
                {staff_visible_case_filter_sql}
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
                """,
                (clinic_id, date_from, date_to),
            ).fetchone()["n"]
            or 0
        )

        bite_cases_period = (
            db.execute(
                f"""
                SELECT COUNT(*) AS n FROM cases c
                WHERE c.clinic_id = ?
                {staff_visible_case_filter_sql}
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
                """,
                (clinic_id, date_from, date_to),
            ).fetchone()["n"]
            or 0
        )

        ongoing_cases = _count_ongoing_cases_in_period(db, clinic_id, date_from, date_to)

        completed_cases_period = _count_completed_cases_in_period(db, clinic_id, date_from, date_to)

        vaccination_completion_pct = (
            round((completed_cases_period / bite_cases_period) * 100) if bite_cases_period else 0
        )

        bite_type_rows = db.execute(
            f"""
            SELECT
              CASE
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%' THEN 'Dogs'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%' THEN 'Cats'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'bat%' THEN 'Bats'
                ELSE COALESCE(NULLIF(TRIM(c.animal_detail), ''), 'Other')
              END AS bite_type,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
              {staff_visible_case_filter_sql}
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
            GROUP BY bite_type
            ORDER BY total DESC
            """,
            (clinic_id, date_from, date_to),
        ).fetchall()
        total_bite_cases = sum(int(row["total"] or 0) for row in bite_type_rows)
        for row in bite_type_rows:
            label = (row["bite_type"] or "Other").strip()
            if label.lower() not in ["dogs", "cats", "bats"]:
                label = label.title()
            count = int(row["total"] or 0)
            pct = round((count / total_bite_cases) * 100) if total_bite_cases else 0
            animal_type_rows.append({"label": label, "percent": pct})

        for yy, mm, label in _admin_month_keys_in_range(date_from, date_to):
            ym = f"{yy:04d}-{mm:02d}"
            cnt_row = db.execute(
                """
                SELECT COUNT(*) AS n FROM cases c
                WHERE c.clinic_id = ?
                  AND COALESCE(c.staff_removed, 0) = 0
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
                  AND strftime('%Y-%m', COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) = ?
                """,
                (clinic_id, date_from, date_to, ym),
            ).fetchone()
            monthly_labels.append(label)
            monthly_counts.append(int(cnt_row["n"] or 0))

    overview_chart_data = {
        "monthly_labels": monthly_labels,
        "monthly_counts": monthly_counts,
        "vaccination_completion_pct": vaccination_completion_pct,
    }

    return {
        "total_users": total_users,
        "bite_cases_period": bite_cases_period,
        "ongoing_cases": ongoing_cases,
        "completed_cases_period": completed_cases_period,
        "vaccination_completion_pct": vaccination_completion_pct,
        "animal_type_rows": animal_type_rows,
        "overview_chart_data": overview_chart_data,
    }


def _admin_reporting_clinic_dict(
    db, clinic_id: int | None, clinic, period: str, date_from: str, date_to: str, yearly_year: int | None
) -> dict:
    """Clinic profile + performance for the Reporting → Clinic tab."""
    if clinic is None or clinic_id is None:
        return {
            "total_patients": 0,
            "appointments_ytd": 0,
            "staff_count": 0,
            "cases_ytd": 0,
            "cases_in_period": 0,
            "appointments_in_period": 0,
            "completed_in_period": 0,
            "ongoing_in_period": 0,
            "no_show_in_period": 0,
            "case_completion_pct_period": 0,
            "risk_rows": [],
            "charts_empty": True,
            "report_year": date.today().year,
            "clinic_chart_data": {
                "visit_labels": [],
                "visit_counts": [],
                "compliance_labels": ["—"],
                "compliance_pcts": [0],
            },
        }

    total_patients = (
        db.execute(
            "SELECT COUNT(DISTINCT c.patient_id) AS n FROM cases c WHERE c.clinic_id = ? AND COALESCE(c.staff_removed, 0) = 0",
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )
    ytd_start = f"{date.today().year}-01-01"
    appointments_ytd = (
        db.execute(
            """
            SELECT COUNT(*) AS n FROM appointments a
            WHERE a.clinic_id = ? AND DATE(a.appointment_datetime) >= DATE(?)
            """,
            (clinic_id, ytd_start),
        ).fetchone()["n"]
        or 0
    )
    cases_ytd = (
        db.execute(
            """
            SELECT COUNT(*) AS n FROM cases c
            WHERE c.clinic_id = ?
              AND COALESCE(c.staff_removed, 0) = 0
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
            """,
            (clinic_id, ytd_start),
        ).fetchone()["n"]
        or 0
    )
    staff_count = (
        db.execute(
            "SELECT COUNT(*) AS n FROM clinic_personnel WHERE clinic_id = ?",
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )

    cases_in_period = _count_total_cases_in_period(db, clinic_id, date_from, date_to)
    appointments_in_period = _count_appointments_in_period(db, clinic_id, date_from, date_to)
    completed_in_period = _count_completed_cases_in_period(db, clinic_id, date_from, date_to)
    ongoing_in_period = _count_ongoing_cases_in_period(db, clinic_id, date_from, date_to)
    no_show_in_period = _count_no_show_cases_in_period(db, clinic_id, date_from, date_to)
    case_completion_pct_period = _case_completion_pct(db, clinic_id, date_from, date_to)

    risk_raw = db.execute(
        """
        SELECT
          COALESCE(NULLIF(TRIM(c.risk_level), ''), NULLIF(TRIM(c.category), ''), 'Unknown') AS risk_label,
          COUNT(*) AS total
        FROM cases c
        WHERE c.clinic_id = ?
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
        GROUP BY risk_label
        ORDER BY total DESC
        LIMIT 12
        """,
        (clinic_id, date_from, date_to),
    ).fetchall()
    total_risk = sum(int(r["total"] or 0) for r in risk_raw)
    risk_rows: list[dict] = []
    for r in risk_raw:
        cnt = int(r["total"] or 0)
        pct = round((cnt / total_risk) * 100) if total_risk else 0
        risk_rows.append(
            {
                "label": (r["risk_label"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": pct,
            }
        )

    visit_labels: list[str] = []
    visit_counts: list[int] = []
    for yy, mm, label in _admin_month_keys_in_range(date_from, date_to):
        ym = f"{yy:04d}-{mm:02d}"
        cnt_row = db.execute(
            """
            SELECT COUNT(*) AS n FROM cases c
            WHERE c.clinic_id = ?
              AND COALESCE(c.staff_removed, 0) = 0
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
              AND strftime('%Y-%m', COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) = ?
            """,
            (clinic_id, date_from, date_to, ym),
        ).fetchone()
        visit_labels.append(label)
        visit_counts.append(int(cnt_row["n"] or 0))

    if period == "yearly" and yearly_year is not None:
        compliance_labels = ["Q1", "Q2", "Q3", "Q4"]
        compliance_pcts = []
        y = yearly_year
        for q in range(1, 5):
            start_m = {1: 1, 2: 4, 3: 7, 4: 10}[q]
            end_m = {1: 3, 2: 6, 3: 9, 4: 12}[q]
            q_start = date(y, start_m, 1).isoformat()
            last_d = calendar.monthrange(y, end_m)[1]
            q_end = date(y, end_m, last_d).isoformat()
            compliance_pcts.append(_case_completion_pct(db, clinic_id, q_start, q_end))
    else:
        compliance_labels = ["Selected period"]
        compliance_pcts = [_case_completion_pct(db, clinic_id, date_from, date_to)]

    clinic_chart_data = {
        "visit_labels": visit_labels,
        "visit_counts": visit_counts,
        "compliance_labels": compliance_labels,
        "compliance_pcts": compliance_pcts,
    }
    charts_empty = sum(visit_counts) == 0

    return {
        "total_patients": total_patients,
        "staff_count": staff_count,
        "cases_ytd": cases_ytd,
        "appointments_ytd": appointments_ytd,
        "cases_in_period": cases_in_period,
        "appointments_in_period": appointments_in_period,
        "completed_in_period": completed_in_period,
        "ongoing_in_period": ongoing_in_period,
        "no_show_in_period": no_show_in_period,
        "case_completion_pct_period": case_completion_pct_period,
        "risk_rows": risk_rows,
        "charts_empty": charts_empty,
        "report_year": date.today().year,
        "clinic_chart_data": clinic_chart_data,
    }


_INSIGHTS_AGE_GROUP_ORDER = (
    "0-4",
    "5-9",
    "10-14",
    "15-19",
    "20-29",
    "30-39",
    "40-49",
    "50-59",
    "60+",
    "Unknown",
)


def _admin_insights_filters_from_request(args) -> dict[str, str]:
    """Parse Program insights filter query params (namespaced to avoid clashes)."""
    return {
        "barangay": (args.get("insights_barangay") or "").strip(),
        "animal": (args.get("insights_animal") or "").strip(),
        "bite_type": (args.get("insights_bite") or "").strip(),
        "gender": (args.get("insights_gender") or "").strip(),
        "age_group": (args.get("insights_age") or "").strip(),
    }


def _insights_filters_query_string(filters: dict[str, str] | None) -> str:
    """Append fragment for preserving insights filters in URLs (starts with & when non-empty)."""
    if not filters:
        return ""
    q: dict[str, str] = {}
    if filters.get("barangay"):
        q["insights_barangay"] = filters["barangay"]
    if filters.get("animal"):
        q["insights_animal"] = filters["animal"]
    if filters.get("bite_type"):
        q["insights_bite"] = filters["bite_type"]
    if filters.get("gender"):
        q["insights_gender"] = filters["gender"]
    if filters.get("age_group"):
        q["insights_age"] = filters["age_group"]
    if not q:
        return ""
    return "&" + urlencode(q)


def _insights_sql_age_val() -> str:
    """SQLite expression: victim age at case date (int or NULL)."""
    return """
        CASE
          WHEN p.date_of_birth IS NOT NULL AND LENGTH(TRIM(p.date_of_birth)) >= 10
            THEN CAST(
              (julianday(DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)))
               - julianday(DATE(SUBSTR(TRIM(p.date_of_birth), 1, 10))))
              / 365.25 AS INTEGER
            )
          WHEN p.age IS NOT NULL THEN CAST(p.age AS INTEGER)
          ELSE NULL
        END
    """


def _insights_sql_age_group() -> str:
    """SQLite expression: age bucket label."""
    av = _insights_sql_age_val()
    return f"""
        CASE
          WHEN ({av}) IS NULL THEN 'Unknown'
          WHEN ({av}) BETWEEN 0 AND 4 THEN '0-4'
          WHEN ({av}) BETWEEN 5 AND 9 THEN '5-9'
          WHEN ({av}) BETWEEN 10 AND 14 THEN '10-14'
          WHEN ({av}) BETWEEN 15 AND 19 THEN '15-19'
          WHEN ({av}) BETWEEN 20 AND 29 THEN '20-29'
          WHEN ({av}) BETWEEN 30 AND 39 THEN '30-39'
          WHEN ({av}) BETWEEN 40 AND 49 THEN '40-49'
          WHEN ({av}) BETWEEN 50 AND 59 THEN '50-59'
          WHEN ({av}) >= 60 THEN '60+'
          ELSE 'Unknown'
        END
    """


def _insights_sql_animal_bucket() -> str:
    return """
        CASE
          WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%' THEN 'Dogs'
          WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%' THEN 'Cats'
          WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'bat%' THEN 'Bats'
          ELSE COALESCE(NULLIF(TRIM(c.animal_detail), ''), 'Other')
        END
    """


def _insights_sql_barangay_seg() -> str:
    """Resolved barangay: dedicated column, else legacy first segment of address."""
    return """
        TRIM(
          COALESCE(
            NULLIF(TRIM(COALESCE(p.barangay, '')), ''),
            CASE
              WHEN TRIM(COALESCE(p.address, '')) = '' THEN ''
              ELSE TRIM(
                CASE
                  WHEN INSTR(TRIM(p.address), ',') > 0
                  THEN SUBSTR(TRIM(p.address), 1, INSTR(TRIM(p.address), ',') - 1)
                  ELSE TRIM(p.address)
                END
              )
            END
          )
        )
    """


def _insights_filter_clause_and_params(filters: dict[str, str] | None) -> tuple[str, list[object]]:
    """Extra JOIN/WHERE fragments for insights (patients joined). All parameterized."""
    if not filters:
        return "", []
    parts: list[str] = []
    params: list[object] = []
    if filters.get("gender"):
        parts.append("TRIM(COALESCE(p.gender, '')) = ?")
        params.append(filters["gender"])
    if filters.get("bite_type"):
        parts.append("TRIM(COALESCE(c.type_of_exposure, '')) = ?")
        params.append(filters["bite_type"])
    if filters.get("animal"):
        parts.append(f"({_insights_sql_animal_bucket()}) = ?")
        params.append(filters["animal"])
    if filters.get("barangay"):
        parts.append(f"({_insights_sql_barangay_seg()}) = ?")
        params.append(filters["barangay"])
    if filters.get("age_group"):
        ag = filters["age_group"]
        ag_expr = _insights_sql_age_group()
        if ag == "Unknown":
            parts.append(f"({ag_expr}) = 'Unknown'")
        elif ag in _INSIGHTS_AGE_GROUP_ORDER:
            parts.append(f"({ag_expr}) = ?")
            params.append(ag)
    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params


def _insights_base_from_where() -> str:
    return """
        FROM cases c
        INNER JOIN patients p ON p.id = c.patient_id
        WHERE c.clinic_id = ?
          AND COALESCE(c.staff_removed, 0) = 0
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
    """


def _insights_vaccination_status_bucket(doses_completed: int, expected_doses: int) -> str:
    if expected_doses <= 0:
        return "Unknown"
    if doses_completed <= 0:
        return "Not started"
    if doses_completed >= expected_doses:
        return "Completed"
    return "In progress"


def _insights_build_vaccination_status_counts(
    db, clinic_id: int, date_from: str, date_to: str, filters: dict[str, str] | None
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """
    Returns (summary_rows with percent, raw case rows for optional export).
    """
    fc, fparams = _insights_filter_clause_and_params(filters)
    base = _insights_base_from_where()
    rows = db.execute(
        f"""
        SELECT c.id,
               COALESCE(NULLIF(TRIM(c.risk_level), ''), NULLIF(TRIM(c.category), ''), 'Category II') AS rk
        {base}
        {fc}
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    if not rows:
        return (
            [
                {"label": "Not started", "count": 0, "percent": 0},
                {"label": "In progress", "count": 0, "percent": 0},
                {"label": "Completed", "count": 0, "percent": 0},
                {"label": "Unknown", "count": 0, "percent": 0},
            ],
            [],
        )
    case_ids = [int(r["id"]) for r in rows]
    rk_by_case = {int(r["id"]): (r["rk"] or "") for r in rows}
    placeholders = ",".join(["?"] * len(case_ids))
    dose_rows = db.execute(
        f"""
        SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
        FROM vaccination_card_doses
        WHERE case_id IN ({placeholders})
        ORDER BY case_id, record_type, day_number
        """,
        case_ids,
    ).fetchall()
    by_case: dict[int, dict[str, dict[int, dict]]] = {}
    for cid in case_ids:
        by_case[cid] = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}
    for dr in dose_rows:
        cid = int(dr["case_id"])
        if cid not in by_case:
            continue
        rt = dr["record_type"]
        dn = dr["day_number"]
        if rt in by_case[cid] and dn is not None:
            by_case[cid][rt][int(dn)] = dict(dr)
    counts: dict[str, int] = {"Not started": 0, "In progress": 0, "Completed": 0, "Unknown": 0}
    raw_cases: list[dict[str, object]] = []
    for cid in case_ids:
        rk = rk_by_case.get(cid, "")
        st = _compute_vaccination_status_for_case(by_case[cid], rk)
        dc = int(st.get("doses_completed") or 0)
        ed = int(st.get("expected_doses") or 0)
        bucket = _insights_vaccination_status_bucket(dc, ed)
        counts[bucket] = counts.get(bucket, 0) + 1
        raw_cases.append(
            {
                "case_id": cid,
                "vaccination_status": bucket,
                "doses_completed": dc,
                "expected_doses": ed,
                "risk_category": rk,
            }
        )
    total = len(case_ids)
    order = ["Not started", "In progress", "Completed", "Unknown"]
    summary = []
    for lab in order:
        cnt = counts.get(lab, 0)
        pct = round((cnt / total) * 100) if total else 0
        summary.append({"label": lab, "count": cnt, "percent": pct})
    return summary, raw_cases


def _admin_reporting_insights_dict(
    db, clinic_id: int, date_from: str, date_to: str, filters: dict[str, str] | None = None
) -> dict:
    """Charts and tables for the Reporting → Insights tab (program oversight)."""
    fc, fparams = _insights_filter_clause_and_params(filters)

    bite_cases = (
        db.execute(
            f"""
            SELECT COUNT(*) AS n
            {_insights_base_from_where()}
            {fc}
            """,
            (clinic_id, date_from, date_to, *fparams),
        ).fetchone()["n"]
        or 0
    )

    completed_cases_kpi = (
        db.execute(
            f"""
            SELECT COUNT(*) AS n
            {_insights_base_from_where()}
              AND LOWER(COALESCE(c.case_status, '')) = 'completed'
            {fc}
            """,
            (clinic_id, date_from, date_to, *fparams),
        ).fetchone()["n"]
        or 0
    )

    ongoing_cases = (
        db.execute(
            f"""
            SELECT COUNT(*) AS n
            {_insights_base_from_where()}
              AND LOWER(COALESCE(c.case_status, 'pending')) = 'pending'
            {fc}
            """,
            (clinic_id, date_from, date_to, *fparams),
        ).fetchone()["n"]
        or 0
    )

    staff_count = (
        db.execute(
            "SELECT COUNT(*) AS n FROM clinic_personnel WHERE clinic_id = ?",
            (clinic_id,),
        ).fetchone()["n"]
        or 0
    )

    kpi = {
        "bite_cases": bite_cases,
        "completed_cases": completed_cases_kpi,
        "ongoing_cases": ongoing_cases,
        "staff_count": staff_count,
    }

    month_keys = _admin_month_keys_in_range(date_from, date_to)
    chart_labels: list[str] = []
    chart_cases: list[int] = []
    chart_vax: list[int] = []
    for yy, mm, lab in month_keys:
        ym = f"{yy:04d}-{mm:02d}"
        chart_labels.append(lab)
        cn = (
            db.execute(
                f"""
                SELECT COUNT(*) AS n
                {_insights_base_from_where()}
                  AND strftime('%Y-%m', COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) = ?
                {fc}
                """,
                (clinic_id, date_from, date_to, ym, *fparams),
            ).fetchone()["n"]
            or 0
        )
        vn = (
            db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM vaccination_card_doses vcd
                JOIN cases c ON c.id = vcd.case_id
                INNER JOIN patients p ON p.id = c.patient_id
                WHERE c.clinic_id = ?
                  AND COALESCE(c.staff_removed, 0) = 0
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
                  AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
                  AND NULLIF(TRIM(vcd.dose_date), '') IS NOT NULL
                  AND strftime(
                    '%Y-%m',
                    DATE(SUBSTR(TRIM(vcd.dose_date), 1, 10))
                  ) = ?
                {fc}
                """,
                (clinic_id, date_from, date_to, ym, *fparams),
            ).fetchone()["n"]
            or 0
        )
        chart_cases.append(int(cn))
        chart_vax.append(int(vn))

    monthly_trends_table: list[dict[str, object]] = []
    for i, lab in enumerate(chart_labels):
        monthly_trends_table.append(
            {
                "month_label": lab,
                "bite_cases": chart_cases[i],
                "vaccinations": chart_vax[i],
            }
        )

    addr_rows = db.execute(
        f"""
        SELECT ({_insights_sql_barangay_seg()}) AS addr
        {_insights_base_from_where()}
          AND TRIM(COALESCE(({_insights_sql_barangay_seg()}), '')) <> ''
        {fc}
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    barangay_counts: dict[str, int] = {}
    for r in addr_rows:
        raw = (r["addr"] or "").strip()
        if not raw:
            continue
        seg = raw.strip()
        if len(seg) > 60:
            seg = seg[:57] + "..."
        barangay_counts[seg] = barangay_counts.get(seg, 0) + 1
    barangay_sorted = sorted(barangay_counts.items(), key=lambda x: (-x[1], x[0].lower()))
    barangay_rows = barangay_sorted[:12]
    max_b = barangay_rows[0][1] if barangay_rows else 1
    total_addr = sum(c for _, c in barangay_sorted) or 1
    barangay_table_rows: list[dict[str, object]] = []
    for name, cnt in barangay_sorted:
        barangay_table_rows.append(
            {
                "barangay": name,
                "count": cnt,
                "percent": round((cnt / total_addr) * 100),
            }
        )

    ag_sql = _insights_sql_age_group()
    age_raw = db.execute(
        f"""
        SELECT ({ag_sql}) AS ag, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY 1
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    age_map = {r["ag"]: int(r["n"] or 0) for r in age_raw}
    total_cases_demo = bite_cases or 1
    age_distribution_rows: list[dict[str, object]] = []
    for lab in _INSIGHTS_AGE_GROUP_ORDER:
        cnt = age_map.get(lab, 0)
        age_distribution_rows.append(
            {
                "label": lab,
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    gender_raw = db.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(p.gender), ''), 'Unknown') AS g, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY g
        ORDER BY n DESC, g ASC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    gender_distribution_rows: list[dict[str, object]] = []
    for r in gender_raw:
        cnt = int(r["n"] or 0)
        gender_distribution_rows.append(
            {
                "label": (r["g"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    bite_type_raw = db.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(c.type_of_exposure), ''), 'Unknown') AS bt, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY bt
        ORDER BY n DESC, bt ASC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    bite_type_rows: list[dict[str, object]] = []
    for r in bite_type_raw:
        cnt = int(r["n"] or 0)
        bite_type_rows.append(
            {
                "label": (r["bt"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    ab_sql = _insights_sql_animal_bucket()
    animal_raw = db.execute(
        f"""
        SELECT ({ab_sql}) AS animal, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY animal
        ORDER BY n DESC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    animal_type_rows: list[dict[str, object]] = []
    for r in animal_raw:
        cnt = int(r["n"] or 0)
        label = (r["animal"] or "Other").strip()
        if label.lower() not in ["dogs", "cats", "bats"]:
            label = label.title()
        animal_type_rows.append(
            {
                "label": label,
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    severity_raw = db.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(c.risk_level), ''), NULLIF(TRIM(c.category), ''), 'Unknown') AS sev, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY sev
        ORDER BY n DESC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    severity_rows: list[dict[str, object]] = []
    for r in severity_raw:
        cnt = int(r["n"] or 0)
        severity_rows.append(
            {
                "label": (r["sev"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    who_category_raw = db.execute(
        f"""
        SELECT
          COALESCE(
            NULLIF(TRIM(c.who_category_final), ''),
            NULLIF(TRIM(c.who_category_auto), ''),
            NULLIF(TRIM(c.risk_level), ''),
            NULLIF(TRIM(c.category), ''),
            'Unknown'
          ) AS wc,
          COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY wc
        ORDER BY n DESC, wc ASC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    who_category_rows: list[dict[str, object]] = []
    for r in who_category_raw:
        cnt = int(r["n"] or 0)
        who_category_rows.append(
            {
                "label": (r["wc"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    case_status_raw = db.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(c.case_status), ''), 'Unknown') AS st, COUNT(*) AS n
        {_insights_base_from_where()}
        {fc}
        GROUP BY st
        ORDER BY n DESC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    case_status_rows: list[dict[str, object]] = []
    for r in case_status_raw:
        cnt = int(r["n"] or 0)
        case_status_rows.append(
            {
                "label": (r["st"] or "Unknown").strip() or "Unknown",
                "count": cnt,
                "percent": round((cnt / total_cases_demo) * 100) if bite_cases else 0,
            }
        )

    vaccination_status_rows, vaccination_case_rows = _insights_build_vaccination_status_counts(
        db, clinic_id, date_from, date_to, filters
    )

    bar_opt = db.execute(
        f"""
        SELECT DISTINCT ({_insights_sql_barangay_seg()}) AS b
        {_insights_base_from_where()}
          AND TRIM(COALESCE(({_insights_sql_barangay_seg()}), '')) <> ''
        {fc}
        ORDER BY b ASC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    insights_barangay_options = [(r["b"] or "").strip() for r in bar_opt if (r["b"] or "").strip()]

    bite_opt = db.execute(
        f"""
        SELECT DISTINCT TRIM(c.type_of_exposure) AS bt
        {_insights_base_from_where()}
          AND TRIM(COALESCE(c.type_of_exposure, '')) <> ''
        {fc}
        ORDER BY bt ASC
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    insights_bite_options = [(r["bt"] or "").strip() for r in bite_opt if (r["bt"] or "").strip()]

    g_opt = db.execute(
        f"""
        SELECT DISTINCT TRIM(p.gender) AS g
        {_insights_base_from_where()}
          AND TRIM(COALESCE(p.gender, '')) <> ''
        ORDER BY g ASC
        """,
        (clinic_id, date_from, date_to),
    ).fetchall()
    insights_gender_options = sorted(
        {(r["g"] or "").strip() for r in g_opt if (r["g"] or "").strip()},
        key=lambda s: s.lower(),
    )

    prio_rows = db.execute(
        f"""
        SELECT
          c.id AS case_id,
          COALESCE(c.case_status, '') AS case_status,
          COALESCE(NULLIF(c.created_at, ''), c.exposure_date) AS reported_at
        FROM cases c
        WHERE c.clinic_id = ?
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
          AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
          AND ({_case_is_high_risk_sql()})
        {fc}
        ORDER BY datetime(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) DESC
        LIMIT 8
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    priority_cases: list[dict] = []
    for pr in prio_rows:
        rep = pr["reported_at"] or ""
        if rep:
            try:
                rep = datetime.fromisoformat(str(rep).replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except ValueError:
                pass
        cs = (pr["case_status"] or "").strip().lower()
        if cs == "completed":
            st = "Completed"
        elif cs == "pending":
            st = "Ongoing"
        else:
            st = "Urgent"
        priority_cases.append(
            {"case_id": pr["case_id"], "status": st, "reported_at": rep}
        )

    vrec_rows = db.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(vcd.given_by), ''), 'Staff') AS staff_name
        FROM vaccination_card_doses vcd
        JOIN cases c ON c.id = vcd.case_id
        INNER JOIN patients p ON p.id = c.patient_id
        WHERE c.clinic_id = ?
          AND NULLIF(TRIM(vcd.dose_date), '') IS NOT NULL
          AND DATE(SUBSTR(TRIM(vcd.dose_date), 1, 10)) >= DATE(?)
          AND DATE(SUBSTR(TRIM(vcd.dose_date), 1, 10)) <= DATE(?)
        {fc}
        """,
        (clinic_id, date_from, date_to, *fparams),
    ).fetchall()
    sp_counts: dict[str, int] = {}
    for vr in vrec_rows:
        nm = (vr["staff_name"] or "Staff").strip()
        sp_counts[nm] = sp_counts.get(nm, 0) + 1
    staff_performance = [
        {"name": k, "count": v} for k, v in sorted(sp_counts.items(), key=lambda x: (-x[1], x[0].lower()))
    ][:8]

    chart_compare = {"labels": chart_labels, "cases": chart_cases, "vaccinations": chart_vax}

    return {
        "kpi": kpi,
        "chart_compare": chart_compare,
        "barangay_rows": barangay_rows,
        "barangay_max": max_b,
        "barangay_table_rows": barangay_table_rows,
        "monthly_trends_table": monthly_trends_table,
        "age_distribution_rows": age_distribution_rows,
        "gender_distribution_rows": gender_distribution_rows,
        "bite_type_rows": bite_type_rows,
        "animal_type_rows_insights": animal_type_rows,
        "severity_rows": severity_rows,
        "who_category_rows": who_category_rows,
        "case_status_rows": case_status_rows,
        "vaccination_status_rows": vaccination_status_rows,
        "vaccination_case_rows": vaccination_case_rows,
        "insights_barangay_options": insights_barangay_options,
        "insights_bite_options": insights_bite_options,
        "insights_animal_options": ["Dogs", "Cats", "Bats", "Other"],
        "insights_gender_options": insights_gender_options,
        "insights_age_options": list(_INSIGHTS_AGE_GROUP_ORDER),
        "insights_filters": filters or {},
        "priority_cases": priority_cases,
        "staff_performance": staff_performance,
    }


_INSIGHTS_EXPORT_FILENAMES = {
    "victim_age": "victim_age_demographics.csv",
    "victim_gender": "victim_gender_distribution.csv",
    "bite_type": "bite_type_report.csv",
    "animal_type": "animal_type_report.csv",
    "barangay": "barangay_case_distribution.csv",
    "severity": "case_severity_distribution.csv",
    "who_category": "who_category_distribution.csv",
    "vaccination_status": "vaccination_status_summary.csv",
    "case_status": "case_status_distribution.csv",
    "monthly_trends": "case_trends_monthly.csv",
}


def _admin_insights_export_csv_body(
    dataset: str, data: dict
) -> tuple[str, str] | tuple[None, None]:
    """Build CSV body and filename for an insights export dataset."""
    if dataset not in _INSIGHTS_EXPORT_FILENAMES:
        return None, None
    fn = _INSIGHTS_EXPORT_FILENAMES[dataset]
    buf = io.StringIO()
    w = csv.writer(buf)
    if dataset == "victim_age":
        w.writerow(["age_group", "case_count", "percent_of_cases"])
        for r in data.get("age_distribution_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "victim_gender":
        w.writerow(["gender", "case_count", "percent_of_cases"])
        for r in data.get("gender_distribution_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "bite_type":
        w.writerow(["bite_type", "case_count", "percent_of_cases"])
        for r in data.get("bite_type_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "animal_type":
        w.writerow(["animal_type", "case_count", "percent_of_cases"])
        for r in data.get("animal_type_rows_insights") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "barangay":
        w.writerow(["barangay", "case_count", "percent_of_cases_with_address"])
        for r in data.get("barangay_table_rows") or []:
            w.writerow([r.get("barangay"), r.get("count"), r.get("percent")])
    elif dataset == "severity":
        w.writerow(["severity_label", "case_count", "percent_of_cases"])
        for r in data.get("severity_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "who_category":
        w.writerow(["who_category", "case_count", "percent_of_cases"])
        for r in data.get("who_category_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "case_status":
        w.writerow(["case_status", "case_count", "percent_of_cases"])
        for r in data.get("case_status_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
    elif dataset == "vaccination_status":
        w.writerow(["vaccination_status_bucket", "case_count", "percent_of_cases"])
        for r in data.get("vaccination_status_rows") or []:
            w.writerow([r.get("label"), r.get("count"), r.get("percent")])
        w.writerow([])
        w.writerow(["case_id", "vaccination_status", "doses_completed", "expected_doses", "risk_category"])
        for r in data.get("vaccination_case_rows") or []:
            w.writerow(
                [
                    r.get("case_id"),
                    r.get("vaccination_status"),
                    r.get("doses_completed"),
                    r.get("expected_doses"),
                    r.get("risk_category"),
                ]
            )
    elif dataset == "monthly_trends":
        w.writerow(["month_label", "bite_cases", "vaccinations_administered"])
        for r in data.get("monthly_trends_table") or []:
            w.writerow([r.get("month_label"), r.get("bite_cases"), r.get("vaccinations")])
    else:
        return None, None
    return buf.getvalue(), fn


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


_NAME_LETTER_PERIOD_RE = re.compile(r"^[A-Za-z .'-]+$")


def _is_letters_period_only(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    return bool(_NAME_LETTER_PERIOD_RE.fullmatch(raw))


def _is_numeric_only(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    return raw.isdigit()


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


def _pre_screening_risk_reasons(
    type_of_exposure: str | None,
    affected_area: str | None,
    wound_description: str | None,
    bleeding_type: str | None,
    animal_status: str | None,
) -> list[dict[str, object]]:
    """Explainable rule hits for DOH-aligned pre-screening risk category."""
    type_of_exposure = (type_of_exposure or "").strip()
    wound_description = (wound_description or "").strip()
    bleeding_type = (bleeding_type or "").strip()
    animal_status = (animal_status or "").strip()
    affected_area = (affected_area or "").strip()

    high_risk_exposures = {"Bite", "Contamination of Mucous Membrane"}
    high_risk_areas = {"Head/Face", "Neck"}
    severe_wounds = {"Punctured", "Lacerated", "Avulsed"}
    high_risk_animal_status = {"Sick", "Died", "Lost"}

    area_tokens = _affected_area_tokens(affected_area)
    reasons: list[dict[str, object]] = []
    if type_of_exposure in high_risk_exposures:
        reasons.append({"code": "HIGH_RISK_EXPOSURE", "label": f"High-risk exposure: {type_of_exposure}"})
    if any(t in high_risk_areas for t in area_tokens):
        reasons.append({"code": "HIGH_RISK_AREA", "label": "High-risk anatomical area (head/face/neck)"})
    if bleeding_type in {"Spontaneous", "Both spontaneous and induced"}:
        reasons.append({"code": "SPONTANEOUS_BLEEDING", "label": f"Bleeding type: {bleeding_type}"})
    if wound_description in severe_wounds:
        reasons.append({"code": "SEVERE_WOUND", "label": f"Severe wound: {wound_description}"})
    if animal_status in high_risk_animal_status:
        reasons.append({"code": "HIGH_RISK_ANIMAL_STATUS", "label": f"High-risk animal status: {animal_status}"})

    if reasons:
        return reasons

    # Category II triggers (when no Category III triggers were hit)
    if type_of_exposure in {"Scratch", "Non-Bite"}:
        reasons.append({"code": "MODERATE_EXPOSURE", "label": f"Moderate exposure: {type_of_exposure}"})
    if wound_description == "Abrasion":
        reasons.append({"code": "MINOR_WOUND", "label": "Minor wound: Abrasion"})
    if bleeding_type == "Induced":
        reasons.append({"code": "INDUCED_BLEEDING", "label": "Induced bleeding"})

    if reasons:
        return reasons

    return [{"code": "LOW_RISK_FALLBACK", "label": "No high-risk features detected"}]


def _bleeding_type_from_flags(spontaneous: str | None, induced: str | None) -> str:
    spontaneous = (spontaneous or "").strip()
    induced = (induced or "").strip()
    if spontaneous == "Yes" and induced == "Yes":
        return "Both spontaneous and induced"
    if spontaneous == "Yes":
        return "Spontaneous"
    if induced == "Yes":
        return "Induced"
    return "None"


def _prescreening_parse_validate_derive(
    form, *, require_demographics: bool = True
) -> tuple[list[str], dict | None]:
    """Parse and validate POST data from pre_screening_form.html; add derived fields for DB inserts."""
    form_type = (form.get("form_type") or "case").strip()
    appointment_slot_id_raw = (form.get("appointment_slot_id") or "").strip()
    appointment_datetime_form = (form.get("appointment_datetime") or "").strip()
    form_clinic_id = (form.get("clinic_id") or "").strip()
    type_of_exposure = (form.get("type_of_exposure") or "").strip()
    exposure_date = (form.get("exposure_date") or "").strip()
    exposure_time = (form.get("exposure_time") or "").strip()
    wound_description = (form.get("wound_description") or "").strip()
    spontaneous_bleeding = (form.get("spontaneous_bleeding") or "").strip()
    induced_bleeding = (form.get("induced_bleeding") or "").strip()
    patient_prev_immunization = (form.get("patient_prev_immunization") or "").strip()
    prev_vaccine_date = (form.get("prev_vaccine_date") or "").strip() or None
    animal_type = (form.get("animal_type") or "").strip()
    other_animal = (form.get("other_animal") or "").strip()
    animal_status = (form.get("animal_status") or "").strip()
    animal_vaccination = (form.get("animal_vaccination") or "").strip()
    local_treatment = (form.get("local_treatment") or "").strip()
    other_treatment = (form.get("other_treatment") or "").strip()
    place_of_exposure = (form.get("place_of_exposure") or "").strip()
    place_of_exposure_other = (form.get("place_of_exposure_other") or "").strip()
    affected_area_values = [a.strip() for a in form.getlist("affected_area") if a.strip()]
    affected_area_other = (form.get("affected_area_other") or "").strip()
    tetanus_immunization = (form.get("tetanus_immunization") or "").strip()
    tetanus_date = (form.get("tetanus_date") or "").strip() or None
    hrtig_immunization = (form.get("hrtig_immunization") or "").strip()
    hrtig_date = (form.get("hrtig_date") or "").strip() or None

    victim_first_name = (form.get("victim_first_name") or "").strip()
    victim_last_name = (form.get("victim_last_name") or "").strip()
    victim_middle_initial = (form.get("victim_middle_initial") or "").strip()
    date_of_birth = (form.get("date_of_birth") or "").strip() or None
    gender = (form.get("gender") or "").strip() or None
    age = (form.get("age") or "").strip()
    barangay = (form.get("barangay") or "").strip()
    victim_address = (form.get("victim_address") or "").strip()
    contact_number = (form.get("contact_number") or "").strip()
    email_address = (form.get("email_address") or "").strip().lower()
    relationship_to_user = (form.get("relationship_to_user") or "Self").strip()

    victim_first_name = normalize_name_case(victim_first_name)
    victim_last_name = normalize_name_case(victim_last_name)
    victim_middle_initial = normalize_name_case(victim_middle_initial)
    barangay = normalize_name_case(barangay)
    victim_address = normalize_name_case(victim_address)
    place_of_exposure_other = normalize_name_case(place_of_exposure_other)
    affected_area_other = normalize_name_case(affected_area_other)
    other_animal = normalize_name_case(other_animal)
    other_treatment = normalize_name_case(other_treatment)
    relationship_to_user = normalize_name_case(relationship_to_user)

    combined_address = None
    if barangay and victim_address:
        combined_address = f"{barangay}, {victim_address}"
    elif barangay:
        combined_address = barangay
    elif victim_address:
        combined_address = victim_address

    first_name = victim_first_name or None
    last_name = victim_last_name or None

    errors: list[str] = []
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
    if require_demographics:
        if not date_of_birth:
            errors.append("Birthday is required.")
        else:
            try:
                _dob_date = date.fromisoformat(date_of_birth[:10])
                if _dob_date > date.today():
                    errors.append("Birthday cannot be in the future.")
            except ValueError:
                errors.append("Birthday is invalid.")
        if not gender:
            errors.append("Gender is required.")
        elif gender not in {"Male", "Female"}:
            errors.append("Gender must be Male or Female.")

    if not _is_letters_period_only(victim_first_name):
        errors.append("First name must contain letters, spaces, apostrophes, hyphens, and periods only.")
    if not _is_letters_period_only(victim_last_name):
        errors.append("Last name must contain letters, spaces, apostrophes, hyphens, and periods only.")
    if not _is_numeric_only(contact_number):
        errors.append("Contact number must contain numbers only.")

    if errors:
        return errors, None

    age_value = _age_from_iso_date(date_of_birth) if date_of_birth else None

    animal_detail = animal_type
    if other_animal and animal_type == "Others":
        animal_detail = f"{animal_type}: {other_animal}"

    final_place_of_exposure = place_of_exposure
    if place_of_exposure == "Other" and place_of_exposure_other:
        final_place_of_exposure = f"Other: {place_of_exposure_other}"

    canonical_area_parts: list[str] = []
    for av in affected_area_values:
        if av == "Other":
            continue
        canonical_area_parts.append(av)
    if has_other_area and affected_area_other:
        canonical_area_parts.append(f"Other: {affected_area_other}")
    final_affected_area = ", ".join(canonical_area_parts)

    final_local_treatment = local_treatment
    if other_treatment and local_treatment == "Others":
        final_local_treatment = f"{local_treatment}: {other_treatment}"

    if spontaneous_bleeding == "Yes" and induced_bleeding == "Yes":
        bleeding_type = "Both spontaneous and induced"
    elif spontaneous_bleeding == "Yes":
        bleeding_type = "Spontaneous"
    elif induced_bleeding == "Yes":
        bleeding_type = "Induced"
    else:
        bleeding_type = "None"

    risk_level = classify_pre_screening_risk(
        type_of_exposure=type_of_exposure,
        affected_area=final_affected_area,
        wound_description=wound_description,
        bleeding_type=bleeding_type,
        animal_status=animal_status,
        animal_vaccination=animal_vaccination,
        patient_prev_immunization=patient_prev_immunization,
    )

    payload = {
        "form_type": form_type,
        "appointment_slot_id_raw": appointment_slot_id_raw,
        "appointment_datetime_form": appointment_datetime_form,
        "form_clinic_id": form_clinic_id,
        "type_of_exposure": type_of_exposure,
        "exposure_date": exposure_date,
        "exposure_time": exposure_time,
        "wound_description": wound_description,
        "spontaneous_bleeding": spontaneous_bleeding,
        "induced_bleeding": induced_bleeding,
        "patient_prev_immunization": patient_prev_immunization,
        "prev_vaccine_date": prev_vaccine_date,
        "animal_type": animal_type,
        "other_animal": other_animal,
        "animal_status": animal_status,
        "animal_vaccination": animal_vaccination,
        "local_treatment": local_treatment,
        "other_treatment": other_treatment,
        "place_of_exposure": place_of_exposure,
        "place_of_exposure_other": place_of_exposure_other,
        "affected_area_values": affected_area_values,
        "affected_area_other": affected_area_other,
        "tetanus_immunization": tetanus_immunization,
        "tetanus_date": tetanus_date,
        "hrtig_immunization": hrtig_immunization,
        "hrtig_date": hrtig_date,
        "victim_first_name": victim_first_name,
        "victim_last_name": victim_last_name,
        "victim_middle_initial": victim_middle_initial,
        "date_of_birth": date_of_birth,
        "gender": gender,
        "age": str(age_value) if age_value is not None else "",
        "barangay": barangay,
        "victim_address": victim_address,
        "contact_number": contact_number,
        "email_address": email_address,
        "relationship_to_user": relationship_to_user,
        # Backward compatibility only; DB writes should use split barangay + address fields.
        "combined_address": combined_address,
        "first_name": first_name,
        "last_name": last_name,
        "animal_detail": animal_detail,
        "final_place_of_exposure": final_place_of_exposure,
        "final_affected_area": final_affected_area,
        "final_local_treatment": final_local_treatment,
        "bleeding_type": bleeding_type,
        "risk_level": risk_level,
        "has_other_area": has_other_area,
    }
    return [], payload


def _patient_defaults_from_prescreening_form(form) -> dict:
    """Build a patient-shaped dict for repopulating pre_screening_form.html after validation errors."""
    barangay = (form.get("barangay") or "").strip()
    victim_address = (form.get("victim_address") or "").strip()
    return {
        "first_name": (form.get("victim_first_name") or "").strip(),
        "last_name": (form.get("victim_last_name") or "").strip(),
        "date_of_birth": (form.get("date_of_birth") or "").strip(),
        "gender": (form.get("gender") or "").strip(),
        "age": (form.get("age") or "").strip(),
        "phone_number": (form.get("contact_number") or "").strip(),
        "email": (form.get("email_address") or "").strip(),
        "barangay": barangay,
        "address": victim_address,
    }


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


DEFAULT_CLINIC_OPERATING_HOURS: dict[str, object] = {
    "mon_sat_open": "08:00",
    "mon_sat_close": "22:00",
    "sunday_open": "08:00",
    "sunday_close": "18:00",
    "lunch_start": "12:00",
    "lunch_end": "13:00",
    "dinner_start": "18:30",
    "dinner_end": "19:30",
    "slot_interval_minutes": 45,
    "horizon_days": 60,
}


def parse_clinic_operating_hours(raw: object | None) -> dict[str, object]:
    base: dict[str, object] = dict(DEFAULT_CLINIC_OPERATING_HOURS)
    if not raw or not str(raw).strip():
        return base
    try:
        data = json.loads(str(raw))
        if isinstance(data, dict):
            for k in DEFAULT_CLINIC_OPERATING_HOURS:
                if k in data and data[k] is not None:
                    base[k] = data[k]
        return base
    except (json.JSONDecodeError, TypeError, ValueError):
        return base


def serialize_clinic_operating_hours(data: dict[str, object]) -> str:
    clean = {k: data.get(k, DEFAULT_CLINIC_OPERATING_HOURS[k]) for k in DEFAULT_CLINIC_OPERATING_HOURS}
    return json.dumps(clean, separators=(",", ":"))


def _parse_hhmm_local(s: object) -> datetime | None:
    raw = (str(s) if s is not None else "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%H:%M")
    except ValueError:
        return None


def _slot_starts_for_day(
    day: date,
    open_dt: datetime | None,
    close_dt: datetime | None,
    lunch_start_dt: datetime | None,
    lunch_end_dt: datetime | None,
    dinner_start_dt: datetime | None,
    dinner_end_dt: datetime | None,
    interval_minutes: int,
    duration_minutes: int,
) -> list[datetime]:
    if open_dt is None or close_dt is None:
        return []
    day_open = datetime.combine(day, open_dt.time())
    day_close = datetime.combine(day, close_dt.time())
    if day_open >= day_close:
        return []

    lunch_a = lunch_end_a = None
    if lunch_start_dt and lunch_end_dt and lunch_start_dt.time() < lunch_end_dt.time():
        lunch_a = datetime.combine(day, lunch_start_dt.time())
        lunch_end_a = datetime.combine(day, lunch_end_dt.time())

    dinner_a = dinner_end_a = None
    if dinner_start_dt and dinner_end_dt and dinner_start_dt.time() < dinner_end_dt.time():
        dinner_a = datetime.combine(day, dinner_start_dt.time())
        dinner_end_a = datetime.combine(day, dinner_end_dt.time())

    interval = timedelta(minutes=interval_minutes)
    duration = timedelta(minutes=duration_minutes)
    out: list[datetime] = []
    cur = day_open
    while cur + duration <= day_close:
        in_lunch = lunch_a is not None and lunch_end_a is not None and lunch_a <= cur < lunch_end_a
        in_dinner = dinner_a is not None and dinner_end_a is not None and dinner_a <= cur < dinner_end_a
        if not in_lunch and not in_dinner:
            out.append(cur)
        cur += interval
    return out


def ensure_availability_from_hours(db, clinic_id: int) -> int:
    """Insert missing availability_slots from clinic operating_hours_json. Returns rows inserted."""
    row = db.execute(
        "SELECT operating_hours_json FROM clinics WHERE id = ?",
        (clinic_id,),
    ).fetchone()
    raw = row["operating_hours_json"] if row else None
    oh = parse_clinic_operating_hours(raw)
    interval_minutes = int(oh.get("slot_interval_minutes") or 45)
    if interval_minutes < 5:
        interval_minutes = 45
    horizon = int(oh.get("horizon_days") or 60)
    if horizon < 1:
        horizon = 60
    if horizon > 365:
        horizon = 365
    duration_minutes = interval_minutes

    mon_sat_o = _parse_hhmm_local(oh.get("mon_sat_open"))
    mon_sat_c = _parse_hhmm_local(oh.get("mon_sat_close"))
    sun_o = _parse_hhmm_local(oh.get("sunday_open"))
    sun_c = _parse_hhmm_local(oh.get("sunday_close"))
    ls = _parse_hhmm_local(oh.get("lunch_start"))
    le = _parse_hhmm_local(oh.get("lunch_end"))
    ds = _parse_hhmm_local(oh.get("dinner_start"))
    de = _parse_hhmm_local(oh.get("dinner_end"))

    today = datetime.now(PHILIPPINES_TZ).date()
    created = 0
    for offset in range(horizon):
        d = today + timedelta(days=offset)
        wd = d.weekday()
        if wd < 6:
            o, c = mon_sat_o, mon_sat_c
        else:
            o, c = sun_o, sun_c
        if o is None or c is None:
            continue
        for slot_start in _slot_starts_for_day(
            d, o, c, ls, le, ds, de, interval_minutes, duration_minutes
        ):
            slot_dt = slot_start.isoformat(timespec="seconds")
            try:
                cur = db.execute(
                    """
                    INSERT OR IGNORE INTO availability_slots (clinic_id, slot_datetime, duration_minutes, max_bookings, is_active)
                    VALUES (?, ?, ?, ?, 1)
                    """,
                    (clinic_id, slot_dt, duration_minutes, 1),
                )
                if getattr(cur, "rowcount", 0) == 1:
                    created += 1
            except sqlite3.Error:
                pass
    db.commit()
    return created


def create_app():
    load_dotenv(Path(__file__).resolve().parent / ".env")

    app = Flask(__name__, instance_relative_config=True)

    app.jinja_env.filters["namecase"] = normalize_name_case

    mail_user = os.getenv("MAIL_USERNAME", "").strip()
    mail_pass = os.getenv("MAIL_PASSWORD", "").strip()
    logger.info(
        "Email delivery: %s",
        "Gmail SMTP configured" if mail_user and mail_pass else "not configured (emails print to console only)",
    )

    secret_key = os.getenv("SECRET_KEY")
    if not secret_key:
        raise RuntimeError("SECRET_KEY is required. Set it in your environment or .env file.")
    app.config["SECRET_KEY"] = secret_key

    def _env_bool(name: str, default: bool = False) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "on"}

    timeout_minutes_raw = (os.getenv("SESSION_TIMEOUT_MINUTES") or "").strip()
    try:
        timeout_minutes = int(timeout_minutes_raw) if timeout_minutes_raw else 20
    except ValueError:
        timeout_minutes = 20
    timeout_minutes = max(1, timeout_minutes)

    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = _env_bool("SESSION_COOKIE_SECURE", False)
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=timeout_minutes)

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

    def _ensure_clinic_personnel_profile_columns():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(clinic_personnel)").fetchall()}
        if "date_of_birth" not in cols:
            db.execute("ALTER TABLE clinic_personnel ADD COLUMN date_of_birth TEXT")
            db.commit()
        if "gender" not in cols:
            db.execute("ALTER TABLE clinic_personnel ADD COLUMN gender TEXT")
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
                  barangay TEXT,
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
                      id, user_id, first_name, last_name, phone_number, barangay, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, relationship_to_user, onboarding_completed
                    )
                    SELECT
                      id, user_id, first_name, last_name, phone_number, NULL, address, date_of_birth,
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
                      id, user_id, first_name, last_name, phone_number, barangay, address, date_of_birth,
                      age, gender, allergies, pre_existing_conditions, current_medications,
                      notification_settings, relationship_to_user, onboarding_completed
                    )
                    SELECT
                      id, user_id, first_name, last_name, phone_number, NULL, address, date_of_birth,
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

    def _ensure_vaccination_card_tetanus_extra_columns():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(vaccination_card)").fetchall()}
        if "tetanus_batch" not in cols:
            db.execute("ALTER TABLE vaccination_card ADD COLUMN tetanus_batch TEXT")
            db.commit()
        if "tetanus_mfg_date" not in cols:
            db.execute("ALTER TABLE vaccination_card ADD COLUMN tetanus_mfg_date TEXT")
            db.commit()
        if "tetanus_expiry" not in cols:
            db.execute("ALTER TABLE vaccination_card ADD COLUMN tetanus_expiry TEXT")
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

    def _ensure_users_is_active_column():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        if "is_active" not in cols:
            db.execute(
                """
                ALTER TABLE users
                ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1
                CHECK(is_active IN (0,1))
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

    def _ensure_admin_page_last_seen_table():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_page_last_seen (
                admin_user_id INTEGER NOT NULL,
                page_key TEXT NOT NULL
                    CHECK(page_key IN ('patients','appointments','reporting','users','session_logs')),
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (admin_user_id, page_key),
                FOREIGN KEY (admin_user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_admin_page_last_seen_user_key ON admin_page_last_seen(admin_user_id, page_key)"
        )
        db.commit()

    def _migrate_admin_page_last_seen_session_logs():
        """Expand page_key CHECK to include session_logs for existing databases."""
        db = get_db()
        row = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='admin_page_last_seen'"
        ).fetchone()
        sql = (row["sql"] or "") if row else ""
        if not sql or "session_logs" in sql:
            return
        try:
            db.execute("BEGIN")
            db.execute(
                """
                CREATE TABLE admin_page_last_seen_new (
                    admin_user_id INTEGER NOT NULL,
                    page_key TEXT NOT NULL
                        CHECK(page_key IN ('patients','appointments','reporting','users','session_logs')),
                    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (admin_user_id, page_key),
                    FOREIGN KEY (admin_user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            db.execute(
                "INSERT INTO admin_page_last_seen_new SELECT * FROM admin_page_last_seen"
            )
            db.execute("DROP TABLE admin_page_last_seen")
            db.execute("ALTER TABLE admin_page_last_seen_new RENAME TO admin_page_last_seen")
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_page_last_seen_user_key ON admin_page_last_seen(admin_user_id, page_key)"
            )
            db.commit()
        except sqlite3.OperationalError:
            db.rollback()

    def _ensure_staff_page_last_seen_table():
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS staff_page_last_seen (
                staff_user_id INTEGER NOT NULL,
                page_key TEXT NOT NULL CHECK(page_key IN ('cases')),
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (staff_user_id, page_key),
                FOREIGN KEY (staff_user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_staff_page_last_seen_user_key ON staff_page_last_seen(staff_user_id, page_key)"
        )
        db.commit()

    def _ensure_cases_staff_completed_at_column():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(cases)").fetchall()}
        if "staff_completed_at" not in cols:
            db.execute("ALTER TABLE cases ADD COLUMN staff_completed_at TEXT")
            db.commit()

    def _ensure_patients_barangay_column():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(patients)").fetchall()}
        if "barangay" not in cols:
            db.execute("ALTER TABLE patients ADD COLUMN barangay TEXT")
            db.commit()

    def _ensure_cases_staff_removed_columns():
        db = get_db()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(cases)").fetchall()}
        if "staff_removed" not in cols:
            db.execute(
                "ALTER TABLE cases ADD COLUMN staff_removed INTEGER NOT NULL DEFAULT 0"
            )
            db.commit()
        cols = {row["name"] for row in db.execute("PRAGMA table_info(cases)").fetchall()}
        if "staff_removed_at" not in cols:
            db.execute("ALTER TABLE cases ADD COLUMN staff_removed_at TEXT")
            db.commit()
        if "staff_removed_by_user_id" not in cols:
            db.execute("ALTER TABLE cases ADD COLUMN staff_removed_by_user_id INTEGER")
            db.commit()
        # Map legacy "Remove Case" rows (archived) onto staff_removed for staff-only hiding.
        db.execute(
            """
            UPDATE cases
            SET staff_removed = 1
            WHERE COALESCE(staff_removed, 0) = 0
              AND LOWER(TRIM(COALESCE(case_status, ''))) = 'archived'
            """
        )
        db.commit()

    def _purge_stale_admin_last_seen_timestamps():
        """Delete admin_page_last_seen rows whose last_seen_at was stored in local-time
        format (contains 'T' or '+'), which breaks UTC datetime() comparisons in SQLite.
        Rows are removed so the epoch fallback is used, making all existing items appear
        as unseen until the admin visits each page (same UX as first login).
        This migration is idempotent and safe to run on every startup.
        """
        db = get_db()
        try:
            db.execute(
                """
                DELETE FROM admin_page_last_seen
                WHERE last_seen_at LIKE '%T%'
                   OR last_seen_at LIKE '%+%'
                """
            )
            db.commit()
        except Exception:
            db.rollback()

    with app.app_context():
        _ensure_patient_onboarding_column()
        _ensure_clinic_personnel_profile_columns()
        _migrate_patients_for_dependents()
        _ensure_appointments_patient_hidden_column()
        _ensure_vaccination_card_tables()
        _ensure_vaccination_card_tetanus_extra_columns()
        _ensure_patient_notifications_table()
        _ensure_user_security_columns()
        _ensure_users_is_active_column()
        _ensure_pending_emails_table()
        _ensure_admin_page_last_seen_table()
        _migrate_admin_page_last_seen_session_logs()
        _purge_stale_admin_last_seen_timestamps()
        _ensure_staff_page_last_seen_table()
        _ensure_cases_staff_completed_at_column()
        _ensure_patients_barangay_column()
        _ensure_cases_staff_removed_columns()
        try:
            _backfill_patients_barangay_address(get_db())
        except Exception:
            pass

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
            END,
            -- Prefer the most complete Self record (DOB/Gender/Name), so blank placeholder rows
            -- don't hide the real profile values.
            CASE WHEN TRIM(COALESCE(p.date_of_birth, '')) <> '' THEN 0 ELSE 1 END,
            CASE WHEN TRIM(COALESCE(p.gender, '')) <> '' THEN 0 ELSE 1 END,
            CASE
                WHEN TRIM(COALESCE(p.first_name, '')) <> '' OR TRIM(COALESCE(p.last_name, '')) <> '' THEN 0
                ELSE 1
            END,
            p.id DESC
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

    def _notify_patients_clinic_schedule_updated(clinic_id: int) -> None:
        """Alert patients with future appointments at this clinic that availability changed."""
        db = get_db()
        msg = (
            "Clinic appointment availability was updated. Please review your upcoming visits "
            "on your dashboard."
        )
        rows = db.execute(
            """
            SELECT DISTINCT a.patient_id
            FROM appointments a
            WHERE a.clinic_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
              AND datetime(REPLACE(TRIM(a.appointment_datetime), ' ', 'T'))
                  > datetime('now', 'localtime')
              AND LOWER(TRIM(COALESCE(a.status, ''))) NOT IN (
                  'cancelled', 'canceled', 'removed', 'expired', 'completed'
              )
            """,
            (clinic_id,),
        ).fetchall()
        for row in rows:
            _insert_patient_notification(
                patient_id=int(row["patient_id"]),
                notif_type="schedule",
                source_id=None,
                message=msg,
            )
        db.commit()

    def _ensure_walk_in_appointments_for_user(user_id: int) -> None:
        """Ensure each case for this login has at least one appointment (walk-in intake)."""
        db = get_db()
        missing = db.execute(
            """
            SELECT c.id AS case_id, c.patient_id, c.clinic_id, c.created_at
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            WHERE p.user_id = ?
              AND NOT EXISTS (SELECT 1 FROM appointments a WHERE a.case_id = c.id)
            """,
            (user_id,),
        ).fetchall()
        if not missing:
            return
        for row in missing:
            raw_created = (row["created_at"] or "").strip()
            if raw_created:
                appt_dt = raw_created.replace(" ", "T", 1) if "T" not in raw_created else raw_created
            else:
                appt_dt = _now_philippines_local_iso()
            wi_status = _walk_in_appointment_status_for_case(db, row["case_id"])
            db.execute(
                """
                INSERT INTO appointments (
                    patient_id, clinic_personnel_id, clinic_id, appointment_datetime,
                    status, type, case_id
                ) VALUES (?, NULL, ?, ?, ?, 'Walk-in', ?)
                """,
                (row["patient_id"], row["clinic_id"], appt_dt, wi_status, row["case_id"]),
            )
        db.commit()

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

    def _insert_next_dose_reminder_if_absent(
        *, patient_id: int, case_id: int, due_date: date
    ) -> bool:
        """
        Insert a one-time (per due_date) vaccination reminder notification.
        Returns True if a row was inserted.
        """
        db = get_db()
        due_display = due_date.strftime("%b %d, %Y")
        msg = f"Reminder: your next rabies vaccine dose is due on {due_display}."
        exists = db.execute(
            """
            SELECT 1 FROM patient_notifications
            WHERE patient_id = ?
              AND type = 'vaccination'
              AND source_id = ?
              AND COALESCE(message, '') = ?
            LIMIT 1
            """,
            (patient_id, case_id, msg),
        ).fetchone()
        if exists:
            return False
        _insert_patient_notification(
            patient_id=patient_id,
            notif_type="vaccination",
            source_id=case_id,
            message=msg,
        )
        return True

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
            return {"appointment": 0, "vaccination": 0, "schedule": 0}

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

        counts: dict[str, int] = {"appointment": 0, "vaccination": 0, "schedule": 0}
        for row in rows:
            notif_type = (row["type"] or "").strip()
            if notif_type in counts:
                counts[notif_type] = int(row["n"] or 0)
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
    ) -> tuple[list[dict], set[int], set[int], bool]:
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
        has_unread_schedule = False
        out: list[dict] = []
        for row in rows:
            r = dict(row)
            ntype = (r.get("type") or "").strip()
            sid = r.get("source_id")
            if ntype == "appointment" and sid is not None:
                highlight_appointment_ids.add(int(sid))
            elif ntype == "vaccination" and sid is not None:
                highlight_case_ids.add(int(sid))
            elif ntype == "schedule":
                has_unread_schedule = True

            recipient_label = _notification_recipient_label(
                r.get("relationship_to_user"),
                r.get("first_name"),
                r.get("last_name"),
            )
            link_href = None
            if ntype == "appointment" and sid is not None:
                link_href = url_for("patient_appointment_view", appointment_id=int(sid))
            elif ntype == "schedule":
                link_href = url_for("patient_dashboard")
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

        return out, highlight_appointment_ids, highlight_case_ids, has_unread_schedule

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
        Walk-in intake rows (type Walk-in) are excluded; those are managed under Cases.
        """
        db = get_db()
        row = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM appointments a
            WHERE a.clinic_id = ?
              AND COALESCE(a.type, '') != 'Walk-in'
              AND LOWER(TRIM(COALESCE(a.status, ''))) IN (
                  'pending', 'queued', 'scheduled', 'rescheduled', 'expired'
              )
            """,
            (clinic_id,),
        ).fetchone()
        return int(row["n"] or 0)

    def _get_staff_due_vaccinations_count(clinic_id: int) -> int:
        """
        Count distinct cases that have a vaccination card dose row due soon/overdue
        (scheduled date present but administration fields incomplete).

        Window: due within next 7 days OR overdue up to 7 days.
        """
        db = get_db()
        today = datetime.now().date()
        start = (today - timedelta(days=7)).isoformat()
        end = (today + timedelta(days=7)).isoformat()
        row = db.execute(
            """
            SELECT COUNT(DISTINCT vcd.case_id) AS n
            FROM vaccination_card_doses vcd
            JOIN cases c ON c.id = vcd.case_id
            WHERE c.clinic_id = ?
              AND COALESCE(TRIM(vcd.dose_date), '') <> ''
              AND DATE(SUBSTR(TRIM(vcd.dose_date), 1, 10)) >= DATE(?)
              AND DATE(SUBSTR(TRIM(vcd.dose_date), 1, 10)) <= DATE(?)
              AND (
                COALESCE(TRIM(vcd.type_of_vaccine), '') = ''
                OR COALESCE(TRIM(vcd.given_by), '') = ''
              )
            """,
            (clinic_id, start, end),
        ).fetchone()
        return int(row["n"] or 0)

    def _resolve_audit_clinic_personnel_id(
        db,
        *,
        clinic_personnel_id: int | None = None,
        clinic_id: int | None = None,
        user_id: int | None = None,
    ) -> int | None:
        if clinic_personnel_id:
            return int(clinic_personnel_id)
        if user_id:
            row = db.execute(
                "SELECT id FROM clinic_personnel WHERE user_id = ? LIMIT 1",
                (user_id,),
            ).fetchone()
            if row:
                return int(row["id"])
        if clinic_id:
            row = db.execute(
                "SELECT id FROM clinic_personnel WHERE clinic_id = ? ORDER BY id ASC LIMIT 1",
                (clinic_id,),
            ).fetchone()
            if row:
                return int(row["id"])
        return None

    def _insert_medical_audit_log(
        db,
        *,
        case_id: int,
        action: str,
        change_reason: str,
        user_id: int | None = None,
        clinic_personnel_id: int | None = None,
        clinic_id: int | None = None,
        field_name: str = "case_history",
        old_value: str | None = None,
        new_value: str | None = None,
    ) -> bool:
        resolved_cp_id = _resolve_audit_clinic_personnel_id(
            db,
            clinic_personnel_id=clinic_personnel_id,
            clinic_id=clinic_id,
            user_id=user_id,
        )
        if resolved_cp_id is None:
            return False
        db.execute(
            """
            INSERT INTO medical_audit_logs (
              clinic_personnel_id, user_id, entity_type, entity_id, case_id, action,
              field_name, old_value, new_value, change_reason
            ) VALUES (?, ?, 'cases', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_cp_id,
                user_id,
                case_id,
                case_id,
                action,
                field_name,
                old_value,
                new_value,
                change_reason,
            ),
        )
        return True

    @app.context_processor
    def _inject_barangay_options():
        return {"barangay_options": CEBU_BARANGAY_NAMES}

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
                """
                SELECT cp.clinic_id, cp.first_name, cp.last_name, cp.title, u.username
                FROM clinic_personnel cp
                JOIN users u ON u.id = cp.user_id
                WHERE cp.user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if staff is None:
                return {}
            cid = int(staff["clinic_id"])
            badges = _staff_nav_badge_counts(db, int(user_id), cid)
            sched = _get_staff_scheduled_appointments_count(cid)
            due_v = _get_staff_due_vaccinations_count(cid)
            badge_sum = sum(int(badges.get(k, 0) or 0) for k in badges)
            return {
                "scheduled_appointments_count": sched,
                "due_vaccinations_count": due_v,
                "staff_nav_badges": badges,
                "staff_initials": _staff_initials(staff),
                "staff_display_name": _staff_display_name(staff),
                "staff_account_type_label": _staff_account_type_label(staff),
                "play_notification_sound": sched > 0 or due_v > 0 or badge_sum > 0,
            }
        except Exception:
            # Never break rendering due to badge computation
            return {}

    @app.context_processor
    def _inject_patient_sidebar_identity():
        try:
            if session.get("role") != "patient":
                return {}
            user_id = session.get("user_id")
            if not user_id:
                return {}
            patient = _get_primary_patient(user_id)
            session_username = session.get("username")
            display = _patient_display_name_from_session(patient, session_username)
            if patient is not None:
                initials = _patient_initials(patient)
            else:
                un = (session_username or "P").strip()
                initials = (un[0] or "P").upper()
            uc = _get_patient_unread_counts(int(user_id))
            unread_total = (
                int(uc.get("appointment", 0))
                + int(uc.get("vaccination", 0))
                + int(uc.get("schedule", 0))
            )
            return {
                "patient_display_name": display,
                "patient_initials": initials,
                "patient_account_type_label": "Patient",
                "unread_appointments_count": uc.get("appointment", 0),
                "unread_vaccinations_count": uc.get("vaccination", 0),
                "unread_schedule_count": uc.get("schedule", 0),
                "play_notification_sound": unread_total > 0,
            }
        except Exception:
            return {}

    @app.context_processor
    def _inject_support_banner_stats():
        try:
            role = session.get("role")
            if role not in ["system_admin", "clinic_personnel"]:
                return {}
            
            user_id = session.get("user_id")
            if not user_id:
                return {}
                
            db = get_db()
            clinic_id = None
            
            if role == "system_admin":
                clinic = _get_singleton_clinic_row(db)
                if clinic:
                    clinic_id = clinic["id"]
            else:
                staff = db.execute("SELECT clinic_id FROM clinic_personnel WHERE user_id = ?", (user_id,)).fetchone()
                if staff:
                    clinic_id = staff["clinic_id"]
            
            if clinic_id:
                # 1. Check for High Risk Cases first (Category III or High Risk)
                high_risk_count = db.execute(
                    """
                    SELECT COUNT(*) as n FROM cases
                    WHERE clinic_id = ?
                    AND COALESCE(staff_removed, 0) = 0
                    AND LOWER(COALESCE(case_status, 'pending')) = 'pending'
                    AND (LOWER(category) = 'category iii' OR LOWER(risk_level) = 'high')
                    """,
                    (clinic_id,)
                ).fetchone()["n"]

                # 2. Count all Pending Cases
                total_pending = db.execute(
                    """
                    SELECT COUNT(*) as n
                    FROM cases
                    WHERE clinic_id = ?
                      AND COALESCE(staff_removed, 0) = 0
                      AND LOWER(COALESCE(case_status, 'pending')) = 'pending'
                    """,
                    (clinic_id,)
                ).fetchone()["n"]
                
                # Determine URLs
                if role == "system_admin":
                    base_url = url_for("admin_patients", status="pending")
                    high_risk_url = url_for("admin_patients", status="pending", category="category iii")
                else:
                    base_url = url_for("staff_patients", status="pending")
                    high_risk_url = url_for("staff_patients", status="pending", category="category iii")
                
                # Priority messaging logic
                if high_risk_count > 0 and role != "system_admin":
                    status = 'high_risk'
                    button_text = 'Review Now'
                    view_url = high_risk_url
                elif total_pending > 0:
                    status = 'pending'
                    button_text = 'View Details'
                    view_url = base_url
                else:
                    status = 'all_clear'
                    button_text = 'View Records'
                    view_url = base_url

                return {
                    "support_banner_count": total_pending,
                    "high_risk_count": high_risk_count,
                    "support_banner_url": view_url,
                    "all_cases_url": base_url,
                    "banner_status": status,
                    "banner_text": "cases awaiting review",
                    "banner_subtext": "You’re keeping the clinic safe",
                    "banner_button_text": button_text,
                    "is_admin": role == "system_admin"
                }
            elif role == "patient":
                # For patients, count unread items (notifications, etc)
                uc = _get_patient_unread_counts(int(user_id))
                total_unread = sum(int(v) for v in uc.values())
                
                return {
                    "support_banner_count": total_unread,
                    "support_banner_url": url_for("patient_notifications"),
                    "banner_text": "unread notifications",
                    "banner_subtext": "You’re staying informed—view details to continue.",
                    "banner_status": "pending" if total_unread > 0 else "all_clear",
                    "banner_button_text": "View Details"
                }
        except Exception:
            pass
        return {}

    @app.context_processor
    def _inject_admin_sidebar_identity():
        try:
            if session.get("role") != "system_admin":
                return {}
            user_id = session.get("user_id")
            if not user_id:
                return {}
            db = get_db()
            clinic = _get_singleton_clinic_row(db)
            cid = int(clinic["id"]) if clinic else None
            badges = dict(_admin_nav_badge_counts(db, int(user_id), cid))
            if (request.endpoint or "") == "admin_session_logs":
                badges["session_logs"] = 0
            badge_sum = sum(int(badges.get(k, 0) or 0) for k in badges)
            return {
                "admin_account_type_label": "System Administrator",
                "admin_nav_badges": badges,
                "play_notification_sound": badge_sum > 0,
            }
        except Exception:
            return {}

    def _run_case_status_maintenance(clinic_id: int):
        db = get_db()

        # Unapproved booking requests past slot: mark Expired (stay on clinic queue; not No Show).
        db.execute(
            """
            UPDATE appointments
            SET status = 'Expired'
            WHERE clinic_id = ?
              AND COALESCE(type, '') != 'Walk-in'
              AND LOWER(TRIM(COALESCE(status, ''))) IN ('pending', 'queued')
              AND datetime(appointment_datetime) < datetime('now', 'localtime')
            """,
            (clinic_id,),
        )

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

            has_cancelled_only = (
                not has_vaccination_update
                and db.execute(
                    """
                    SELECT 1
                    FROM appointments a
                    WHERE a.case_id = ?
                      AND a.clinic_id = ?
                      AND COALESCE(a.type, '') != 'Walk-in'
                      AND LOWER(TRIM(COALESCE(a.status, ''))) IN ('cancelled', 'canceled')
                    LIMIT 1
                    """,
                    (case_id, clinic_id),
                ).fetchone()
                is not None
                and db.execute(
                    """
                    SELECT 1
                    FROM appointments a
                    WHERE a.case_id = ?
                      AND a.clinic_id = ?
                      AND COALESCE(a.type, '') != 'Walk-in'
                      AND LOWER(TRIM(COALESCE(a.status, ''))) NOT IN (
                        'removed', 'cancelled', 'canceled', 'expired', 'completed'
                      )
                    LIMIT 1
                    """,
                    (case_id, clinic_id),
                ).fetchone()
                is None
            )

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
                  AND COALESCE(type, '') != 'Walk-in'
                  AND LOWER(TRIM(COALESCE(status, ''))) NOT IN (
                      'removed', 'cancelled', 'canceled',
                      'expired', 'pending', 'queued'
                  )
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
            elif has_cancelled_only:
                desired_status = "Cancelled"
            elif no_show_eligible:
                desired_status = "No Show"
            elif current_status in ("queued", "scheduled", "archived"):
                desired_status = case_row["case_status"] or "Pending"
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
                      AND COALESCE(a.type, '') != 'Walk-in'
                      AND LOWER(TRIM(COALESCE(a.status, ''))) NOT IN (
                          'removed', 'cancelled', 'canceled', 'no show', 'missed',
                          'expired', 'pending', 'queued'
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
                      AND COALESCE(type, '') != 'Walk-in'
                      AND LOWER(TRIM(COALESCE(status, ''))) NOT IN (
                          'removed', 'cancelled', 'canceled', 'no show', 'missed',
                          'expired', 'pending', 'queued'
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

            # Walk-in intake: Pending until vaccination is recorded, then Scheduled; never No Show.
            for wrow in db.execute(
                """
                SELECT id, status FROM appointments
                WHERE case_id = ? AND clinic_id = ? AND COALESCE(type, '') = 'Walk-in'
                """,
                (case_id, clinic_id),
            ).fetchall():
                st = (wrow["status"] or "").strip().lower()
                if st in ("completed", "cancelled", "canceled", "removed"):
                    continue
                want = _walk_in_appointment_status_for_case(db, case_id)
                if (wrow["status"] or "").strip() != want:
                    db.execute(
                        "UPDATE appointments SET status = ? WHERE id = ?",
                        (want, wrow["id"]),
                    )

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

    @app.after_request
    def _set_no_cache_headers(response):
        # Prevent browser cache/back-forward cache from re-showing protected HTML after logout.
        if (response.mimetype or "").lower() == "text/html":
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            response.headers["Vary"] = "Cookie"
        return response

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
        _ensure_walk_in_appointments_for_user(session["user_id"])
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
            has_unread_schedule,
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
                p.relationship_to_user AS victim_relationship,
                COALESCE(
                  (
                    SELECT MAX(created_at)
                    FROM patient_notifications pn
                    WHERE (pn.type = 'appointment' AND pn.source_id = a.id)
                       OR (pn.type = 'vaccination' AND pn.source_id = c.id)
                  ),
                  a.created_at,
                  c.created_at
                ) AS last_change_at
            FROM appointments a
            JOIN cases c ON c.id = a.case_id
            JOIN patients p ON p.id = a.patient_id
            WHERE p.user_id = ?
              AND COALESCE(a.patient_hidden, 0) = 0
            ORDER BY last_change_at DESC
            """,
            (session["user_id"],),
        ).fetchall()

        # Optional status filter for dashboard chips
        status_filter = (request.args.get("status") or "").strip().lower()
        if status_filter in ("no show", "no-show", "no_show"):
            status_filter = "missed"
        show_all = (request.args.get("view") or "").strip().lower() == "all"

        def _bucket_status(row: sqlite3.Row) -> str:
            status_value = (row["status"] or "").strip().lower()
            if status_value in ("cancelled", "canceled", "removed"):
                return "canceled"
            if status_value in ("completed",):
                return "completed"
            if status_value in ("no show", "missed"):
                return "missed"
            if status_value == "expired":
                return "expired"
            if status_value in ("pending", "queued"):
                return "pending"
            # Treat all other active / future-like statuses as "scheduled"
            return "scheduled"

        if status_filter == "scheduled":
            # Include pending/queued walk-ins so they appear under "Scheduled" as upcoming visits.
            filtered_rows = [
                row
                for row in all_appointments_rows
                if _bucket_status(row) in ("scheduled", "pending")
            ]
        elif status_filter in ("pending", "completed", "canceled", "missed", "expired"):
            filtered_rows = [
                row for row in all_appointments_rows if _bucket_status(row) == status_filter
            ]
        else:
            filtered_rows = list(all_appointments_rows)

        # Vaccination summary drives card dates (next_due_date) and sort order
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

        # Per-account appointment #1, #2, … by earliest effective next-dose or slot date
        sorted_for_sequence = sorted(
            all_appointments_rows,
            key=lambda r: _dashboard_appointment_sequence_key(r, _compute_vaccination_summary),
        )
        appointment_number_map: dict[int, int] = {}
        seq = 0
        for row in sorted_for_sequence:
            seq += 1
            appointment_number_map[row["id"]] = seq

        # Sort by last_change_at DESC as requested by user ("sorted depending on how recent each case is changed")
        filtered_rows = sorted(
            filtered_rows,
            key=lambda r: (r["last_change_at"] or ""),
            reverse=True
        )

        def _ordinal(n: int) -> str:
            if n <= 0:
                return ""
            if 10 <= (n % 100) <= 20:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
            return f"{n}{suffix}"

        enriched_appointments: list[dict] = []
        inserted_due_reminders = False
        today = datetime.now().date()
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
            appt_type = (appt.get("type") or "").strip()
            if appt_type in ("Walk-in", "Pre-screening"):
                display_type = appt_type
            else:
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
            appt_status = (appt.get("status") or "").strip().lower()
            is_upcoming = appt_status not in ("cancelled", "canceled", "removed", "expired", "completed")
            appt["notification_highlight"] = (
                appt["id"] in highlight_appointment_ids
                or (cid is not None and cid in highlight_case_ids)
                or (has_unread_schedule and is_upcoming)
            )

            if next_due_date and cid is not None:
                # Reminder window: due within the next 7 days or overdue (up to 7 days).
                days_until = (next_due_date - today).days
                if days_until <= 7 and days_until >= -7:
                    if _insert_next_dose_reminder_if_absent(
                        patient_id=int(appt["patient_id"]),
                        case_id=int(cid),
                        due_date=next_due_date,
                    ):
                        inserted_due_reminders = True

            enriched_appointments.append(appt)

        if inserted_due_reminders:
            db.commit()
            # Refresh unread counts + dashboard notification feed so reminders show immediately.
            unread_counts = _get_patient_unread_counts(session["user_id"])
            (
                dashboard_notifications,
                highlight_appointment_ids,
                highlight_case_ids,
                has_unread_schedule,
            ) = _get_unread_patient_notifications_for_user(session["user_id"])

        clinics = db.execute("SELECT id, name FROM clinics ORDER BY name").fetchall()

        has_any_appointments = len(all_appointments_rows) > 0

        _mark_patient_notifications_read(session["user_id"], "schedule")
        unread_counts = _get_patient_unread_counts(session["user_id"])

        return render_template(
            "patient_dashboard.html",
            patient=patient,
            cases=cases,
            appointments=enriched_appointments,
            show_all_appointments=show_all,
            clinics=clinics,
            has_any_appointments=has_any_appointments,
            selected_status=(
                status_filter
                if status_filter
                in ("pending", "completed", "canceled", "scheduled", "missed", "expired")
                else ""
            ),
            active_page="dashboard",
            unread_appointments_count=unread_counts.get("appointment", 0),
            unread_vaccinations_count=unread_counts.get("vaccination", 0),
            unread_schedule_count=unread_counts.get("schedule", 0),
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
        patient = _get_primary_patient(session["user_id"])
        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        return render_template("patient_help.html", patient=patient, active_page="help")

    @app.get("/patient/vaccinations")
    @role_required("patient")
    def patient_vaccinations():
        if not session.get("patient_onboarding_done"):
            return redirect(url_for("patient_onboarding"))

        db = get_db()
        _, _, vaccination_highlight_case_ids, _ = _get_unread_patient_notifications_for_user(
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
                psd.patient_prev_immunization,
                psd.prev_vaccine_date,
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
            has_first_dose = _case_has_first_dose_recorded(db, int(case_id))
            has_card_content = _vaccination_card_has_visible_content(vaccination_card)
            if not has_first_dose and not has_card_content:
                continue

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

            prev_imm = (appt.get("patient_prev_immunization") or "").strip()
            prev_date = (appt.get("prev_vaccine_date") or "").strip()
            pre_screening_vaccination_note = ""
            if prev_imm or prev_date:
                parts = []
                if prev_imm:
                    parts.append(f"Prior immunization: {prev_imm}")
                if prev_date:
                    parts.append(f"Prior vaccine date: {prev_date}")
                pre_screening_vaccination_note = " | ".join(parts)

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
            dose_course_sections = _dose_sections_for_patient_card(
                card_doses_by_type, category_value
            )
            first_sec = dose_course_sections[0]
            course_label = first_sec["course_label"]
            course_rows = first_sec["course_rows"]
            expected_doses = first_sec["expected_doses"]
            doses_completed = first_sec["doses_completed"]
            if len(dose_course_sections) > 1:
                course_label = ", ".join(s["course_label"] for s in dose_course_sections)

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
                    "dose_course_sections": dose_course_sections,
                    "dose_type_label": course_label,
                    "expected_doses": expected_doses,
                    "doses_completed": doses_completed,
                    "pre_screening_vaccination_note": pre_screening_vaccination_note,
                }
            )

        return render_template(
            "patient_vaccinations.html",
            patient=patient,
            vaccination_items=vaccination_items,
            vaccination_highlight_case_ids=vaccination_highlight_case_ids,
            active_page="vaccinations",
            unread_appointments_count=unread_counts.get("appointment", 0),
            unread_vaccinations_count=unread_counts.get("vaccination", 0),
            unread_schedule_count=unread_counts.get("schedule", 0),
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
            SELECT a.id, a.status, a.type, a.case_id
            FROM appointments a
            JOIN patients p ON p.id = a.patient_id
            WHERE a.id = ? AND p.user_id = ?
            """,
            (appointment_id, session["user_id"]),
        ).fetchone()

        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("patient_dashboard"))

        status_lower = ((appt["status"] or "").strip()).lower()
        if status_lower in ("cancelled", "canceled"):
            flash("Appointment is already cancelled.", "info")
            return redirect(url_for("patient_dashboard"))

        if not _patient_can_modify_appointment(db, appt):
            flash("This appointment can no longer be cancelled.", "info")
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
              c.who_category_auto,
              c.who_category_final,
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
        # When a patient opens a case appointment, also clear case-level vaccination highlights
        # so the dashboard card highlight disappears after it has been viewed once.
        _mark_vaccination_notifications_read_for_case(
            session["user_id"], int(appt["case_id"])
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
        can_edit = status_lower in ("pending", "queued", "scheduled", "no show", "expired")
        can_patient_modify = _patient_can_modify_appointment(db, appt)

        # Vaccination card data (shared with staff case view, read-only for patients)
        case_id = appt["case_id"]
        vc_row = db.execute(
            "SELECT * FROM vaccination_card WHERE case_id = ?", (case_id,)
        ).fetchone()
        vaccination_card = dict(vc_row) if vc_row else {}
        _normalize_vaccination_card_date_fields(vaccination_card)
        vaccination_card["form_vc_anti_rabies_vaccine"] = _anti_rabies_vaccine_prefill_from_db(vaccination_card)

        vaccination_card_doses_rows = db.execute(
            """
            SELECT id, case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
            FROM vaccination_card_doses
            WHERE case_id = ?
            ORDER BY record_type, day_number
            """,
            (case_id,),
        ).fetchall()
        has_vr = (
            db.execute(
                "SELECT 1 FROM vaccination_records WHERE case_id = ? LIMIT 1",
                (case_id,),
            ).fetchone()
            is not None
        )
        has_vaccination_card_data = (
            vc_row is not None or len(vaccination_card_doses_rows) > 0 or has_vr
        )
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
            can_patient_modify=can_patient_modify,
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
                psd.patient_prev_immunization,
                psd.prev_vaccine_date,
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
        _normalize_vaccination_card_date_fields(vaccination_card)
        vaccination_card["form_vc_anti_rabies_vaccine"] = _anti_rabies_vaccine_prefill_from_db(vaccination_card)

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
        dose_course_sections = _dose_sections_for_patient_card(card_doses_by_type, category_value)
        first_sec = dose_course_sections[0]
        course_label = first_sec["course_label"]
        course_rows = first_sec["course_rows"]
        expected_doses = first_sec["expected_doses"]
        doses_completed = first_sec["doses_completed"]

        prev_imm = (appt.get("patient_prev_immunization") or "").strip()
        prev_date = (appt.get("prev_vaccine_date") or "").strip()
        pre_screening_vaccination_note = ""
        if prev_imm or prev_date:
            parts = []
            if prev_imm:
                parts.append(f"Prior immunization: {prev_imm}")
            if prev_date:
                parts.append(f"Prior vaccine date: {prev_date}")
            pre_screening_vaccination_note = " | ".join(parts)

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
            "dose_course_sections": dose_course_sections,
            "expected_doses": expected_doses,
            "doses_completed": doses_completed,
            "pre_screening_vaccination_note": pre_screening_vaccination_note,
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
        db = get_db()
        has_first_dose = _case_has_first_dose_recorded(db, int(context["case_id"]))
        has_card_content = _vaccination_card_has_visible_content(
            context.get("vaccination_card")
        )
        if not has_first_dose and not has_card_content:
            flash("This vaccination card is available after at least one recorded dose.", "info")
            return redirect(url_for("patient_vaccinations"))

        _mark_vaccination_notifications_read_for_case(
            session["user_id"], int(context["case_id"])
        )

        patient = _get_primary_patient(session["user_id"])
        if patient is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        return render_template(
            "patient_vaccination_card_view.html",
            patient=patient,
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
        db = get_db()
        has_first_dose = _case_has_first_dose_recorded(db, int(context["case_id"]))
        has_card_content = _vaccination_card_has_visible_content(
            context.get("vaccination_card")
        )
        if not has_first_dose and not has_card_content:
            flash("This vaccination card is available after at least one recorded dose.", "info")
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

        if not _patient_can_modify_appointment(db, appt):
            flash("This appointment can no longer be rescheduled.", "info")
            return redirect(url_for("patient_appointment_view", appointment_id=appointment_id))

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
        rows = db.execute(
            """
            SELECT s.id, s.slot_datetime, s.max_bookings,
                   (SELECT COUNT(*) FROM appointments a2
                    WHERE a2.clinic_id = s.clinic_id
                      AND a2.appointment_datetime = s.slot_datetime
                      AND a2.id != ?
                      AND LOWER(COALESCE(a2.status, '')) NOT IN ('cancelled', 'canceled')) AS booking_count
            FROM availability_slots s
            WHERE s.clinic_id = ?
              AND s.is_active = 1
              AND datetime(REPLACE(s.slot_datetime, 'T', ' ')) > datetime('now', 'localtime')
            ORDER BY s.slot_datetime ASC
            """,
            (appointment_id, appt["clinic_id"]),
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

        if not _patient_can_modify_appointment(db, appt):
            flash("This appointment can no longer be rescheduled.", "info")
            return redirect(url_for("patient_appointment_view", appointment_id=appointment_id))

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

        if _is_slot_in_past(slot_datetime):
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
              AND LOWER(COALESCE(status, '')) NOT IN ('cancelled', 'canceled')
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
                      AND LOWER(COALESCE(a.status, '')) NOT IN ('cancelled', 'canceled')) AS booking_count
            FROM availability_slots s
            WHERE s.clinic_id = ?
              AND s.is_active = 1
              AND DATE(REPLACE(s.slot_datetime, 'T', ' ')) >= ?
              AND DATE(REPLACE(s.slot_datetime, 'T', ' ')) <= ?
              AND datetime(REPLACE(s.slot_datetime, 'T', ' ')) > datetime('now', 'localtime')
            ORDER BY s.slot_datetime ASC
            """,
            (clinic_id, from_date, to_date),
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

        errors, pdata = _prescreening_parse_validate_derive(request.form)
        if errors:
            for error in errors:
                flash(error, "error")
            return redirect(url_for("patient_dashboard"))

        form_type = pdata["form_type"]
        if form_type == "appointment" and pdata["form_clinic_id"]:
            try:
                fid = int(pdata["form_clinic_id"])
                row = db.execute("SELECT id FROM clinics WHERE id = ?", (fid,)).fetchone()
                if row:
                    clinic_id = row["id"]
            except ValueError:
                pass

        type_of_exposure = pdata["type_of_exposure"]
        exposure_date = pdata["exposure_date"]
        exposure_time = pdata["exposure_time"]
        wound_description = pdata["wound_description"]
        patient_prev_immunization = pdata["patient_prev_immunization"]
        prev_vaccine_date = pdata["prev_vaccine_date"]
        animal_status = pdata["animal_status"]
        animal_vaccination = pdata["animal_vaccination"]
        tetanus_immunization = pdata["tetanus_immunization"]
        tetanus_date = pdata["tetanus_date"]
        hrtig_immunization = pdata["hrtig_immunization"]
        hrtig_date = pdata["hrtig_date"]
        date_of_birth = pdata["date_of_birth"]
        gender = pdata["gender"]
        age = pdata["age"]
        barangay = pdata["barangay"]
        victim_address = pdata["victim_address"]
        contact_number = pdata["contact_number"]
        email_address = pdata["email_address"]
        relationship_to_user = pdata["relationship_to_user"]
        first_name = pdata["first_name"]
        last_name = pdata["last_name"]
        animal_detail = pdata["animal_detail"]
        final_place_of_exposure = pdata["final_place_of_exposure"]
        final_affected_area = pdata["final_affected_area"]
        final_local_treatment = pdata["final_local_treatment"]
        bleeding_type = pdata["bleeding_type"]
        risk_level = pdata["risk_level"]
        appointment_slot_id_raw = pdata["appointment_slot_id_raw"]
        appointment_datetime_form = pdata["appointment_datetime_form"]

        who_category_auto = risk_level
        who_version = WHO_RULES_VERSION + "+doh-risk-v1"
        who_reasons = _pre_screening_risk_reasons(
            type_of_exposure=type_of_exposure,
            affected_area=final_affected_area,
            wound_description=wound_description,
            bleeding_type=bleeding_type,
            animal_status=animal_status,
        )
        who_category_reasons_json = json.dumps(who_reasons, ensure_ascii=False)

        target_patient_id = patient["id"]
        has_victim_info = bool(first_name or last_name or date_of_birth or gender or age or barangay or victim_address or contact_number or email_address)
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
                new_barangay = barangay if barangay else (patient.get("barangay") or "")
                new_address = victim_address if victim_address else patient["address"]
                new_phone = contact_number if contact_number else patient["phone_number"]

                db.execute(
                    """
                    UPDATE patients
                    SET first_name = ?,
                        last_name = ?,
                        date_of_birth = ?,
                        gender = ?,
                        age = ?,
                        barangay = ?,
                        address = ?,
                        phone_number = ?,
                        relationship_to_user = ?
                    WHERE id = ?
                    """,
                    (new_first_name, new_last_name, new_date_of_birth, new_gender, parsed_age, new_barangay or None, new_address, new_phone, "Self", patient["id"]),
                )

                if email_address:
                    db.execute("UPDATE users SET email = ? WHERE id = ?", (email_address, session["user_id"]))
            else:
                db.execute(
                    """
                    INSERT INTO patients (
                        user_id, first_name, last_name, phone_number, barangay, address, date_of_birth, gender, age,
                        relationship_to_user, onboarding_completed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session["user_id"],
                        first_name,
                        last_name,
                        contact_number or None,
                        barangay or None,
                        victim_address or None,
                        date_of_birth,
                        gender,
                        parsed_age,
                        relationship_to_user,
                        1,
                    ),
                )
                target_patient_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        # Insert into cases table
        try:
            case_cur = db.execute(
                """
                INSERT INTO cases (
                    patient_id, clinic_id, exposure_date, exposure_time,
                    place_of_exposure, affected_area,
                    type_of_exposure, animal_detail, animal_condition, animal_vaccination,
                    risk_level, case_status, tetanus_prophylaxis_status,
                    who_category_auto, who_category_final, who_category_reasons_json, who_category_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    animal_vaccination,
                    risk_level,
                    "Queued" if form_type == "appointment" else "Active",
                    tetanus_immunization,
                    who_category_auto,
                    who_category_auto,
                    who_category_reasons_json,
                    who_version,
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

                if _is_slot_in_past(slot_datetime):
                    db.rollback()
                    flash("The selected slot is in the past. Please choose another date and time.", "error")
                    return redirect(url_for("patient_dashboard"))

                existing_count = db.execute(
                    """
                    SELECT COUNT(*) AS n FROM appointments
                    WHERE clinic_id = ? AND appointment_datetime = ?
                    AND LOWER(COALESCE(status, '')) NOT IN ('cancelled', 'canceled')
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

    @app.post("/patient/pre-screening/risk-preview")
    @role_required("patient")
    def pre_screening_risk_preview():
        """
        Server-authoritative risk preview for the patient pre-screening summary.
        Uses the same DOH-aligned classifier as case creation.
        """
        payload = request.get_json(silent=True) or {}

        type_of_exposure = (payload.get("type_of_exposure") or "").strip()
        wound_description = (payload.get("wound_description") or "").strip()
        animal_status = (payload.get("animal_status") or "").strip()
        animal_vaccination = (payload.get("animal_vaccination") or "").strip()
        patient_prev_immunization = (payload.get("patient_prev_immunization") or "").strip()
        spontaneous_bleeding = (payload.get("spontaneous_bleeding") or "").strip()
        induced_bleeding = (payload.get("induced_bleeding") or "").strip()
        bleeding_type = _bleeding_type_from_flags(spontaneous_bleeding, induced_bleeding)

        affected_area = payload.get("affected_area")
        affected_tokens: list[str] = []
        if isinstance(affected_area, list):
            for t in affected_area:
                s = (str(t) if t is not None else "").strip()
                if s:
                    affected_tokens.append(s)
        elif isinstance(affected_area, str):
            affected_tokens = [p.strip() for p in affected_area.replace(";", ",").split(",") if p.strip()]
        affected_area_str = ", ".join(affected_tokens)

        risk_level = classify_pre_screening_risk(
            type_of_exposure=type_of_exposure,
            affected_area=affected_area_str,
            wound_description=wound_description,
            bleeding_type=bleeding_type,
            animal_status=animal_status,
            animal_vaccination=animal_vaccination,
            patient_prev_immunization=patient_prev_immunization,
        )
        reasons = _pre_screening_risk_reasons(
            type_of_exposure=type_of_exposure,
            affected_area=affected_area_str,
            wound_description=wound_description,
            bleeding_type=bleeding_type,
            animal_status=animal_status,
        )
        return jsonify(
            {
                "risk_level": risk_level,
                "bleeding_type": bleeding_type,
                "reasons": reasons,
                "version": "doh-risk-v1",
            }
        )

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
        first_name = normalize_name_case(request.form.get("first_name", ""))
        last_name = normalize_name_case(request.form.get("last_name", ""))
        date_of_birth = request.form.get("date_of_birth", "").strip()
        gender = normalize_name_case(request.form.get("gender", ""))
        barangay = normalize_name_case(request.form.get("barangay", ""))
        address = normalize_name_case(request.form.get("address", ""))
        phone_number = request.form.get("phone_number", "").strip()
        email = request.form.get("email", "").strip().lower()
        allergies = normalize_name_case(request.form.get("allergies", ""))
        pre_existing_conditions = normalize_name_case(request.form.get("pre_existing_conditions", ""))
        current_medications = normalize_name_case(request.form.get("current_medications", ""))
        
        # Password change fields (optional)
        password_change_intent = (request.form.get("password_change_intent") or "").strip()
        new_password = request.form.get("new_password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        # Validation
        errors = []
        
        if not email:
            errors.append("Email is required.")
        elif "@" not in email:
            errors.append("Email must be valid.")
        
        changing_password = password_change_intent == "1"
        if changing_password:
            if not new_password or not confirm_password:
                errors.append("To change your password, please fill in both password fields.")
            elif len(new_password) < 8:
                errors.append("Password must be at least 8 characters.")
            elif new_password != confirm_password:
                errors.append("Passwords do not match.")
        
        if errors:
            for error in errors:
                flash(error, "error")
            return render_template(
                "patient_profile.html",
                patient=patient,
                active_page="profile",
                show_password_fields=changing_password,
            )

        derived_age = _age_from_iso_date(date_of_birth) if date_of_birth else None

        # Update patients table
        db.execute(
            """
            UPDATE patients
            SET first_name = ?,
                last_name = ?,
                date_of_birth = ?,
                gender = ?,
                age = ?,
                barangay = ?,
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
                derived_age if derived_age is not None else patient.get("age"),
                barangay if barangay else None,
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

        # Update password only when explicitly requested
        if changing_password:
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

        staff_display_name = _staff_display_name(staff)
        welcome_name = (
            f"{(staff['title'] or '').strip()} {(staff['last_name'] or '').strip()}".strip()
            or (staff["first_name"] or "").strip()
            or staff["username"]
        )

        current_date = datetime.now().strftime("%A, %d %B %Y")
        clinic_id = staff["clinic_id"]
        _run_case_status_maintenance(clinic_id)
        staff_visible_case_filter_sql = f"""
              AND {_SQL_STAFF_CASE_NOT_REMOVED}
              AND LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')
        """

        total_patients = db.execute(
            f"""
            SELECT COUNT(DISTINCT c.patient_id) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            {staff_visible_case_filter_sql}
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
              AND COALESCE(c.staff_removed, 0) = 0
              AND LOWER(COALESCE(c.case_status, 'pending')) = 'pending'
            """,
            (clinic_id,),
        ).fetchone()["total"]

        high_risk_cases = db.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            {staff_visible_case_filter_sql}
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
                  AND COALESCE(c.staff_removed, 0) = 0
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
            f"""
            SELECT
              CASE
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%' THEN 'Dogs'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%' THEN 'Cats'
                WHEN LOWER(COALESCE(c.animal_detail, '')) LIKE 'bat%' THEN 'Bats'
                ELSE COALESCE(NULLIF(TRIM(c.animal_detail), ''), 'Other')
              END AS bite_type,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            {staff_visible_case_filter_sql}
            GROUP BY bite_type
            ORDER BY total DESC
            """,
            (clinic_id,),
        ).fetchall()
        total_bite_cases = sum(row["total"] for row in bite_type_rows)
        common_bite_types = []
        for row in bite_type_rows:
            label = (row["bite_type"] or "Other").strip()
            if label.lower() not in ["dogs", "cats", "bats"]:
                label = label.title()
            count = row["total"]
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
              AND LOWER(COALESCE(a.status, '')) NOT IN ('removed', 'cancelled', 'canceled', 'pending', 'queued')
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

        staff_display_name = _staff_display_name(staff)
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

        section = (request.form.get("update_section") or "").strip()

        def _profile_breadcrumbs():
            return [
                {"label": "Home", "href": url_for("staff_dashboard")},
                {"label": "Profile", "href": None},
            ]

        if section == "personal":
            first_name = normalize_name_case(request.form.get("first_name") or "")
            last_name = normalize_name_case(request.form.get("last_name") or "")
            phone_number = (request.form.get("phone_number") or "").strip()
            specialty = normalize_name_case(request.form.get("specialty") or "")
            date_of_birth = (request.form.get("date_of_birth") or "").strip()
            gender = normalize_name_case(request.form.get("gender") or "")

            db.execute(
                """
                UPDATE clinic_personnel
                SET first_name = ?,
                    last_name = ?,
                    phone_number = ?,
                    specialty = ?,
                    date_of_birth = ?,
                    gender = ?
                WHERE user_id = ?
                """,
                (
                    first_name or None,
                    last_name or None,
                    phone_number or None,
                    specialty or None,
                    date_of_birth or None,
                    gender or None,
                    session["user_id"],
                ),
            )
            db.commit()

            flash("Personal information updated.", "success")
            return redirect(url_for("staff_profile"))

        if section == "account":
            username = (request.form.get("username") or "").strip()
            email = (request.form.get("email") or "").strip().lower()
            new_password = (request.form.get("new_password") or "").strip()
            confirm_password = (request.form.get("confirm_password") or "").strip()

            errors: list[str] = []
            if not username:
                errors.append("Username is required.")
            if not email:
                errors.append("Email is required.")
            elif "@" not in email:
                errors.append("Email must be valid.")
            if new_password:
                if len(new_password) < 8:
                    errors.append("Password must be at least 8 characters.")
                elif new_password != confirm_password:
                    errors.append("Passwords do not match.")

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
                merged = {k: staff[k] for k in staff.keys()}
                merged["username"] = username
                merged["email"] = email
                return render_template(
                    "staff_profile.html",
                    staff=merged,
                    staff_display_name=_staff_display_name(merged),
                    breadcrumbs=_profile_breadcrumbs(),
                    active_page="profile",
                    highlight_section="account",
                    show_staff_password_fields=bool(new_password or confirm_password),
                )

            db.execute(
                """
                UPDATE users
                SET username = ?, email = ?
                WHERE id = ?
                """,
                (username, email, session["user_id"]),
            )
            if new_password:
                db.execute(
                    """
                    UPDATE users
                    SET password_hash = ?
                    WHERE id = ?
                    """,
                    (generate_password_hash(new_password), session["user_id"]),
                )
            db.commit()

            session["username"] = username
            session["email"] = email

            flash("Account security updated.", "success")
            return redirect(url_for("staff_profile"))

        flash("Invalid update request.", "error")
        return redirect(url_for("staff_profile"))

    @app.route("/staff/patient/new", methods=["POST"])
    def staff_new_patient_account():
        role = session.get("role")
        if role not in ["system_admin", "clinic_personnel"]:
            return redirect(url_for("auth.login"))

        db = get_db()
        staff = None
        clinic_id = None
        staff_id = None
        display_name = "Administrator"

        if role == "system_admin":
            clinic_row = _get_singleton_clinic_row(db)
            if clinic_row:
                clinic_id = clinic_row["id"]
        else:
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
            clinic_id = staff["clinic_id"]
            staff_id = staff["id"]
            display_name = _staff_display_name(staff)

        clinic_row = db.execute(
            "SELECT id, name FROM clinics WHERE id = ?",
            (clinic_id,),
        ).fetchone()
        clinics = [clinic_row] if clinic_row else []

        empty_patient = {
            "first_name": "",
            "last_name": "",
            "date_of_birth": "",
            "gender": "",
            "age": "",
            "barangay": "",
            "phone_number": "",
            "email": "",
            "address": "",
        }

        if request.method == "POST":
            parse_errors, pdata = _prescreening_parse_validate_derive(request.form)
            errs = list(parse_errors)
            if pdata is not None:
                if pdata["form_type"].strip() != "case":
                    errs.append(
                        "Walk-in registration uses the standard case flow only. "
                        "Appointment booking is available from the patient portal."
                    )
                email_chk = (pdata["email_address"] or "").strip().lower()
                if not email_chk or "@" not in email_chk or "." not in email_chk.split("@")[-1]:
                    errs.append("A valid patient email is required.")
                elif db.execute("SELECT 1 FROM users WHERE email = ? LIMIT 1", (email_chk,)).fetchone():
                    errs.append("That email is already used by another account.")
            if errs:
                for err in errs:
                    flash(err, "error")
            elif pdata is not None:
                email = pdata["email_address"].strip().lower()
                first_name = pdata["first_name"]
                last_name = pdata["last_name"]
                dob = pdata["date_of_birth"]
                gender = pdata["gender"]
                phone_number = pdata["contact_number"] or None
                barangay = pdata["barangay"] or None
                address = pdata["victim_address"] or None
                age_value = _age_from_iso_date(dob) if dob else None

                username_seed = email.split("@", 1)[0]
                if first_name or last_name:
                    username_seed = ".".join(
                        part for part in [(first_name or "").lower(), (last_name or "").lower()] if part
                    )
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
                          user_id, first_name, last_name, age, phone_number, barangay, address, date_of_birth,
                          gender, relationship_to_user, onboarding_completed
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Self', 1)
                        """,
                        (user_id, first_name, last_name, age_value, phone_number, barangay, address, dob, gender),
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

                    who_category_auto = pdata["risk_level"]
                    who_version = WHO_RULES_VERSION + "+doh-risk-v1"
                    who_reasons = _pre_screening_risk_reasons(
                        type_of_exposure=pdata["type_of_exposure"],
                        affected_area=pdata["final_affected_area"],
                        wound_description=pdata["wound_description"],
                        bleeding_type=pdata["bleeding_type"],
                        animal_status=pdata["animal_status"],
                    )
                    who_category_reasons_json = json.dumps(who_reasons, ensure_ascii=False)

                    cur_case = db.execute(
                        """
                        INSERT INTO cases (
                            patient_id, clinic_id, exposure_date, exposure_time,
                            place_of_exposure, affected_area,
                            type_of_exposure, animal_detail, animal_condition, animal_vaccination,
                            category, risk_level, case_status, tetanus_prophylaxis_status,
                            who_category_auto, who_category_final, who_category_reasons_json, who_category_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            patient_id,
                            clinic_id,
                            pdata["exposure_date"],
                            pdata["exposure_time"] or None,
                            pdata["final_place_of_exposure"],
                            pdata["final_affected_area"],
                            pdata["type_of_exposure"],
                            pdata["animal_detail"],
                            pdata["animal_status"],
                            pdata.get("animal_vaccination") or "",
                            pdata["risk_level"],
                            pdata["risk_level"],
                            "Pending",
                            pdata["tetanus_immunization"],
                            who_category_auto,
                            who_category_auto,
                            who_category_reasons_json,
                            who_version,
                        ),
                    )
                    case_id = cur_case.lastrowid

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
                            pdata["wound_description"] or None,
                            pdata["bleeding_type"],
                            pdata["final_local_treatment"] or None,
                            pdata["patient_prev_immunization"] or None,
                            pdata["prev_vaccine_date"],
                            pdata["tetanus_date"],
                            1 if pdata["hrtig_immunization"] == "Yes" else 0,
                            pdata["hrtig_date"] if pdata["hrtig_immunization"] == "Yes" else None,
                        ),
                    )

                    # Staff-created cases go directly to Cases (no appointment record needed).
                    # Only patient-submitted pre-screenings create appointment records.
                    _insert_medical_audit_log(
                        db,
                        case_id=case_id,
                        action="INSERT",
                        field_name="case_status",
                        old_value=None,
                        new_value="Pending",
                        change_reason="Case created",
                        user_id=session.get("user_id"),
                        clinic_personnel_id=staff_id,
                        clinic_id=clinic_id,
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
                    if role == "system_admin":
                        return redirect(url_for("admin_case_details", case_id=case_id))
                    return redirect(url_for("view_patient_case", case_id=case_id))
                except Exception:
                    db.rollback()
                    flash("Failed to create new patient record. Please try again.", "error")

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Cases", "href": url_for("staff_patients")},
            {"label": "New Patient", "href": None},
        ]
        patient_ctx = empty_patient
        if request.method == "POST":
            patient_ctx = _patient_defaults_from_prescreening_form(request.form)
        return render_template(
            "staff_new_patient.html",
            staff=staff,
            staff_display_name=display_name,
            patient=patient_ctx,
            clinics=clinics,
            breadcrumbs=breadcrumbs,
            active_page="cases",
            pre_screening_embedded=False,
            pre_screening_form_action=url_for("staff_new_patient_account"),
            pre_screening_cancel_url=url_for("admin_patients") if role == "system_admin" else url_for("staff_patients"),
            pre_screening_submit_label="Create patient account",
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

        staff_display_name = _staff_display_name(staff)

        form_data = {
            "first_name": "",
            "last_name": "",
            "age": "",
            "phone_number": "",
            "address": "",
            "barangay": "",
            "victim_address": "",
            "exposure_date": "",
            "exposure_time": "",
            "type_of_exposure": "",
            "animal_type": "",
            "other_animal": "",
            "animal_status": "",
            "animal_vaccination": "",
            "place_of_exposure": "",
            "place_of_exposure_other": "",
            "affected_area_values": [],
            "affected_area_other": "",
            "risk_level": "",
            "wound_description": "",
            "spontaneous_bleeding": "No",
            "induced_bleeding": "No",
            "local_treatment": "",
            "other_treatment": "",
            "patient_prev_immunization": "",
            "prev_vaccine_date": "",
            "tetanus_immunization": "",
            "tetanus_date": "",
            "hrtig_immunization": "",
            "hrtig_date": "",
        }
        vaccination_card = {}
        card_doses_by_type = {"pre_exposure": {}, "post_exposure": {}, "booster": {}}

        if request.method == "POST":
            for key in form_data:
                if key == "affected_area_values":
                    form_data[key] = [v.strip() for v in request.form.getlist("affected_area") if v.strip()]
                else:
                    form_data[key] = (request.form.get(key) or "").strip()
            for _cap_key in (
                "first_name",
                "last_name",
                "address",
                "barangay",
                "victim_address",
                "other_animal",
                "place_of_exposure_other",
                "affected_area_other",
                "other_treatment",
            ):
                form_data[_cap_key] = normalize_name_case(form_data[_cap_key])
            def _v(name: str) -> str:
                return (request.form.get(name) or "").strip()

            vc_pvrv_r, vc_erig_r = _anti_rabies_vaccine_from_form(_v("vc_anti_rabies_vaccine"))
            ttox_r, ats_r, htig_r = _tetanus_triple_from_agent(_v("vc_tetanus_agent"))
            vaccination_card = {
                "anti_rabies": "",
                "pvrv": vc_pvrv_r,
                "pcec_batch": _v("vc_pcec_batch"),
                "pcec_mfg_date": _v("vc_pcec_mfg_date"),
                "pcec_expiry": _v("vc_pcec_expiry"),
                "erig_hrig": vc_erig_r,
                "tetanus_prophylaxis": "",
                "tetanus_toxoid": ttox_r,
                "ats": ats_r,
                "htig": htig_r,
                "tetanus_batch": _v("vc_tetanus_batch"),
                "tetanus_mfg_date": _v("vc_tetanus_mfg_date"),
                "tetanus_expiry": _v("vc_tetanus_expiry"),
                "remarks": normalize_name_case(_v("vc_remarks")),
                "form_vc_anti_rabies_vaccine": _v("vc_anti_rabies_vaccine"),
                "form_vc_tetanus_agent": _v("vc_tetanus_agent"),
            }
            master_type_r = _anti_rabies_type_label_from_form(_v("vc_anti_rabies_vaccine"))
            for record_type, prefix, days in _VC_DOSE_SCHEDULES:
                for day in days:
                    dcomb = _dose_value_from_form(
                        _v(f"{prefix}_{day}_dose_sel"),
                        _v(f"{prefix}_{day}_dose_other"),
                    )
                    card_doses_by_type[record_type][day] = {
                        "dose_date": _v(f"{prefix}_{day}_date"),
                        "type_of_vaccine": _v(f"{prefix}_{day}_type"),
                        "dose": dcomb,
                        "route_site": normalize_name_case(_v(f"{prefix}_{day}_route_site")),
                        "given_by": normalize_name_case(_v(f"{prefix}_{day}_given_by")),
                    }
            _owners_sticky = _vaccination_dose_date_owners_from_getter(_v)
            _vaccination_card_doses_apply_resolved_dates(card_doses_by_type, _owners_sticky)
            _vaccination_card_doses_apply_master_type_to_dated_rows(card_doses_by_type, master_type_r)

            class _AddExistingPreScreeningFormProxy:
                def __init__(self, raw_form):
                    self.raw_form = raw_form

                def get(self, key, default=None):
                    mapped = {
                        "victim_first_name": self.raw_form.get("first_name"),
                        "victim_last_name": self.raw_form.get("last_name"),
                        "victim_middle_initial": "",
                        "date_of_birth": self.raw_form.get("date_of_birth"),
                        "gender": self.raw_form.get("gender"),
                        "age": self.raw_form.get("age"),
                        "barangay": self.raw_form.get("barangay"),
                        "victim_address": self.raw_form.get("victim_address"),
                        "contact_number": self.raw_form.get("phone_number"),
                        "email_address": "",
                        "relationship_to_user": "Self",
                    }
                    if key in mapped:
                        value = mapped[key]
                        return value if value is not None else default
                    return self.raw_form.get(key, default)

                def getlist(self, key):
                    if key == "affected_area":
                        return self.raw_form.getlist("affected_area")
                    return self.raw_form.getlist(key)

            parse_errors, pdata = _prescreening_parse_validate_derive(
                _AddExistingPreScreeningFormProxy(request.form),
                require_demographics=False,
            )
            errors = list(parse_errors)
            if not form_data["first_name"] and not form_data["last_name"]:
                errors.append("Patient first name or last name is required.")

            final_risk_level = ""
            who_category_auto = ""
            if pdata is not None:
                who_category_auto = (pdata.get("risk_level") or "").strip()
                final_risk_level = who_category_auto
                manual_risk = (form_data.get("risk_level") or "").strip()
                if manual_risk:
                    normalized_manual_risk = ""
                    if manual_risk.lower() in {"category 1", "category i", "1", "i"}:
                        normalized_manual_risk = "Category I"
                    elif manual_risk.lower() in {"category 2", "category ii", "2", "ii"}:
                        normalized_manual_risk = "Category II"
                    elif manual_risk.lower() in {"category 3", "category iii", "3", "iii"}:
                        normalized_manual_risk = "Category III"
                    elif manual_risk.lower() == "unknown":
                        normalized_manual_risk = "Unknown"
                    else:
                        errors.append("Invalid manual Category / risk override.")
                    if normalized_manual_risk:
                        final_risk_level = normalized_manual_risk
                        form_data["risk_level"] = normalized_manual_risk
                else:
                    # Keep selector blank for auto mode; backend still persists computed risk.
                    form_data["risk_level"] = ""

                # Normalize repopulated values from shared parser outputs.
                form_data["animal_type"] = pdata.get("animal_type") or form_data["animal_type"]
                form_data["barangay"] = pdata.get("barangay") or form_data["barangay"]
                form_data["victim_address"] = pdata.get("victim_address") or form_data["victim_address"]
                form_data["address"] = pdata.get("victim_address") or form_data["address"]
                form_data["other_animal"] = pdata.get("other_animal") or ""
                form_data["animal_status"] = pdata.get("animal_status") or ""
                form_data["animal_vaccination"] = pdata.get("animal_vaccination") or ""
                form_data["place_of_exposure"] = pdata.get("place_of_exposure") or ""
                form_data["place_of_exposure_other"] = pdata.get("place_of_exposure_other") or ""
                form_data["affected_area_values"] = pdata.get("affected_area_values") or form_data["affected_area_values"]
                form_data["affected_area_other"] = pdata.get("affected_area_other") or ""
                form_data["spontaneous_bleeding"] = pdata.get("spontaneous_bleeding") or "No"
                form_data["induced_bleeding"] = pdata.get("induced_bleeding") or "No"
                form_data["tetanus_immunization"] = pdata.get("tetanus_immunization") or ""
                form_data["hrtig_immunization"] = pdata.get("hrtig_immunization") or ""
                form_data["hrtig_date"] = pdata.get("hrtig_date") or ""
                form_data["exposure_time"] = pdata.get("exposure_time") or ""
                form_data["wound_description"] = pdata.get("wound_description") or ""
                form_data["patient_prev_immunization"] = pdata.get("patient_prev_immunization") or ""
                form_data["local_treatment"] = pdata.get("local_treatment") or ""
                form_data["other_treatment"] = pdata.get("other_treatment") or ""
            if errors:
                for err in errors:
                    flash(err, "error")
            else:

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
                          user_id, first_name, last_name, age, phone_number, barangay, address, relationship_to_user, onboarding_completed
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Walk-in', 1)
                        """,
                        (
                            session["user_id"],
                            form_data["first_name"] or None,
                            form_data["last_name"] or None,
                            age_value,
                            form_data["phone_number"] or None,
                            (pdata.get("barangay") if pdata else form_data["barangay"]) or None,
                            (pdata.get("victim_address") if pdata else form_data["address"]) or None,
                        ),
                    )
                    patient_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

                    who_category_auto = who_category_auto or final_risk_level
                    who_version = WHO_RULES_VERSION + "+doh-risk-v1"
                    who_reasons = _pre_screening_risk_reasons(
                        type_of_exposure=(pdata.get("type_of_exposure") or ""),
                        affected_area=(pdata.get("final_affected_area") or ""),
                        wound_description=(pdata.get("wound_description") or ""),
                        bleeding_type=(pdata.get("bleeding_type") or "None"),
                        animal_status=(pdata.get("animal_status") or ""),
                    )
                    who_category_reasons_json = json.dumps(who_reasons, ensure_ascii=False)
                    cur = db.execute(
                        """
                        INSERT INTO cases (
                          patient_id, clinic_id, exposure_date, exposure_time, place_of_exposure,
                          affected_area, type_of_exposure, animal_detail, animal_condition, animal_vaccination,
                          tetanus_prophylaxis_status,
                          risk_level, category, case_status,
                          who_category_auto, who_category_final, who_category_reasons_json, who_category_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?, ?, ?, ?)
                        """,
                        (
                            patient_id,
                            staff["clinic_id"],
                            pdata.get("exposure_date"),
                            pdata.get("exposure_time") or None,
                            pdata.get("final_place_of_exposure") or None,
                            pdata.get("final_affected_area") or None,
                            pdata.get("type_of_exposure"),
                            pdata.get("animal_detail"),
                            pdata.get("animal_status") or None,
                            pdata.get("animal_vaccination") or None,
                            pdata.get("tetanus_immunization") or None,
                            final_risk_level,
                            final_risk_level,
                            who_category_auto,
                            final_risk_level,
                            who_category_reasons_json,
                            who_version,
                        ),
                    )
                    case_id = cur.lastrowid
                    _insert_medical_audit_log(
                        db,
                        case_id=case_id,
                        action="INSERT",
                        field_name="case_status",
                        old_value=None,
                        new_value="Pending",
                        change_reason="Case created",
                        user_id=session["user_id"],
                        clinic_personnel_id=staff["clinic_personnel_id"] if "clinic_personnel_id" in staff.keys() else staff["id"],
                        clinic_id=staff["clinic_id"],
                    )
                    hrtig_value = None
                    if (pdata.get("hrtig_immunization") or "").strip() in {"Yes", "No"}:
                        hrtig_value = 1 if (pdata.get("hrtig_immunization") or "").strip() == "Yes" else 0
                    db.execute(
                        """
                        INSERT INTO pre_screening_details (
                          case_id, wound_description, bleeding_type, local_treatment,
                          patient_prev_immunization, prev_vaccine_date, tetanus_date, hrtig_immunization, hrtig_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            pdata.get("wound_description") or None,
                            pdata.get("bleeding_type") or None,
                            pdata.get("final_local_treatment") or None,
                            pdata.get("patient_prev_immunization") or None,
                            pdata.get("prev_vaccine_date") or None,
                            pdata.get("tetanus_date") or None,
                            hrtig_value,
                            pdata.get("hrtig_date") or None,
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
                    vc_tetanus_mfg_date = _normalize_iso_date_input(_v("vc_tetanus_mfg_date"))
                    vc_tetanus_expiry = _normalize_iso_date_input(_v("vc_tetanus_expiry"))
                    today_iso = datetime.now().date().isoformat()
                    if vc_pcec_expiry and vc_pcec_expiry < today_iso:
                        flash("Expiry date cannot be earlier than today.", "error")
                        return redirect(url_for("staff_create_case_record"))
                    if vc_tetanus_expiry and vc_tetanus_expiry < today_iso:
                        flash("Tetanus expiry date cannot be earlier than today.", "error")
                        return redirect(url_for("staff_create_case_record"))

                    if vc_pcec_mfg_date and vc_pcec_expiry and vc_pcec_mfg_date > vc_pcec_expiry:
                        flash("Anti-rabies Mfg. date cannot be later than Expiry date.", "error")
                        return redirect(url_for("staff_create_case_record"))
                    if vc_tetanus_mfg_date and vc_tetanus_expiry and vc_tetanus_mfg_date > vc_tetanus_expiry:
                        flash("Tetanus Mfg. date cannot be later than Expiry date.", "error")
                        return redirect(url_for("staff_create_case_record"))

                    vc_pvrv_ins, vc_erig_ins = _anti_rabies_vaccine_from_form(_v("vc_anti_rabies_vaccine"))
                    ttox_ins, ats_ins, htig_ins = _tetanus_triple_from_agent(_v("vc_tetanus_agent"))
                    master_type_ins = _anti_rabies_type_label_from_form(_v("vc_anti_rabies_vaccine"))

                    db.execute(
                        """
                        INSERT INTO vaccination_card (
                            case_id, anti_rabies, pvrv, pcec_batch, pcec_mfg_date, pcec_expiry,
                            erig_hrig, tetanus_prophylaxis, tetanus_toxoid, ats, htig,
                            tetanus_batch, tetanus_mfg_date, tetanus_expiry,
                            remarks
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            case_id,
                            "",
                            vc_pvrv_ins,
                            _v("vc_pcec_batch"),
                            vc_pcec_mfg_date,
                            vc_pcec_expiry,
                            vc_erig_ins,
                            "",
                            ttox_ins,
                            ats_ins,
                            htig_ins,
                            _v("vc_tetanus_batch"),
                            vc_tetanus_mfg_date,
                            vc_tetanus_expiry,
                            normalize_name_case(_v("vc_remarks")),
                        ),
                    )

                    dose_date_owners_ins = _vaccination_dose_date_owners_from_getter(_v)
                    for record_type, prefix, days in _VC_DOSE_SCHEDULES:
                        for day in days:
                            dose_date = _v(f"{prefix}_{day}_date")
                            resolved_date_ins = _vaccination_resolved_dose_date_iso(
                                record_type,
                                dose_date,
                                dose_date_owners_ins,
                            )
                            type_of_vaccine = _vaccination_type_for_dose_row(
                                _v(f"{prefix}_{day}_type"),
                                master_type_ins,
                                resolved_date_ins,
                            )
                            dose = _dose_value_from_form(
                                _v(f"{prefix}_{day}_dose_sel"),
                                _v(f"{prefix}_{day}_dose_other"),
                            )
                            route_site = normalize_name_case(_v(f"{prefix}_{day}_route_site"))
                            given_by = normalize_name_case(_v(f"{prefix}_{day}_given_by"))
                            if _vaccination_dose_row_should_insert(
                                resolved_date_ins,
                                type_of_vaccine,
                                dose,
                                route_site,
                                given_by,
                            ):
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
                                        resolved_date_ins,
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
            {"label": "Cases", "href": url_for("staff_patients")},
            {"label": "Add Existing Record", "href": None},
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
            active_page="cases_add",
        )

    @app.get("/staff/patients")
    @role_required("clinic_personnel", "system_admin")
    def staff_patients():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        _staff_mark_page_seen(db, session["user_id"], "cases")
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

        staff_display_name = _staff_display_name(staff)
        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        q = request.args.to_dict(flat=True)
        search = (q.get("search") or "").strip()
        category = (request.args.get("category") or "all").strip().lower()
        if category not in {"all", "category i", "category ii", "category iii"}:
            category = "all"
        case_status = (request.args.get("status") or "all").strip().lower()
        if case_status not in {"all", "pending", "completed", "no show", "cancelled"}:
            case_status = "all"

        # Extended filters (reasonable subset + inventory joins)
        gender = (q.get("gender") or "all").strip()
        if gender.lower() not in {"all", "male", "female", "other"}:
            gender = "all"
        age_min_raw = (q.get("age_min") or "").strip()
        age_max_raw = (q.get("age_max") or "").strip()
        barangay = (q.get("barangay") or "").strip()
        site_of_bite = (q.get("site") or "").strip()
        animal_type = (q.get("animal_type") or "all").strip()
        if animal_type.lower() not in {"all", "dog", "cat", "others"}:
            animal_type = "all"
        animal_status = (q.get("animal_status") or "all").strip()
        if animal_status.lower() not in {"all", "healthy", "killed", "sick", "lost", "died"}:
            animal_status = "all"
        animal_vacc = (q.get("animal_vaccination") or "all").strip()
        if animal_vacc.lower() not in {"all", "updated", "not updated", "none"}:
            animal_vacc = "all"
        bio = (q.get("bio") or "all").strip()
        if bio.lower() not in {"all", "anti-rabies", "hrig/erig", "tetanus"}:
            bio = "all"
        batch = (q.get("batch") or "").strip()
        date_from = (q.get("date_from") or "").strip()
        date_to = (q.get("date_to") or "").strip()

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = 1 if page < 1 else page
        per_page = 10

        where_clauses = [
            "c.clinic_id = ?",
            _SQL_STAFF_CASE_NOT_REMOVED,
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
            ]
            search_params: list[object] = [search_like]

            case_id_search = search.lower().removeprefix("c-")
            if case_id_search.isdigit():
                search_parts.append("c.id = ?")
                search_params.append(int(case_id_search))

            where_clauses.append("(" + " OR ".join(search_parts) + ")")
            params.extend(search_params)

        if gender.lower() != "all":
            where_clauses.append("LOWER(COALESCE(p.gender, '')) = ?")
            params.append(gender.lower())

        def _try_int(s: str) -> int | None:
            try:
                return int(s)
            except Exception:
                return None

        age_min = _try_int(age_min_raw) if age_min_raw else None
        age_max = _try_int(age_max_raw) if age_max_raw else None
        if age_min is not None:
            where_clauses.append("COALESCE(p.age, 0) >= ?")
            params.append(age_min)
        if age_max is not None:
            where_clauses.append("COALESCE(p.age, 0) <= ?")
            params.append(age_max)

        if barangay:
            where_clauses.append(_sql_patient_barangay_lowercase_like())
            params.append(f"%{barangay.lower()}%")

        if site_of_bite:
            where_clauses.append("LOWER(COALESCE(c.affected_area, '')) LIKE ?")
            params.append(f"%{site_of_bite.lower()}%")

        if animal_type.lower() != "all":
            if animal_type.lower() == "dog":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%'")
            elif animal_type.lower() == "cat":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%'")
            else:
                where_clauses.append(
                    "(LOWER(COALESCE(c.animal_detail, '')) LIKE 'others%' OR (LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'dog%' AND LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'cat%'))"
                )

        if animal_status.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_condition, '')) = ?")
            params.append(animal_status.lower())

        if animal_vacc.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_vaccination, '')) = ?")
            params.append(animal_vacc.lower())

        if date_from:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)")
            params.append(date_from)
        if date_to:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)")
            params.append(date_to)

        if batch:
            where_clauses.append(
                """(
                    LOWER(COALESCE(vc.pcec_batch, '')) LIKE ?
                    OR EXISTS (
                        SELECT 1 FROM vaccination_records vr
                        WHERE vr.case_id = c.id
                          AND LOWER(COALESCE(vr.vaccine_brand_batch, '')) LIKE ?
                        LIMIT 1
                    )
                )"""
            )
            b_like = f"%{batch.lower()}%"
            params.extend([b_like, b_like])

        if bio.lower() != "all":
            if bio.lower() == "anti-rabies":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.anti_rabies, '')), '') IS NOT NULL
                      OR EXISTS (
                        SELECT 1 FROM vaccination_card_doses vcd
                        WHERE vcd.case_id = c.id AND NULLIF(TRIM(COALESCE(vcd.dose_date,'')), '') IS NOT NULL
                        LIMIT 1
                      )
                    )"""
                )
            elif bio.lower() == "hrig/erig":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.erig_hrig, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.htig, '')), '') IS NOT NULL
                      OR COALESCE(psd.hrtig_immunization, 0) = 1
                    )"""
                )
            else:
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.tetanus_prophylaxis, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.tetanus_toxoid, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.ats, '')), '') IS NOT NULL
                    )"""
                )

        where_sql = " AND ".join(where_clauses)

        count_sql = (
            """
            SELECT COUNT(*) AS total
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            LEFT JOIN vaccination_card vc ON vc.case_id = c.id
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
                          AND datetime(a.appointment_datetime) >= datetime('now', 'localtime')
                        ORDER BY datetime(a.appointment_datetime) ASC, a.id ASC
                        LIMIT 1
                    ),
                    'N/A'
                ) AS initial_schedule,
                (
                    SELECT MIN(vcd.dose_date)
                    FROM vaccination_card_doses vcd
                    WHERE vcd.case_id = c.id
                      AND NULLIF(TRIM(vcd.dose_date), '') IS NOT NULL
                      AND DATE(vcd.dose_date) >= DATE('now', 'localtime')
                ) AS next_dose_date
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            LEFT JOIN vaccination_card vc ON vc.case_id = c.id
            WHERE
            """
            + where_sql
            + """
            ORDER BY c.id DESC
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
            {"label": "Cases", "href": None},
        ]


        return render_template(
            "staff_patients.html",
            staff=staff,
            staff_display_name=staff_display_name,
            cases=cases,
            selected_category=category,
            selected_status=case_status,
            search=search,
            selected_gender=gender,
            selected_age_min=age_min_raw,
            selected_age_max=age_max_raw,
            selected_barangay=barangay,
            selected_site=site_of_bite,
            selected_animal_type=animal_type,
            selected_animal_status=animal_status,
            selected_animal_vaccination=animal_vacc,
            selected_bio=bio,
            selected_batch=batch,
            selected_date_from=date_from,
            selected_date_to=date_to,
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

        staff_display_name = _staff_display_name(staff)
        maintenance = _run_case_status_maintenance(staff["clinic_id"])

        search = (request.args.get("search") or "").strip().lower()
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        page = 1 if page < 1 else page
        per_page = 10

        appt_filter = (request.args.get("filter") or "all").strip().lower()
        if appt_filter not in ("active", "expired", "all"):
            appt_filter = "all"

        if appt_filter == "expired":
            status_clause = "LOWER(TRIM(COALESCE(a.status, ''))) = 'expired'"
        elif appt_filter == "all":
            status_clause = (
                "LOWER(TRIM(COALESCE(a.status, ''))) IN ("
                "'pending', 'queued', 'scheduled', 'rescheduled', 'expired'"
                ")"
            )
        else:
            status_clause = (
                "LOWER(TRIM(COALESCE(a.status, ''))) IN ("
                "'pending', 'queued', 'scheduled', 'rescheduled'"
                ")"
            )

        where_clauses = [
            "a.clinic_id = ?",
            "COALESCE(a.type, '') != 'Walk-in'",
            _SQL_STAFF_CASE_NOT_REMOVED,
            status_clause,
        ]
        params: list[object] = [staff["clinic_id"]]

        if search:
            where_clauses.append(
                """
                (
                    LOWER(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')) LIKE ?
                    OR CAST(a.id AS TEXT) LIKE ?
                )
                """
            )
            search_like = f"%{search}%"
            params.extend([search_like, search_like])

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
              a.status,
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
            st = (row["status"] or "").strip().lower()
            items.append(
                {
                    "id": row["id"],
                    "appointment_code": f"APT-{row['id']}",
                    "patient_name": row["patient_name"],
                    "appointment_datetime": display_datetime,
                    "category": row["category"],
                    "is_expired": st == "expired",
                    "status_key": st,
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
            appt_filter=appt_filter,
            breadcrumbs=breadcrumbs,
            active_page="appointments",
        )

    @app.get("/staff/cases/export.csv")
    @role_required("clinic_personnel", "system_admin")
    def staff_cases_export_csv():
        db = get_db()
        role = session.get("role")
        clinic_id = None
        clinic_name = "Clinic"

        if role == "system_admin":
            clinic_row = _get_singleton_clinic_row(db)
            if clinic_row:
                clinic_id = clinic_row["id"]
                clinic_name = clinic_row["name"]
        else:
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
            clinic_id = staff["clinic_id"]
            clinic_name = staff["clinic_name"]

        # Reuse staff_patients query params by calling that route's logic shape.
        q = request.args.to_dict(flat=True)
        # Build a minimal where clause identical to staff_patients
        # (kept inline to avoid a large refactor).
        search = (q.get("search") or "").strip()
        category = (q.get("category") or "all").strip().lower()
        if category not in {"all", "category i", "category ii", "category iii"}:
            category = "all"
        case_status = (q.get("status") or "all").strip().lower()
        if case_status not in {"all", "pending", "completed", "no show"}:
            case_status = "all"
        gender = (q.get("gender") or "all").strip()
        if gender.lower() not in {"all", "male", "female", "other"}:
            gender = "all"
        age_min_raw = (q.get("age_min") or "").strip()
        age_max_raw = (q.get("age_max") or "").strip()
        barangay = (q.get("barangay") or "").strip()
        site_of_bite = (q.get("site") or "").strip()
        animal_type = (q.get("animal_type") or "all").strip()
        if animal_type.lower() not in {"all", "dog", "cat", "others"}:
            animal_type = "all"
        animal_status = (q.get("animal_status") or "all").strip()
        if animal_status.lower() not in {"all", "healthy", "killed", "sick", "lost", "died"}:
            animal_status = "all"
        animal_vacc = (q.get("animal_vaccination") or "all").strip()
        if animal_vacc.lower() not in {"all", "updated", "not updated", "none"}:
            animal_vacc = "all"
        bio = (q.get("bio") or "all").strip()
        if bio.lower() not in {"all", "anti-rabies", "hrig/erig", "tetanus"}:
            bio = "all"
        batch = (q.get("batch") or "").strip()
        date_from = (q.get("date_from") or "").strip()
        date_to = (q.get("date_to") or "").strip()

        where_clauses = [
            "c.clinic_id = ?",
            _SQL_STAFF_CASE_NOT_REMOVED,
            "LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')",
        ]
        params: list[object] = [clinic_id]
        if category != "all":
            where_clauses.append("LOWER(COALESCE(c.risk_level, c.category, '')) = ?")
            params.append(category)
        if case_status != "all":
            where_clauses.append("LOWER(COALESCE(c.case_status, 'pending')) = ?")
            params.append(case_status)
        if search:
            search_like = f"%{search.lower()}%"
            search_parts = ["LOWER(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')) LIKE ?"]
            search_params: list[object] = [search_like]
            case_id_search = search.lower().removeprefix("c-")
            if case_id_search.isdigit():
                search_parts.append("c.id = ?")
                search_params.append(int(case_id_search))
            where_clauses.append("(" + " OR ".join(search_parts) + ")")
            params.extend(search_params)
        if gender.lower() != "all":
            where_clauses.append("LOWER(COALESCE(p.gender, '')) = ?")
            params.append(gender.lower())

        def _try_int(s: str) -> int | None:
            try:
                return int(s)
            except Exception:
                return None

        age_min = _try_int(age_min_raw) if age_min_raw else None
        age_max = _try_int(age_max_raw) if age_max_raw else None
        if age_min is not None:
            where_clauses.append("COALESCE(p.age, 0) >= ?")
            params.append(age_min)
        if age_max is not None:
            where_clauses.append("COALESCE(p.age, 0) <= ?")
            params.append(age_max)
        if barangay:
            where_clauses.append(_sql_patient_barangay_lowercase_like())
            params.append(f"%{barangay.lower()}%")
        if site_of_bite:
            where_clauses.append("LOWER(COALESCE(c.affected_area, '')) LIKE ?")
            params.append(f"%{site_of_bite.lower()}%")
        if animal_type.lower() != "all":
            if animal_type.lower() == "dog":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%'")
            elif animal_type.lower() == "cat":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%'")
            else:
                where_clauses.append(
                    "(LOWER(COALESCE(c.animal_detail, '')) LIKE 'others%' OR (LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'dog%' AND LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'cat%'))"
                )
        if animal_status.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_condition, '')) = ?")
            params.append(animal_status.lower())
        if animal_vacc.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_vaccination, '')) = ?")
            params.append(animal_vacc.lower())
        if date_from:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)")
            params.append(date_from)
        if date_to:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)")
            params.append(date_to)
        if batch:
            where_clauses.append(
                """(
                    LOWER(COALESCE(vc.pcec_batch, '')) LIKE ?
                    OR EXISTS (
                        SELECT 1 FROM vaccination_records vr
                        WHERE vr.case_id = c.id
                          AND LOWER(COALESCE(vr.vaccine_brand_batch, '')) LIKE ?
                        LIMIT 1
                    )
                )"""
            )
            b_like = f"%{batch.lower()}%"
            params.extend([b_like, b_like])
        if bio.lower() != "all":
            if bio.lower() == "anti-rabies":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.anti_rabies, '')), '') IS NOT NULL
                      OR EXISTS (
                        SELECT 1 FROM vaccination_card_doses vcd
                        WHERE vcd.case_id = c.id AND NULLIF(TRIM(COALESCE(vcd.dose_date,'')), '') IS NOT NULL
                        LIMIT 1
                      )
                    )"""
                )
            elif bio.lower() == "hrig/erig":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.erig_hrig, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.htig, '')), '') IS NOT NULL
                      OR COALESCE(psd.hrtig_immunization, 0) = 1
                    )"""
                )
            else:
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.tetanus_prophylaxis, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.tetanus_toxoid, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.ats, '')), '') IS NOT NULL
                    )"""
                )

        where_sql = " AND ".join(where_clauses)
        rows = db.execute(
            """
            SELECT
              c.id AS case_id,
              COALESCE(NULLIF(TRIM(p.first_name), ''), '') AS first_name,
              COALESCE(NULLIF(TRIM(p.last_name), ''), '') AS last_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown'
              ) AS patient_name,
              p.gender,
              p.age,
              p.phone_number,
              p.barangay,
              p.address,
              c.exposure_date,
              c.exposure_time,
              c.place_of_exposure,
              c.affected_area,
              c.type_of_exposure,
              c.animal_detail,
              c.animal_condition,
              c.animal_vaccination,
              COALESCE(
                NULLIF(TRIM(c.who_category_final), ''),
                NULLIF(TRIM(c.who_category_auto), ''),
                NULLIF(TRIM(c.risk_level), ''),
                NULLIF(TRIM(c.category), ''),
                'Unknown'
              ) AS category,
              COALESCE(c.case_status, 'Pending') AS case_status,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.prev_vaccine_date,
              psd.tetanus_date,
              psd.hrtig_immunization,
              vc.anti_rabies,
              vc.pvrv,
              vc.pcec_batch,
              vc.pcec_mfg_date,
              vc.pcec_expiry,
              vc.erig_hrig,
              vc.tetanus_prophylaxis,
              vc.tetanus_toxoid,
              vc.ats,
              vc.htig,
              vc.tetanus_batch,
              vc.tetanus_mfg_date,
              vc.tetanus_expiry,
              vc.remarks
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            LEFT JOIN vaccination_card vc ON vc.case_id = c.id
            WHERE
            """
            + where_sql
            + """
            ORDER BY datetime(c.created_at) DESC, c.id DESC
            """,
            params,
        ).fetchall()

        import csv
        import io
        output = io.StringIO()
        w = csv.writer(output)
        case_ids = [int(r["case_id"]) for r in rows]
        doses_by_case: dict[int, list[str]] = {}
        if case_ids:
            placeholders = ",".join(["?"] * len(case_ids))
            dose_rows = db.execute(
                f"""
                SELECT case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                FROM vaccination_card_doses
                WHERE case_id IN ({placeholders})
                ORDER BY case_id, record_type, day_number
                """,
                case_ids,
            ).fetchall()
            for dr in dose_rows:
                cid = int(dr["case_id"])
                label = (
                    f"{dr['record_type']} Day {dr['day_number']}: "
                    f"{(dr['dose_date'] or '').strip()} | {(dr['type_of_vaccine'] or '').strip()} | "
                    f"{(dr['dose'] or '').strip()} | {(dr['route_site'] or '').strip()} | {(dr['given_by'] or '').strip()}"
                ).strip()
                doses_by_case.setdefault(cid, []).append(label)

        w.writerow(
            [
                "case_id",
                "case_code",
                "patient_name",
                "first_name",
                "last_name",
                "gender",
                "age",
                "phone_number",
                "address",
                "barangay",
                "exposure_date",
                "exposure_time",
                "place_of_exposure",
                "site_of_bite",
                "type_of_exposure",
                "wound_description",
                "bleeding_type",
                "local_treatment",
                "patient_prev_immunization",
                "prev_vaccine_date",
                "tetanus_date",
                "hrtig_immunization",
                "category",
                "status",
                "animal_detail",
                "animal_status",
                "animal_vaccination",
                "vc_anti_rabies",
                "vc_pvrv",
                "vc_pcec_batch",
                "vc_pcec_mfg_date",
                "vc_pcec_expiry",
                "vc_erig_hrig",
                "vc_tetanus_prophylaxis",
                "vc_tetanus_toxoid",
                "vc_ats",
                "vc_htig",
                "vc_tetanus_batch",
                "vc_tetanus_mfg_date",
                "vc_tetanus_expiry",
                "vc_remarks",
                "vaccination_card_doses",
            ]
        )
        for r in rows:
            cid = int(r["case_id"])
            w.writerow(
                [
                    cid,
                    f"C-000{cid}",
                    r["patient_name"],
                    r["first_name"] or "",
                    r["last_name"] or "",
                    r["gender"] or "",
                    r["age"] if r["age"] is not None else "",
                    r["phone_number"] or "",
                    r["address"] or "",
                    _barangay_export_value(r["barangay"], r["address"]),
                    r["exposure_date"] or "",
                    r["exposure_time"] or "",
                    r["place_of_exposure"] or "",
                    r["affected_area"] or "",
                    r["type_of_exposure"] or "",
                    r["wound_description"] or "",
                    r["bleeding_type"] or "",
                    r["local_treatment"] or "",
                    r["patient_prev_immunization"] or "",
                    r["prev_vaccine_date"] or "",
                    r["tetanus_date"] or "",
                    r["hrtig_immunization"] if r["hrtig_immunization"] is not None else "",
                    r["category"] or "",
                    r["case_status"] or "",
                    r["animal_detail"] or "",
                    r["animal_condition"] or "",
                    r["animal_vaccination"] or "",
                    r["anti_rabies"] or "",
                    r["pvrv"] or "",
                    r["pcec_batch"] or "",
                    r["pcec_mfg_date"] or "",
                    r["pcec_expiry"] or "",
                    r["erig_hrig"] or "",
                    r["tetanus_prophylaxis"] or "",
                    r["tetanus_toxoid"] or "",
                    r["ats"] or "",
                    r["htig"] or "",
                    r["tetanus_batch"] or "",
                    r["tetanus_mfg_date"] or "",
                    r["tetanus_expiry"] or "",
                    r["remarks"] or "",
                    " || ".join(doses_by_case.get(cid, [])),
                ]
            )
        csv_data = output.getvalue().encode("utf-8-sig")
        resp = make_response(csv_data)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = 'attachment; filename="cases_export.csv"'
        return resp

    @app.get("/staff/cases/export.pdf")
    @role_required("clinic_personnel", "system_admin")
    def staff_cases_export_pdf():
        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable. Please contact the clinic.", "error")
            return redirect(url_for("staff_patients"))

        db = get_db()
        role = session.get("role")
        clinic_id = None
        clinic_name = "Clinic"

        if role == "system_admin":
            clinic_row = _get_singleton_clinic_row(db)
            if clinic_row:
                clinic_id = clinic_row["id"]
                clinic_name = clinic_row["name"]
        else:
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
            clinic_id = staff["clinic_id"]
            clinic_name = staff["clinic_name"]

        # Fetch same rows as CSV export by calling the SQL snippet directly.
        q = request.args.to_dict(flat=True)
        # Build where clause exactly like CSV route by delegating via an internal request.
        # For maintainability, this calls the CSV route's query logic via a shared helper pattern.
        # (Kept simple: repeat the SELECT with same filters as CSV route.)
        # NOTE: We re-run the same filter builder by calling staff_cases_export_csv's logic above is not possible here.
        # Instead, we reuse the same SQL+params construction by importing it via a local function scope duplication.
        # This is acceptable given the tight coupling to the export feature.
        # -- Begin duplicated filter builder (kept in sync with CSV export) --
        search = (q.get("search") or "").strip()
        category = (q.get("category") or "all").strip().lower()
        if category not in {"all", "category i", "category ii", "category iii"}:
            category = "all"
        case_status = (q.get("status") or "all").strip().lower()
        if case_status not in {"all", "pending", "completed", "no show"}:
            case_status = "all"
        gender = (q.get("gender") or "all").strip()
        if gender.lower() not in {"all", "male", "female", "other"}:
            gender = "all"
        age_min_raw = (q.get("age_min") or "").strip()
        age_max_raw = (q.get("age_max") or "").strip()
        barangay = (q.get("barangay") or "").strip()
        site_of_bite = (q.get("site") or "").strip()
        animal_type = (q.get("animal_type") or "all").strip()
        if animal_type.lower() not in {"all", "dog", "cat", "others"}:
            animal_type = "all"
        animal_status = (q.get("animal_status") or "all").strip()
        if animal_status.lower() not in {"all", "healthy", "killed", "sick", "lost", "died"}:
            animal_status = "all"
        animal_vacc = (q.get("animal_vaccination") or "all").strip()
        if animal_vacc.lower() not in {"all", "updated", "not updated", "none"}:
            animal_vacc = "all"
        bio = (q.get("bio") or "all").strip()
        if bio.lower() not in {"all", "anti-rabies", "hrig/erig", "tetanus"}:
            bio = "all"
        batch = (q.get("batch") or "").strip()
        date_from = (q.get("date_from") or "").strip()
        date_to = (q.get("date_to") or "").strip()

        where_clauses = [
            "c.clinic_id = ?",
            _SQL_STAFF_CASE_NOT_REMOVED,
            "LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')",
        ]
        params: list[object] = [clinic_id]
        if category != "all":
            where_clauses.append("LOWER(COALESCE(c.risk_level, c.category, '')) = ?")
            params.append(category)
        if case_status != "all":
            where_clauses.append("LOWER(COALESCE(c.case_status, 'pending')) = ?")
            params.append(case_status)
        if search:
            search_like = f"%{search.lower()}%"
            search_parts = ["LOWER(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')) LIKE ?"]
            search_params: list[object] = [search_like]
            case_id_search = search.lower().removeprefix("c-")
            if case_id_search.isdigit():
                search_parts.append("c.id = ?")
                search_params.append(int(case_id_search))
            where_clauses.append("(" + " OR ".join(search_parts) + ")")
            params.extend(search_params)
        if gender.lower() != "all":
            where_clauses.append("LOWER(COALESCE(p.gender, '')) = ?")
            params.append(gender.lower())

        def _try_int(s: str) -> int | None:
            try:
                return int(s)
            except Exception:
                return None

        age_min = _try_int(age_min_raw) if age_min_raw else None
        age_max = _try_int(age_max_raw) if age_max_raw else None
        if age_min is not None:
            where_clauses.append("COALESCE(p.age, 0) >= ?")
            params.append(age_min)
        if age_max is not None:
            where_clauses.append("COALESCE(p.age, 0) <= ?")
            params.append(age_max)
        if barangay:
            where_clauses.append(_sql_patient_barangay_lowercase_like())
            params.append(f"%{barangay.lower()}%")
        if site_of_bite:
            where_clauses.append("LOWER(COALESCE(c.affected_area, '')) LIKE ?")
            params.append(f"%{site_of_bite.lower()}%")
        if animal_type.lower() != "all":
            if animal_type.lower() == "dog":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'dog%'")
            elif animal_type.lower() == "cat":
                where_clauses.append("LOWER(COALESCE(c.animal_detail, '')) LIKE 'cat%'")
            else:
                where_clauses.append(
                    "(LOWER(COALESCE(c.animal_detail, '')) LIKE 'others%' OR (LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'dog%' AND LOWER(COALESCE(c.animal_detail, '')) NOT LIKE 'cat%'))"
                )
        if animal_status.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_condition, '')) = ?")
            params.append(animal_status.lower())
        if animal_vacc.lower() != "all":
            where_clauses.append("LOWER(COALESCE(c.animal_vaccination, '')) = ?")
            params.append(animal_vacc.lower())
        if date_from:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)")
            params.append(date_from)
        if date_to:
            where_clauses.append("DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)")
            params.append(date_to)
        if batch:
            where_clauses.append(
                """(
                    LOWER(COALESCE(vc.pcec_batch, '')) LIKE ?
                    OR EXISTS (
                        SELECT 1 FROM vaccination_records vr
                        WHERE vr.case_id = c.id
                          AND LOWER(COALESCE(vr.vaccine_brand_batch, '')) LIKE ?
                        LIMIT 1
                    )
                )"""
            )
            b_like = f"%{batch.lower()}%"
            params.extend([b_like, b_like])
        if bio.lower() != "all":
            if bio.lower() == "anti-rabies":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.anti_rabies, '')), '') IS NOT NULL
                      OR EXISTS (
                        SELECT 1 FROM vaccination_card_doses vcd
                        WHERE vcd.case_id = c.id AND NULLIF(TRIM(COALESCE(vcd.dose_date,'')), '') IS NOT NULL
                        LIMIT 1
                      )
                    )"""
                )
            elif bio.lower() == "hrig/erig":
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.erig_hrig, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.htig, '')), '') IS NOT NULL
                      OR COALESCE(psd.hrtig_immunization, 0) = 1
                    )"""
                )
            else:
                where_clauses.append(
                    """(
                      NULLIF(TRIM(COALESCE(vc.tetanus_prophylaxis, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.tetanus_toxoid, '')), '') IS NOT NULL
                      OR NULLIF(TRIM(COALESCE(vc.ats, '')), '') IS NOT NULL
                    )"""
                )
        where_sql = " AND ".join(where_clauses)
        # -- End duplicated filter builder --

        raw = db.execute(
            """
            SELECT
              c.id AS case_id,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown'
              ) AS patient_name,
              p.gender,
              p.age,
              p.phone_number,
              p.barangay,
              p.address,
              c.exposure_date,
              c.exposure_time,
              c.place_of_exposure,
              c.affected_area,
              c.type_of_exposure,
              c.animal_detail,
              c.animal_condition,
              c.animal_vaccination,
              COALESCE(
                NULLIF(TRIM(c.who_category_final), ''),
                NULLIF(TRIM(c.who_category_auto), ''),
                NULLIF(TRIM(c.risk_level), ''),
                NULLIF(TRIM(c.category), ''),
                'Unknown'
              ) AS category,
              COALESCE(c.case_status, 'Pending') AS case_status,
              psd.wound_description,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.prev_vaccine_date,
              psd.tetanus_date,
              psd.hrtig_immunization,
              vc.anti_rabies,
              vc.pvrv,
              vc.pcec_batch,
              vc.pcec_mfg_date,
              vc.pcec_expiry,
              vc.erig_hrig,
              vc.tetanus_prophylaxis,
              vc.tetanus_toxoid,
              vc.ats,
              vc.htig,
              vc.tetanus_batch,
              vc.tetanus_mfg_date,
              vc.tetanus_expiry,
              vc.remarks
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            LEFT JOIN vaccination_card vc ON vc.case_id = c.id
            WHERE
            """
            + where_sql
            + """
            ORDER BY datetime(c.created_at) DESC, c.id DESC
            """,
            params,
        ).fetchall()

        case_ids = [int(r["case_id"]) for r in raw]
        doses_by_case: dict[int, list[dict[str, object]]] = {}
        if case_ids:
            placeholders = ",".join(["?"] * len(case_ids))
            dose_rows = db.execute(
                f"""
                SELECT case_id, record_type, day_number, dose_date, type_of_vaccine, dose, route_site, given_by
                FROM vaccination_card_doses
                WHERE case_id IN ({placeholders})
                ORDER BY case_id, record_type, day_number
                """,
                case_ids,
            ).fetchall()
            for dr in dose_rows:
                cid = int(dr["case_id"])
                doses_by_case.setdefault(cid, []).append(dict(dr))

        rows = []
        for r in raw:
            d = dict(r)
            cid = int(d["case_id"])
            d["case_code"] = f"C-000{cid}"
            d["barangay"] = _barangay_export_value(d.get("barangay"), d.get("address"))
            d["vaccination_doses"] = doses_by_case.get(cid, [])
            rows.append(d)

        filters_summary = "Exported with current filters."
        html = render_template(
            "staff_cases_export_pdf.html",
            clinic_name=clinic_name,
            generated_at=datetime.now().strftime("%b %d, %Y %I:%M %p"),
            filters_summary=filters_summary,
            rows=rows,
        )
        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again.", "error")
            return redirect(url_for("staff_patients", **q))
        data = pdf_io.getvalue()
        resp = make_response(data)
        resp.headers["Content-Type"] = "application/pdf"
        resp.headers["Content-Disposition"] = 'attachment; filename="cases_export.pdf"'
        return resp

    def _canonical_vaccination_dose_preset(raw_value: str) -> str:
        value = (raw_value or "").strip().lower().replace(" ", "")
        if value in {"0.1", "0.1ml"}:
            return "0.1ml"
        if value in {"0.5", "0.5ml"}:
            return "0.5ml"
        if value in {"1", "1.0", "1ml", "1.0ml"}:
            return "1ml"
        if value in {"other", "others"}:
            return "others"
        return ""

    def _dose_query_from_preset(dose_preset: str) -> str:
        if dose_preset == "0.1ml":
            return "0.1"
        if dose_preset == "0.5ml":
            return "0.5"
        if dose_preset == "1ml":
            return "1.0"
        return ""

    def _resolve_vaccinations_dose_filter(value_getter) -> tuple[str, str, str]:
        raw_dose_query = (value_getter("dose_query") or "").strip()
        dose_preset = _canonical_vaccination_dose_preset(value_getter("dose_preset") or "")
        dose_other = (value_getter("dose_other") or "").strip()

        if dose_preset in {"0.1ml", "0.5ml", "1ml"}:
            dose_query = _dose_query_from_preset(dose_preset)
            dose_other = ""
        elif dose_preset == "others":
            dose_query = dose_other
        else:
            inferred_preset = _canonical_vaccination_dose_preset(raw_dose_query)
            if inferred_preset in {"0.1ml", "0.5ml", "1ml"}:
                dose_preset = inferred_preset
                dose_other = ""
                dose_query = _dose_query_from_preset(inferred_preset)
            elif raw_dose_query:
                dose_preset = "others"
                dose_other = raw_dose_query
                dose_query = raw_dose_query
            else:
                dose_query = ""
                dose_preset = ""
                dose_other = ""

        return dose_query, dose_preset, dose_other

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

        staff_display_name = _staff_display_name(staff)

        vaccine_type = (request.args.get("vaccine_type") or "").strip()
        dose_query, dose_preset, dose_other = _resolve_vaccinations_dose_filter(
            lambda key: request.args.get(key, "")
        )
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

        # Default view: vaccinations from the past week and the next week.
        if not date_from and not date_to:
            today = datetime.now().date()
            date_to = (today + timedelta(days=7)).isoformat()
            date_from = (today - timedelta(days=7)).isoformat()

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
            date_filters_records += " AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)"
            date_filters_card += " AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)"
            base_records_params.append(date_from)
            base_card_params.append(date_from)
        if date_to:
            date_filters_records += " AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)"
            date_filters_card += " AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)"
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
              AND COALESCE(c.staff_removed, 0) = 0
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
              AND COALESCE(c.staff_removed, 0) = 0
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
            dose_preset=dose_preset,
            dose_other=dose_other,
            date_from=date_from,
            date_to=date_to,
            administered_by=administered_by,
            administered_by_options=administered_by_options,
            sort_by=sort_by,
            sort_dir=sort_dir,
            breadcrumbs=breadcrumbs,
            active_page="vaccinations",
        )

    def _staff_vaccinations_export_rows(staff, q: dict[str, str]) -> list[dict[str, object]]:
        db = get_db()

        vaccine_type = (q.get("vaccine_type") or "").strip()
        dose_query, dose_preset, dose_other = _resolve_vaccinations_dose_filter(
            lambda key: q.get(key, "")
        )
        q["dose_query"] = dose_query
        q["dose_preset"] = dose_preset
        q["dose_other"] = dose_other
        date_from = (q.get("date_from") or "").strip()
        date_to = (q.get("date_to") or "").strip()
        administered_by = (q.get("administered_by") or "").strip()
        sort_by = (q.get("sort_by") or "date").strip().lower()
        sort_dir = (q.get("sort_dir") or "desc").strip().lower()
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
            date_from = ""
            date_to = ""

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
                NULLIF(TRIM(cp.title || ' ' || cp.first_name || ' ' || cp.last_name), ''),
                NULLIF(TRIM(cp.first_name || ' ' || cp.last_name), ''),
                NULLIF(TRIM(u_staff.username), ''),
                'Unknown Staff'
              ) AS administered_by_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown'
              ) AS patient_name
            FROM vaccination_records vr
            JOIN cases c ON c.id = vr.case_id
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN clinic_personnel cp ON cp.id = vr.administered_by_personnel_id
            LEFT JOIN users u_staff ON u_staff.id = cp.user_id
            WHERE c.clinic_id = ?
            """,
            (staff["clinic_id"],),
        ).fetchall()

        card_rows = db.execute(
            """
            SELECT
              vcd.rowid AS id,
              vcd.case_id,
              vcd.type_of_vaccine AS vaccine_type,
              CAST(vcd.day_number AS TEXT) AS dose_number,
              vcd.dose AS dose_amount,
              vcd.dose_date AS date_administered,
              COALESCE(NULLIF(TRIM(vcd.given_by), ''), '') AS administered_by_name,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown'
              ) AS patient_name
            FROM vaccination_card_doses vcd
            JOIN cases c ON c.id = vcd.case_id
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            WHERE c.clinic_id = ?
              AND TRIM(COALESCE(vcd.dose_date, '')) <> ''
              AND TRIM(COALESCE(vcd.type_of_vaccine, '')) <> ''
              AND TRIM(COALESCE(vcd.given_by, '')) <> ''
            """,
            (staff["clinic_id"],),
        ).fetchall()

        normalized_rows: list[dict[str, object]] = []
        seen_keys: set[tuple] = set()

        def _safe_date(value: str) -> str:
            return _normalize_iso_date(value)

        for source, rows in [("records", records_rows), ("card", card_rows)]:
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
                        "case_id": int(row["case_id"]),
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
        filtered_rows: list[dict[str, object]] = []
        for row in normalized_rows:
            if vaccine_type_l and vaccine_type_l not in (row["vaccine_type"] or "").lower():
                continue
            if dose_query_l:
                dose_haystack = f"{row.get('dose_number','')} {row.get('dose_amount','')}".lower()
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

        def _sort_key(r: dict[str, object]):
            if sort_by == "vaccine_type":
                return (r.get("vaccine_type") or "").__str__().lower()
            if sort_by == "dose":
                return f"{r.get('dose_number','')} {r.get('dose_amount','')}".lower()
            if sort_by == "administered_by":
                return (r.get("administered_by_name") or "").__str__().lower()
            if sort_by == "patient":
                return (r.get("patient_name") or "").__str__().lower()
            return r.get("date_iso") or ""

        filtered_rows.sort(key=_sort_key, reverse=(sort_dir == "desc"))

        out: list[dict[str, object]] = []
        for r in filtered_rows:
            date_display = r.get("date_iso") or "N/A"
            if r.get("date_iso"):
                try:
                    date_display = datetime.fromisoformat(str(r["date_iso"])).strftime("%b %d, %Y")
                except ValueError:
                    date_display = r.get("date_iso") or "N/A"
            dose_display = (r.get("dose_number") or "").__str__()
            dose_amount = (r.get("dose_amount") or "").__str__()
            source = (r.get("source") or "").__str__().strip().lower()
            # In exports, card rows should show the actual dose amount only,
            # not schedule day numbers (e.g., day 0, day 3).
            if source == "card":
                dose_display = dose_amount or "N/A"
            elif dose_amount:
                dose_display = f"{dose_display} ({dose_amount})" if dose_display else dose_amount
            out.append(
                {
                    "source": r.get("source") or "",
                    "case_id": r.get("case_id"),
                    "case_code": r.get("case_code") or "",
                    "patient_name": r.get("patient_name") or "N/A",
                    "vaccine_type": r.get("vaccine_type") or "N/A",
                    "dose_display": dose_display or "N/A",
                    "date_given": date_display or "N/A",
                    "administered_by_name": r.get("administered_by_name") or "N/A",
                }
            )
        return out

    @app.get("/staff/vaccinations/export.csv")
    @role_required("clinic_personnel", "system_admin")
    def staff_vaccinations_export_csv():
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

        q = {
            "vaccine_type": request.args.get("vaccine_type", ""),
            "dose_query": request.args.get("dose_query", ""),
            "dose_preset": request.args.get("dose_preset", ""),
            "dose_other": request.args.get("dose_other", ""),
            "date_from": request.args.get("date_from", ""),
            "date_to": request.args.get("date_to", ""),
            "administered_by": request.args.get("administered_by", ""),
            "sort_by": request.args.get("sort_by", ""),
            "sort_dir": request.args.get("sort_dir", ""),
        }
        dose_query, dose_preset, dose_other = _resolve_vaccinations_dose_filter(
            lambda key: q.get(key, "")
        )
        q["dose_query"] = dose_query
        q["dose_preset"] = dose_preset
        q["dose_other"] = dose_other
        rows = _staff_vaccinations_export_rows(staff, q)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "Case ID",
                "Case Code",
                "Patient",
                "Vaccine Type",
                "Dose",
                "Date Given",
                "Administered By",
                "Source",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.get("case_id") or "",
                    r.get("case_code") or "",
                    r.get("patient_name") or "",
                    r.get("vaccine_type") or "",
                    r.get("dose_display") or "",
                    r.get("date_given") or "",
                    r.get("administered_by_name") or "",
                    r.get("source") or "",
                ]
            )

        data = output.getvalue().encode("utf-8-sig")
        resp = make_response(data)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = 'attachment; filename="vaccinations_export.csv"'
        return resp

    @app.get("/staff/vaccinations/export.pdf")
    @role_required("clinic_personnel", "system_admin")
    def staff_vaccinations_export_pdf():
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))
        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable. Please contact the clinic.", "error")
            return redirect(url_for("staff_vaccinations"))

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

        clinic_row = db.execute(
            "SELECT name FROM clinics WHERE id = ?",
            (staff["clinic_id"],),
        ).fetchone()
        clinic_name = clinic_row["name"] if clinic_row else ""

        q = {
            "vaccine_type": request.args.get("vaccine_type", ""),
            "dose_query": request.args.get("dose_query", ""),
            "dose_preset": request.args.get("dose_preset", ""),
            "dose_other": request.args.get("dose_other", ""),
            "date_from": request.args.get("date_from", ""),
            "date_to": request.args.get("date_to", ""),
            "administered_by": request.args.get("administered_by", ""),
            "sort_by": request.args.get("sort_by", ""),
            "sort_dir": request.args.get("sort_dir", ""),
        }
        dose_query, dose_preset, dose_other = _resolve_vaccinations_dose_filter(
            lambda key: q.get(key, "")
        )
        q["dose_query"] = dose_query
        q["dose_preset"] = dose_preset
        q["dose_other"] = dose_other
        rows = _staff_vaccinations_export_rows(staff, q)

        filters_parts = []
        for label, key in [
            ("Vaccine type", "vaccine_type"),
            ("Dose", "dose_query"),
            ("From", "date_from"),
            ("To", "date_to"),
            ("Administered by", "administered_by"),
        ]:
            v = (q.get(key) or "").strip()
            if v:
                filters_parts.append(f"{label}: {v}")
        filters_summary = ", ".join(filters_parts) if filters_parts else "All"

        html = render_template(
            "staff_vaccinations_export_pdf.html",
            clinic_name=clinic_name,
            generated_at=datetime.now().strftime("%b %d, %Y %I:%M %p"),
            filters_summary=filters_summary,
            rows=rows,
        )
        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again.", "error")
            return redirect(url_for("staff_vaccinations", **q))
        data = pdf_io.getvalue()
        resp = make_response(data)
        resp.headers["Content-Type"] = "application/pdf"
        resp.headers["Content-Disposition"] = 'attachment; filename="vaccinations_export.pdf"'
        return resp

    @app.get("/staff/reports")
    @role_required("clinic_personnel", "system_admin")
    def staff_reports():
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

        staff_display_name = _staff_display_name(staff)

        def _normalize_iso_date(raw_value: str) -> str:
            value = (raw_value or "").strip()
            if not value:
                return ""
            try:
                return datetime.fromisoformat(value).date().isoformat()
            except ValueError:
                return ""

        today = datetime.now().date()
        date_from = _normalize_iso_date(request.args.get("date_from") or "")
        date_to = _normalize_iso_date(request.args.get("date_to") or "")
        if not date_from and not date_to:
            date_to = today.isoformat()
            date_from = (today - timedelta(days=13)).isoformat()
        elif date_from and not date_to:
            date_to = today.isoformat()
        elif date_to and not date_from:
            try:
                date_from = (date.fromisoformat(date_to) - timedelta(days=13)).isoformat()
            except ValueError:
                date_from = (today - timedelta(days=13)).isoformat()
        if date_from and date_to and date_from > date_to:
            flash("Date range is invalid. 'From' date must be on or before 'To' date.", "error")
            date_to = today.isoformat()
            date_from = (today - timedelta(days=13)).isoformat()

        try:
            recent_page = int(request.args.get("recent_page", "1"))
        except ValueError:
            recent_page = 1
        recent_page = 1 if recent_page < 1 else recent_page
        recent_per_page = 12

        clinic_id = staff["clinic_id"]
        _run_case_status_maintenance(clinic_id)
        staff_visible_case_filter_sql = f"""
              AND {_SQL_STAFF_CASE_NOT_REMOVED}
              AND LOWER(COALESCE(c.case_status, 'pending')) NOT IN ('archived', 'queued', 'scheduled')
        """

        case_status_row = db.execute(
            """
            SELECT
              COUNT(*) AS total_cases,
              SUM(CASE WHEN LOWER(COALESCE(c.case_status, 'pending')) = 'pending' THEN 1 ELSE 0 END) AS pending_cases,
              SUM(CASE WHEN LOWER(COALESCE(c.case_status, 'pending')) = 'completed' THEN 1 ELSE 0 END) AS completed_cases,
              SUM(CASE WHEN LOWER(COALESCE(c.case_status, 'pending')) = 'no show' THEN 1 ELSE 0 END) AS no_show_cases
            FROM cases c
            WHERE c.clinic_id = ?
            """
            + staff_visible_case_filter_sql
            + """
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) <= DATE(?)
            """,
            (clinic_id, date_from, date_to),
        ).fetchone()

        appointment_status_row = db.execute(
            """
            SELECT
              COUNT(*) AS total_appointments
            FROM appointments a
            WHERE a.clinic_id = ?
              AND DATE(a.appointment_datetime) >= DATE(?)
              AND DATE(a.appointment_datetime) <= DATE(?)
            """,
            (clinic_id, date_from, date_to),
        ).fetchone()

        category_rows = db.execute(
            """
            SELECT
              CASE
                WHEN LOWER(COALESCE(c.risk_level, c.category, '')) IN ('category i', 'category 1', 'i', '1') THEN 'Category I'
                WHEN LOWER(COALESCE(c.risk_level, c.category, '')) IN ('category ii', 'category 2', 'ii', '2') THEN 'Category II'
                WHEN LOWER(COALESCE(c.risk_level, c.category, '')) IN ('category iii', 'category 3', 'iii', '3') THEN 'Category III'
                ELSE 'Unspecified'
              END AS category_label,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            """
            + staff_visible_case_filter_sql
            + """
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
            GROUP BY category_label
            ORDER BY total DESC, category_label ASC
            """,
            (clinic_id, date_from, date_to),
        ).fetchall()

        records_rows = db.execute(
            """
            SELECT
              vr.case_id,
              vr.vaccine_type,
              vr.dose_number,
              vr.dose_amount,
              vr.date_administered,
              COALESCE(
                NULLIF(TRIM(COALESCE(cp.title, '') || ' ' || COALESCE(cp.first_name, '') || ' ' || COALESCE(cp.last_name, '')), ''),
                au.username,
                'Unknown Staff'
              ) AS administered_by_name
            FROM vaccination_records vr
            JOIN cases c ON c.id = vr.case_id
            LEFT JOIN clinic_personnel cp ON cp.id = vr.administered_by_personnel_id
            LEFT JOIN users au ON au.id = cp.user_id
            WHERE c.clinic_id = ?
              AND DATE(vr.date_administered) >= DATE(?)
              AND DATE(vr.date_administered) <= DATE(?)
            """,
            (clinic_id, date_from, date_to),
        ).fetchall()

        card_rows = db.execute(
            """
            SELECT
              vcd.case_id,
              vcd.type_of_vaccine AS vaccine_type,
              CAST(vcd.day_number AS TEXT) AS dose_number,
              vcd.dose AS dose_amount,
              vcd.dose_date AS date_administered,
              TRIM(COALESCE(vcd.given_by, '')) AS administered_by_name
            FROM vaccination_card_doses vcd
            JOIN cases c ON c.id = vcd.case_id
            WHERE c.clinic_id = ?
              AND DATE(vcd.dose_date) >= DATE(?)
              AND DATE(vcd.dose_date) <= DATE(?)
              AND TRIM(COALESCE(vcd.dose_date, '')) <> ''
              AND TRIM(COALESCE(vcd.type_of_vaccine, '')) <> ''
              AND TRIM(COALESCE(vcd.given_by, '')) <> ''
            """,
            (clinic_id, date_from, date_to),
        ).fetchall()

        normalized_vax_rows = []
        seen_vax_keys = set()

        def _safe_date(raw_value: str) -> str:
            try:
                return datetime.fromisoformat((raw_value or "").strip()).date().isoformat()
            except ValueError:
                return ""

        for rows in (records_rows, card_rows):
            for row in rows:
                date_iso = _safe_date(row["date_administered"] or "")
                vaccine_type = (row["vaccine_type"] or "").strip()
                dose_number = (row["dose_number"] or "").strip()
                dose_amount = (row["dose_amount"] or "").strip()
                administered_by_name = (row["administered_by_name"] or "").strip()
                dedupe_key = (
                    row["case_id"],
                    date_iso,
                    vaccine_type.lower(),
                    dose_number.lower(),
                    dose_amount.lower(),
                    administered_by_name.lower(),
                )
                if dedupe_key in seen_vax_keys:
                    continue
                seen_vax_keys.add(dedupe_key)
                normalized_vax_rows.append(
                    {
                        "case_id": row["case_id"],
                        "date_iso": date_iso,
                        "vaccine_type": vaccine_type or "N/A",
                        "administered_by_name": administered_by_name or "Unknown Staff",
                    }
                )

        total_vaccinations = len(normalized_vax_rows)
        vaccine_type_counts: dict[str, int] = {}
        administered_by_counts: dict[str, int] = {}
        for row in normalized_vax_rows:
            vt = row["vaccine_type"]
            vaccine_type_counts[vt] = vaccine_type_counts.get(vt, 0) + 1
            staff_name = row["administered_by_name"]
            administered_by_counts[staff_name] = administered_by_counts.get(staff_name, 0) + 1

        vaccine_type_breakdown = [
            {"label": label, "count": count}
            for label, count in sorted(vaccine_type_counts.items(), key=lambda x: (-x[1], x[0].lower()))
        ][:8]
        top_administered_by = [
            {"name": name, "count": count}
            for name, count in sorted(administered_by_counts.items(), key=lambda x: (-x[1], x[0].lower()))
        ][:8]

        day_cursor = date.fromisoformat(date_from)
        day_end = date.fromisoformat(date_to)
        day_keys = []
        while day_cursor <= day_end:
            day_keys.append(day_cursor.isoformat())
            day_cursor += timedelta(days=1)

        cases_by_day_rows = db.execute(
            """
            SELECT
              DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) AS day_key,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            """
            + staff_visible_case_filter_sql
            + """
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) <= DATE(?)
            GROUP BY DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at))
            """,
            (clinic_id, date_from, date_to),
        ).fetchall()
        cases_by_day = {row["day_key"]: int(row["total"] or 0) for row in cases_by_day_rows}
        vaccinations_by_day: dict[str, int] = {}
        for row in normalized_vax_rows:
            day_key = row["date_iso"]
            if not day_key:
                continue
            vaccinations_by_day[day_key] = vaccinations_by_day.get(day_key, 0) + 1

        daily_labels = []
        daily_case_counts = []
        daily_vaccination_counts = []
        daily_activity_rows = []
        for day_key in day_keys:
            try:
                label = datetime.fromisoformat(day_key).strftime("%b %d")
                full_label = datetime.fromisoformat(day_key).strftime("%b %d, %Y")
            except ValueError:
                label = day_key
                full_label = day_key
            case_count = int(cases_by_day.get(day_key, 0))
            vaccination_count = int(vaccinations_by_day.get(day_key, 0))
            daily_labels.append(label)
            daily_case_counts.append(case_count)
            daily_vaccination_counts.append(vaccination_count)
            daily_activity_rows.append(
                {
                    "day_iso": day_key,
                    "day_label": full_label,
                    "new_cases_count": case_count,
                    "vaccinations_count": vaccination_count,
                }
            )

        daily_cases_total = sum(daily_case_counts)
        daily_vaccinations_total = sum(daily_vaccination_counts)
        daily_activity_empty = (daily_cases_total == 0 and daily_vaccinations_total == 0)

        peak_cases_row = (
            max(daily_activity_rows, key=lambda row: row["new_cases_count"])
            if daily_activity_rows
            else None
        )
        peak_vax_row = (
            max(daily_activity_rows, key=lambda row: row["vaccinations_count"])
            if daily_activity_rows
            else None
        )
        daily_activity_summary = {
            "total_new_cases": daily_cases_total,
            "total_vaccinations": daily_vaccinations_total,
            "peak_cases_count": peak_cases_row["new_cases_count"] if peak_cases_row else 0,
            "peak_cases_day": (
                peak_cases_row["day_label"] if peak_cases_row and peak_cases_row["new_cases_count"] > 0 else "N/A"
            ),
            "peak_vaccinations_count": peak_vax_row["vaccinations_count"] if peak_vax_row else 0,
            "peak_vaccinations_day": (
                peak_vax_row["day_label"] if peak_vax_row and peak_vax_row["vaccinations_count"] > 0 else "N/A"
            ),
        }

        total_category = sum(int(row["total"] or 0) for row in category_rows)
        category_breakdown = []
        for row in category_rows:
            count = int(row["total"] or 0)
            pct = round((count / total_category) * 100) if total_category else 0
            category_breakdown.append({"label": row["category_label"], "count": count, "percent": pct})

        total_recent = db.execute(
            """
            SELECT COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
            """
            + staff_visible_case_filter_sql
            + """
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) <= DATE(?)
            """,
            (clinic_id, date_from, date_to),
        ).fetchone()["total"]
        total_recent = int(total_recent or 0)

        recent_pages = max((total_recent + recent_per_page - 1) // recent_per_page, 1)
        if recent_page > recent_pages:
            recent_page = recent_pages
        recent_offset = (recent_page - 1) * recent_per_page

        recent_case_rows = db.execute(
            """
            SELECT
              c.id,
              c.exposure_date,
              COALESCE(c.risk_level, c.category, 'N/A') AS category,
              COALESCE(c.case_status, 'Pending') AS case_status,
              COALESCE(
                NULLIF(TRIM(COALESCE(p.first_name, '') || ' ' || COALESCE(p.last_name, '')), ''),
                u.username,
                'Unknown Patient'
              ) AS patient_name
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            WHERE c.clinic_id = ?
            """
            + staff_visible_case_filter_sql
            + """
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.exposure_date, ''), c.created_at)) <= DATE(?)
            ORDER BY DATE(c.exposure_date) DESC, c.id DESC
            LIMIT ? OFFSET ?
            """,
            (clinic_id, date_from, date_to, recent_per_page, recent_offset),
        ).fetchall()
        recent_cases = []
        for row in recent_case_rows:
            exposure_date = (row["exposure_date"] or "").strip()
            exposure_display = exposure_date
            if exposure_date:
                try:
                    exposure_display = datetime.fromisoformat(exposure_date).strftime("%b %d, %Y")
                except ValueError:
                    exposure_display = exposure_date
            recent_cases.append(
                {
                    "id": row["id"],
                    "case_code": f"C-000{row['id']}",
                    "patient_name": row["patient_name"],
                    "category": row["category"],
                    "case_status": row["case_status"],
                    "exposure_date": exposure_display or "N/A",
                }
            )

        recent_cases_pagination = SimplePagination(
            recent_cases, page=recent_page, per_page=recent_per_page, total=total_recent
        )

        kpi = {
            "total_cases": int(case_status_row["total_cases"] or 0),
            "pending_cases": int(case_status_row["pending_cases"] or 0),
            "completed_cases": int(case_status_row["completed_cases"] or 0),
            "no_show_cases": int(case_status_row["no_show_cases"] or 0),
            "total_vaccinations": total_vaccinations,
            "total_appointments": int(appointment_status_row["total_appointments"] or 0),
        }

        breadcrumbs = [
            {"label": "Home", "href": url_for("staff_dashboard")},
            {"label": "Operations", "href": None},
        ]

        return render_template(
            "staff_reports.html",
            staff=staff,
            staff_display_name=staff_display_name,
            date_from=date_from,
            date_to=date_to,
            kpi=kpi,
            category_breakdown=category_breakdown,
            vaccine_type_breakdown=vaccine_type_breakdown,
            top_administered_by=top_administered_by,
            daily_labels=daily_labels,
            daily_case_counts=daily_case_counts,
            daily_vaccination_counts=daily_vaccination_counts,
            daily_activity_rows=daily_activity_rows,
            daily_activity_summary=daily_activity_summary,
            daily_activity_empty=daily_activity_empty,
            recent_cases=recent_cases_pagination,
            breadcrumbs=breadcrumbs,
            active_page="reports",
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

        staff_display_name = _staff_display_name(staff)

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
                      AND LOWER(COALESCE(a.status, '')) NOT IN ('cancelled', 'canceled')) AS booking_count
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

    @app.post("/staff/appointments/availability/<int:slot_id>/deactivate")
    @role_required("clinic_personnel", "system_admin")
    def staff_availability_deactivate(slot_id: int):
        # Staff never physically removes availability_slots rows; only is_active=0 (hidden from booking).
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
            flash("Slot removed from the booking schedule (record kept).", "success")
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

        staff_display_name = _staff_display_name(staff)

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
              COALESCE(
                NULLIF(TRIM(c.who_category_final), ''),
                NULLIF(TRIM(c.who_category_auto), ''),
                NULLIF(TRIM(c.risk_level), ''),
                NULLIF(TRIM(c.category), ''),
                'N/A'
              ) AS category,
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

        if (appt["type"] or "").strip() == "Walk-in":
            return redirect(url_for("view_patient_case", case_id=appt["case_id"]))

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

        appt_status_lower = (appt["status"] or "").strip().lower()
        appointment_is_expired = appt_status_lower == "expired"

        return render_template(
            "staff_appointment_view.html",
            staff=staff,
            staff_display_name=staff_display_name,
            appointment=appt,
            patient_name=patient_name,
            appointment_date=appt_date,
            appointment_time=appt_time,
            requested_schedule_display=requested_schedule_display,
            appointment_is_expired=appointment_is_expired,
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
            "SELECT id, case_id, patient_id, status FROM appointments WHERE id = ? AND clinic_id = ?",
            (appointment_id, staff["clinic_id"]),
        ).fetchone()
        if appt is None:
            flash("Appointment not found.", "error")
            return redirect(url_for("staff_appointments"))

        if (appt["status"] or "").strip().lower() == "expired":
            flash(
                "This booking request expired. Reschedule to a new slot before approving.",
                "error",
            )
            return redirect(url_for("view_appointment", appointment_id=appointment_id))

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

        if _is_slot_in_past(slot_datetime):
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

        staff_display_name = _staff_display_name(staff)

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
              AND COALESCE(c.staff_removed, 0) = 0
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()

        if case_row is None:
            return None

        case_dict = dict(case_row)
        who_reasons: list[dict] = []
        raw_reasons = (case_dict.get("who_category_reasons_json") or "").strip()
        if raw_reasons:
            try:
                parsed = json.loads(raw_reasons)
                if isinstance(parsed, list):
                    who_reasons = [r for r in parsed if isinstance(r, dict)]
            except Exception:
                who_reasons = []
        case_dict["who_category_reasons"] = who_reasons

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
        _normalize_vaccination_card_date_fields(vaccination_card)
        vaccination_card["form_vc_anti_rabies_vaccine"] = _anti_rabies_vaccine_prefill_from_db(vaccination_card)
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
            {"label": "Cases", "href": url_for("staff_patients")},
            {"label": case_row["patient_name"], "href": None},
        ]

        return {
            "db": db,
            "staff": staff,
            "staff_display_name": staff_display_name,
            "case": case_dict,
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

    @app.post("/staff/cases/<int:case_id>/who-category/override")
    @role_required("clinic_personnel", "system_admin")
    def staff_override_who_category(case_id: int):
        if session.get("role") == "system_admin":
            return redirect(url_for("admin_dashboard"))

        db = get_db()
        staff = db.execute(
            """
            SELECT cp.id AS clinic_personnel_id, cp.clinic_id
            FROM clinic_personnel cp
            WHERE cp.user_id = ?
            """,
            (session["user_id"],),
        ).fetchone()
        if staff is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        new_final = (request.form.get("who_category_final") or "").strip()
        override_reason = (request.form.get("override_reason") or "").strip()
        allowed = {"Category I", "Category II", "Category III", "Unknown"}
        if new_final not in allowed:
            flash("Invalid WHO category selection.", "error")
            return redirect(url_for("edit_patient_case", case_id=case_id))
        if not override_reason:
            flash("Override reason is required.", "error")
            return redirect(url_for("edit_patient_case", case_id=case_id))
        if len(override_reason) > 300:
            flash("Override reason is too long (max 300 characters).", "error")
            return redirect(url_for("edit_patient_case", case_id=case_id))

        case_row = db.execute(
            """
            SELECT id, who_category_auto, who_category_final
            FROM cases
            WHERE id = ? AND clinic_id = ?
              AND COALESCE(staff_removed, 0) = 0
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        old_final = (case_row["who_category_final"] or "").strip() or (case_row["who_category_auto"] or "").strip()
        if old_final == new_final:
            flash("WHO category unchanged.", "info")
            return redirect(url_for("edit_patient_case", case_id=case_id))

        try:
            db.execute(
                """
                UPDATE cases
                SET who_category_final = ?,
                    who_category_overridden_by_user_id = ?,
                    who_category_overridden_at = CURRENT_TIMESTAMP,
                    who_category_override_reason = ?
                WHERE id = ? AND clinic_id = ?
                """,
                (new_final, session["user_id"], override_reason, case_id, staff["clinic_id"]),
            )
            _insert_medical_audit_log(
                db,
                case_id=case_id,
                action="UPDATE",
                field_name="who_category_final",
                old_value=old_final,
                new_value=new_final,
                change_reason=override_reason,
                user_id=session["user_id"],
                clinic_personnel_id=staff["clinic_personnel_id"],
            )
            db.commit()
        except Exception:
            db.rollback()
            flash("Failed to save WHO category override. Please try again.", "error")
            return redirect(url_for("edit_patient_case", case_id=case_id))

        flash("WHO category updated.", "success")
        return redirect(url_for("edit_patient_case", case_id=case_id))

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
              AND COALESCE(staff_removed, 0) = 0
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
            SELECT id, COALESCE(staff_removed, 0) AS staff_removed
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))
        if int(case_row["staff_removed"] or 0) == 1:
            flash("This case is already removed from the clinic list.", "info")
            return redirect(url_for("staff_patients"))

        removed_at = datetime.now().isoformat(timespec="seconds")
        db.execute(
            """
            UPDATE cases
            SET staff_removed = 1,
                staff_removed_at = ?,
                staff_removed_by_user_id = ?
            WHERE id = ? AND clinic_id = ?
            """,
            (removed_at, session["user_id"], case_id, staff["clinic_id"]),
        )
        _insert_medical_audit_log(
            db,
            case_id=case_id,
            action="DELETE",
            field_name="staff_removed",
            old_value="0",
            new_value="1",
            change_reason="Removed from staff list",
            user_id=session["user_id"],
            clinic_id=staff["clinic_id"],
        )
        db.commit()

        flash("Case removed from clinic list (record retained).", "success")
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
            SELECT id, COALESCE(case_status, 'Pending') AS case_status
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        marked_at = datetime.now().isoformat(timespec="seconds")
        db.execute(
            """
            UPDATE cases
            SET case_status = 'Completed',
                staff_completed_at = COALESCE(staff_completed_at, ?)
            WHERE id = ? AND clinic_id = ?
            """,
            (marked_at, case_id, staff["clinic_id"]),
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
        _insert_medical_audit_log(
            db,
            case_id=case_id,
            action="UPDATE",
            field_name="case_status",
            old_value=str(case_row["case_status"] or "Pending"),
            new_value="Completed",
            change_reason="Case marked completed",
            user_id=session["user_id"],
            clinic_id=staff["clinic_id"],
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
            SELECT cp.*, cp.id AS clinic_personnel_id, u.username
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

        case_row = db.execute(
            """
            SELECT
              c.id AS case_id,
              c.clinic_id,
              c.exposure_date,
              c.affected_area,
              c.animal_condition,
              c.type_of_exposure,
              c.animal_detail,
              c.category,
              c.risk_level,
              c.who_category_auto,
              c.who_category_final,
              c.who_category_reasons_json,
              c.who_category_override_reason,
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
              p.date_of_birth,
              p.gender,
              p.age,
              p.barangay,
              p.address,
              p.phone_number,
              u.email
            FROM cases c
            JOIN patients p ON p.id = c.patient_id
            LEFT JOIN users u ON u.id = p.user_id
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE c.id = ?
              AND c.clinic_id = ?
              AND COALESCE(c.staff_removed, 0) = 0
            """,
            (case_id, staff["clinic_id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("staff_patients"))

        case_patient = dict(case_row)
        def _editable_vaccination_record_types(final_category: str | None) -> set[str]:
            cat = (final_category or "").strip().lower()
            if cat == "category i":
                return {"pre_exposure", "booster"}
            if cat in {"category ii", "category iii"}:
                return {"post_exposure", "booster"}
            # Unknown/blank: keep all editable to avoid accidental data lockout.
            return {"pre_exposure", "post_exposure", "booster"}

        who_reasons_edit: list[dict] = []
        raw_r = (case_patient.get("who_category_reasons_json") or "").strip()
        if raw_r:
            try:
                parsed = json.loads(raw_r)
                if isinstance(parsed, list):
                    who_reasons_edit = [r for r in parsed if isinstance(r, dict)]
            except Exception:
                who_reasons_edit = []
        case_patient["who_category_reasons"] = who_reasons_edit

        if request.method == "POST":
            full_name = (request.form.get("full_name") or "").strip()
            age_raw = (request.form.get("age") or "").strip()
            date_of_birth = (request.form.get("date_of_birth") or "").strip()
            gender = normalize_name_case((request.form.get("gender") or "").strip())
            barangay = normalize_name_case((request.form.get("barangay") or "").strip())
            address = normalize_name_case((request.form.get("address") or "").strip())
            phone_number = (request.form.get("phone_number") or "").strip()
            email = (request.form.get("email") or "").strip().lower()
            exposure_date = (request.form.get("exposure_date") or "").strip()
            type_of_exposure = (request.form.get("type_of_exposure") or "").strip()
            if not type_of_exposure:
                type_of_exposure = (case_patient.get("type_of_exposure") or "").strip()
            animal_type = (request.form.get("animal_type") or "").strip()
            other_animal = normalize_name_case((request.form.get("other_animal") or "").strip())
            if animal_type == "Others" and other_animal:
                animal_detail = f"Others: {other_animal}"
            elif animal_type == "Others":
                animal_detail = "Others"
            elif animal_type:
                animal_detail = animal_type
            else:
                animal_detail = (case_patient.get("animal_detail") or "").strip()
            wound_description = (request.form.get("wound_description") or "").strip()
            if not wound_description:
                wound_description = (case_patient.get("wound_description") or "").strip()
            bleeding_type = (request.form.get("bleeding_type") or "").strip()
            if not bleeding_type:
                bleeding_type = (case_patient.get("bleeding_type") or "").strip()
            local_treatment_base = (request.form.get("local_treatment") or "").strip()
            other_treatment = normalize_name_case((request.form.get("other_treatment") or "").strip())
            if not local_treatment_base:
                local_treatment = (case_patient.get("local_treatment") or "").strip()
            elif local_treatment_base == "Others" and other_treatment:
                local_treatment = f"Others: {other_treatment}"
            elif local_treatment_base == "Others":
                local_treatment = "Others"
            else:
                local_treatment = local_treatment_base
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
                full_name = normalize_name_case(full_name)
                parts = full_name.split(" ", 1)
                first_name = parts[0]
                last_name = parts[1] if len(parts) > 1 else ""

            age = case_patient["age"]
            derived_age = _age_from_iso_date(date_of_birth) if date_of_birth else None
            if derived_age is not None:
                age = derived_age
            elif age_raw:
                try:
                    age = int(age_raw)
                except ValueError:
                    flash("Age must be a number.", "error")
                    return redirect(url_for("edit_patient_case", case_id=case_id))

            affected_area_use = (case_patient.get("affected_area") or "").strip()
            animal_status_use = (case_patient.get("animal_condition") or "").strip()
            risk_level = classify_pre_screening_risk(
                type_of_exposure=type_of_exposure,
                affected_area=affected_area_use,
                wound_description=wound_description,
                bleeding_type=bleeding_type,
                animal_status=animal_status_use,
                animal_vaccination="",
                patient_prev_immunization=patient_prev_immunization,
            )
            who_reasons_edit_save = _pre_screening_risk_reasons(
                type_of_exposure=type_of_exposure,
                affected_area=affected_area_use,
                wound_description=wound_description,
                bleeding_type=bleeding_type,
                animal_status=animal_status_use,
            )
            who_category_reasons_json_save = json.dumps(who_reasons_edit_save, ensure_ascii=False)
            who_ver = WHO_RULES_VERSION + "+doh-risk-v1"

            who_final_in = (request.form.get("who_category_final") or "").strip()
            override_reason_in = normalize_name_case((request.form.get("override_reason") or "").strip())
            allowed_who = {"Category I", "Category II", "Category III", "Unknown"}
            if who_final_in not in allowed_who:
                who_final_in = risk_level
            editable_record_types = _editable_vaccination_record_types(who_final_in)
            if len(override_reason_in) > 300:
                flash("Override reason is too long (max 300 characters).", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))

            old_who_final = (
                (case_patient.get("who_category_final") or "").strip()
                or (case_patient.get("who_category_auto") or "").strip()
            )

            # Require an override reason only when the user is actively changing the final category
            # to something different from the system category. If the final category was already
            # different (previous override) and the user didn't change it, don't block saving.
            if who_final_in != risk_level and who_final_in != old_who_final and not override_reason_in:
                flash(
                    "Reason for override is required when the final WHO category differs from the system category.",
                    "error",
                )
                return redirect(url_for("edit_patient_case", case_id=case_id))

            if who_final_in != risk_level:
                o_uid = session["user_id"]
                o_reason = override_reason_in
                o_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            else:
                o_uid = None
                o_reason = None
                o_at = None

            db.execute(
                """
                UPDATE patients
                SET first_name = ?,
                    last_name = ?,
                    date_of_birth = ?,
                    gender = ?,
                    age = ?,
                    barangay = ?,
                    address = ?,
                    phone_number = ?
                WHERE id = ?
                """,
                (
                    first_name if first_name is not None else case_patient["first_name"],
                    last_name if last_name is not None else case_patient["last_name"],
                    date_of_birth if date_of_birth else case_patient["date_of_birth"],
                    gender if gender else case_patient["gender"],
                    age,
                    barangay if barangay else case_patient.get("barangay"),
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
                    risk_level = ?,
                    who_category_auto = ?,
                    who_category_final = ?,
                    who_category_reasons_json = ?,
                    who_category_version = ?,
                    who_category_overridden_by_user_id = ?,
                    who_category_overridden_at = ?,
                    who_category_override_reason = ?
                WHERE id = ? AND clinic_id = ?
                """,
                (
                    exposure_date if exposure_date else case_patient["exposure_date"],
                    type_of_exposure if type_of_exposure else case_patient["type_of_exposure"],
                    animal_detail if animal_detail else case_patient["animal_detail"],
                    risk_level,
                    risk_level,
                    risk_level,
                    who_final_in,
                    who_category_reasons_json_save,
                    who_ver,
                    o_uid,
                    o_at,
                    o_reason,
                    case_id,
                    staff["clinic_id"],
                ),
            )

            if old_who_final != who_final_in:
                audit_reason = (
                    override_reason_in
                    if who_final_in != risk_level
                    else "Final category matches system category."
                )
                _insert_medical_audit_log(
                    db,
                    case_id=case_id,
                    action="UPDATE",
                    field_name="who_category_final",
                    old_value=old_who_final,
                    new_value=who_final_in,
                    change_reason=audit_reason,
                    user_id=session["user_id"],
                    clinic_personnel_id=staff["clinic_personnel_id"],
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
            vc_tetanus_mfg_date = _normalize_iso_date_input(_v("vc_tetanus_mfg_date"))
            vc_tetanus_expiry = _normalize_iso_date_input(_v("vc_tetanus_expiry"))
            today_iso = datetime.now().date().isoformat()
            if vc_pcec_expiry and vc_pcec_expiry < today_iso:
                flash("Expiry date cannot be earlier than today.", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))
            if vc_tetanus_expiry and vc_tetanus_expiry < today_iso:
                flash("Tetanus expiry date cannot be earlier than today.", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))

            if vc_pcec_mfg_date and vc_pcec_expiry and vc_pcec_mfg_date > vc_pcec_expiry:
                flash("Anti-rabies Mfg. date cannot be later than Expiry date.", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))
            if vc_tetanus_mfg_date and vc_tetanus_expiry and vc_tetanus_mfg_date > vc_tetanus_expiry:
                flash("Tetanus Mfg. date cannot be later than Expiry date.", "error")
                return redirect(url_for("edit_patient_case", case_id=case_id))

            existing_vc = db.execute(
                "SELECT anti_rabies, tetanus_prophylaxis FROM vaccination_card WHERE case_id = ?",
                (case_id,),
            ).fetchone()
            anti_rabies_preserved = (existing_vc["anti_rabies"] or "") if existing_vc else ""
            tetanus_prophylaxis_preserved = (existing_vc["tetanus_prophylaxis"] or "") if existing_vc else ""

            vc_pvrv, vc_erig_hrig = _anti_rabies_vaccine_from_form(_v("vc_anti_rabies_vaccine"))
            ttox, ats_val, htig_val = _tetanus_triple_from_agent(_v("vc_tetanus_agent"))
            master_vaccine_type = _anti_rabies_type_label_from_form(_v("vc_anti_rabies_vaccine"))

            db.execute(
                """
                INSERT INTO vaccination_card (
                    case_id, anti_rabies, pvrv, pcec_batch, pcec_mfg_date, pcec_expiry,
                    erig_hrig, tetanus_prophylaxis, tetanus_toxoid, ats, htig,
                    tetanus_batch, tetanus_mfg_date, tetanus_expiry,
                    remarks
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    tetanus_batch = excluded.tetanus_batch,
                    tetanus_mfg_date = excluded.tetanus_mfg_date,
                    tetanus_expiry = excluded.tetanus_expiry,
                    remarks = excluded.remarks
                """,
                (
                    case_id,
                    anti_rabies_preserved,
                    vc_pvrv,
                    _v("vc_pcec_batch"),
                    vc_pcec_mfg_date,
                    vc_pcec_expiry,
                    vc_erig_hrig,
                    tetanus_prophylaxis_preserved,
                    ttox,
                    ats_val,
                    htig_val,
                    _v("vc_tetanus_batch"),
                    vc_tetanus_mfg_date,
                    vc_tetanus_expiry,
                    normalize_name_case(_v("vc_remarks")),
                ),
            )

            placeholders = ",".join("?" for _ in editable_record_types)
            db.execute(
                f"DELETE FROM vaccination_card_doses WHERE case_id = ? AND record_type IN ({placeholders})",
                (case_id, *sorted(editable_record_types)),
            )
            dose_date_owners = _vaccination_dose_date_owners_from_getter(_v)
            for record_type, prefix, days in _VC_DOSE_SCHEDULES:
                if record_type not in editable_record_types:
                    continue
                for day in days:
                    dose_date = _v(f"{prefix}_{day}_date")
                    resolved_date = _vaccination_resolved_dose_date_iso(record_type, dose_date, dose_date_owners)
                    type_of_vaccine = _vaccination_type_for_dose_row(
                        _v(f"{prefix}_{day}_type"),
                        master_vaccine_type,
                        resolved_date,
                    )
                    dose = _dose_value_from_form(
                        _v(f"{prefix}_{day}_dose_sel"),
                        _v(f"{prefix}_{day}_dose_other"),
                    )
                    route_site = normalize_name_case(_v(f"{prefix}_{day}_route_site"))
                    given_by = normalize_name_case(_v(f"{prefix}_{day}_given_by"))
                    if _vaccination_dose_row_should_insert(
                        resolved_date,
                        type_of_vaccine,
                        dose,
                        route_site,
                        given_by,
                    ):
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
                                resolved_date,
                                type_of_vaccine or None,
                                dose or None,
                                route_site or None,
                                given_by or None,
                            ),
                        )

            # Notify the patient that the vaccination record for this case was updated.
            _insert_patient_notification(
                patient_id=case_patient["patient_id"],
                notif_type="vaccination",
                source_id=case_id,
                message="Your vaccination record has been updated by the clinic.",
            )
            _insert_medical_audit_log(
                db,
                case_id=case_id,
                action="UPDATE",
                field_name="case_record",
                old_value=None,
                new_value=None,
                change_reason="Case details updated",
                user_id=session["user_id"],
                clinic_personnel_id=staff["clinic_personnel_id"],
                clinic_id=staff["clinic_id"],
            )

            db.commit()

            flash("Case information updated.", "success")
            return redirect(url_for("view_patient_case", case_id=case_id))

        staff_display_name = _staff_display_name(staff)

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
        _normalize_vaccination_card_date_fields(vaccination_card)
        vaccination_card["form_vc_anti_rabies_vaccine"] = _anti_rabies_vaccine_prefill_from_db(vaccination_card)
        vaccination_card["form_vc_tetanus_agent"] = _tetanus_agent_prefill_from_db(vaccination_card)
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

        _vc_master_display = _anti_rabies_type_label_from_form(
            (vaccination_card.get("form_vc_anti_rabies_vaccine") or "").strip()
        )
        _vaccination_card_doses_apply_master_type_to_dated_rows(card_doses_by_type, _vc_master_display)

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
            {"label": "Cases", "href": url_for("staff_patients")},
            {"label": patient_name, "href": url_for("view_patient_case", case_id=case_id)},
            {"label": "Edit", "href": None},
        ]
        effective_final_category = (
            (case_patient.get("who_category_final") or "").strip()
            or (case_patient.get("who_category_auto") or "").strip()
        )
        editable_record_types = _editable_vaccination_record_types(effective_final_category)

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
            can_edit_pre_exposure="pre_exposure" in editable_record_types,
            can_edit_post_exposure="post_exposure" in editable_record_types,
            active_page="cases",
        )

    @app.get("/admin/dashboard")
    @role_required("system_admin")
    def admin_dashboard():
        q = request.args.to_dict(flat=True)
        q["tab"] = "overview"
        return redirect(url_for("admin_analytics", **q))

    @app.get("/admin/clinic")
    @role_required("system_admin")
    def admin_clinic():
        q = request.args.to_dict(flat=True)
        q["tab"] = "clinic"
        return redirect(url_for("admin_analytics", **q))

    @app.get("/admin/clinic/export.csv")
    @role_required("system_admin")
    def admin_clinic_export_csv():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_analytics", tab="clinic"))

        clinic_d = dict(clinic)
        cid = clinic_d.get("id")
        if cid is None:
            flash("Clinic record missing an ID.", "error")
            return redirect(url_for("admin_analytics", tab="clinic"))
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Clinic performance summary"])
        w.writerow(["Clinic", clinic_d.get("name") or ""])
        w.writerow(["Address", clinic_d.get("address") or ""])
        w.writerow(["Period mode", period])
        w.writerow(["Date from", date_from])
        w.writerow(["Date to", date_to])
        if yearly_year is not None:
            w.writerow(["Yearly year", yearly_year])
        w.writerow([])
        w.writerow(
            [
                "Cases (period)",
                _count_total_cases_in_period(db, cid, date_from, date_to),
            ]
        )
        w.writerow(
            [
                "Appointments (period)",
                _count_appointments_in_period(db, cid, date_from, date_to),
            ]
        )
        w.writerow(
            [
                "Completed cases (period)",
                _count_completed_cases_in_period(db, cid, date_from, date_to),
            ]
        )
        w.writerow(
            [
                "Ongoing cases (period)",
                _count_ongoing_cases_in_period(db, cid, date_from, date_to),
            ]
        )
        w.writerow(
            [
                "No-show cases (period)",
                _count_no_show_cases_in_period(db, cid, date_from, date_to),
            ]
        )
        w.writerow(
            [
                "Case completion % (period)",
                _case_completion_pct(db, cid, date_from, date_to),
            ]
        )
        w.writerow([])
        w.writerow(["Risk level (case date in period)", "Count"])
        risk_raw = db.execute(
            """
            SELECT
              COALESCE(NULLIF(TRIM(c.risk_level), ''), NULLIF(TRIM(c.category), ''), 'Unknown') AS risk_label,
              COUNT(*) AS total
            FROM cases c
            WHERE c.clinic_id = ?
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) >= DATE(?)
              AND DATE(COALESCE(NULLIF(c.created_at, ''), c.exposure_date)) <= DATE(?)
            GROUP BY risk_label
            ORDER BY total DESC
            """,
            (cid, date_from, date_to),
        ).fetchall()
        for r in risk_raw:
            w.writerow([(r["risk_label"] or "Unknown").strip() or "Unknown", int(r["total"] or 0)])

        fn = f"clinic-performance-{date_from}-to-{date_to}.csv"
        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fn}"'},
        )

    @app.get("/admin/clinic/export.pdf")
    @role_required("system_admin")
    def admin_clinic_export_pdf():
        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable. Please contact admin.", "error")
            return redirect(url_for("admin_analytics", tab="clinic"))

        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_analytics", tab="clinic"))

        clinic_id = clinic["id"]
        clinic_ctx = _admin_reporting_clinic_dict(db, clinic_id, clinic, period, date_from, date_to, yearly_year)
        html = render_template(
            "admin_clinic_report_pdf.html",
            clinic=clinic,
            period=period,
            date_from=date_from,
            date_to=date_to,
            yearly_year=yearly_year,
            **clinic_ctx,
        )

        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again.", "error")
            return redirect(url_for("admin_analytics", tab="clinic", period=period, date_from=date_from, date_to=date_to))

        pdf_data = pdf_io.getvalue()
        if not pdf_data:
            flash("PDF generation produced an empty file.", "error")
            return redirect(url_for("admin_analytics", tab="clinic", period=period, date_from=date_from, date_to=date_to))

        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        fn = f"clinic-performance-{date_from}-to-{date_to}.pdf"
        response.headers["Content-Disposition"] = f'attachment; filename="{fn}"'
        return response

    @app.get("/admin/analytics/insights/export.csv")
    @role_required("system_admin")
    def admin_insights_export_csv():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        filters = _admin_insights_filters_from_request(request.args)
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_analytics", tab="insights"))
        dataset = (request.args.get("dataset") or "").strip()
        data = _admin_reporting_insights_dict(db, clinic["id"], date_from, date_to, filters)
        body, fn = _admin_insights_export_csv_body(dataset, data)
        if body is None or fn is None:
            flash("Unknown or missing export dataset.", "error")
            red_args: dict[str, object] = {
                "tab": "insights",
                "period": period,
                "date_from": date_from,
                "date_to": date_to,
            }
            if yearly_year is not None:
                red_args["year"] = yearly_year
            if filters.get("barangay"):
                red_args["insights_barangay"] = filters["barangay"]
            if filters.get("animal"):
                red_args["insights_animal"] = filters["animal"]
            if filters.get("bite_type"):
                red_args["insights_bite"] = filters["bite_type"]
            if filters.get("gender"):
                red_args["insights_gender"] = filters["gender"]
            if filters.get("age_group"):
                red_args["insights_age"] = filters["age_group"]
            return redirect(url_for("admin_analytics", **red_args))
        return Response(
            "\ufeff" + body,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fn}"'},
        )

    @app.get("/admin/analytics/forensic-report.pdf")
    @role_required("system_admin")
    def admin_forensic_report_pdf():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        try:
            from xhtml2pdf import pisa  # type: ignore[import]
        except Exception:
            flash("PDF generation is temporarily unavailable.", "error")
            return redirect(url_for("admin_analytics", tab="insights"))

        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        filters = _admin_insights_filters_from_request(request.args)
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_analytics", tab="insights"))

        data = _admin_reporting_insights_dict(db, clinic["id"], date_from, date_to, filters)
        html = render_template(
            "admin_forensic_report_pdf.html",
            admin=admin,
            clinic=clinic,
            period=period,
            date_from=date_from,
            date_to=date_to,
            yearly_year=yearly_year,
            **data,
        )
        pdf_io = io.BytesIO()
        err = pisa.CreatePDF(html, dest=pdf_io, encoding="utf-8")
        if err.err:
            flash("PDF generation failed. Please try again.", "error")
            return redirect(url_for("admin_analytics", tab="insights", period=period, date_from=date_from, date_to=date_to, year=yearly_year))

        pdf_data = pdf_io.getvalue()
        if not pdf_data:
            flash("PDF generation produced an empty file.", "error")
            return redirect(url_for("admin_analytics", tab="insights"))

        fn = f"forensic-bite-analytics-{date_from}-to-{date_to}.pdf"
        response = make_response(pdf_data)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = f'attachment; filename="{fn}"'
        return response

    @app.get("/admin/analytics/forensic-report.csv")
    @role_required("system_admin")
    def admin_forensic_report_csv():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        filters = _admin_insights_filters_from_request(request.args)
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_analytics", tab="insights"))

        data = _admin_reporting_insights_dict(db, clinic["id"], date_from, date_to, filters)
        kpi = data.get("kpi") or {}

        buf = io.StringIO()
        w = csv.writer(buf)

        w.writerow(["Forensic Bite Analytics Report (CSV)"])
        w.writerow(["Clinic", clinic["name"] if clinic and clinic["name"] else ""])
        w.writerow(["Address", clinic["address"] if clinic and clinic["address"] else ""])
        w.writerow(["Period mode", period])
        w.writerow(["Date from", date_from])
        w.writerow(["Date to", date_to])
        if yearly_year is not None:
            w.writerow(["Yearly year", yearly_year])
        if filters:
            w.writerow(["Filters (insights)"])
            for k in sorted(filters.keys()):
                w.writerow([k, filters.get(k) or ""])
        w.writerow([])

        w.writerow(["Key indicators"])
        w.writerow(["metric", "value"])
        w.writerow(["Total bite cases", kpi.get("bite_cases", 0)])
        w.writerow(["Completed cases (in period)", kpi.get("completed_cases", 0)])
        w.writerow(["Ongoing cases (all)", kpi.get("ongoing_cases", 0)])
        w.writerow(["Staff members", kpi.get("staff_count", 0)])
        w.writerow([])

        def _write_dist_section(title: str, rows: list[dict], key_label: str) -> None:
            w.writerow([title])
            w.writerow([key_label, "case_count", "percent"])
            for r in rows or []:
                w.writerow([r.get("label"), r.get("count"), r.get("percent")])
            w.writerow([])

        _write_dist_section("Victim age groups", data.get("age_distribution_rows") or [], "age_group")
        _write_dist_section("Gender distribution", data.get("gender_distribution_rows") or [], "gender")
        _write_dist_section("Bite type distribution", data.get("bite_type_rows") or [], "bite_type")
        _write_dist_section("Animal type distribution", data.get("animal_type_rows_insights") or [], "animal_type")
        _write_dist_section("Case severity distribution", data.get("severity_rows") or [], "severity_label")
        _write_dist_section("WHO category distribution", data.get("who_category_rows") or [], "who_category")
        _write_dist_section("Case status distribution", data.get("case_status_rows") or [], "case_status")
        _write_dist_section(
            "Vaccination status distribution",
            data.get("vaccination_status_rows") or [],
            "vaccination_status",
        )

        w.writerow(["Barangay case distribution"])
        w.writerow(["barangay", "case_count", "percent"])
        for r in data.get("barangay_table_rows") or []:
            w.writerow([r.get("barangay"), r.get("count"), r.get("percent")])
        w.writerow([])

        w.writerow(["Monthly trends"])
        w.writerow(["month", "bite_cases", "vaccinations_administered"])
        for r in data.get("monthly_trends_table") or []:
            w.writerow([r.get("month_label"), r.get("bite_cases"), r.get("vaccinations")])
        w.writerow([])

        fn = f"forensic-bite-analytics-{date_from}-to-{date_to}.csv"
        return Response(
            "\ufeff" + buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{fn}"'},
        )

    @app.get("/admin/patients")
    @role_required("system_admin")
    def admin_patients():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        _admin_mark_page_seen(db, session["user_id"], "patients")
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            return render_template(
                "admin_patients.html",
                admin=admin,
                admin_display_name=_admin_display_name(admin),
                admin_initials=_admin_initials(admin),
                clinic=None,
                cases=SimplePagination([], page=1, per_page=10, total=0),
                search="",
                selected_category="all",
                selected_status="all",
                pending_count=0,
                high_risk_count=0,
                active_page="patients",
                include_notification_strip=True,
                dashboard_notifications=[],
            )

        clinic_id = clinic["id"]
        search = (request.args.get("search") or "").strip()
        category = (request.args.get("category") or "all").strip().lower()
        if category not in {"all", "category i", "category ii", "category iii"}:
            category = "all"
        case_status = (request.args.get("status") or "all").strip().lower()
        if case_status not in {"all", "pending", "completed", "no show"}:
            case_status = "all"

        # Dynamic Stats
        pending_count = db.execute(
            "SELECT COUNT(*) AS n FROM cases WHERE clinic_id = ? AND LOWER(COALESCE(case_status, 'pending')) = 'pending'",
            (clinic_id,)
        ).fetchone()["n"]
        high_risk_count = db.execute(
            "SELECT COUNT(*) AS n FROM cases WHERE clinic_id = ? AND LOWER(COALESCE(risk_level, category, '')) = 'category iii'",
            (clinic_id,)
        ).fetchone()["n"]

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
        params: list[object] = [clinic_id]

        if category != "all":
            where_clauses.append("LOWER(COALESCE(c.risk_level, c.category, '')) = ?")
            params.append(category)
        if case_status != "all":
            where_clauses.append("LOWER(COALESCE(c.case_status, 'pending')) = ?")
            params.append(case_status)

        if search:
            search_clean = search.strip().lower()
            # If it looks like a case code (C-00001), extract the numeric ID part
            q_id = search_clean.removeprefix("c-").lstrip("0")
            if not q_id and "0" in search_clean: # handle "C-00000" or just "0"
                q_id = "0"
            
            where_clauses.append("""
                (CAST(c.id AS TEXT) LIKE ? 
                 OR LOWER(p.first_name) LIKE ? 
                 OR LOWER(p.last_name) LIKE ? 
                 OR LOWER(u.email) LIKE ?)
            """)
            params.extend([f"%{q_id}%", f"%{search_clean}%", f"%{search_clean}%", f"%{search_clean}%"])

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
                c.exposure_date,
                COALESCE(c.risk_level, c.category, 'N/A') AS category,
                COALESCE(c.case_status, 'Pending') AS case_status,
                COALESCE(c.staff_removed, 0) AS staff_removed
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

        case_items: list[dict] = []
        for row in cases_rows:
            exp_disp = row["exposure_date"] or "N/A"
            if exp_disp != "N/A":
                try:
                    exp_disp = datetime.fromisoformat(str(exp_disp).replace("Z", "+00:00")).strftime(
                        "%b %d, %Y"
                    )
                except ValueError:
                    pass
            case_items.append(
                {
                    "id": row["case_id"],
                    "case_code": f"C-{row['case_id']:05d}",
                    "exposure_date": exp_disp,
                    "category": row["category"],
                    "case_status": row["case_status"],
                    "staff_removed": int(row["staff_removed"] or 0),
                }
            )

        cases = SimplePagination(case_items, page=page, per_page=per_page, total=total)

        return render_template(
            "admin_patients.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic,
            cases=cases,
            search=search,
            selected_category=category,
            selected_status=case_status,
            pending_count=pending_count,
            high_risk_count=high_risk_count,
            active_page="patients",
            include_notification_strip=True,
            dashboard_notifications=_admin_notifications_for_page(db, clinic_id, "patients"),
        )

    @app.post("/admin/cases/<int:case_id>/restore")
    @role_required("system_admin")
    def admin_restore_case(case_id: int):
        db = get_db()
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_patients"))

        case_row = db.execute(
            """
            SELECT id, COALESCE(staff_removed, 0) AS staff_removed
            FROM cases
            WHERE id = ? AND clinic_id = ?
            """,
            (case_id, clinic["id"]),
        ).fetchone()
        if case_row is None:
            flash("Case not found.", "error")
            return redirect(url_for("admin_patients"))

        if int(case_row["staff_removed"] or 0) == 0:
            flash("This case is already active.", "info")
        else:
            db.execute(
                """
                UPDATE cases
                SET staff_removed = 0,
                    staff_removed_at = NULL,
                    staff_removed_by_user_id = NULL
                WHERE id = ? AND clinic_id = ?
                """,
                (case_id, clinic["id"]),
            )
            _insert_medical_audit_log(
                db,
                case_id=case_id,
                action="UPDATE",
                field_name="staff_removed",
                old_value="1",
                new_value="0",
                change_reason="Restored to staff list",
                user_id=session["user_id"],
                clinic_id=clinic["id"],
            )
            db.commit()
            flash("Case restored to staff case list.", "success")

        return_to = (request.form.get("return_to") or "").strip()
        if return_to.startswith("/admin/"):
            return redirect(return_to)
        return redirect(url_for("admin_patients"))

    @app.get("/admin/cases/<int:case_id>/reporting-summary")
    @role_required("system_admin")
    def admin_case_reporting_summary(case_id: int):
        return redirect(url_for("admin_case_details", case_id=case_id) + "#exposure-summary")

    @app.get("/admin/cases/<int:case_id>/vaccination")
    @role_required("system_admin")
    def admin_case_vaccination(case_id: int):
        return redirect(url_for("admin_case_details", case_id=case_id) + "#vaccination-record")

    @app.get("/admin/cases/<int:case_id>/details")
    @role_required("system_admin")
    def admin_case_details(case_id: int):
        """Combined admin view: reporting summary + vaccination record (no patient PII)."""
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_patients"))

        row = db.execute(
            """
            SELECT
              c.id,
              COALESCE(c.staff_removed, 0) AS staff_removed,
              c.staff_removed_at,
              c.exposure_date,
              c.exposure_time,
              c.place_of_exposure,
              c.affected_area,
              c.type_of_exposure,
              c.animal_detail,
              c.animal_condition,
              c.category,
              c.risk_level,
              c.tetanus_prophylaxis_status,
              psd.bleeding_type,
              psd.local_treatment,
              psd.patient_prev_immunization,
              psd.hrtig_immunization
            FROM cases c
            LEFT JOIN pre_screening_details psd ON psd.case_id = c.id
            WHERE c.id = ? AND c.clinic_id = ?
            """,
            (case_id, clinic["id"]),
        ).fetchone()

        if row is None:
            flash("Case not found.", "error")
            return redirect(url_for("admin_patients"))

        def _fmt_date(val: object) -> str:
            if not val:
                return "—"
            s = str(val).strip()
            if not s:
                return "—"
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except ValueError:
                return s

        def _yes_no_unknown(val: object) -> str:
            if val is None:
                return "—"
            if val in (0, "0"):
                return "No"
            if val in (1, "1"):
                return "Yes"
            return str(val)

        summary = {
            "case_code": f"C-{row['id']:05d}",
            "exposure_date": _fmt_date(row["exposure_date"]),
            "exposure_time": (row["exposure_time"] or "").strip() or "—",
            "place_of_exposure": (row["place_of_exposure"] or "").strip() or "—",
            "affected_area": (row["affected_area"] or "").strip() or "—",
            "type_of_exposure": (row["type_of_exposure"] or "").strip() or "—",
            "animal_detail": (row["animal_detail"] or "").strip() or "—",
            "animal_condition": (row["animal_condition"] or "").strip() or "—",
            "category": (row["category"] or "").strip() or "—",
            "risk_level": (row["risk_level"] or "").strip() or "—",
            "tetanus_prophylaxis_status": (row["tetanus_prophylaxis_status"] or "").strip() or "—",
            "bleeding_type": (row["bleeding_type"] or "").strip() or "—",
            "local_treatment": (row["local_treatment"] or "").strip() or "—",
            "patient_prev_immunization": (row["patient_prev_immunization"] or "").strip() or "—",
            "hrtig_immunization": _yes_no_unknown(row["hrtig_immunization"]),
        }

        history_rows = db.execute(
            """
            SELECT
              mal.id,
              mal.action,
              mal.change_reason,
              mal.changed_at,
              u.username AS actor_username,
              u.role AS actor_role,
              cp.title AS actor_title,
              cp.first_name AS actor_first_name,
              cp.last_name AS actor_last_name
            FROM medical_audit_logs mal
            LEFT JOIN users u ON u.id = mal.user_id
            LEFT JOIN clinic_personnel cp ON cp.id = mal.clinic_personnel_id
            WHERE mal.case_id = ?
            ORDER BY datetime(mal.changed_at) DESC, mal.id DESC
            """,
            (case_id,),
        ).fetchall()

        def _history_actor_label(row_obj: sqlite3.Row) -> str:
            role_val = (row_obj["actor_role"] or "").strip().lower()
            username_val = (row_obj["actor_username"] or "").strip()
            title_val = (row_obj["actor_title"] or "").strip()
            first_val = (row_obj["actor_first_name"] or "").strip()
            last_val = (row_obj["actor_last_name"] or "").strip()
            full_name_val = " ".join(part for part in [title_val, first_val, last_val] if part).strip()
            if role_val == "system_admin":
                return f"System Admin ({username_val})" if username_val else "System Admin"
            if full_name_val:
                return full_name_val
            if username_val:
                return username_val
            return "Clinic Personnel"

        def _history_time_label(raw_value: object) -> str:
            raw_text = (str(raw_value or "")).strip()
            if not raw_text:
                return "—"
            try:
                return datetime.fromisoformat(raw_text.replace("Z", "+00:00")).strftime("%b %d, %Y %I:%M %p")
            except ValueError:
                return raw_text

        case_history = [
            {
                "changed_at": _history_time_label(hr["changed_at"]),
                "action": (hr["change_reason"] or hr["action"] or "Updated").strip(),
                "changed_by": _history_actor_label(hr),
            }
            for hr in history_rows
        ]

        vacc_ctx = _admin_case_vaccination_context(db, case_id, clinic["id"])
        if vacc_ctx is None:
            flash("Case not found.", "error")
            return redirect(url_for("admin_patients"))

        vacc_ctx = dict(vacc_ctx)
        vacc_ctx.pop("case_code", None)

        return render_template(
            "admin_case_details.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic,
            case_id=case_id,
            case_code=summary["case_code"],
            summary=summary,
            case_history=case_history,
            case_is_removed=int(row["staff_removed"] or 0) == 1,
            case_removed_at=row["staff_removed_at"],
            active_page="patients",
            include_notification_strip=True,
            dashboard_notifications=_admin_notifications_for_page(db, clinic["id"], "patients"),
            **vacc_ctx,
        )

    @app.get("/admin/appointments")
    @role_required("system_admin")
    def admin_appointments():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        _admin_mark_page_seen(db, session["user_id"], "appointments")
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            return render_template(
                "admin_appointments.html",
                admin=admin,
                admin_display_name=_admin_display_name(admin),
                admin_initials=_admin_initials(admin),
                clinic=None,
                appointments=SimplePagination([], page=1, per_page=10, total=0),
                date_filter="all",
                date_from="",
                date_to="",
                active_page="appointments",
                include_notification_strip=False,
                dashboard_notifications=[],
            )

        clinic_id = clinic["id"]
        date_filter = (request.args.get("date_filter") or "all").strip().lower()
        raw_from = (request.args.get("date_from") or "").strip()
        raw_to = (request.args.get("date_to") or "").strip()

        def _parse_admin_date(s: str) -> str | None:
            if not s:
                return None
            try:
                datetime.strptime(s, "%Y-%m-%d")
                return s
            except ValueError:
                return None

        date_from = _parse_admin_date(raw_from) or ""
        date_to = _parse_admin_date(raw_to) or ""
        if date_from and date_to and date_from > date_to:
            date_from, date_to = date_to, date_from

        try:
            page = int(request.args.get("page") or 1)
        except ValueError:
            page = 1
        per_page = 10

        where = ["a.clinic_id = ?"]
        params: list[object] = [clinic_id]

        range_active = bool(date_from or date_to)
        if range_active:
            if date_from:
                where.append("DATE(a.appointment_datetime) >= ?")
                params.append(date_from)
            if date_to:
                where.append("DATE(a.appointment_datetime) <= ?")
                params.append(date_to)
        else:
            if date_filter == "today":
                where.append("DATE(a.appointment_datetime) = DATE('now', 'localtime')")
            elif date_filter == "week":
                where.append("DATE(a.appointment_datetime) >= DATE('now', '-6 days', 'localtime')")

        where_sql = " AND ".join(where)

        total = db.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM appointments a
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchone()["n"]

        offset = (page - 1) * per_page
        rows = db.execute(
            f"""
            SELECT
              a.id,
              a.patient_id,
              a.appointment_datetime,
              a.status,
              a.case_id,
              c.exposure_date
            FROM appointments a
            INNER JOIN cases c ON c.id = a.case_id
            WHERE {where_sql}
            ORDER BY datetime(a.appointment_datetime) DESC, a.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params) + (per_page, offset),
        ).fetchall()

        appt_items: list[dict] = []
        for row in rows:
            dt_raw = row["appointment_datetime"] or ""
            dt_display = dt_raw
            if dt_raw:
                try:
                    dt_display = datetime.fromisoformat(dt_raw.replace("Z", "+00:00")).strftime("%Y-%m-%d, %I:%M %p")
                except ValueError:
                    pass
            st = (row["status"] or "").strip().lower()
            if st in ("cancelled", "canceled", "removed"):
                badge = "Cancelled"
            elif st == "completed":
                badge = "Completed"
            elif st == "expired":
                badge = "Expired"
            else:
                badge = "Scheduled"
            case_id = int(row["case_id"])
            exp_raw = row["exposure_date"] or ""
            exp_display = exp_raw
            if exp_raw:
                try:
                    exp_display = datetime.fromisoformat(str(exp_raw).replace("Z", "+00:00")).strftime("%Y-%m-%d")
                except ValueError:
                    exp_display = str(exp_raw)[:10]
            appt_items.append(
                {
                    "display_id": f"A-{row['id']:05d}",
                    "patient_display_id": f"P-{int(row['patient_id']):05d}",
                    "case_id": case_id,
                    "case_code": f"C-{case_id:05d}",
                    "exposure_date_display": exp_display or "—",
                    "datetime_display": dt_display,
                    "badge": badge,
                    "raw_status": row["status"] or "",
                }
            )

        appointments = SimplePagination(appt_items, page=page, per_page=per_page, total=total)

        return render_template(
            "admin_appointments.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic,
            appointments=appointments,
            date_filter=date_filter,
            date_from=date_from,
            date_to=date_to,
            active_page="appointments",
            include_notification_strip=False,
            dashboard_notifications=_admin_notifications_for_page(db, clinic_id, "appointments"),
        )

    @app.get("/admin/analytics")
    @role_required("system_admin")
    def admin_analytics():
        if len(request.args) == 0:
            return redirect(url_for("admin_analytics", tab="overview", period="30d"))
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        _admin_mark_page_seen(db, session["user_id"], "reporting")

        raw_tab = (request.args.get("tab") or "overview").strip().lower()
        if raw_tab not in ("overview", "clinic", "insights"):
            raw_tab = "overview"
        tab = raw_tab

        period, date_from, date_to, yearly_year = _admin_resolve_period_dates()
        clinic = _get_singleton_clinic_row(db)
        clinic_id = clinic["id"] if clinic else None
        insights_filters = _admin_insights_filters_from_request(request.args)

        ctx: dict = {
            "admin": admin,
            "admin_display_name": _admin_display_name(admin),
            "admin_initials": _admin_initials(admin),
            "clinic": clinic,
            "tab": tab,
            "period": period,
            "date_from": date_from,
            "date_to": date_to,
            "yearly_year": yearly_year,
            "admin_year_options": _admin_year_dropdown_options(),
            "active_page": "reporting",
            "include_notification_strip": False,
            "dashboard_notifications": [],
            "insights_filters": insights_filters,
            "insights_filter_query_js": _insights_filters_query_string(insights_filters),
        }

        if tab == "overview":
            ctx.update(_admin_reporting_overview_dict(db, clinic_id, date_from, date_to))
        elif tab == "clinic":
            ctx.update(_admin_reporting_clinic_dict(db, clinic_id, clinic, period, date_from, date_to, yearly_year))
        else:
            if clinic is None:
                ctx.update(
                    {
                        "kpi": {
                            "bite_cases": 0,
                            "completed_cases": 0,
                            "ongoing_cases": 0,
                            "staff_count": 0,
                        },
                        "chart_compare": {"labels": [], "cases": [], "vaccinations": []},
                        "barangay_rows": [],
                        "barangay_max": 1,
                        "barangay_table_rows": [],
                        "monthly_trends_table": [],
                        "age_distribution_rows": [],
                        "gender_distribution_rows": [],
                        "bite_type_rows": [],
                        "animal_type_rows_insights": [],
                        "severity_rows": [],
                        "case_status_rows": [],
                        "vaccination_status_rows": [],
                        "vaccination_case_rows": [],
                        "insights_barangay_options": [],
                        "insights_bite_options": [],
                        "insights_animal_options": ["Dogs", "Cats", "Bats", "Other"],
                        "insights_gender_options": [],
                        "insights_age_options": list(_INSIGHTS_AGE_GROUP_ORDER),
                        "priority_cases": [],
                        "staff_performance": [],
                    }
                )
            else:
                ctx.update(
                    _admin_reporting_insights_dict(db, clinic_id, date_from, date_to, insights_filters)
                )

        return render_template("admin_reporting.html", **ctx)

    @app.post("/admin/users/<int:user_id>/set-active")
    @role_required("system_admin")
    def admin_user_set_active(user_id: int):
        db = get_db()
        if user_id == session["user_id"]:
            flash("You cannot change your own account status here.", "error")
            return redirect(url_for("admin_users"))
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_users"))
        if not _admin_user_manageable_in_clinic(db, clinic["id"], user_id):
            flash("User is not managed under this clinic.", "error")
            return redirect(url_for("admin_users"))
        active_raw = (request.form.get("active") or "").strip()
        set_active = active_raw == "1"
        try:
            db.execute(
                """
                UPDATE users SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
                """,
                (1 if set_active else 0, user_id),
            )
            db.commit()
        except Exception:
            db.rollback()
            flash("Could not update account status.", "error")
        else:
            flash("Account reactivated." if set_active else "Account deactivated.", "success")
        return redirect(url_for("admin_users"))

    @app.route("/admin/users/new-staff", methods=["GET", "POST"])
    @role_required("system_admin")
    def admin_new_staff():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        clinic = _get_singleton_clinic_row(db)
        if clinic is None:
            flash("No clinic configured.", "error")
            return redirect(url_for("admin_users"))

        if request.method == "GET":
            return redirect(url_for("admin_users", open_staff_modal="1"))

        username = (request.form.get("username") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        employee_id = (request.form.get("employee_id") or "").strip()
        title = (request.form.get("title") or "").strip()
        first_name = normalize_optional(request.form.get("first_name"))
        last_name = normalize_optional(request.form.get("last_name"))
        license_number = (request.form.get("license_number") or "").strip() or None
        date_of_birth = (request.form.get("date_of_birth") or "").strip() or None
        gender = normalize_name_case(request.form.get("gender") or "")

        errors: list[str] = []
        if not username:
            errors.append("Username is required.")
        if not email or "@" not in email:
            errors.append("A valid email is required.")
        if not employee_id:
            errors.append("Employee ID is required.")
        if title not in ("Doctor", "Nurse"):
            errors.append("Title must be Doctor or Nurse.")
        if not _is_letters_period_only(first_name):
            errors.append("First name must contain letters and periods only.")
        if not _is_letters_period_only(last_name):
            errors.append("Last name must contain letters and periods only.")
        if date_of_birth:
            try:
                dob_date = date.fromisoformat(date_of_birth[:10])
                if dob_date > date.today():
                    errors.append("Date of birth cannot be in the future.")
            except ValueError:
                errors.append("Date of birth is invalid.")
        if gender and gender not in {"Male", "Female"}:
            errors.append("Gender must be Male or Female.")

        if not errors:
            dup_user = db.execute(
                "SELECT 1 FROM users WHERE username = ? OR email = ? LIMIT 1",
                (username, email),
            ).fetchone()
            if dup_user:
                errors.append("Username or email is already in use.")
            dup_emp = db.execute(
                "SELECT 1 FROM clinic_personnel WHERE employee_id = ? LIMIT 1",
                (employee_id,),
            ).fetchone()
            if dup_emp:
                errors.append("Employee ID already exists.")
            if license_number:
                dup_lic = db.execute(
                    "SELECT 1 FROM clinic_personnel WHERE license_number = ? LIMIT 1",
                    (license_number,),
                ).fetchone()
                if dup_lic:
                    errors.append("License number already exists.")

        if errors:
            for e in errors:
                flash(e, "error")
            session["new_staff_form"] = {
                "username": username,
                "email": email,
                "employee_id": employee_id,
                "title": title,
                "first_name": first_name or "",
                "last_name": last_name or "",
                "license_number": license_number or "",
                "date_of_birth": date_of_birth or "",
                "gender": gender or "",
            }
            return redirect(url_for("admin_users", open_staff_modal="1"))

        password = _generate_strong_password(14)
        password_hash = generate_password_hash(password)
        try:
            cur = db.execute(
                """
                INSERT INTO users (username, email, password_hash, role, must_change_password, is_active)
                VALUES (?, ?, ?, 'clinic_personnel', 1, 1)
                """,
                (username, email, password_hash),
            )
            uid = cur.lastrowid
            db.execute(
                """
                INSERT INTO clinic_personnel (
                  user_id, clinic_id, first_name, last_name, employee_id, license_number, title, date_of_birth, gender
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uid,
                    clinic["id"],
                    first_name,
                    last_name,
                    employee_id,
                    license_number,
                    title,
                    date_of_birth,
                    gender or None,
                ),
            )
            db.commit()
        except Exception:
            db.rollback()
            flash("Could not create staff account.", "error")
            return redirect(url_for("admin_users", open_staff_modal="1"))

        try:
            send_email(
                to_email=email,
                subject="RabiesResQ clinic staff account",
                body=(
                    "Hello,\n\n"
                    "A system administrator created your RabiesResQ clinic staff account.\n\n"
                    f"Username: {username}\n"
                    f"Email: {email}\n"
                    f"Temporary password: {password}\n\n"
                    "You must change this password at first login.\n"
                ),
            )
        except Exception:
            logger.exception("Failed to email new staff credentials")

        flash("Staff account created. Credentials were emailed when mail is configured.", "success")
        return redirect(url_for("admin_users"))

    @app.get("/admin/users")
    @role_required("system_admin")
    def admin_users():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))
        _admin_mark_page_seen(db, session["user_id"], "users")
        clinic = _get_singleton_clinic_row(db)
        search_raw = (request.args.get("search") or "").strip()
        search = search_raw.lower()
        role_filter = (request.args.get("role_filter") or "all").strip().lower()
        try:
            page = int(request.args.get("page") or 1)
        except ValueError:
            page = 1
        per_page = 10

        merged: list[dict] = []
        staff_total = 0
        patient_total = 0
        active_users_total = 0
        if clinic is not None:
            clinic_id = clinic["id"]
            # Match clinic-staff Total Patients semantics:
            # count distinct patient records linked to visible clinic cases,
            # including multiple patients under the same user account.
            patient_total = int(
                (
                    db.execute(
                        """
                        SELECT COUNT(DISTINCT c.patient_id) AS total
                        FROM cases c
                        WHERE c.clinic_id = ?
                          AND COALESCE(c.staff_removed, 0) = 0
                        """,
                        (clinic_id,),
                    ).fetchone()["total"]
                )
                or 0
            )
            staff_rows = db.execute(
                """
                SELECT u.id, u.username, u.email, u.role, u.created_at, u.must_change_password, u.is_active,
                  TRIM(COALESCE(cp.title, '') || ' ' || COALESCE(cp.first_name, '') || ' ' || COALESCE(cp.last_name, '')) AS display_name
                FROM users u
                JOIN clinic_personnel cp ON cp.user_id = u.id
                WHERE cp.clinic_id = ?
                """,
                (clinic_id,),
            ).fetchall()
            for sr in staff_rows:
                merged.append(
                    {
                        "user_id": sr["id"],
                        "name": (sr["display_name"] or "").strip() or sr["username"],
                        "email": sr["email"] or "",
                        "role": sr["role"],
                        "created_at": sr["created_at"] or "",
                        "must_change_password": sr["must_change_password"] or 0,
                        "is_active": int(sr["is_active"]) if sr["is_active"] is not None else 1,
                    }
                )

            patient_rows = db.execute(
                """
                SELECT DISTINCT u.id, u.username, u.email, u.role, u.created_at, u.must_change_password, u.is_active
                FROM users u
                WHERE u.role = 'patient'
                  AND EXISTS (
                    SELECT 1 FROM patients p
                    INNER JOIN cases c ON c.patient_id = p.id
                    WHERE p.user_id = u.id AND c.clinic_id = ?
                  )
                """,
                (clinic_id,),
            ).fetchall()
            for pr in patient_rows:
                nm_row = db.execute(
                    """
                    SELECT COALESCE(TRIM(p.first_name || ' ' || p.last_name), u.username) AS display_name
                    FROM patients p
                    JOIN users u ON u.id = p.user_id
                    WHERE p.user_id = ?
                    ORDER BY p.id ASC
                    LIMIT 1
                    """,
                    (pr["id"],),
                ).fetchone()
                display_name = (nm_row["display_name"] if nm_row else None) or pr["username"]
                merged.append(
                    {
                        "user_id": pr["id"],
                        "name": display_name,
                        "email": pr["email"] or "",
                        "role": pr["role"],
                        "created_at": pr["created_at"] or "",
                        "must_change_password": pr["must_change_password"] or 0,
                        "is_active": int(pr["is_active"]) if pr["is_active"] is not None else 1,
                    }
                )

        def _matches(u: dict) -> bool:
            if role_filter == "staff" and u["role"] != "clinic_personnel":
                return False
            if role_filter == "patients" and u["role"] != "patient":
                return False
            if search:
                blob = f"{u['name']} {u['email']}".lower()
                if search not in blob:
                    return False
            return True

        def _merged_user_is_active(u: dict) -> int:
            # Must not use (x or 1): is_active == 0 is valid and must stay 0.
            v = u.get("is_active")
            return 1 if v is None else int(v)

        staff_total = sum(1 for u in merged if u["role"] == "clinic_personnel")
        # Keep this account-level value for list filtering behavior only.
        # The dashboard card already uses patient-record-level counting above.
        active_users_total = sum(1 for u in merged if _merged_user_is_active(u) == 1)

        filtered: list[dict] = [u for u in merged if _matches(u)]
        filtered.sort(key=lambda x: (x["created_at"] or ""), reverse=True)
        total = len(filtered)
        offset = (page - 1) * per_page
        page_items = filtered[offset : offset + per_page]

        user_rows: list[dict] = []
        for ui in page_items:
            joined = ui["created_at"]
            if joined:
                try:
                    joined = datetime.fromisoformat(joined.replace("Z", "+00:00")).strftime("%Y-%m-%d")
                except ValueError:
                    joined = joined[:10]
            ia = _merged_user_is_active(ui)
            if ia == 0:
                status_lbl = "Deactivated"
            elif ui["must_change_password"]:
                status_lbl = "Setup required"
            else:
                status_lbl = "Active"
            user_rows.append(
                {
                    "user_id": ui["user_id"],
                    "name": ui["name"],
                    "email": ui["email"],
                    "role": ui["role"],
                    "status": status_lbl,
                    "date_joined": joined,
                    "is_active": ia,
                }
            )

        users_page = SimplePagination(user_rows, page=page, per_page=per_page, total=total)

        new_staff_prefill = session.pop("new_staff_form", None)
        if new_staff_prefill is None:
            new_staff_prefill = {}
        open_staff_modal = (request.args.get("open_staff_modal") == "1") or bool(new_staff_prefill)

        return render_template(
            "admin_users.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic,
            users=users_page,
            search=search_raw,
            role_filter=role_filter,
            staff_total=staff_total,
            patient_total=patient_total,
            active_users_total=active_users_total,
            current_session_user_id=session["user_id"],
            active_page="users",
            include_notification_strip=True,
            dashboard_notifications=_admin_notifications_for_page(
                db, clinic["id"] if clinic else None, "users"
            ),
            open_staff_modal=open_staff_modal,
            new_staff_prefill=new_staff_prefill,
        )

    @app.route("/admin/settings", methods=["GET", "POST"])
    @role_required("system_admin")
    def admin_settings():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        def _crumbs():
            return [
                {"label": "Home", "href": url_for("admin_dashboard")},
                {"label": "Profile", "href": None},
            ]

        if request.method == "POST":
            section = (request.form.get("update_section") or "").strip()

            if section == "personal":
                first_name = normalize_optional(request.form.get("first_name"))
                last_name = normalize_optional(request.form.get("last_name"))
                try:
                    db.execute(
                        """
                        UPDATE system_admins
                        SET first_name = ?, last_name = ?
                        WHERE user_id = ?
                        """,
                        (first_name, last_name, session["user_id"]),
                    )
                    db.commit()
                except Exception:
                    db.rollback()
                    flash("Failed to update profile.", "error")
                else:
                    flash("Profile updated.", "success")
                    return redirect(url_for("admin_settings", highlight="personal"))

            elif section == "account":
                username = (request.form.get("username") or "").strip()
                email = (request.form.get("email") or "").strip().lower()
                new_password = request.form.get("new_password")
                confirm_password = request.form.get("confirm_password")

                errors = []
                if not username:
                    errors.append("Username is required.")
                if not email or "@" not in email or "." not in email.split("@")[-1]:
                    errors.append("A valid email is required.")
                
                if new_password:
                    if len(new_password) < 8:
                        errors.append("Password must be at least 8 characters.")
                    if new_password != confirm_password:
                        errors.append("Passwords do not match.")

                if not errors:
                    dup = db.execute(
                        """
                        SELECT 1 FROM users
                        WHERE (username = ? OR email = ?)
                          AND id != ?
                        LIMIT 1
                        """,
                        (username, email, session["user_id"]),
                    ).fetchone()
                    if dup:
                        errors.append("Username or email is already in use.")
                
                if errors:
                    for msg in errors:
                        flash(msg, "error")
                else:
                    try:
                        if new_password:
                            hashed = generate_password_hash(new_password)
                            db.execute(
                                """
                                UPDATE users
                                SET username = ?, email = ?, password_hash = ?
                                WHERE id = ?
                                """,
                                (username, email, hashed, session["user_id"]),
                            )
                        else:
                            db.execute(
                                """
                                UPDATE users
                                SET username = ?, email = ?
                                WHERE id = ?
                                """,
                                (username, email, session["user_id"]),
                            )
                        db.commit()
                    except Exception:
                        db.rollback()
                        flash("Failed to update account security.", "error")
                    else:
                        session["username"] = username
                        session["email"] = email
                        flash("Account security updated.", "success")
                        return redirect(url_for("admin_settings", highlight="account"))

            elif section == "clinic_hours":
                return redirect(url_for("admin_clinic_hours"))
            else:
                flash("Invalid update request.", "error")

            # Refresh admin row after attempted updates
            admin = _admin_fetch_user(db, session["user_id"])

        highlight_section = (request.args.get("highlight") or "").strip()
        return render_template(
            "admin_settings.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=_get_singleton_clinic_row(db),
            breadcrumbs=_crumbs(),
            highlight_section=highlight_section,
            active_page="settings",
            include_notification_strip=False,
            dashboard_notifications=[],
        )

    @app.route("/admin/clinic-hours", methods=["GET", "POST"])
    @role_required("system_admin")
    def admin_clinic_hours():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        def _crumbs():
            return [
                {"label": "Home", "href": url_for("admin_dashboard")},
                {"label": "Clinic Hours", "href": None},
            ]

        clinic_row = _get_singleton_clinic_row(db)
        if clinic_row is None:
            flash("No clinic record found.", "error")
            return render_template(
                "admin_clinic_hours.html",
                admin_display_name=_admin_display_name(admin),
                admin_initials=_admin_initials(admin),
                clinic=None,
                operating_hours=dict(DEFAULT_CLINIC_OPERATING_HOURS),
                breadcrumbs=_crumbs(),
                active_page="clinic_hours",
                include_notification_strip=False,
                dashboard_notifications=[],
            )

        if request.method == "POST":
            oh: dict[str, object] = dict(DEFAULT_CLINIC_OPERATING_HOURS)
            oh["mon_sat_open"] = (request.form.get("mon_sat_open") or "08:00").strip()
            oh["mon_sat_close"] = (request.form.get("mon_sat_close") or "22:00").strip()
            oh["sunday_open"] = (request.form.get("sunday_open") or "08:00").strip()
            oh["sunday_close"] = (request.form.get("sunday_close") or "18:00").strip()
            oh["lunch_start"] = (request.form.get("lunch_start") or "12:00").strip()
            oh["lunch_end"] = (request.form.get("lunch_end") or "13:00").strip()
            oh["dinner_start"] = (request.form.get("dinner_start") or "18:30").strip()
            oh["dinner_end"] = (request.form.get("dinner_end") or "19:30").strip()
            try:
                oh["slot_interval_minutes"] = max(
                    5, int((request.form.get("slot_interval_minutes") or "45").strip())
                )
            except ValueError:
                oh["slot_interval_minutes"] = 45
            try:
                oh["horizon_days"] = min(
                    365, max(1, int((request.form.get("horizon_days") or "60").strip()))
                )
            except ValueError:
                oh["horizon_days"] = 60
            payload = serialize_clinic_operating_hours(oh)
            try:
                db.execute(
                    """
                    UPDATE clinics
                    SET operating_hours_json = ?
                    WHERE id = ?
                    """,
                    (payload, int(clinic_row["id"])),
                )
                db.commit()
                ensure_availability_from_hours(db, int(clinic_row["id"]))
                _notify_patients_clinic_schedule_updated(int(clinic_row["id"]))
                flash(
                    "Clinic operating hours saved. Availability slots were updated.",
                    "success",
                )
                return redirect(url_for("admin_clinic_hours"))
            except Exception:
                db.rollback()
                flash("Failed to save clinic hours.", "error")

        operating_hours = parse_clinic_operating_hours(clinic_row["operating_hours_json"])
        return render_template(
            "admin_clinic_hours.html",
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic_row,
            operating_hours=operating_hours,
            breadcrumbs=_crumbs(),
            active_page="clinic_hours",
            include_notification_strip=False,
            dashboard_notifications=[],
        )

    @app.get("/admin/session-logs")
    @role_required("system_admin")
    def admin_session_logs():
        db = get_db()
        admin = _admin_fetch_user(db, session["user_id"])
        if admin is None:
            session.clear()
            flash("Account profile missing, contact admin.", "error")
            return redirect(url_for("auth.login"))

        _admin_mark_page_seen(db, session["user_id"], "session_logs")

        page = request.args.get("page", 1, type=int) or 1
        if page < 1:
            page = 1
        per_page = 10

        total_row = db.execute("SELECT COUNT(*) AS n FROM user_session_logs").fetchone()
        total = int(total_row["n"] or 0)

        offset = (page - 1) * per_page
        rows = db.execute(
            """
            SELECT l.id, l.user_id, l.role_at_login, l.logged_in_at, l.logged_out_at, u.username
            FROM user_session_logs l
            JOIN users u ON u.id = l.user_id
            ORDER BY datetime(l.logged_in_at) DESC
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        ).fetchall()

        log_items = []
        for row in rows:
            lo = row["logged_out_at"]
            log_items.append(
                {
                    "username": row["username"] or "—",
                    "role_at_login": row["role_at_login"],
                    "role_label": _session_log_role_label(
                        row["role_at_login"] if row else None
                    ),
                    "logged_in_at": _format_session_timestamp(row["logged_in_at"]),
                    "logged_out_display": (
                        "Active"
                        if lo is None or str(lo).strip() == ""
                        else _format_session_timestamp(str(lo))
                    ),
                }
            )

        clinic = _get_singleton_clinic_row(db)
        pagination = SimplePagination(log_items, page=page, per_page=per_page, total=total)

        breadcrumbs = [
            {"label": "Home", "href": url_for("admin_dashboard")},
            {"label": "Session Logs", "href": None},
        ]
        return render_template(
            "admin_session_logs.html",
            admin=admin,
            admin_display_name=_admin_display_name(admin),
            admin_initials=_admin_initials(admin),
            clinic=clinic,
            breadcrumbs=breadcrumbs,
            logs=pagination,
            active_page="session_logs",
            include_notification_strip=False,
            dashboard_notifications=[],
        )

    # =========================
    # Admin-only account creation (CLI)
    # =========================

    @app.cli.command("create-clinic")
    @click.option("--name", required=True)
    @click.option("--address", default=None)
    def create_clinic_command(name, address):
        db = get_db()
        try:
            db.execute(
                "INSERT INTO clinics (name, address) VALUES (?, ?)",
                (normalize_name_case(name), normalize_optional(address)),
            )
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

        first_name_n = normalize_optional(first_name)
        last_name_n = normalize_optional(last_name)

        try:
            cur = db.execute(
                "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)",
                (username, email_norm, generate_password_hash(password), "system_admin"),
            )
            user_id = cur.lastrowid
            db.execute(
                "INSERT INTO system_admins (user_id, first_name, last_name, employee_id) VALUES (?, ?, ?, ?)",
                (user_id, first_name_n, last_name_n, employee_id),
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

        first_name_n = normalize_optional(first_name)
        last_name_n = normalize_optional(last_name)

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
                (user_id, clinic_id, first_name_n, last_name_n, employee_id, license_number, title),
            )
            db.commit()
        except Exception as e:
            db.rollback()
            raise click.ClickException(f"Failed to create staff: {e}")

        click.echo("Staff created.")

    @app.cli.command("seed-demo-reset")
    @click.option(
        "--confirm",
        is_flag=True,
        help="Required. Wipes all application data except the admin@example.com system admin, then reseeds demo accounts.",
    )
    def seed_demo_reset_command(confirm: bool):
        """Reset DB to a demo dataset (destructive).

        Keeps only users.system_admin with email admin@example.com. Reuses the existing clinic row
        (creates one clinic only if the table is empty).

        Seeds: 10 patients (patient1@gmail.com ... patient10@gmail.com, password patient123!),
        15 cases (patients 1-5 have two cases each; 6-10 have one), 2 nurses and 1 doctor
        (staff123!). Appointments: first 7 cases -> nurse1, remaining 8 -> nurse2.
        """
        if not confirm:
            raise click.UsageError(
                "Refusing to run without --confirm (this deletes almost all data). "
                "Example: flask --app app seed-demo-reset --confirm"
            )

        db = get_db()
        admin_row = db.execute(
            """
            SELECT id FROM users
            WHERE LOWER(TRIM(COALESCE(email, ''))) = ? AND role = ?
            LIMIT 1
            """,
            ("admin@example.com", "system_admin"),
        ).fetchone()
        if not admin_row:
            raise click.ClickException(
                "No system_admin user with email admin@example.com. Create one first, e.g.:\n"
                '  flask --app app create-admin --username admin --email admin@example.com '
                "--password <secret> --employee-id ADM-001"
            )
        admin_id = int(admin_row["id"])

        patient_pw_hash = generate_password_hash("patient123!")
        staff_pw_hash = generate_password_hash("staff123!")
        who_ver = f"{WHO_RULES_VERSION}+doh-risk-v1"
        who_reasons_json = json.dumps([], ensure_ascii=False)

        try:
            db.execute("BEGIN")

            db.execute("DELETE FROM medical_audit_logs")
            db.execute("DELETE FROM reports")
            db.execute("DELETE FROM notifications")
            db.execute("DELETE FROM cases")
            db.execute("DELETE FROM availability_slots")
            db.execute("DELETE FROM password_reset_codes")
            db.execute("DELETE FROM pending_emails")

            try:
                db.execute("DELETE FROM staff_page_last_seen")
            except sqlite3.OperationalError:
                pass
            db.execute("DELETE FROM admin_page_last_seen WHERE admin_user_id != ?", (admin_id,))

            db.execute("DELETE FROM users WHERE id != ?", (admin_id,))

            clinic_row = db.execute("SELECT id FROM clinics ORDER BY id LIMIT 1").fetchone()
            if clinic_row:
                clinic_id = int(clinic_row["id"])
            else:
                cur_clinic = db.execute(
                    "INSERT INTO clinics (name, address) VALUES (?, ?)",
                    ("RabiesResQ Clinic", None),
                )
                clinic_id = int(cur_clinic.lastrowid)

            patient_ids: list[int] = []
            patient_usernames = [
                "juan_dela_cruz",
                "maria_santos",
                "jose_reyes",
                "ana_garcia",
                "pedro_lopez",
                "luisa_ramos",
                "carlos_mendoza",
                "rosalie_bautista",
                "miguel_fernandez",
                "sofia_castillo",
            ]
            patient_names = [
                ("Juan", "Dela Cruz"),
                ("Maria", "Santos"),
                ("Jose", "Reyes"),
                ("Ana", "Garcia"),
                ("Pedro", "Lopez"),
                ("Luisa", "Ramos"),
                ("Carlos", "Mendoza"),
                ("Rosalie", "Bautista"),
                ("Miguel", "Fernandez"),
                ("Sofia", "Castillo"),
            ]
            for i in range(10):
                n = i + 1
                email = f"patient{n}@gmail.com"
                username = patient_usernames[i]
                first_name, last_name = patient_names[i]
                ucur = db.execute(
                    """
                    INSERT INTO users (username, email, password_hash, role, must_change_password, is_active)
                    VALUES (?, ?, ?, 'patient', 0, 1)
                    """,
                    (username, email, patient_pw_hash),
                )
                uid = int(ucur.lastrowid)
                pcur = db.execute(
                    """
                    INSERT INTO patients (
                        user_id, first_name, last_name, relationship_to_user, onboarding_completed
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (uid, first_name, last_name, "Self", 1),
                )
                patient_ids.append(int(pcur.lastrowid))

            def _insert_staff(username: str, email: str, title: str, employee_id: str, first: str, last: str) -> int:
                u = db.execute(
                    """
                    INSERT INTO users (username, email, password_hash, role, must_change_password, is_active)
                    VALUES (?, ?, ?, 'clinic_personnel', 0, 1)
                    """,
                    (username, email, staff_pw_hash),
                )
                uid = int(u.lastrowid)
                cp = db.execute(
                    """
                    INSERT INTO clinic_personnel (
                        user_id, clinic_id, first_name, last_name, employee_id, license_number, title
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (uid, clinic_id, first, last, employee_id, title),
                )
                return int(cp.lastrowid)

            nurse1_cp_id = _insert_staff(
                "clara_delos_reyes", "nurse1@gmail.com", "Nurse", "NURSE-001", "Clara", "Reyes"
            )
            nurse2_cp_id = _insert_staff("mark_villanueva", "nurse2@gmail.com", "Nurse", "NURSE-002", "Mark", "Villa")
            _insert_staff("dr_rafael_torres", "doctor@gmail.com", "Doctor", "DOC-001", "Rafael", "Torres")

            case_patient_index: list[int] = []
            for pi in range(5):
                case_patient_index.extend([pi, pi])
            for pi in range(5, 10):
                case_patient_index.append(pi)

            base_exposure = date.today() - timedelta(days=30)
            case_ids: list[int] = []
            for seq, pat_idx in enumerate(case_patient_index):
                pid = patient_ids[pat_idx]
                exposure_d = base_exposure + timedelta(days=seq)
                exposure_s = exposure_d.isoformat()
                risk = "Category II"
                ccur = db.execute(
                    """
                    INSERT INTO cases (
                        patient_id, clinic_id, exposure_date, exposure_time,
                        place_of_exposure, affected_area,
                        type_of_exposure, animal_detail, animal_condition, animal_vaccination,
                        category, risk_level, case_status, tetanus_prophylaxis_status,
                        who_category_auto, who_category_final, who_category_reasons_json, who_category_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pid,
                        clinic_id,
                        exposure_s,
                        "09:00:00",
                        "Demo barangay",
                        "Left hand",
                        "Scratch",
                        "Dog",
                        "alive",
                        "Unknown",
                        risk,
                        risk,
                        "Pending",
                        "Unknown",
                        risk,
                        risk,
                        who_reasons_json,
                        who_ver,
                    ),
                )
                cid = int(ccur.lastrowid)
                case_ids.append(cid)
                db.execute(
                    """
                    INSERT INTO pre_screening_details (
                        case_id, wound_description, bleeding_type, local_treatment,
                        patient_prev_immunization, prev_vaccine_date, tetanus_date,
                        hrtig_immunization, hrtig_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cid,
                        "Demo wound (seed)",
                        "Minor",
                        None,
                        None,
                        None,
                        None,
                        0,
                        None,
                    ),
                )

            appt_base = datetime.now(PHILIPPINES_TZ) + timedelta(days=1)
            for idx, cid in enumerate(case_ids):
                pat_idx = case_patient_index[idx]
                pid = patient_ids[pat_idx]
                nurse_cp = nurse1_cp_id if idx < 7 else nurse2_cp_id
                appt_dt = (appt_base + timedelta(hours=2 * idx)).replace(tzinfo=None).isoformat(
                    timespec="seconds"
                )
                db.execute(
                    """
                    INSERT INTO appointments (
                        patient_id, clinic_personnel_id, clinic_id, appointment_datetime,
                        status, type, case_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (pid, nurse_cp, clinic_id, appt_dt, "Pending", "Walk-in", cid),
                )

            db.commit()
        except Exception as e:
            db.rollback()
            raise click.ClickException(f"seed-demo-reset failed: {e}") from e

        click.echo(
            "seed-demo-reset complete.\n"
            f"  Preserved admin user id: {admin_id} (admin@example.com)\n"
            f"  Clinic id: {clinic_id}\n"
            f"  Patients: 10 (patient1@gmail.com ... patient10@gmail.com) password: patient123!\n"
            f"  Staff: nurse1@gmail.com, nurse2@gmail.com, doctor@gmail.com password: staff123!\n"
            f"  Cases: {len(case_ids)} (cases 1-7 -> nurse1 appointments; 8-15 -> nurse2)\n"
        )

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

