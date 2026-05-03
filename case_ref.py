"""Branch-aware public case identifiers (e.g. CLINIC1-0001)."""

from __future__ import annotations

import re
from typing import Any

_BRANCH_TAIL_RE = re.compile(r"^(.+)-(\d+)$", re.IGNORECASE)
_LEGACY_C_RE = re.compile(r"^c-0*(\d+)$", re.IGNORECASE)
_BRANCH_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{1,30}$")


def normalize_branch_code(raw: str | None) -> str:
    """Uppercase branch slug safe for case IDs."""
    s = (raw or "").strip().upper().replace(" ", "_")
    return s


def validate_branch_code(code: str | None) -> bool:
    c = normalize_branch_code(code)
    return bool(c and _BRANCH_CODE_RE.fullmatch(c))


def legacy_case_code(case_id: int) -> str:
    return f"C-{int(case_id):05d}"


def public_case_code(case_row: Any, *, case_id_key: str = "id") -> str:
    """Resolve display code from a sqlite Row/dict with optional case_ref."""
    if case_row is None:
        return ""
    keys = case_row.keys() if hasattr(case_row, "keys") else []
    ref = ""
    if "case_ref" in keys:
        ref = (case_row["case_ref"] or "").strip()
    if ref:
        return ref
    cid = case_row[case_id_key] if case_id_key in keys else None
    if cid is None and "case_id" in keys:
        cid = case_row["case_id"]
    try:
        return legacy_case_code(int(cid))
    except (TypeError, ValueError):
        return ""


def allocate_case_ref(conn, clinic_id: int) -> str:
    """
    Atomically allocate the next case_ref for a clinic (caller must be in a transaction).
    """
    row = conn.execute(
        "SELECT branch_code FROM clinics WHERE id = ?", (int(clinic_id),)
    ).fetchone()
    if row is None:
        raise ValueError("Clinic not found")
    branch = normalize_branch_code(row["branch_code"] if "branch_code" in row.keys() else "")
    if not validate_branch_code(branch):
        raise ValueError("Clinic branch code is missing or invalid")

    conn.execute(
        """
        INSERT INTO clinic_case_sequences (clinic_id, next_seq)
        VALUES (?, 1)
        ON CONFLICT(clinic_id) DO UPDATE SET next_seq = clinic_case_sequences.next_seq + 1
        """,
        (int(clinic_id),),
    )
    seq_row = conn.execute(
        "SELECT next_seq FROM clinic_case_sequences WHERE clinic_id = ?",
        (int(clinic_id),),
    ).fetchone()
    seq = int(seq_row["next_seq"] or 1)
    ref = f"{branch}-{seq:04d}"
    clash = conn.execute(
        "SELECT 1 FROM cases WHERE case_ref = ? LIMIT 1", (ref,)
    ).fetchone()
    if clash:
        raise RuntimeError(f"Case ref collision for {ref!r}")
    return ref


def parse_case_search_tokens(search_raw: str) -> tuple[str | None, int | None, str]:
    """
    Returns (branch_prefix_for_exact_match, legacy_numeric_id, like_fragment_for_case_ref).
    """
    search_clean = (search_raw or "").strip()
    if not search_clean:
        return None, None, ""

    lowered = search_clean.lower()
    legacy_id: int | None = None
    m_legacy = _LEGACY_C_RE.match(lowered)
    if m_legacy:
        legacy_id = int(m_legacy.group(1))

    branch_prefix: str | None = None
    m_bt = _BRANCH_TAIL_RE.match(search_clean.strip())
    if m_bt:
        branch_prefix = normalize_branch_code(m_bt.group(1))
        # numeric tail handled separately via case_ref full string match in SQL
    like_frag = f"%{search_clean.lower()}%"
    return branch_prefix, legacy_id, like_frag
