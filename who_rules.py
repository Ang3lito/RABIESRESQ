from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


WHO_RULES_VERSION = "who-v1"


def _norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()


def _norm_lower(s: Any) -> str:
    return _norm(s).lower()


def _split_areas(value: Any) -> list[str]:
    raw = _norm(value)
    if not raw:
        return []
    # Stored variously as "A, B, C" or newline-separated.
    parts = []
    for seg in raw.replace("\n", ",").split(","):
        p = seg.strip()
        if p:
            parts.append(p)
    # Preserve original casing but de-dup case-insensitively.
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


@dataclass(frozen=True)
class WhoFacts:
    type_of_exposure: str
    affected_areas: list[str]
    wound_description: str
    bleeding_type: str
    animal_condition: str


def normalize_case_facts(source: dict[str, Any]) -> WhoFacts:
    """
    Normalize inputs used for WHO category decision support.

    `source` can be a request.form-like dict, a sqlite3.Row converted to dict,
    or any mapping containing the expected keys.
    """
    return WhoFacts(
        type_of_exposure=_norm(source.get("type_of_exposure")),
        affected_areas=_split_areas(source.get("affected_area") or source.get("affected_areas")),
        wound_description=_norm(source.get("wound_description")),
        bleeding_type=_norm(source.get("bleeding_type")),
        animal_condition=_norm(source.get("animal_condition")),
    )


def _has_any(haystack: str, needles: Iterable[str]) -> bool:
    h = haystack
    for n in needles:
        if n and n in h:
            return True
    return False


def classify_who_category(facts: WhoFacts) -> tuple[str, list[dict[str, Any]], str]:
    """
    Returns (category_label, reasons, version).

    Category labels match existing UI conventions: "Category I/II/III" plus "Unknown".
    This is **decision-support** logic; it is intentionally conservative and explainable.
    """
    reasons: list[dict[str, Any]] = []

    exp_l = _norm_lower(facts.type_of_exposure)
    wound_l = _norm_lower(facts.wound_description)
    bleed_l = _norm_lower(facts.bleeding_type)
    animal_l = _norm_lower(facts.animal_condition)
    area_l = [a.lower() for a in (facts.affected_areas or [])]

    # Default
    category = "Unknown"

    # Category I: touch/lick on intact skin; no wound.
    if _has_any(exp_l, ["touch only", "lick on intact skin", "intact skin"]):
        category = "Category I"
        reasons.append(
            {"code": "CONTACT_INTACT_SKIN", "label": "Contact on intact skin (no wound)", "weight": 1}
        )

    # Mucous membrane contamination is Category III.
    if _has_any(exp_l, ["mucous", "contamination of mucous membrane", "eyes", "nose", "mouth"]):
        category = "Category III"
        reasons.append(
            {"code": "MUCOUS_MEMBRANE_EXPOSURE", "label": "Mucous membrane contamination", "weight": 3}
        )

    # Bite/scratch/non-bite exposures.
    if _has_any(exp_l, ["bite", "scratch", "non-bite", "non bite"]):
        # If there's an explicit "no wound" description, keep lower unless other high-risk criteria apply.
        if _has_any(wound_l, ["none", "no wound"]):
            if category == "Unknown":
                category = "Category I"
            reasons.append({"code": "NO_WOUND_REPORTED", "label": "No wound reported", "weight": 1})
        else:
            # Start at Category II for any bite/scratch that breaks skin (minor) unless escalated.
            if category in ("Unknown", "Category I"):
                category = "Category II"
            reasons.append(
                {"code": "SKIN_BREAK_POSSIBLE", "label": "Exposure may involve skin break", "weight": 2}
            )

        # Escalate for transdermal/deep wounds or bleeding.
        if _has_any(wound_l, ["punctured", "lacerated", "avulsed", "deep", "transdermal"]):
            category = "Category III"
            reasons.append(
                {"code": "TRANSDERMAL_OR_DEEP_WOUND", "label": "Transdermal or deep wound", "weight": 3}
            )
        if _has_any(bleed_l, ["profuse", "severe", "heavy", "active"]):
            category = "Category III"
            reasons.append({"code": "SEVERE_BLEEDING", "label": "Severe bleeding", "weight": 3})
        elif _has_any(bleed_l, ["minimal", "mild", "oozing", "yes", "bleeding"]):
            # Bleeding pushes toward Cat III for bites/scratches; keep Cat II for abrasions if mild.
            if not _has_any(wound_l, ["abrasion", "superficial"]):
                category = "Category III"
                reasons.append({"code": "BLEEDING_PRESENT", "label": "Bleeding present", "weight": 3})
            else:
                reasons.append({"code": "MINOR_BLEEDING", "label": "Minor bleeding", "weight": 2})

    # High-risk anatomical locations (does not *define* WHO category alone but increases urgency).
    if any(_has_any(a, ["head", "face", "neck", "hand"]) for a in area_l):
        reasons.append(
            {
                "code": "HIGH_RISK_LOCATION",
                "label": "High-risk anatomical location (head/neck/hands)",
                "weight": 1,
                "detail": facts.affected_areas,
            }
        )

    # Animal behavior/condition is supportive; may increase urgency but not a strict WHO category determinant.
    if _has_any(animal_l, ["unprovoked", "aggressive", "stray", "unknown"]):
        reasons.append(
            {
                "code": "ANIMAL_RISK_CONTEXT",
                "label": "Higher-risk animal context (supports urgency)",
                "weight": 1,
                "detail": facts.animal_condition,
            }
        )

    # De-dup reasons by code (keep first occurrence).
    seen_codes: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for r in reasons:
        code = _norm(r.get("code"))
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        deduped.append(r)

    return category, deduped, WHO_RULES_VERSION

