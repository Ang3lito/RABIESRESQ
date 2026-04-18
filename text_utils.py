"""Shared text normalization for stored display fields (names, addresses, prose)."""


def normalize_name_case(value: object | None) -> str:
    """
    Title-style capitalization for names and free text: lowercase, then capitalize
    first letter of each word after spaces, hyphens, apostrophes, periods, and newlines.
    Does not alter emails, usernames, or codes — apply only to appropriate fields at save time.
    """
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if "\n" in raw or "\r" in raw:
        raw = raw.replace("\r\n", "\n").replace("\r", "\n")
        return "\n".join(
            _normalize_name_single_line(seg) for seg in raw.split("\n")
        )
    return _normalize_name_single_line(raw)


def _normalize_name_single_line(s: str) -> str:
    s = " ".join(s.split()).lower()
    if not s:
        return ""
    out: list[str] = []
    cap_next = True
    for ch in s:
        if cap_next and ch.isalpha():
            out.append(ch.upper())
            cap_next = False
        else:
            out.append(ch)
        if ch in (" ", "-", "'", ".", "\n"):
            cap_next = True
    return "".join(out)


def normalize_optional(value: object | None) -> str | None:
    """Return None if empty after normalize, else normalized string."""
    n = normalize_name_case(value)
    return n if n else None
