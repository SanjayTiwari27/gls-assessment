"""Small, pure parsing helpers used by adapters.

These exist as standalone functions so that they can be unit-tested in
isolation and reused across vendors. None of them perform I/O.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Final

from dateutil import parser as dtparse

# Minimal alias map for tz abbreviations that python-dateutil does not handle
# out of the box (notably regional ones our sample vendors use). Always prefer
# numeric offsets in production payloads — these are last-resort.
_TZ_ALIASES: Final[dict[str, int]] = {
    "WIB": 7 * 3600,    # Western Indonesia Time, UTC+7
    "WITA": 8 * 3600,   # Central Indonesia, UTC+8
    "WIT": 9 * 3600,    # Eastern Indonesia, UTC+9
    "JST": 9 * 3600,
    "KST": 9 * 3600,
    "HKT": 8 * 3600,
    "SGT": 8 * 3600,
    "IST": 5 * 3600 + 1800,
    "CET": 1 * 3600,
    "CEST": 2 * 3600,
    "EST": -5 * 3600,
    "EDT": -4 * 3600,
    "PST": -8 * 3600,
    "PDT": -7 * 3600,
}


def parse_timestamp(value: str) -> datetime:
    """Parse arbitrary vendor timestamps into a tz-aware UTC datetime.

    Accepts ISO-8601 with offsets, common European (DD/MM/YYYY HH:MM) shapes,
    and a handful of regional tz abbreviations (e.g. "WIB"). Returns UTC.
    """

    if not isinstance(value, str) or not value.strip():
        raise ValueError("empty timestamp")

    raw = value.strip()
    tzinfos = {abbr: offset for abbr, offset in _TZ_ALIASES.items()}

    try:
        dt = dtparse.parse(raw, tzinfos=tzinfos, dayfirst=False)
    except (ValueError, OverflowError):
        # Retry with day-first for European/Asian shapes ("28/04/2026 09:42 WIB").
        dt = dtparse.parse(raw, tzinfos=tzinfos, dayfirst=True)

    if dt.tzinfo is None:
        # Default to UTC if the vendor genuinely sent a naive timestamp. This
        # is documented as ambiguous and gets logged downstream.
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(UTC)


_AMOUNT_TRAILING_DECIMAL_RE = re.compile(r"[.,]\d{1,2}$")


def parse_money(value: str) -> tuple[str, int]:
    """Parse a vendor money string like 'EUR 24.350,75' or 'USD 1,234.56' or
    '$1234.5' or '24350' into ``(currency, amount_minor)``.

    Strategy:
      1. Extract the 3-letter currency code (defaults to ``USD`` if a single
         currency symbol is given but no code).
      2. Strip everything that is not a digit, dot, or comma.
      3. Detect the decimal separator from the *last* dot/comma occurrence —
         that disambiguates European (``24.350,75``) vs US (``1,234.56``).
      4. Convert to integer minor units (cents/equivalent), which are what
         the database stores.
    """

    if not isinstance(value, str) or not value.strip():
        raise ValueError("empty amount")

    s = value.strip()
    currency = "USD"
    m = re.search(r"\b([A-Z]{3})\b", s)
    if m:
        currency = m.group(1)
        s = s.replace(m.group(0), "")
    elif s.startswith("$"):
        currency = "USD"
    elif s.startswith("€"):
        currency = "EUR"
    elif s.startswith("£"):
        currency = "GBP"

    digits = re.sub(r"[^0-9.,\-]", "", s)
    if not digits:
        raise ValueError(f"no digits in amount: {value!r}")

    last_dot = digits.rfind(".")
    last_com = digits.rfind(",")
    decimal_pos = max(last_dot, last_com)

    if decimal_pos == -1:
        # Pure integer like "24350" — assume zero fractional part.
        major = int(digits)
        return currency, major * 100

    decimal_sep = digits[decimal_pos]
    fractional = digits[decimal_pos + 1 :]
    integer_part = digits[:decimal_pos]

    # If the fractional part is more than 2 digits, treat the last separator
    # as a thousands separator instead.
    if len(fractional) not in (1, 2):
        joined = digits.replace(",", "").replace(".", "")
        return currency, int(joined) * 100 if joined.isdigit() else _raise(value)

    # Strip the *other* separator (thousands) from the integer part.
    other = "," if decimal_sep == "." else "."
    integer_part = integer_part.replace(other, "").replace(decimal_sep, "")

    try:
        major = int(integer_part) if integer_part else 0
        minor = int(fractional.ljust(2, "0")[:2])
    except ValueError as exc:
        raise ValueError(f"unparseable amount: {value!r}") from exc

    return currency, major * 100 + minor


def _raise(value: str) -> int:  # pragma: no cover - branch helper
    raise ValueError(f"unparseable amount: {value!r}")
