"""Offline diagnostics to verify which keys/addresses are loaded at runtime.

This script:
- Reads `config/accounts.json` (raw values)
- Loads accounts via `ConfigLoader` (applies env fallbacks the code uses)
- Prints the effective keys/addresses per account (Opinion & Polymarket)
- Warns if values are empty, placeholders, mismatched, or malformed

Safety guardrails:
- No network calls are made
- Existing project files are not modified
- Only this file is created under `diagnostics/`
"""

from __future__ import annotations

import base64
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.models import AccountCredentials, ExchangeName  # noqa: E402
from utils.config_loader import ConfigLoader  # noqa: E402


RAW_ACCOUNTS_PATH = ROOT / "config" / "accounts.json"


def read_raw_accounts(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("accounts.json must contain an object with 'accounts' key")
    return data


def is_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return value.startswith("${") and value.endswith("}")


def base64_valid(value: Optional[str]) -> bool:
    if not value:
        return False
    try:
        base64.urlsafe_b64decode(value)
        return True
    except Exception:
        return False


def normalize_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def analyze_field(name: str, raw: Any, resolved: Any) -> Dict[str, Any]:
    raw_str = normalize_str(raw)
    resolved_str = normalize_str(resolved)
    status = "OK"
    notes: List[str] = []

    if not resolved_str:
        status = "EMPTY"
    if is_placeholder(raw):
        status = "PLACEHOLDER"
        notes.append("placeholder in accounts.json; resolved via env?") if resolved_str else None
    if raw_str and resolved_str and raw_str != resolved_str and not is_placeholder(raw):
        status = "MISMATCH"
        notes.append("resolved differs from raw (env/default applied)")
    if raw_str and not resolved_str:
        notes.append("raw present but resolved empty")
    if not raw_str and resolved_str:
        notes.append("resolved filled by env/default")

    return {
        "field": name,
        "raw": raw_str,
        "resolved": resolved_str,
        "status": status,
        "notes": notes,
    }


def analyze_account(raw_entry: Dict[str, Any], resolved: AccountCredentials) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    fields = [
        "api_key",
        "secret_key",
        "passphrase",
        "wallet_address",
        "private_key",
        "multi_sig_address",
        "rpc_url",
        "host",
        "ws_url",
        "conditional_tokens_addr",
        "multisend_addr",
        "proxy",
        "chain_id",
    ]

    for field in fields:
        raw_val = raw_entry.get(field)
        resolved_val = getattr(resolved, field, None)
        checks.append(analyze_field(field, raw_val, resolved_val))

    # Additional validation for polymarket secret base64
    extra_notes: List[str] = []
    if resolved.exchange == ExchangeName.POLYMARKET:
        if resolved.secret_key and not base64_valid(resolved.secret_key):
            extra_notes.append("secret_key is not valid base64 (Polymarket expects urlsafe base64)")
        if not resolved.passphrase:
            extra_notes.append("passphrase missing for Polymarket")
        if not resolved.wallet_address:
            extra_notes.append("wallet_address missing for Polymarket")

    if resolved.exchange == ExchangeName.OPINION:
        if not resolved.private_key:
            extra_notes.append("private_key missing for Opinion signer")
        if not resolved.rpc_url:
            extra_notes.append("rpc_url missing for Opinion")
        if not resolved.multi_sig_address and not resolved.wallet_address:
            extra_notes.append("multi_sig_address/wallet_address missing for Opinion")

    return {
        "account_id": resolved.account_id,
        "exchange": resolved.exchange.value,
        "checks": checks,
        "warnings": extra_notes,
    }


def print_env_fallbacks() -> None:
    env_keys = [
        "OPINION_API_KEY",
        "OPINION_SIGNER_PRIVATE_KEY",
        "OPINION_RPC_URL",
        "OPINION_MULTI_SIG_ADDR",
    ]
    print("\n=== ENVIRONMENT FALLBACKS (Opinion) ===")
    for key in env_keys:
        val = os.getenv(key)
        if val:
            print(f"{key} = {val}")
        else:
            print(f"{key} = <unset>")


def main() -> None:
    if not RAW_ACCOUNTS_PATH.exists():
        print(f"accounts.json not found at {RAW_ACCOUNTS_PATH}")
        sys.exit(1)

    print("Reading raw accounts.json ...")
    raw_data = read_raw_accounts(RAW_ACCOUNTS_PATH)
    raw_accounts = raw_data.get("accounts", [])
    print(f"Found {len(raw_accounts)} account entries in accounts.json\n")

    print("=== RAW accounts.json content ===")
    print(json.dumps(raw_data, indent=2))

    print("\nLoading accounts via ConfigLoader (applies env fallbacks used by code) ...")
    loader = ConfigLoader(base_path=ROOT)
    resolved_accounts = loader.load_accounts()
    print(f"Resolved accounts: {len(resolved_accounts)}\n")

    # Map raw by account_id for comparison
    raw_by_id = {entry.get("account_id"): entry for entry in raw_accounts}

    reports: List[Dict[str, Any]] = []
    for acc in resolved_accounts:
        raw_entry = raw_by_id.get(acc.account_id, {})
        reports.append(analyze_account(raw_entry, acc))

    print("=== PER-ACCOUNT CHECKS ===")
    for report in reports:
        print(f"\nAccount: {report['account_id']} ({report['exchange']})")
        for check in report["checks"]:
            notes_str = f" | notes: {', '.join(check['notes'])}" if check["notes"] else ""
            print(
                f" - {check['field']}: raw='{check['raw']}' | resolved='{check['resolved']}' | status={check['status']}{notes_str}"
            )
        if report["warnings"]:
            print("   Warnings:")
            for w in report["warnings"]:
                print(f"    * {w}")

    print_env_fallbacks()

    # Summary of empties/placeholders/mismatches
    print("\n=== SUMMARY ===")
    total_warnings = 0
    for report in reports:
        for check in report["checks"]:
            if check["status"] in {"EMPTY", "PLACEHOLDER", "MISMATCH"}:
                total_warnings += 1
                print(
                    f"[{report['account_id']}] {check['field']}: status={check['status']} | raw='{check['raw']}' | resolved='{check['resolved']}'"
                )
        for w in report["warnings"]:
            total_warnings += 1
            print(f"[{report['account_id']}] warning: {w}")

    if total_warnings == 0:
        print("No empty, placeholder, or mismatched keys detected.")
    else:
        print(f"Total issues detected: {total_warnings}")


if __name__ == "__main__":
    main()





