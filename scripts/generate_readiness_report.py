from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

from utils.config_loader import ConfigLoader


def build_requirements_section() -> str:
    items = [
        ("Market hedge loop with dry-run default", "Implemented; pairs execute in dry_run unless toggled"),
        ("Double-limit placement with cancel-on-fill", "Implemented in OrderManager; guarded by double_limit_enabled"),
        ("Telegram observability (/status /health /simulate)", "Implemented; read-only actions only"),
        ("DB persistence (orders, fills, simulated runs)", "Implemented; migrations up to 002_simulated_runs"),
        ("Read-only healthcheck", "Implemented in core.healthcheck.HealthcheckService"),
        ("Safe startup/shutdown and heartbeat", "Startup notice + optional heartbeat in Telegram"),
    ]
    lines = ["## Requirements Status", "", "| Requirement | Status |", "| --- | --- |"]
    for req, status in items:
        lines.append(f"| {req} | {status} |")
    return "\n".join(lines)


def build_tests_section(tests: List[str]) -> str:
    bullet = "\n".join(f"- {name}" for name in tests) if tests else "- (no tests discovered)"
    return "\n".join(
        [
            "## Test Coverage",
            "",
            "Current unit tests (from tests/):",
            bullet,
            "",
            "Key verifications:",
            "- Order manager flows, hedger, reconciler, risk, discovery, webhook sync",
            "- New coverage: Telegram command routing, healthcheck handling, simulated run persistence",
        ]
    )


def build_health_section() -> str:
    return "\n".join(
        [
            "## Runtime Validation (/health, /simulate)",
            "",
            "- `/health`: fetches Polymarket + Opinion orderbooks per enabled pair, computes net spreads, reports OK/FAIL.",
            "- `/simulate <pair> [size]`: builds order plan without placing orders, logs to DB (simulated_runs), reports legs and expected net profit.",
            "- Both commands respect dry_run and never place live orders.",
        ]
    )


def build_risks_section() -> str:
    return "\n".join(
        [
            "## Remaining Risks (to review before production)",
            "",
            "- REST polling latency or exchange-side slowdowns could delay fills; monitor reconciler.metrics.",
            "- Exchange API changes (orderbook schemas, auth) may break fetches; /health will surface failures.",
            "- Wallet balances/allowances and trading enablement must be confirmed before disabling dry_run.",
            "- Google Sheets/webhook sync optional paths are not covered by /simulate; verify manually if used.",
        ]
    )


def build_go_live_section(settings) -> str:
    return "\n".join(
        [
            "## Steps to Move from Dry-Run to Live",
            "",
            "- Confirm config: set `dry_run: false`, ensure correct `primary/secondary` exchanges and account ids.",
            "- Verify balances/allowances on Polymarket + Opinion; ensure rate limits align with keys.",
            "- Run `/health` and `/simulate <pair>` to sanity check orderbooks and plans.",
            "- Enable Telegram chat_id and optionally heartbeat for runtime visibility.",
            "- Start bot; watch startup message and first heartbeat; monitor reconciler metrics for duplicates/errors.",
        ]
    )


def main() -> None:
    loader = ConfigLoader()
    settings = loader.load_settings()
    accounts = loader.load_accounts()
    tests = sorted(p.stem for p in Path("tests").glob("test_*.py"))
    now = datetime.utcnow().isoformat()
    report_parts = [
        f"# Readiness Report\n\nGenerated: {now} UTC",
        f"- Dry-run default: {settings.dry_run}",
        f"- Pairs configured: {len(settings.market_pairs)}",
        f"- Accounts loaded: {len(accounts)}",
        build_requirements_section(),
        build_tests_section(tests),
        build_health_section(),
        build_risks_section(),
        build_go_live_section(settings),
    ]
    output = "\n\n".join(report_parts) + "\n"
    dest = Path("docs/readiness_report.md")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(output, encoding="utf-8")
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()


