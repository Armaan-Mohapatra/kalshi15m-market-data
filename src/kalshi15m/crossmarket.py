from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .classify import decimal, duration_seconds
from .paper import resolve_fee_terms
from .strategy import load_frozen_strategy


CONFIG_RELATIVE_PATH = Path("strategy") / "reflex_crossmarket_paper_v1.json"
AUTHORITY_RELATIVE_PATH = Path("strategy") / "reflex_crossmarket_paper_authority.json"
PROFILE_ID = "REFLEX_CROSSMARKET_PAPER:REFLEX_CROSSMARKET_PAPER_V1"
EXPECTED_SCOPE = "VALIDATED_BINARY_900S_NON_BTC"
SUPPORTED_FEE_TYPES = {"quadratic", "quadratic_with_maker_fees"}
YES_RESOLUTION = re.compile(r"\b(?:market\s+)?resolves?\s+to\s+yes\b", re.IGNORECASE)


class CrossMarketStrategyError(RuntimeError):
    pass


def load_crossmarket_strategy(project_root: Path | None = None) -> tuple[dict[str, Any], str]:
    root = project_root or Path(__file__).resolve().parents[2]
    base, base_hash = load_frozen_strategy(root)
    config_path = root / CONFIG_RELATIVE_PATH
    authority_path = root / AUTHORITY_RELATIVE_PATH
    try:
        raw = config_path.read_bytes()
        config = json.loads(raw)
        authority = json.loads(authority_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossMarketStrategyError(
            f"cross-market paper configuration unavailable: {type(exc).__name__}"
        ) from exc

    digest = hashlib.sha256(raw).hexdigest()
    if digest != str(authority.get("config_sha256", "")).lower():
        raise CrossMarketStrategyError("cross-market config hash does not match its authority manifest")
    if base_hash != str(authority.get("parent_config_sha256", "")).lower():
        raise CrossMarketStrategyError("cross-market parent BTC config hash changed")
    if not authority.get("evaluator_enabled") or authority.get("live_trading_enabled") is not False:
        raise CrossMarketStrategyError("cross-market paper authority is disabled or not paper-only")
    if config.get("revision") != authority.get("revision") or config.get("revision") != "REFLEX_CROSSMARKET_PAPER_V1":
        raise CrossMarketStrategyError("cross-market profile revision is not the authorized revision")
    if config.get("execution", {}).get("mode") != "PAPER" or config.get("execution", {}).get("live_authorized") is not False:
        raise CrossMarketStrategyError("cross-market profile must remain PAPER and live-disabled")

    inherited = copy.deepcopy(base["strategy"])
    candidate = copy.deepcopy(config.get("strategy", {}))
    inherited.pop("market_series", None)
    candidate.pop("market_series", None)
    candidate.pop("market_series_scope", None)
    if candidate != inherited:
        raise CrossMarketStrategyError("cross-market profile changed inherited BTC entry or sizing rules")
    if config.get("strategy", {}).get("market_series_scope") != EXPECTED_SCOPE:
        raise CrossMarketStrategyError("cross-market profile has an unexpected scope")
    if config.get("market_data") != base.get("market_data"):
        # The policy explanation may differ; the executable freshness and gate values may not.
        base_market_data = dict(base.get("market_data", {}))
        cross_market_data = dict(config.get("market_data", {}))
        base_market_data.pop("reference_data_policy", None)
        cross_market_data.pop("reference_data_policy", None)
        if cross_market_data != base_market_data:
            raise CrossMarketStrategyError("cross-market profile changed inherited market-data gates")
    if any(config.get("strategy", {}).get("optional_gates", {}).values()):
        raise CrossMarketStrategyError("cross-market profile enabled an inherited optional strategy gate")
    return config, digest


def crossmarket_eligibility(market: dict[str, Any]) -> tuple[str, str]:
    """Return an auditable classification for the non-BTC paper extension."""
    ticker = str(market.get("ticker") or "")
    if ticker.startswith("KXBTC15M-"):
        return "EXCLUDED", "RESERVED_FOR_FROZEN_BTC_PROFILE"
    if str(market.get("market_status", "")).lower() not in {"active", "open", "initialized", "unopened"}:
        return "EXCLUDED", "MARKET_NOT_OPEN_OR_UPCOMING"
    if str(market.get("paper_compatibility")) != "BINARY_SINGLE_MARKET":
        return "BLOCKED", "UNSUPPORTED_MULTI_OUTCOME_OR_MARKET_STRUCTURE"
    if not market.get("is_15m_confirmed") or duration_seconds(market) != 900:
        return "BLOCKED", "15_MINUTE_DURATION_NOT_CONFIRMED_AS_900_SECONDS"
    if str(market.get("market_type", "")).lower() != "binary":
        return "BLOCKED", "OFFICIAL_MARKET_TYPE_NOT_BINARY"
    rules = " ".join(str(market.get(field) or "") for field in ("rules_primary", "rules_secondary"))
    if not YES_RESOLUTION.search(rules):
        return "BLOCKED", "BINARY_YES_RESOLUTION_RULE_NOT_VERIFIED"
    result = str(market.get("result") or "").lower()
    if result and result not in {"yes", "no"}:
        return "BLOCKED", "OFFICIAL_RESULT_NOT_BINARY_YES_NO"
    fee_type, multiplier = resolve_fee_terms(market)
    if fee_type not in SUPPORTED_FEE_TYPES:
        return "BLOCKED", "FEE_SCHEDULE_NOT_VERIFIED_FOR_MARKET"
    parsed_multiplier = decimal(multiplier)
    if parsed_multiplier is None or parsed_multiplier < 0:
        return "BLOCKED", "FEE_MULTIPLIER_NOT_VERIFIED"
    return "ELIGIBLE", "FROZEN_EXECUTABLE_BOOK_RULES_AND_OFFICIAL_BINARY_SETTLEMENT"


def validate_market_reference_policy(config: dict[str, Any]) -> bool:
    """The inherited rule has no underlying input or enabled reference gate."""
    return (
        not any(config.get("strategy", {}).get("optional_gates", {}).values())
        and not config.get("market_data", {}).get("standard_brti_strategy_gate")
        and not config.get("market_data", {}).get("optional_brti_5hz_required")
        and config.get("market_data", {}).get("reference_data_policy", "").startswith(
            "NOT_REQUIRED_BY_INHERITED_RULES"
        )
    )
