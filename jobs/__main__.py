"""Command-line entrypoint for batch jobs."""

from __future__ import annotations

import argparse
import os
from typing import Iterable

from jobs.config import TARGET_MARKETS, MarketConfig, iter_markets
from jobs.load_all import main as run_load_all


def _format_market(market: MarketConfig) -> str:
    fred = market.fred_series_id or "(none)"
    return (
        f"{market.key}: geo={market.geo_level}:{market.geo_id} name='{market.geo_name}' "
        f"year={market.year} fred_series={fred}"
    )


def _resolve_markets_from_cli(keys: Iterable[str] | None) -> tuple[MarketConfig, ...]:
    if not keys:
        return tuple()
    markets = tuple(iter_markets(keys))
    unknown = set(keys) - {m.key for m in markets}
    if unknown:
        raise SystemExit(f"Unknown market keys: {', '.join(sorted(unknown))}")
    return markets


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CRE Market Signals job runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    load_parser = subparsers.add_parser(
        "load-all", help="Fetch all configured market signals and persist to DuckDB"
    )
    load_parser.add_argument(
        "--markets",
        help="Comma-separated list of market keys to load (defaults to all configured)",
    )
    load_parser.add_argument(
        "--log-level",
        help="Override LOG_LEVEL for this invocation (e.g. DEBUG, INFO)",
    )

    subparsers.add_parser("list-markets", help="Show configured market metadata")

    args = parser.parse_args(argv)

    if args.command == "list-markets":
        for market in TARGET_MARKETS:
            print(_format_market(market))
        return 0

    if args.command == "load-all":
        markets_arg = args.markets.split(",") if args.markets else None
        markets_arg = [item.strip() for item in markets_arg or [] if item.strip()]
        markets = _resolve_markets_from_cli(markets_arg)
        if args.log_level:
            os.environ["LOG_LEVEL"] = args.log_level
        if markets:
            return run_load_all(markets)
        return run_load_all(None)

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
