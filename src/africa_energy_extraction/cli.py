"""Command line entry point: uv run africa_energy <command>."""
from __future__ import annotations

import argparse
import json
import sys

from . import config, extract, load, transform
from .catalog import CATALOG, load_countries
from .client import CloudflareBlocked, PortalError, make_client


def cmd_probe(args) -> int:
    """Answer the open questions about the endpoint before a full run."""
    group, topic = "Electricity", "Access"
    indicators = CATALOG[(group, topic)]
    with make_client() as client:
        client.warm_up()
        one = client.post_database(group, topic, indicators[:1], [2022], ["Algeria", "Angola"])
        print("Sample record:")
        print(json.dumps(one[0]["data"][0], indent=2))
        many = client.post_database(group, topic, indicators[:1], config.YEARS, ["Algeria"])
        years = sorted({r["year"] for block in many for r in block["data"]})
        print(f"\nRequested {config.YEARS[0]}-{config.YEARS[-1]} in one call, got years: {years}")
        if len(years) == len(config.YEARS):
            print("-> the endpoint accepts all years at once (faster extraction possible)")
        else:
            print("-> one year per request; the extractor already works this way")
    return 0


def cmd_extract(args) -> int:
    countries = load_countries(args.countries_file or config.COUNTRIES_FILE or None)
    years = list(range(args.start, args.end + 1))
    with make_client() as client:
        client.warm_up()
        stats = extract.extract(client, countries, years, force=args.force)
    print(f"\nfetched={stats['fetched']} skipped={stats['skipped']} failed={len(stats['failed'])}")
    if stats["missing_countries"]:
        print("Requested but never returned (check spelling against the portal): "
              + ", ".join(stats["missing_countries"]))
    return 1 if stats["failed"] else 0


def cmd_transform(args) -> int:
    transform.run()
    return 0


def cmd_load(args) -> int:
    load.run()
    return 0


def cmd_all(args) -> int:
    code = cmd_extract(args)
    if code == 0:
        cmd_transform(args)
        cmd_load(args)
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="africa_energy")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_extract_args(p):
        p.add_argument("--start", type=int, default=config.YEARS[0])
        p.add_argument("--end", type=int, default=config.YEARS[-1])
        p.add_argument("--force", action="store_true", help="re-download existing files")
        p.add_argument("--countries-file", help="text file with one portal country name per line")

    sub.add_parser("probe", help="test the endpoint with two small requests").set_defaults(func=cmd_probe)
    add_extract_args(sub.add_parser("extract", help="download raw JSON (resumable)"))
    sub.choices["extract"].set_defaults(func=cmd_extract)
    sub.add_parser("transform", help="raw JSON -> long CSV").set_defaults(func=cmd_transform)
    sub.add_parser("load", help="CSV -> MongoDB (idempotent upsert)").set_defaults(func=cmd_load)
    add_extract_args(sub.add_parser("all", help="extract, transform, load"))
    sub.choices["all"].set_defaults(func=cmd_all)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except CloudflareBlocked as exc:
        print(f"\nBLOCKED: {exc}", file=sys.stderr)
        return 2
    except PortalError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
