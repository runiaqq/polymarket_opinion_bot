from __future__ import annotations

import argparse
import json
import sys

from core.market_mapper import MarketMapper


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage market mappings.")
    parser.add_argument("--path", default="data/mappings.yaml", help="Path to mapping file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Add or update a mapping")
    add_parser.add_argument("--poly", required=True, help="Polymarket market id")
    add_parser.add_argument("--op", required=True, help="Opinion market id")
    add_parser.add_argument("--info", default="{}", help="Metadata JSON string")

    remove_parser = subparsers.add_parser("remove", help="Remove a mapping")
    remove_parser.add_argument("--poly", help="Polymarket market id")
    remove_parser.add_argument("--op", help="Opinion market id")

    subparsers.add_parser("list", help="List stored mappings")

    export_parser = subparsers.add_parser("export", help="Export mappings")
    export_parser.add_argument("--format", choices=["yaml", "csv"], default="yaml")
    export_parser.add_argument("--output", required=True, help="Destination file")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    mapper = MarketMapper(args.path)

    if args.command == "add":
        try:
            metadata = json.loads(args.info) if args.info else {}
        except json.JSONDecodeError as exc:
            parser.error(f"Invalid metadata JSON: {exc}")
        mapper.save_mapping(args.poly, args.op, metadata)
        print(f"Mapping saved: {args.poly} -> {args.op}")
        return 0

    if args.command == "remove":
        if not args.poly and not args.op:
            parser.error("At least one of --poly or --op must be provided.")
        removed = mapper.remove_mapping(args.poly, args.op)
        if removed:
            print("Mapping removed.")
        else:
            print("Mapping not found.")
        return 0

    if args.command == "list":
        mappings = mapper.list_mappings()
        if not mappings:
            print("No mappings stored.")
            return 0
        for entry in mappings:
            meta = json.dumps(entry.get("metadata", {}))
            print(f"{entry['polymarket']} -> {entry['opinion']} | {meta}")
        return 0

    if args.command == "export":
        mapper.export(args.output, args.format)
        print(f"Mappings exported to {args.output}")
        return 0

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":
    sys.exit(main())

