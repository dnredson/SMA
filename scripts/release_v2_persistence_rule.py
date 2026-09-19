#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.magistrala import AtomClient, AtomConfig, RulesClient
from smarter_adapter.magistrala.rules import RulesError


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Release one Magistrala Rules slot by deleting only a persistence "
            "rule explicitly marked as managed by Smarter Adapter."
        )
    )
    p.add_argument("workspace_id", help="workspace that currently owns the managed persistence rule")
    p.add_argument("rule_id", help="rule id to inspect and delete")
    p.add_argument(
        "--yes",
        action="store_true",
        help="perform deletion; without this flag the command is dry-run only",
    )
    return p


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    atom_url = env("ATOM_URL", "http://127.0.0.1")
    rules_url = env("MAGISTRALA_RULES_URL", atom_url)
    token = env("ATOM_SERVICE_TOKEN") or env("ATOM_ADMIN_TOKEN") or env("ATOM_TOKEN")
    username = env("ATOM_USERNAME", "admin")
    password = env("ATOM_PASSWORD")
    if not token and not password:
        print("ERROR: set ATOM_SERVICE_TOKEN/ATOM_ADMIN_TOKEN or ATOM_PASSWORD", file=sys.stderr)
        return 2

    atom = AtomClient(
        AtomConfig(
            base_url=atom_url,
            graphql_url=env("ATOM_GRAPHQL_URL"),
            token=token,
            username=username,
            password=password,
        )
    )
    rules = RulesClient(rules_url, atom.token, invalidate_token=atom.tokens.invalidate)

    try:
        rule = rules.get_rule(args.workspace_id, args.rule_id)
    except RulesError as exc:
        print(f"ERROR: unable to read rule: {exc}", file=sys.stderr)
        return 1

    print(f"Rule ID:       {rule.get('id')}")
    print(f"Name:          {rule.get('name')}")
    print(f"Workspace:     {rule.get('workspace') or args.workspace_id}")
    print(f"Input channel: {rule.get('input_channel')}")
    print(f"Status:        {rule.get('status')}")
    print(f"Metadata:      {rule.get('metadata')}")
    print(f"Tags:          {rule.get('tags')}")
    print(f"Outputs:       {rule.get('outputs')}")

    if not rules.is_managed_persistence_rule(rule):
        print(
            "REFUSED: this rule is not positively identified as a Smarter Adapter-managed "
            "SenML persistence rule. Nothing was deleted.",
            file=sys.stderr,
        )
        return 3

    if not args.yes:
        print("DRY RUN: rule is eligible for release. Re-run with --yes to delete it.")
        return 0

    try:
        rules.delete_rule(args.workspace_id, args.rule_id)
    except RulesError as exc:
        print(f"ERROR: unable to delete rule: {exc}", file=sys.stderr)
        return 1

    print("DELETED: managed persistence rule released successfully.")
    print("Existing Timescale rows are not deleted by this operation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
