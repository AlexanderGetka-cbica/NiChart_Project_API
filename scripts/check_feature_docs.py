#!/usr/bin/env python3
"""
Report completeness of pipeline *feature documentation* — the semantic
interpretation layer under ``results.batch_features.features`` in each pipeline
YAML (see resources/pipelines/SCHEMA.md).

For every pipeline it reports, at a glance:
  - whether it declares a ``results`` block and a ``batch_features`` CSV,
  - whether that CSV's columns are documented (via ``features`` entries, or via a
    ``label_map`` for ROI/segmentation volumes — which documents columns a
    different way, so an empty ``features`` block is fine there),
  - for each authored ``features`` entry, which fields are missing or still hold
    placeholder/TODO text: definition, keywords, references, and DOIs.

It is a *reporter*, not a validator of column coverage: the actual CSV columns
are only known at runtime, so this checks the authored entries, not that every
emitted column has one.

Usage:
    python scripts/check_feature_docs.py            # human-readable report
    python scripts/check_feature_docs.py --strict   # exit 1 if actionable gaps

Exit status:
    0  report printed (no actionable gaps, or not --strict)
    1  --strict and one or more pipelines need feature docs / have broken entries
    2  a resources directory or YAML could not be read
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_DIR = REPO_ROOT / "resources" / "pipelines"

# Utility/test pipelines that intentionally have no result outputs.
SKIP = {"dummy_pipeline"}

# Substrings that mark a value as an unfilled template rather than real content.
_PLACEHOLDER_TOKENS = ("todo", "10.xxxx", "xxxxx", "author et al. yyyy", "replace", "<")


def _looks_placeholder(value: str) -> bool:
    low = value.lower()
    return any(tok in low for tok in _PLACEHOLDER_TOKENS)


def _load(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


class Report:
    """Accumulated status for one pipeline."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.has_results = False
        self.has_batch_features = False
        self.has_label_map = False
        self.n_features = 0
        self.blocking: list[str] = []   # actionable — fails --strict
        self.warnings: list[str] = []   # recommended — informational

    @property
    def status(self) -> str:
        if not self.has_results:
            return "no-results"
        if not self.has_batch_features:
            return "no-batch-features"
        if self.blocking:
            return "PARTIAL"
        if self.n_features > 0:
            return "OK"
        if self.has_label_map:
            return "label_map"
        return "MISSING"


_STATUS_GLYPH = {
    "OK": "OK ",
    "PARTIAL": "!! ",
    "MISSING": "XX ",
    "label_map": "~  ",
    "no-batch-features": "-  ",
    "no-results": "-  ",
}


def _check_feature_entry(rep: Report, col: str, entry: dict) -> None:
    """Populate rep.blocking / rep.warnings for a single feature-dict entry."""
    if not isinstance(entry, dict):
        rep.blocking.append(f"'{col}': entry is not a mapping")
        return

    definition = str(entry.get("definition") or "").strip()
    if not definition:
        rep.blocking.append(f"'{col}': missing definition")
    elif _looks_placeholder(definition):
        rep.blocking.append(f"'{col}': definition is placeholder text")

    if not (entry.get("keywords") or []):
        rep.warnings.append(f"'{col}': no keywords")

    references = entry.get("references") or []
    if not references:
        rep.warnings.append(f"'{col}': no references")
    for i, ref in enumerate(references):
        if not isinstance(ref, dict):
            rep.warnings.append(f"'{col}': reference #{i + 1} is not a mapping")
            continue
        citation = str(ref.get("citation") or "").strip()
        if not citation or _looks_placeholder(citation):
            rep.warnings.append(f"'{col}': reference #{i + 1} has placeholder/missing citation")
        doi = str(ref.get("doi") or "").strip()
        url = str(ref.get("url") or "").strip()
        if (not doi and not url) or _looks_placeholder(doi):
            rep.warnings.append(f"'{col}': reference #{i + 1} missing DOI/URL")


def _analyze(pipeline_id: str, data: dict) -> Report:
    rep = Report(pipeline_id)
    results = data.get("results") or {}
    rep.has_results = bool(results)

    bf = results.get("batch_features") or {}
    rep.has_batch_features = bool(bf)
    if not bf:
        return rep

    rep.has_label_map = bool(bf.get("label_map"))
    features = bf.get("features") or {}
    rep.n_features = len(features)

    for col, entry in features.items():
        _check_feature_entry(rep, col, entry)

    return rep


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 if any pipeline needs feature docs or has broken entries.",
    )
    args = parser.parse_args()

    if not PIPELINES_DIR.is_dir():
        print(f"ERROR: expected {PIPELINES_DIR} to exist", file=sys.stderr)
        return 2

    reports: list[Report] = []
    for pp in sorted(PIPELINES_DIR.glob("*.yaml")):
        if pp.stem in SKIP:
            continue
        try:
            data = _load(pp)
        except Exception as exc:
            print(f"ERROR: could not parse pipeline {pp.name}: {exc}", file=sys.stderr)
            return 2
        if isinstance(data, dict):
            reports.append(_analyze(pp.stem, data))

    # ── Per-pipeline status table ────────────────────────────────────────────
    print("NiChart feature-documentation completeness")
    print("=" * 44)
    print()
    with_bf = [r for r in reports if r.has_batch_features]
    print(
        f"{len(reports)} pipelines | {len(with_bf)} with batch_features | "
        f"{len([r for r in reports if not r.has_results])} without result outputs"
    )
    print()

    note = {
        "OK": "documented",
        "PARTIAL": "entries incomplete",
        "MISSING": "needs feature docs",
        "label_map": "columns via label_map (features optional)",
        "no-batch-features": "results, but no batch_features CSV",
        "no-results": "no result outputs",
    }
    for r in reports:
        glyph = _STATUS_GLYPH[r.status]
        extra = f"{r.n_features} col(s)" if r.n_features else ""
        print(f"  {glyph} {r.name:<38} {note[r.status]:<42} {extra}")

    # ── Detailed gaps ────────────────────────────────────────────────────────
    missing = [r for r in with_bf if r.status == "MISSING"]
    partial = [r for r in with_bf if r.status == "PARTIAL"]
    warned = [r for r in reports if r.warnings]

    if missing:
        print()
        print(f"Needs feature docs — batch_features present, no `features`, no label_map ({len(missing)}):")
        for r in missing:
            print(f"  - {r.name}")

    if partial:
        print()
        print(f"Broken/incomplete feature entries ({len(partial)}):")
        for r in partial:
            print(f"  {r.name}:")
            for msg in r.blocking:
                print(f"      • {msg}")

    if warned:
        print()
        print(f"Recommended additions — keywords / references / DOIs ({len(warned)}):")
        for r in warned:
            print(f"  {r.name}:")
            for msg in r.warnings:
                print(f"      • {msg}")

    # ── Bottom line ──────────────────────────────────────────────────────────
    print()
    actionable = len(missing) + len(partial)
    if actionable:
        print(f"{actionable} pipeline(s) need attention (feature docs / broken entries); "
              f"{len(warned)} with recommended additions.")
    else:
        print(f"No blocking gaps. {len(warned)} pipeline(s) have recommended additions.")

    if args.strict and actionable:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
