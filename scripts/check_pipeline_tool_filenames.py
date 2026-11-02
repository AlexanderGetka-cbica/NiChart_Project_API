#!/usr/bin/env python3
"""
Check that file-type outputs agree on their filename between pipelines and tools.

Because of how output files are mounted (the parent directory is bind-mounted and
the tool writes to its ``path_in_container`` basename inside it), a file output only
lands where the pipeline expects it if:

    basename(tool.mounts[label].path_in_container) == basename(step.outputs[label])

Parent paths may differ freely; only the filename must match. Directory outputs
have no such constraint. A mismatch produces a silently-empty output at runtime
(the tool writes one name, the pipeline looks for another), so this guards against
that whole class of bug.

Also cross-checks the two places a per-subject filename is declared, so they
cannot drift apart:

    tool   outputs.{label}.contents[].pattern      (e.g. "{MRID}_DLMUSE.nii.gz")
    pipe   results.per_subject[].pattern           (e.g. "dlmuse_seg/{MRID}_DLMUSE.nii.gz")

A pipeline's per-subject pattern whose directory is a step output must agree
with what the producing tool declares it writes there. The tool declaration is
what an intermediate upload is validated against, and the pipeline declaration
is what the viewer fetches, so a disagreement means one of the two is lying
about the data on disk.

Usage:
    python scripts/check_pipeline_tool_filenames.py

Exit status:
    0  all file outputs consistent (and every output label wired to a tool)
    1  one or more mismatches / missing wiring found
    2  a resources directory or YAML could not be read
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_DIR = REPO_ROOT / "resources" / "pipelines"
TOOLS_DIR = REPO_ROOT / "resources" / "tools"


def _load(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def main() -> int:
    if not PIPELINES_DIR.is_dir() or not TOOLS_DIR.is_dir():
        print(f"ERROR: expected {PIPELINES_DIR} and {TOOLS_DIR} to exist", file=sys.stderr)
        return 2

    tools: dict[str, dict] = {}
    for tp in sorted(TOOLS_DIR.glob("*.yaml")):
        try:
            data = _load(tp)
        except Exception as exc:
            print(f"ERROR: could not parse tool {tp.name}: {exc}", file=sys.stderr)
            return 2
        if isinstance(data, dict):
            tools[tp.stem] = data

    mismatches: list[str] = []
    missing: list[str] = []
    content_mismatches: list[str] = []
    checked = 0
    contents_checked = 0

    for pp in sorted(PIPELINES_DIR.glob("*.yaml")):
        try:
            pdef = _load(pp)
        except Exception as exc:
            print(f"ERROR: could not parse pipeline {pp.name}: {exc}", file=sys.stderr)
            return 2
        if not isinstance(pdef, dict):
            continue

        for step in pdef.get("steps") or []:
            tool_id = step.get("tool")
            step_id = step.get("id")
            tdef = tools.get(tool_id)
            if tdef is None:
                missing.append(f"{pp.name} step '{step_id}': tool '{tool_id}' not found")
                continue

            t_outputs = tdef.get("outputs") or {}
            t_mounts = tdef.get("mounts") or {}
            for label, pipe_path in (step.get("outputs") or {}).items():
                tout = t_outputs.get(label)
                if tout is None:
                    missing.append(
                        f"{pp.name} step '{step_id}': output '{label}' not declared in tool '{tool_id}'"
                    )
                    continue
                if tout.get("type") != "file":
                    continue  # directory outputs have no filename constraint

                mount = t_mounts.get(label)
                if mount is None:
                    missing.append(
                        f"{pp.name} step '{step_id}': file output '{label}' has no mount in tool '{tool_id}'"
                    )
                    continue

                checked += 1
                pic = mount.get("path_in_container", "")
                tool_base = os.path.basename(pic)
                pipe_base = os.path.basename(str(pipe_path))
                if tool_base != pipe_base:
                    mismatches.append(
                        f"{pp.name}  step '{step_id}' (tool {tool_id}), output '{label}':\n"
                        f"      tool path_in_container : {tool_base!r}   ({pic})\n"
                        f"      pipeline output        : {pipe_base!r}   ({pipe_path})"
                    )

        # ── per_subject patterns vs the producing tool's declared contents ──
        step_output_dirs: dict[str, tuple[str, str, str]] = {}
        for step in pdef.get("steps") or []:
            tdef = tools.get(step.get("tool")) or {}
            t_outputs = tdef.get("outputs") or {}
            for label, pipe_path in (step.get("outputs") or {}).items():
                path = str(pipe_path)
                if os.path.splitext(path)[1]:
                    continue  # file output, not a directory of per-subject files
                rel = path.replace("${STUDY}/", "").rstrip("/")
                step_output_dirs[rel] = (
                    step.get("id"), step.get("tool"), label,
                )

        for item in ((pdef.get("results") or {}).get("per_subject") or []):
            pattern = item.get("pattern")
            if not pattern:
                continue
            directory, _, filename = str(pattern).rpartition("/")
            producer = step_output_dirs.get(directory)
            if producer is None:
                continue  # not a step output (e.g. a raw modality directory)
            step_id, tool_id, label = producer
            declared = [
                c.get("pattern")
                for c in ((tools.get(tool_id) or {}).get("outputs") or {})
                               .get(label, {}).get("contents") or []
                if isinstance(c, dict)
            ]
            if not declared:
                continue  # tool declares nothing; nothing to disagree with
            contents_checked += 1
            if filename not in declared:
                content_mismatches.append(
                    f"{pp.name}  results.per_subject '{item.get('id')}' vs tool "
                    f"{tool_id} output '{label}' (step '{step_id}'):\n"
                    f"      pipeline per_subject pattern : {filename!r}\n"
                    f"      tool declares                : {declared!r}"
                )

    if mismatches:
        print("File-output basename MISMATCHES:")
        for m in mismatches:
            print("  " + m)
    if missing:
        print("Missing tool/label/mount wiring:")
        for m in missing:
            print("  " + m)
    if content_mismatches:
        print("Per-subject pattern vs tool-declared contents MISMATCHES:")
        for m in content_mismatches:
            print("  " + m)

    if mismatches or missing or content_mismatches:
        print(
            f"\nFAIL: {len(mismatches)} mismatch(es), {len(missing)} missing-wiring "
            f"issue(s), {len(content_mismatches)} content mismatch(es)."
        )
        return 1

    print(
        f"OK: {checked} file output(s) and {contents_checked} per-subject pattern(s) "
        "checked across all pipelines; all filenames consistent."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
