"""
Manual upload of intermediate pipeline data, for starting a run from a later step.

Some users already hold part of a pipeline's results but cannot upload the raw
images they came from. This service lets them supply a later step's inputs
directly, then start the pipeline from that step.

The feature is deliberately narrow, so that it cannot become a general file
store inside a project:

* **The client never sends a destination path.** It sends a pipeline, a start
  step and a slot ID; the destination is derived from the pipeline YAML here.
* **Only slots a skipped earlier step would have produced can be written.**
  Raw project data (``t1/``, ``participants/participants.csv``) is reported but
  must go through its own uploader, and anything a step that *will* run produces
  is reported and left alone.
* **Uploads are checked against what the pipeline actually needs**: the paths
  the running steps read, plus any ``contents`` the producing and consuming
  tools declare.

Slot classification reuses the orchestrator's own path resolution and
containment test (``job_service._resolve_path`` / ``_paths_overlap``), so a slot
is matched to a step exactly as the real run would match it.
"""

import csv
import fnmatch
import json
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException, UploadFile

from app import modalities
from app.backends.base import ContentRule
from app.config import Settings
from app.models.catalog import PipelineStep
from app.models.files import (
    IntermediateInputsResponse,
    IntermediateSlot,
    IntermediateUploadResult,
    ProjectInputHint,
    RejectedUploadFile,
    SlotConsumer,
    SlotContentRequirement,
    SlotCoverage,
)
from app.services import (
    catalog_service,
    file_service,
    job_service,
    manual_upload_store,
    s3_sync_service,
)
from app.services.path_security import (
    PathEscapeError,
    assert_no_symlinks,
    assert_safe_path,
    assert_safe_upload_filename,
    is_skipped_member,
    safe_unzip_filtered,
)

# Extensions accepted when a slot declares no contents of its own. Deliberately
# narrow: these are the only file kinds any current tool reads or writes.
_DEFAULT_EXTENSIONS = frozenset({".nii", ".nii.gz", ".csv", ".tsv", ".json", ".txt"})

_PARTICIPANTS_REL = "participants/participants.csv"
_PROVENANCE = "_provenance.json"
_STAGING_SUBDIR = Path("_upload") / "intermediate"
_CHUNK_BYTES = 1024 * 1024


# ── Slot model (internal) ─────────────────────────────────────────────────────

@dataclass
class _Slot:
    slot_id: str
    path: str                      # project-relative
    type: str                      # "directory" | "file"
    kind: str                      # "upload" | "project_input" | "generated"
    produced_by_step: str | None = None
    consumers: list[SlotConsumer] = field(default_factory=list)
    rules: list[tuple[ContentRule, str]] = field(default_factory=list)  # (rule, source)


def _rel(path: str | Path, study_dir: Path) -> str:
    """Project-relative form of a resolved path, for display."""
    try:
        return str(Path(path).relative_to(study_dir))
    except ValueError:
        return str(path)


def _slot_type(resolved: str) -> str:
    """File vs directory, by the orchestrator's own convention.

    A suffixed path is a file output and an unsuffixed one a directory — the
    same rule ``_verify_outputs_present`` applies, so classification here cannot
    disagree with what a real run expects to find.
    """
    return "file" if Path(resolved).suffix else "directory"


# ── Subject set ───────────────────────────────────────────────────────────────

def _participants_mrids(project_path: Path) -> list[str]:
    """MRIDs listed in participants.csv, or [] when it is absent or unusable."""
    csv_path = project_path / _PARTICIPANTS_REL
    if not csv_path.exists():
        return []
    try:
        with csv_path.open(newline="") as f:
            reader = csv.DictReader(f)
            col = next(
                (c for c in (reader.fieldnames or []) if c.lower() == "mrid"), None
            )
            if not col:
                return []
            return sorted({
                str(row[col]).strip()
                for row in reader
                if row.get(col) and str(row[col]).strip()
            })
    except Exception:
        return []


def subject_set(project_path: Path) -> list[str] | None:
    """Subjects a per-subject rule is measured against, or None when unknowable.

    Mirrors what ``readiness_service`` counts for ``min_subjects.constraints``,
    so coverage here agrees with the readiness panel the user has already seen:
    imaging subjects intersected with participants.csv when both exist.

    Returns None only when the project has neither imaging nor a usable
    participants.csv. That is the normal state for the very case this feature
    exists for — a user who could not upload raw data — so coverage is then
    reported without an expected count rather than being called complete or
    incomplete on no evidence.
    """
    imaging = file_service.collect_project_mrids(project_path)
    participants = _participants_mrids(project_path)
    if imaging and participants:
        return sorted(set(imaging) & set(participants))
    if imaging:
        return sorted(imaging)
    if participants:
        return participants
    return None


# ── Classification ────────────────────────────────────────────────────────────

def _resolve_steps(
    pipeline_steps: list[PipelineStep], study_dir: Path
) -> list[tuple[PipelineStep, dict[str, str], dict[str, str]]]:
    """Resolve every step's inputs and outputs, threading step references."""
    step_outputs: dict[str, dict[str, str]] = {}
    resolved: list[tuple[PipelineStep, dict[str, str], dict[str, str]]] = []
    for step in pipeline_steps:
        ins = {
            label: job_service._resolve_path(val, study_dir, step_outputs)
            for label, val in (step.inputs or {}).items()
        }
        outs = {
            label: job_service._resolve_path(val, study_dir, step_outputs)
            for label, val in (step.outputs or {}).items()
        }
        step_outputs[step.id] = outs
        resolved.append((step, ins, outs))
    return resolved


def _project_input_hint(rel_path: str) -> ProjectInputHint:
    """Which uploader covers a raw-project-data path."""
    head = Path(rel_path).parts[0] if Path(rel_path).parts else ""
    if head in modalities.CODES:
        return ProjectInputHint(type="modality", code=head)
    if rel_path == _PARTICIPANTS_REL:
        return ProjectInputHint(type="participants")
    if head == "idat":
        return ProjectInputHint(type="idat")
    return ProjectInputHint(type="other")


def _tool_rules(tools_path: Path, tool_id: str, label: str, side: str) -> list[ContentRule]:
    """Declared contents for one tool slot; [] when the tool declares none."""
    try:
        spec = catalog_service.load_tool_spec(tools_path, tool_id)
    except Exception:
        return []
    table = spec.output_contents if side == "output" else spec.input_contents
    return list(table.get(label) or [])


def build_slots(
    study_dir: Path,
    pipeline_steps: list[PipelineStep],
    start_from_step: str,
    tools_path: Path,
) -> list[_Slot]:
    """Classify every path the steps from *start_from_step* onwards will read.

    Starting at the first step is allowed and yields no ``upload`` slots, since
    nothing is skipped — consistent with the plan and submit routes, which also
    accept it.
    """
    start_index = next(
        (i for i, s in enumerate(pipeline_steps) if s.id == start_from_step), None
    )
    if start_index is None:
        raise HTTPException(
            400,
            f"Unknown start_from_step '{start_from_step}'. Steps are: "
            + ", ".join(s.id for s in pipeline_steps),
        )

    resolved = _resolve_steps(pipeline_steps, study_dir)
    running = resolved[start_index:]
    skipped = resolved[:start_index]

    slots: dict[str, _Slot] = {}
    order: list[str] = []

    def _slot(slot_id: str, **kwargs) -> _Slot:
        if slot_id not in slots:
            slots[slot_id] = _Slot(slot_id=slot_id, **kwargs)
            order.append(slot_id)
        return slots[slot_id]

    for step, ins, _outs in running:
        for label, path in ins.items():
            p = Path(path)

            producer = next(
                (
                    (s.id, out_label, out_path)
                    for s, _i, outs in running
                    for out_label, out_path in outs.items()
                    if job_service._paths_overlap(p, Path(out_path))
                ),
                None,
            )
            if producer is not None:
                step_id, out_label, out_path = producer
                slot = _slot(
                    f"{step_id}.{out_label}",
                    path=_rel(out_path, study_dir),
                    type=_slot_type(out_path),
                    kind="generated",
                    produced_by_step=step_id,
                )
                slot.consumers.append(
                    SlotConsumer(step_id=step.id, input=label, path=_rel(path, study_dir))
                )
                continue

            producer = next(
                (
                    (s.id, out_label, out_path, s.tool)
                    for s, _i, outs in skipped
                    for out_label, out_path in outs.items()
                    if job_service._paths_overlap(p, Path(out_path))
                ),
                None,
            )
            if producer is not None:
                step_id, out_label, out_path, tool_id = producer
                slot = _slot(
                    f"{step_id}.{out_label}",
                    path=_rel(out_path, study_dir),
                    type=_slot_type(out_path),
                    kind="upload",
                    produced_by_step=step_id,
                )
                if not slot.rules:
                    # The producing tool's description of what it writes.
                    slot.rules += [
                        (r, "tool_declared")
                        for r in _tool_rules(tools_path, tool_id, out_label, "output")
                    ]
                slot.consumers.append(
                    SlotConsumer(step_id=step.id, input=label, path=_rel(path, study_dir))
                )
                # A step reading a path *inside* the slot makes that exact file
                # required, whatever any tool declares.
                inner: str | None = None
                if Path(out_path) != p:
                    try:
                        inner = str(p.relative_to(Path(out_path)))
                    except ValueError:
                        inner = None
                    if inner:
                        slot.rules.append(
                            (ContentRule(pattern=inner, per_subject=False), "pipeline_path")
                        )
                # What this consumer requires of it. A consuming *file* slot
                # declares its columns with no pattern, because from the tool's
                # point of view there is only one file; against a directory slot
                # those columns belong to the inner path the step actually reads,
                # not to the directory itself.
                for rule in _tool_rules(tools_path, step.tool, label, "input"):
                    if rule.pattern is None and slot.type == "directory":
                        if inner is None:
                            continue
                        rule = ContentRule(
                            pattern=inner,
                            per_subject=False,
                            csv_columns=rule.csv_columns,
                        )
                    slot.rules.append((rule, "tool_declared"))
                continue

            slot = _slot(
                f"project.{_rel(path, study_dir)}",
                path=_rel(path, study_dir),
                type=_slot_type(path),
                kind="project_input",
            )
            slot.consumers.append(
                SlotConsumer(step_id=step.id, input=label, path=_rel(path, study_dir))
            )

    return [slots[sid] for sid in order]


# ── Evaluation ────────────────────────────────────────────────────────────────

def _dir_entries(path: Path) -> list[Path]:
    """Real files in a slot directory, ignoring our own provenance marker."""
    if not path.is_dir():
        return []
    return [
        e for e in path.rglob("*")
        if e.is_file() and e.name != _PROVENANCE and e.suffix != ".tmp"
    ]


def _is_present(path: Path, slot_type: str) -> bool:
    if slot_type == "file":
        return path.is_file() and path.stat().st_size > 0
    return bool(_dir_entries(path))


def _csv_problem(path: Path, columns: tuple[str, ...]) -> str | None:
    """None when *path* parses as CSV and holds every column, else why not."""
    try:
        with path.open(newline="") as f:
            header = next(csv.reader(f), None)
    except Exception as exc:
        return f"could not be read as CSV: {exc}"
    if not header:
        return "is empty"
    present = {h.strip().lower() for h in header}
    missing = [c for c in columns if c.lower() not in present]
    if missing:
        return "is missing required column(s): " + ", ".join(missing)
    return None


def _dedupe_rules(
    rules: list[tuple[ContentRule, str]]
) -> list[tuple[ContentRule, str]]:
    """One requirement per (pattern, per_subject), with column sets unioned.

    The same file is often described more than once — by the tool that writes
    it, by the tool that reads it, and by the pipeline path that names it. Those
    are one requirement, and reporting it three times would just be noise. The
    column checks are additive, so they are unioned rather than overwritten, and
    'pipeline_path' wins the source because a path a step reads is required
    whatever any tool declares.
    """
    best: dict[tuple, tuple[ContentRule, str]] = {}
    for rule, source in rules:
        key = (rule.pattern, rule.per_subject)
        existing = best.get(key)
        if existing is None:
            best[key] = (rule, source)
            continue
        prev_rule, prev_source = existing
        columns = tuple(dict.fromkeys(
            (prev_rule.csv_columns or ()) + (rule.csv_columns or ())
        )) or None
        best[key] = (
            ContentRule(
                pattern=rule.pattern,
                per_subject=rule.per_subject,
                csv_columns=columns,
            ),
            "pipeline_path" if "pipeline_path" in (prev_source, source) else source,
        )
    return list(best.values())


def _evaluate_rule(
    slot_path: Path,
    slot_type: str,
    rule: ContentRule,
    source: str,
    subjects: list[str] | None,
) -> tuple[SlotContentRequirement, set[str]]:
    """Check one rule. Returns the requirement and the MRIDs it matched."""
    matched_mrids: set[str] = set()

    # A file slot's own file.
    if rule.pattern is None or slot_type == "file":
        pattern = rule.pattern or slot_path.name
        if not slot_path.is_file() or slot_path.stat().st_size == 0:
            return SlotContentRequirement(
                pattern=pattern, per_subject=False,
                csv_columns=list(rule.csv_columns) if rule.csv_columns else None,
                source=source, status="missing",
                problem="the file is missing or empty",
            ), matched_mrids
        if rule.csv_columns:
            problem = _csv_problem(slot_path, rule.csv_columns)
            if problem:
                return SlotContentRequirement(
                    pattern=pattern, per_subject=False,
                    csv_columns=list(rule.csv_columns),
                    source=source, status="invalid",
                    problem=f"{slot_path.name} {problem}",
                ), matched_mrids
        return SlotContentRequirement(
            pattern=pattern, per_subject=False,
            csv_columns=list(rule.csv_columns) if rule.csv_columns else None,
            source=source, status="met",
        ), matched_mrids

    cols = list(rule.csv_columns) if rule.csv_columns else None

    if rule.per_subject:
        if subjects is None:
            # No expected subject set: report what is there without a verdict
            # against a cohort we cannot know.
            found = [
                f for f in _dir_entries(slot_path)
                if _matches(rule.pattern, f.relative_to(slot_path), None)
            ]
            status = "met" if found else "missing"
            return SlotContentRequirement(
                pattern=rule.pattern, per_subject=True, csv_columns=cols,
                source=source, status=status,
                problem=None if found else "no matching files found",
            ), matched_mrids
        for mrid in subjects:
            target = rule.pattern.replace("{MRID}", mrid)
            if list(slot_path.glob(target)):
                matched_mrids.add(mrid)
        missing = [m for m in subjects if m not in matched_mrids]
        if not missing:
            return SlotContentRequirement(
                pattern=rule.pattern, per_subject=True, csv_columns=cols,
                source=source, status="met",
            ), matched_mrids
        return SlotContentRequirement(
            pattern=rule.pattern, per_subject=True, csv_columns=cols,
            source=source,
            status="missing" if not matched_mrids else "met",
            problem=(
                f"{len(missing)} of {len(subjects)} subject(s) have no matching file, "
                f"e.g. {', '.join(missing[:5])}"
            ),
        ), matched_mrids

    # Single fixed file (or glob) inside a directory slot.
    hits = list(slot_path.glob(rule.pattern))
    hits = [h for h in hits if h.is_file()]
    if not hits:
        return SlotContentRequirement(
            pattern=rule.pattern, per_subject=False, csv_columns=cols,
            source=source, status="missing",
            problem=f"no file matching '{rule.pattern}'",
        ), matched_mrids
    if rule.csv_columns:
        problem = _csv_problem(hits[0], rule.csv_columns)
        if problem:
            return SlotContentRequirement(
                pattern=rule.pattern, per_subject=False, csv_columns=cols,
                source=source, status="invalid",
                problem=f"{hits[0].name} {problem}",
            ), matched_mrids
    return SlotContentRequirement(
        pattern=rule.pattern, per_subject=False, csv_columns=cols,
        source=source, status="met",
    ), matched_mrids


def _matches(pattern: str, rel: Path, mrid: str | None) -> bool:
    """Whether *rel* matches a {MRID}-bearing pattern."""
    probe = pattern.replace("{MRID}", mrid if mrid is not None else "*")
    return fnmatch.fnmatch(str(rel), probe)


def evaluate_slot(
    slot: _Slot, study_dir: Path, subjects: list[str] | None
) -> IntermediateSlot:
    """Turn an internal slot into its API form, checking it against disk."""
    path = study_dir / slot.path
    present = _is_present(path, slot.type)
    problems: list[str] = []
    requirements: list[SlotContentRequirement] = []
    coverage: SlotCoverage | None = None

    if slot.kind == "generated":
        return IntermediateSlot(
            slot_id=slot.slot_id, path=slot.path, type=slot.type, kind=slot.kind,
            produced_by_step=slot.produced_by_step, consumed_by=slot.consumers,
            present=present, status="met", problems=[],
        )

    if slot.kind == "project_input":
        return IntermediateSlot(
            slot_id=slot.slot_id, path=slot.path, type=slot.type, kind=slot.kind,
            consumed_by=slot.consumers,
            project_input_hint=_project_input_hint(slot.path),
            present=present,
            status="met" if present else "missing",
            problems=[] if present else ["No data at this path yet."],
        )

    rules = _dedupe_rules(slot.rules)
    per_subject_matched: set[str] | None = None
    has_per_subject = any(r.per_subject for r, _s in rules)

    # Evaluated whether or not the slot exists yet: a client needs to show what
    # a slot must contain *before* anything has been uploaded to it, so an
    # absent slot reports its requirements as missing rather than reporting none.
    for rule, source in rules:
        requirement, matched = _evaluate_rule(
            path, slot.type, rule, source, subjects
        )
        requirements.append(requirement)
        if rule.per_subject:
            per_subject_matched = (
                matched if per_subject_matched is None
                else per_subject_matched & matched
            )
        if requirement.problem:
            problems.append(requirement.problem)

    if has_per_subject:
        matched_set = per_subject_matched or set()
        if subjects is None:
            found = set()
            if present:
                for rule, _source in rules:
                    if not rule.per_subject or not rule.pattern:
                        continue
                    for f in _dir_entries(path):
                        if _matches(rule.pattern, f.relative_to(path), None):
                            found.add(str(f.relative_to(path)))
            coverage = SlotCoverage(
                expected_subjects=None, matched=len(found), missing_mrids=[]
            )
            problems.append(
                "This project has no imaging and no participants.csv, so per-subject "
                "coverage cannot be checked. Verify the file count yourself."
            )
        else:
            coverage = SlotCoverage(
                expected_subjects=len(subjects),
                matched=len(matched_set),
                missing_mrids=[m for m in subjects if m not in matched_set],
            )

    if not present:
        status = "missing"
    elif any(r.status != "met" for r in requirements):
        status = "partial"
    elif coverage is not None and coverage.expected_subjects is not None and (
        coverage.matched < coverage.expected_subjects
    ):
        status = "partial"
    else:
        status = "met"

    return IntermediateSlot(
        slot_id=slot.slot_id, path=slot.path, type=slot.type, kind=slot.kind,
        produced_by_step=slot.produced_by_step, consumed_by=slot.consumers,
        required_contents=requirements, present=present, coverage=coverage,
        status=status, problems=problems,
    )


def get_requirements(
    settings: Settings, study_dir: Path, pipeline_id: str, start_from_step: str
) -> IntermediateInputsResponse:
    """Read-only view of what the run needs and what is already in place."""
    pipeline = catalog_service.get_pipeline(
        settings.pipelines_path, pipeline_id, None, settings.tools_path
    )
    slots = build_slots(
        study_dir, pipeline.steps, start_from_step, settings.tools_path
    )
    subjects = subject_set(study_dir)
    return IntermediateInputsResponse(
        pipeline_id=pipeline_id,
        start_from_step=start_from_step,
        slots=[evaluate_slot(s, study_dir, subjects) for s in slots],
    )


# ── Upload ────────────────────────────────────────────────────────────────────

def _allowed_extensions(slot: _Slot) -> frozenset[str]:
    """Extensions accepted for a slot: those its rules imply, else the default."""
    declared: set[str] = set()
    for rule, _source in slot.rules:
        pattern = rule.pattern or ""
        for ext in _DEFAULT_EXTENSIONS:
            if pattern.endswith(ext):
                declared.add(ext)
    return frozenset(declared) if declared else _DEFAULT_EXTENSIONS


def _extension_of(name: str) -> str:
    lowered = name.lower()
    if lowered.endswith(".nii.gz"):
        return ".nii.gz"
    return Path(lowered).suffix


def _find_slot(slots: list[_Slot], slot_id: str) -> _Slot:
    slot = next((s for s in slots if s.slot_id == slot_id), None)
    if slot is None:
        raise HTTPException(
            400,
            f"Unknown slot '{slot_id}' for this pipeline and start step. "
            "Re-read the intermediate-inputs endpoint; slot IDs are only valid "
            "for the (pipeline, start step) they were produced for.",
        )
    if slot.kind != "upload":
        detail = {
            "project_input": (
                "This is raw project data that no pipeline step produces. Upload it "
                "through the images or participants uploader instead."
            ),
            "generated": (
                "A step that will run produces this path, so there is nothing to "
                "upload."
            ),
        }[slot.kind]
        raise HTTPException(400, f"Slot '{slot_id}' cannot be uploaded to. {detail}")
    return slot


async def _stage_files(
    files: list[UploadFile],
    staging: Path,
    slot: _Slot,
    settings: Settings,
) -> tuple[int, list[RejectedUploadFile], list[str]]:
    """Write acceptable parts into *staging*. Returns (written, rejected, skipped)."""
    allow_subdirs = slot.type == "directory"
    allowed = _allowed_extensions(slot)
    file_cap = settings.max_upload_file_mb * 1024 * 1024
    request_cap = settings.max_upload_request_mb * 1024 * 1024

    written = 0
    total = 0
    rejected: list[RejectedUploadFile] = []
    skipped: list[str] = []

    for upload in files:
        name = (upload.filename or "").replace("\\", "/")
        if not name:
            rejected.append(RejectedUploadFile(name="(unnamed)", reason="no filename"))
            continue

        # Safety first: an escape must fail the request loudly, never be
        # quietly reclassified as something droppable.
        assert_safe_upload_filename(name, allow_subdirs=allow_subdirs)

        # Dropped, not rejected: Finder metadata and dotfiles are never written.
        if is_skipped_member(name):
            skipped.append(name)
            continue

        ext = _extension_of(name)
        if ext not in allowed:
            rejected.append(RejectedUploadFile(
                name=name,
                reason=(
                    f"'{ext or Path(name).name}' is not an accepted file type here; "
                    f"expected one of {', '.join(sorted(allowed))}"
                ),
            ))
            continue

        target = staging / name if allow_subdirs else staging / Path(name).name
        assert_safe_path(staging, target)
        target.parent.mkdir(parents=True, exist_ok=True)

        size = 0
        oversized = False
        with target.open("wb") as out:
            while chunk := await upload.read(_CHUNK_BYTES):
                size += len(chunk)
                if size > file_cap:
                    oversized = True
                    break
                total += len(chunk)
                if total > request_cap:
                    raise HTTPException(
                        413,
                        f"Upload exceeds the per-request limit of "
                        f"{settings.max_upload_request_mb} MiB. Nothing was saved; "
                        "split the upload into smaller batches or send a zip.",
                    )
                out.write(chunk)
        if oversized:
            target.unlink(missing_ok=True)
            total -= size
            rejected.append(RejectedUploadFile(
                name=name,
                reason=(
                    f"file is larger than the {settings.max_upload_file_mb} MiB "
                    "per-file limit"
                ),
            ))
            continue
        written += 1

    return written, rejected, skipped


def _stage_zip_members(
    staging: Path, slot: _Slot, settings: Settings
) -> tuple[int, list[RejectedUploadFile], list[str]]:
    """Filter an already-extracted archive in place, applying the slot's rules."""
    allowed = _allowed_extensions(slot)
    file_cap = settings.max_upload_file_mb * 1024 * 1024
    rejected: list[RejectedUploadFile] = []
    written = 0

    # A Finder-zipped folder nests everything under one top-level directory.
    # Strip it so the archive's inner layout lines up with the slot.
    entries = [e for e in staging.iterdir()]
    if len(entries) == 1 and entries[0].is_dir():
        inner = entries[0]
        for child in list(inner.iterdir()):
            shutil.move(str(child), str(staging / child.name))
        inner.rmdir()

    for path in sorted(staging.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(staging))
        ext = _extension_of(rel)
        if ext not in allowed:
            rejected.append(RejectedUploadFile(
                name=rel,
                reason=(
                    f"'{ext or path.name}' is not an accepted file type here; "
                    f"expected one of {', '.join(sorted(allowed))}"
                ),
            ))
            path.unlink()
            continue
        if path.stat().st_size > file_cap:
            rejected.append(RejectedUploadFile(
                name=rel,
                reason=(
                    f"file is larger than the {settings.max_upload_file_mb} MiB "
                    "per-file limit"
                ),
            ))
            path.unlink()
            continue
        written += 1

    return written, rejected, []


def _move_into_place(staging: Path, dest: Path, slot: _Slot, replace: bool) -> None:
    """Move staged content into the slot. Merge by default, clear first if *replace*."""
    if slot.type == "file":
        staged = staging / dest.name
        if not staged.is_file():
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged), str(dest))
        return

    if replace and dest.exists():
        for entry in list(dest.iterdir()):
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
    dest.mkdir(parents=True, exist_ok=True)
    for staged in sorted(staging.rglob("*")):
        if not staged.is_file():
            continue
        target = dest / staged.relative_to(staging)
        assert_safe_path(dest, target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged), str(target))


async def _push_slot_to_s3(
    settings: Settings,
    user,
    project_id: str,
    staging: Path,
    slot: _Slot,
    replace: bool,
) -> None:
    """Upload staged slot content to the slot's own S3 keys.

    The staging directory already mirrors the slot, so syncing it against the
    slot's remote prefix lands every file at the key the slot occupies. A
    replace additionally clears the remote prefix first, since the sync only
    adds and overwrites and would otherwise leave remote files the user meant
    to delete — which the next pre-run download would bring back.

    No-op outside cloud mode, or when no data bucket is configured.
    """
    if settings.execution_mode != "cloud" or not settings.s3_data_bucket:
        return

    project = s3_sync_service.project_prefix(
        settings.s3_data_prefix, user.sub, project_id
    )
    # A file slot's staged file is named after the slot, so its remote prefix is
    # the slot's parent; a directory slot maps onto its own prefix.
    rel = str(Path(slot.path).parent) if slot.type == "file" else slot.path
    prefix = project if rel in (".", "") else f"{project}/{rel}"

    try:
        if replace and slot.type == "directory":
            await s3_sync_service.delete_s3_prefix(settings.s3_data_bucket, prefix)
        await s3_sync_service.sync_to_s3(staging, settings.s3_data_bucket, prefix)
    except Exception as exc:
        raise HTTPException(
            502,
            "Nothing was saved: the upload could not be written to cloud storage, "
            "and saving it locally alone would leave data that a run would "
            f"silently discard. {exc}",
        )


async def upload_to_slot(
    settings: Settings,
    user,
    project_id: str,
    study_dir: Path,
    pipeline_id: str,
    start_from_step: str,
    slot_id: str,
    replace: bool,
    files: list[UploadFile] | None = None,
    archive: UploadFile | None = None,
) -> IntermediateUploadResult:
    """Populate one upload slot by hand.

    Content is staged under ``_upload/intermediate/`` first and only moved into
    the slot once every check has passed, so a rejected request leaves the slot
    exactly as it was.

    Escapes — traversal, absolute paths, symlinks — fail the whole request with
    a 400 and write nothing. A disallowed extension or an oversized file is a
    per-file rejection and the rest of the upload still proceeds, since those
    are ordinary mistakes rather than attacks.
    """
    pipeline = catalog_service.get_pipeline(
        settings.pipelines_path, pipeline_id, None, settings.tools_path
    )
    slots = build_slots(study_dir, pipeline.steps, start_from_step, settings.tools_path)
    slot = _find_slot(slots, slot_id)

    dest = study_dir / slot.path
    try:
        assert_safe_path(study_dir, dest)
    except PathEscapeError as exc:
        raise HTTPException(400, f"Slot resolves outside the project: {exc}")

    staging = study_dir / _STAGING_SUBDIR / uuid.uuid4().hex
    staging.mkdir(parents=True, exist_ok=True)
    try:
        try:
            if archive is not None:
                archive_path = staging.parent / f"{staging.name}.zip"
                with archive_path.open("wb") as out:
                    while chunk := await archive.read(_CHUNK_BYTES):
                        out.write(chunk)
                try:
                    _extracted, skipped = safe_unzip_filtered(archive_path, staging)
                finally:
                    archive_path.unlink(missing_ok=True)
                written, rejected, _ = _stage_zip_members(staging, slot, settings)
            else:
                written, rejected, skipped = await _stage_files(
                    files or [], staging, slot, settings
                )
        except PathEscapeError as exc:
            raise HTTPException(
                400,
                f"Upload rejected and nothing was saved: {exc}. File names must be "
                "relative, must not contain '..', and must not be symlinks.",
            )

        # A file slot is saved under the slot's own name whatever the upload was
        # called, because the consuming step reads that exact path. Renaming here
        # rather than at move time keeps the staged tree an exact mirror of the
        # destination, which is what makes the S3 push below land on the right key.
        if slot.type == "file":
            staged_files = [f for f in sorted(staging.rglob("*")) if f.is_file()]
            if len(staged_files) > 1:
                raise HTTPException(
                    400,
                    f"Slot '{slot.slot_id}' is a single file, but "
                    f"{len(staged_files)} files were uploaded.",
                )
            if staged_files and staged_files[0].name != dest.name:
                staged_files[0].rename(staging / dest.name)

        # A link created inside staging could redirect the move below; the
        # lexical checks cannot see it because it only exists once bytes land.
        try:
            assert_no_symlinks(staging)
        except PathEscapeError as exc:
            raise HTTPException(400, f"Upload rejected and nothing was saved: {exc}")

        if written == 0:
            if rejected:
                raise HTTPException(
                    400,
                    "No acceptable files in the upload: "
                    + "; ".join(f"{r.name} — {r.reason}" for r in rejected[:5]),
                )
            raise HTTPException(
                400,
                "The upload contained no files to save."
                + (f" {len(skipped)} system file(s) were skipped." if skipped else ""),
            )

        total_bytes = sum(f.stat().st_size for f in staging.rglob("*") if f.is_file())

        # Cloud mode: the slot's files go to S3 *before* the local move, so a
        # failed push leaves no local copy that disagrees with S3. Pushing
        # afterwards would leave exactly that, and the next run's pre-run
        # download would silently revert the upload.
        await _push_slot_to_s3(settings, user, project_id, staging, slot, replace)

        _move_into_place(staging, dest, slot, replace)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    # The producing step did not make what is now on disk, so its cached result
    # must not be reused by a later full run.
    cleared = job_service.clear_cache_entries_producing(study_dir, dest)

    manual_upload_store.append_record(study_dir, {
        "slot_id": slot.slot_id,
        "path": slot.path,
        "pipeline_id": pipeline_id,
        "start_from_step": start_from_step,
        "produced_by_step": slot.produced_by_step,
        "replace": replace,
        "files": written,
        "total_bytes": total_bytes,
        "user_id": user.sub,
        "via": "zip" if archive is not None else "files",
    })
    _write_slot_provenance(dest, slot, pipeline_id, user.sub, written, total_bytes)

    # The bookkeeping written just above — the cleared cache entries and the
    # upload record that keeps a legacy entry from being reused — has to reach
    # S3 too, or the next run's pre-run download restores the old copies and
    # undoes it.
    await file_service.publish_uploads(settings, user, project_id, study_dir)

    subjects = subject_set(study_dir)
    refreshed = build_slots(
        study_dir, pipeline.steps, start_from_step, settings.tools_path
    )
    return IntermediateUploadResult(
        slot=evaluate_slot(_find_slot(refreshed, slot_id), study_dir, subjects),
        written=written,
        rejected=rejected,
        skipped=skipped,
        replaced=replace,
        cache_entries_cleared=len(cleared),
    )


def _write_slot_provenance(
    dest: Path, slot: _Slot, pipeline_id: str, user_id: str, files: int, total_bytes: int
) -> None:
    """Mark a hand-populated directory so provenance reports are not silent about it."""
    target = dest.parent if slot.type == "file" else dest
    payload = {
        "source": "manual_intermediate_upload",
        "slot_id": slot.slot_id,
        "pipeline_id": pipeline_id,
        "step_id": slot.produced_by_step,
        "uploaded_by": user_id,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
        "total_bytes": total_bytes,
        "note": (
            "Supplied by hand, not produced by a pipeline run on this project. "
            "Results derived from it are not reproducible from this project's raw data."
        ),
    }
    try:
        target.mkdir(parents=True, exist_ok=True)
        (target / _PROVENANCE).write_text(json.dumps(payload, indent=2))
    except Exception:
        # Provenance is a record, not a gate; a failure here must not undo an
        # upload that has already been written and synced.
        pass
