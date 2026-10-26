"""
Tests for readiness evaluation, focused on ``min_subjects.constraints`` —
per-cohort minimum counts such as "at least 30 subjects with DX == CN".

check_readiness() is a pure function over a project directory, so these build a
project on disk rather than going through the HTTP layer.
"""

import csv

from app.services import readiness_service

CN_CONTROLS = [{"label": "Cognitively normal controls", "where": [{"column": "DX", "equals": "CN"}]}]


def _write_imaging(project, mrids):
    d = project / "t1"
    d.mkdir(parents=True, exist_ok=True)
    for mrid in mrids:
        (d / f"{mrid}.nii.gz").write_bytes(b"x")


def _write_participants(project, rows, columns=("MRID", "DX", "Age", "Sex")):
    d = project / "participants"
    d.mkdir(parents=True, exist_ok=True)
    with (d / "participants.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _requires(required=30, recommended=30, constraints=None):
    spec = {"required": required, "recommended": recommended}
    if constraints is not None:
        spec["constraints"] = constraints
    return ["needs_T1", {"min_subjects": spec}]


def _cohort(project, n_cn, n_other, dx_value="CN", extra=None):
    """Build a project with n_cn CN subjects and n_other non-CN subjects."""
    rows = []
    for i in range(n_cn):
        rows.append({"MRID": f"cn{i:03d}", "DX": dx_value, "Age": "70", "Sex": "M", **(extra or {})})
    for i in range(n_other):
        rows.append({"MRID": f"ad{i:03d}", "DX": "AD", "Age": "72", "Sex": "F", **(extra or {})})
    _write_imaging(project, [r["MRID"] for r in rows])
    _write_participants(project, rows)
    return rows


def _check(project, requires):
    return readiness_service.check_readiness(project, "run_dlmuse_harmonized", requires)


# ── The headline case ─────────────────────────────────────────────────────────

def test_constraint_satisfied_when_enough_subjects_match(tmp_path):
    _cohort(tmp_path, n_cn=30, n_other=10)

    report = _check(tmp_path, _requires(constraints=CN_CONTROLS))
    sc = report.subject_count

    assert sc.actual == 40            # total, unconstrained
    constraint = sc.constraints[0]
    assert constraint.label == "Cognitively normal controls"
    assert constraint.actual == 30
    assert constraint.satisfied and constraint.recommended_met
    assert constraint.unevaluable_reason is None
    assert len(constraint.matching_mrids) == 30
    assert all(m.startswith("cn") for m in constraint.matching_mrids)
    assert sc.satisfied and report.satisfied


def test_constraint_fails_even_when_total_is_plentiful(tmp_path):
    """200 subjects but only 10 controls must not read as ready."""
    _cohort(tmp_path, n_cn=10, n_other=190)

    sc = _check(tmp_path, _requires(constraints=CN_CONTROLS)).subject_count

    assert sc.actual == 200
    assert sc.constraints[0].actual == 10
    assert not sc.constraints[0].satisfied
    # The parent flag folds in constraints so a client can gate on it alone.
    assert not sc.satisfied
    assert not sc.recommended_met


# ── Independent parent / child thresholds ─────────────────────────────────────

def test_total_and_cohort_thresholds_are_independent(tmp_path):
    """Expresses '100 total, at least 30 CN'."""
    requires = _requires(
        required=100, recommended=100,
        constraints=[{"required": 30, "where": [{"column": "DX", "equals": "CN"}]}],
    )

    # Cohort met, total short.
    _cohort(tmp_path, n_cn=30, n_other=20)
    sc = _check(tmp_path, requires).subject_count
    assert sc.actual == 50
    assert sc.constraints[0].satisfied      # 30 >= 30
    assert not sc.satisfied                 # 50 < 100


def test_both_thresholds_met(tmp_path):
    requires = _requires(
        required=100, recommended=100,
        constraints=[{"required": 30, "where": [{"column": "DX", "equals": "CN"}]}],
    )
    _cohort(tmp_path, n_cn=30, n_other=70)

    sc = _check(tmp_path, requires).subject_count

    assert sc.actual == 100
    assert sc.constraints[0].actual == 30
    assert sc.satisfied and sc.recommended_met


def test_constraint_thresholds_default_sensibly(tmp_path):
    _cohort(tmp_path, n_cn=5, n_other=5)

    # No required/recommended on the constraint → inherits the parent's 30/30.
    sc = _check(tmp_path, _requires(required=30, recommended=30, constraints=CN_CONTROLS)).subject_count
    assert (sc.constraints[0].required, sc.constraints[0].recommended) == (30, 30)

    # required given, recommended omitted → recommended tracks its own required.
    sc = _check(tmp_path, _requires(
        required=10, recommended=99,
        constraints=[{"required": 4, "where": [{"column": "DX", "equals": "CN"}]}],
    )).subject_count
    assert (sc.constraints[0].required, sc.constraints[0].recommended) == (4, 4)


# ── Matching semantics ────────────────────────────────────────────────────────

def test_string_matching_is_case_and_whitespace_insensitive(tmp_path):
    rows = [
        {"MRID": "a", "DX": "CN", "Age": "70", "Sex": "M"},
        {"MRID": "b", "DX": "cn", "Age": "70", "Sex": "M"},
        {"MRID": "c", "DX": " Cn ", "Age": "70", "Sex": "M"},
        {"MRID": "d", "DX": "AD", "Age": "70", "Sex": "M"},
    ]
    _write_imaging(tmp_path, [r["MRID"] for r in rows])
    _write_participants(tmp_path, rows)

    constraint = _check(tmp_path, _requires(
        required=1, constraints=[{"required": 3, "where": [{"column": "dx", "equals": "cn"}]}],
    )).subject_count.constraints[0]

    assert constraint.matching_mrids == ["a", "b", "c"]
    assert constraint.satisfied


def test_in_and_numeric_range_predicates(tmp_path):
    rows = [
        {"MRID": "a", "DX": "CN", "Age": "65", "Sex": "M"},
        {"MRID": "b", "DX": "MCI", "Age": "80", "Sex": "F"},
        {"MRID": "c", "DX": "CN", "Age": "40", "Sex": "M"},   # too young
        {"MRID": "d", "DX": "AD", "Age": "70", "Sex": "F"},   # excluded DX
    ]
    _write_imaging(tmp_path, [r["MRID"] for r in rows])
    _write_participants(tmp_path, rows)

    constraint = _check(tmp_path, _requires(
        required=1,
        constraints=[{
            "required": 2,
            "where": [
                {"column": "DX", "in": ["CN", "MCI"]},
                {"column": "Age", "min": 60, "max": 90},
            ],
        }],
    )).subject_count.constraints[0]

    assert constraint.matching_mrids == ["a", "b"]
    assert constraint.satisfied
    # The predicate is echoed back for display, with 'in' keyed as written.
    assert constraint.where[0].in_ == ["CN", "MCI"]
    assert constraint.where[1].min == 60


def test_empty_cells_and_rowless_subjects_do_not_count(tmp_path):
    rows = [
        {"MRID": "a", "DX": "CN", "Age": "70", "Sex": "M"},
        {"MRID": "b", "DX": "", "Age": "70", "Sex": "M"},      # blank DX
    ]
    # 'c' has imaging but no covariate row at all.
    _write_imaging(tmp_path, ["a", "b", "c"])
    _write_participants(tmp_path, rows)

    sc = _check(tmp_path, _requires(required=1, constraints=CN_CONTROLS)).subject_count

    assert sc.actual == 3                       # total counts imaging
    assert sc.constraints[0].matching_mrids == ["a"]


def test_csv_row_without_imaging_does_not_count(tmp_path):
    rows = [{"MRID": f"cn{i}", "DX": "CN", "Age": "70", "Sex": "M"} for i in range(5)]
    _write_imaging(tmp_path, ["cn0", "cn1"])    # only two actually have scans
    _write_participants(tmp_path, rows)

    constraint = _check(tmp_path, _requires(required=1, constraints=CN_CONTROLS)).subject_count.constraints[0]

    assert constraint.matching_mrids == ["cn0", "cn1"]
    assert constraint.actual == 2


# ── Unevaluable cohorts ───────────────────────────────────────────────────────

def test_missing_column_is_unevaluable_not_zero(tmp_path):
    """'No DX column' must be distinguishable from 'nobody is CN'."""
    rows = [{"MRID": "a", "Age": "70", "Sex": "M"}]
    _write_imaging(tmp_path, ["a"])
    _write_participants(tmp_path, rows, columns=("MRID", "Age", "Sex"))

    constraint = _check(tmp_path, _requires(required=1, constraints=CN_CONTROLS)).subject_count.constraints[0]

    assert constraint.actual == 0
    assert not constraint.satisfied
    assert constraint.unevaluable_reason is not None
    assert "DX" in constraint.unevaluable_reason


def test_missing_participants_csv_is_unevaluable(tmp_path):
    _write_imaging(tmp_path, ["a", "b"])

    constraint = _check(tmp_path, _requires(required=1, constraints=CN_CONTROLS)).subject_count.constraints[0]

    assert constraint.unevaluable_reason is not None
    assert not constraint.satisfied


def test_constraint_without_conditions_is_unevaluable(tmp_path):
    _cohort(tmp_path, n_cn=5, n_other=0)

    constraint = _check(tmp_path, _requires(
        required=1, constraints=[{"label": "broken", "where": []}],
    )).subject_count.constraints[0]

    assert constraint.unevaluable_reason is not None
    assert not constraint.satisfied


# ── Backwards compatibility ───────────────────────────────────────────────────

def test_min_subjects_without_constraints_is_unchanged(tmp_path):
    _cohort(tmp_path, n_cn=2, n_other=3)

    sc = _check(tmp_path, _requires(required=5, recommended=5)).subject_count

    assert sc.constraints == []
    assert sc.actual == 5
    assert sc.satisfied and sc.recommended_met
