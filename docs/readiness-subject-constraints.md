# Readiness: subject-count constraints (front-end reference)

New: per-cohort minimum subject counts, e.g. *"at least 30 subjects with
DX == CN"*. The first pipeline using it is `run_dlmuse_harmonized`.

Two endpoints are involved:

| Endpoint | Gives you |
|---|---|
| `GET /catalog/pipelines/{pipeline_id}` | `min_subjects` — what the pipeline **declares**, available before any data is uploaded |
| `GET /projects/{project_id}/readiness/{pipeline_id}` | `subject_count` — how a **specific project** measures up |

Both are existing endpoints; the fields below are additions.

## What the pipeline declares — `min_subjects`

`PipelineDetail.min_subjects` (null when a pipeline declares no subject-count
requirement) lets you render "needs ≥30 cognitively normal controls" on a
pipeline card with no project selected:

```jsonc
"min_subjects": {
  "required": 30,
  "recommended": 30,
  "constraints": [
    {
      "label": "Cognitively normal controls",
      "where": [{ "column": "DX", "equals": "CN", "in": null, "min": null, "max": null }],
      "required": 30,
      "recommended": 30
    }
  ]
}
```

Thresholds come back with YAML defaults **already resolved** — don't reimplement
the fallback rules.

Also fixed in the same change: `PipelineDetail.requires` used to contain
stringified Python dicts like `"{'min_subjects': {'required': 30}}"`. It now
contains clean requirement names only — `["needs_T1", "needs_demographics",
"csv_has_columns", "min_subjects"]` — safe to render directly as chips. The
structured detail lives in `column_schemas` and `min_subjects`.

## What a project measures up to — `subject_count`

```
GET /projects/{project_id}/readiness/{pipeline_id}
```

Unchanged endpoint. The addition is inside the existing `subject_count` object.

## Response shape

```jsonc
{
  "pipeline_id": "run_dlmuse_harmonized",
  "satisfied": false,
  "imaging": [ /* unchanged */ ],
  "csv": { /* unchanged */ },
  "subject_count": {
    "actual": 200,              // total subjects with imaging
    "required": 30,             // total threshold
    "recommended": 30,
    "satisfied": false,         // total AND every constraint
    "recommended_met": false,
    "constraints": [
      {
        "label": "Cognitively normal controls",   // may be null
        "where": [
          { "column": "DX", "equals": "CN", "in": null, "min": null, "max": null }
        ],
        "actual": 10,            // subjects matching this cohort
        "required": 30,
        "recommended": 30,
        "satisfied": false,
        "recommended_met": false,
        "matching_mrids": ["sub-0001", "sub-0014"],
        "unevaluable_reason": null
      }
    ]
  }
}
```

`constraints` is `[]` for every pipeline that doesn't declare any, so existing
rendering keeps working untouched.

## How to gate

**Use `subject_count.satisfied` as the single gate for the whole block.** It is
already `total >= required AND every constraint satisfied`. You don't need to
AND the constraints yourself — and `report.satisfied` (the top-level flag)
folds it in as before.

One caveat inherited from existing behaviour: the top-level `report.satisfied`
gates on `recommended_met`, not `satisfied`, because under-powered
harmonization is treated as not-ready. So a project can have
`subject_count.satisfied == true` while `report.satisfied == false`. Show the
*recommended* number when nudging the user toward more data.

## Three states to render per constraint

| State | Condition | Suggested message |
|---|---|---|
| Met | `satisfied` | "30 of 30 cognitively normal controls" |
| Short | `!satisfied && unevaluable_reason == null` | "10 of 30 cognitively normal controls — add 20 more" |
| Unevaluable | `unevaluable_reason != null` | Show the reason verbatim |

`unevaluable_reason` exists precisely so you don't tell a user "0 subjects are
CN" when the real problem is that `participants.csv` has no `DX` column at all.
Its values are human-readable and safe to display, e.g.:

- `"participants.csv has no column named 'DX'"`
- `"participants.csv is missing, empty, or has no MRID column"`
- `"constraint declares no usable 'where' conditions"`

When unevaluable, `actual` is `0` and both `satisfied` and `recommended_met`
are `false` — don't present that `0` as a count.

## Rendering `where`

Each entry is one condition on a `participants.csv` column, and **conditions
combine with AND**. Fields are nullable; render only the non-null ones:

| Field | Meaning |
|---|---|
| `column` | the CSV column |
| `equals` | must equal this value (case-insensitive) |
| `in` | must be one of these values (case-insensitive) |
| `min` / `max` | inclusive numeric bounds |

So `[{column: "DX", in: ["CN","MCI"]}, {column: "Age", min: 60, max: 90}]`
reads as "DX is CN or MCI **and** Age is 60–90".

Prefer `label` for the headline when present; fall back to a generated
description from `where`.

## Matching semantics worth surfacing to users

These explain counts that otherwise look wrong:

- **A subject must have both imaging and a `participants.csv` row.** A scan with
  no covariate row is not counted, and a CSV row with no scan is not counted.
  This is why a constraint's `actual` can be lower than both the imaging count
  and the CSV row count.
- **String matching is case- and whitespace-insensitive.** `CN`, `cn` and `" Cn "`
  all match `equals: CN`.
- **Blank cells never match.** An empty `DX` is not evidence of a diagnosis.
- `subject_count.actual` (the total) still counts subjects by **imaging only** —
  it is not filtered by any constraint.

## Don't re-implement the matching client-side

`matching_mrids` gives you the exact subject list the server counted — use it
for drill-down ("which subjects count?") and for highlighting rows in a
participants table. Re-deriving cohort membership in the UI from the raw CSV
will drift from the server's answer (case folding, blank handling, the
imaging∩CSV intersection).

Note `matching_mrids` is a full list, so it can be large on big projects —
prefer `actual` for counts and treat the list as drill-down data.
