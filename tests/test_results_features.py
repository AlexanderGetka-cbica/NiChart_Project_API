"""
Feature-definition interpretation layer: pipeline YAML `results.batch_features.features`
is parsed and flows through the results service onto BatchFeaturesResult.
"""

import textwrap
from pathlib import Path

from app.services import results_service


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


def _setup(tmp_path: Path) -> tuple[Path, Path, Path]:
    pipelines = tmp_path / "pipelines"
    resources = tmp_path / "resources"
    project = tmp_path / "project"
    resources.mkdir(parents=True, exist_ok=True)

    _write(
        pipelines / "feat_pipe.yaml",
        """
        pipeline_name: Feat Pipe
        steps: []
        results:
          batch_features:
            file: "out/predictions.csv"
            mrid_column: "MRID"
            features:
              y_score:
                label: "Amyloid-positivity score"
                definition: "Model logit for amyloid positivity."
                units: "logit"
                direction: "higher = more likely amyloid-positive"
                keywords: ["amyloid PET", "Centiloid"]
                references:
                  - citation: "Doe et al. 2024, J Test"
                    doi: "10.1234/test"
              never_present:
                definition: "Documented but not emitted by this run."
        """,
    )

    _write(project / "out" / "predictions.csv", "MRID,y_score\nsub-01,-5.4\nsub-02,2.1\n")
    return project, resources, pipelines


def test_feature_definitions_flow_through(tmp_path):
    project, resources, pipelines = _setup(tmp_path)

    detail = results_service.get_pipeline_result_detail(
        project, resources, pipelines, "feat_pipe", "Feat Pipe"
    )

    bf = detail.batch_features
    assert bf is not None and bf.available
    assert bf.columns == ["y_score"]

    defs = bf.feature_definitions
    assert defs is not None
    # Present column is documented; absent-but-documented column is filtered out.
    assert set(defs) == {"y_score"}
    assert "never_present" not in defs

    y = defs["y_score"]
    assert y.label == "Amyloid-positivity score"
    assert y.units == "logit"
    assert y.keywords == ["amyloid PET", "Centiloid"]
    assert len(y.references) == 1
    assert y.references[0].doi == "10.1234/test"
    assert y.references[0].citation == "Doe et al. 2024, J Test"


def test_no_features_block_yields_none(tmp_path):
    pipelines = tmp_path / "pipelines"
    resources = tmp_path / "resources"
    project = tmp_path / "project"
    resources.mkdir(parents=True, exist_ok=True)

    _write(
        pipelines / "plain.yaml",
        """
        pipeline_name: Plain
        steps: []
        results:
          batch_features:
            file: "out/p.csv"
            mrid_column: "MRID"
        """,
    )
    _write(project / "out" / "p.csv", "MRID,score\nsub-01,1\n")

    detail = results_service.get_pipeline_result_detail(
        project, resources, pipelines, "plain", "Plain"
    )
    assert detail.batch_features is not None
    assert detail.batch_features.feature_definitions is None
