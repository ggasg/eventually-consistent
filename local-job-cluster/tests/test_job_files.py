"""The job files and the cluster definition have to agree on names, paths and labels."""
from pathlib import Path

import pytest
import yaml

CODE = Path(__file__).resolve().parents[1] / "code"
JOBS = sorted((CODE / "jobs").glob("*.yaml"))
KIND = yaml.safe_load((CODE / "infra" / "kind-config.yml").read_text())
INFRA = list(yaml.safe_load_all((CODE / "infra" / "spark-jobs.yml").read_text()))


def node_labels():
    return {n.get("labels", {}).get("spark-node") for n in KIND["nodes"]} - {None}


@pytest.mark.parametrize("path", JOBS, ids=lambda p: p.name)
def test_job_points_at_existing_code(path):
    job = yaml.safe_load(path.read_text())
    script = job["spec"]["mainApplicationFile"].removeprefix("local:///opt/app/")
    assert (CODE / "app" / script).is_file()


@pytest.mark.parametrize("path", JOBS, ids=lambda p: p.name)
def test_job_places_pods_on_labeled_nodes(path):
    conf = yaml.safe_load(path.read_text())["spec"]["sparkConf"]
    selectors = {v for k, v in conf.items() if k.endswith("node.selector.spark-node")}
    assert selectors == {"driver", "worker"}
    assert selectors <= node_labels()


@pytest.mark.parametrize("path", JOBS, ids=lambda p: p.name)
def test_job_uses_the_driver_service_account(path):
    job = yaml.safe_load(path.read_text())
    accounts = {(d["metadata"]["namespace"], d["metadata"]["name"])
                for d in INFRA if d["kind"] == "ServiceAccount"}
    assert (job["metadata"]["namespace"], job["spec"]["driver"]["serviceAccount"]) in accounts
