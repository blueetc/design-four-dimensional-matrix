"""Load and expand configuration from YAML policy files."""

from __future__ import annotations

import os
import pathlib

import yaml


def expand_path(p: str) -> str:
    """Expand ``~`` and resolve to an absolute path."""
    return os.path.abspath(os.path.expanduser(p))


def load_policy(path: str) -> dict:
    """Read *policy.yaml* and normalise ``workspace_root``."""
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    data["workspace_root"] = expand_path(data["workspace_root"])
    pathlib.Path(data["workspace_root"]).mkdir(parents=True, exist_ok=True)
    return data
