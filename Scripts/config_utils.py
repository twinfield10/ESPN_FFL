"""Load league configuration and credentials from config.yaml.

The ``lg_vars`` dictionary this builds used to be constructed by an identical
~25-line block duplicated in both ``populateGoogleSheet.py`` and the notebook's
first cell. That block mapped snake_case league keys to display names through a
hardcoded dict with a bare ``[league_name]`` subscript, so adding a league to
the YAML without also editing both copies raised ``KeyError``. Display names
now live in ``config.yaml`` alongside the league they name.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from Scripts.paths import REPO_ROOT

CONFIG_PATH = REPO_ROOT / "config.yaml"


def load_config(path: Optional[Path] = None) -> Dict[str, Any]:
    """Read the raw config.yaml.

    Args:
        path: Override the config location. Defaults to ``<repo>/config.yaml``.

    Returns:
        dict: Parsed YAML.

    Raises:
        FileNotFoundError: If the config is missing, with a pointer to the
            example file.
    """
    path = CONFIG_PATH if path is None else Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Copy config.example.yaml to config.yaml and fill "
            "in your ESPN credentials. config.yaml is gitignored."
        )
    with open(path) as f:
        return yaml.safe_load(f)


def get_season(config: Optional[Dict[str, Any]] = None) -> int:
    """The season the pipeline is configured to operate on.

    Args:
        config: Pre-loaded config. Loaded from disk when omitted.

    Returns:
        int: Season year, e.g. ``2026``.
    """
    config = load_config() if config is None else config
    return int(config["season"])


def build_lg_vars(config: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """Build the display-name-keyed league dictionary used across the pipeline.

    Credentials fall back to the shared ``credentials.espn_id`` / ``s_id`` pair
    when a league does not define its own.

    Args:
        config: Pre-loaded config. Loaded from disk when omitted.

    Returns:
        dict: ``{display_name: {ID, ESPN_S2, SWID, start, end, primary_own, key}}``.
    """
    config = load_config() if config is None else config
    creds = config["credentials"]

    lg_vars: Dict[str, Dict[str, Any]] = {}
    for key, data in config["leagues"].items():
        display = data.get("display_name", key)
        lg_vars[display] = {
            "ID": data["id"],
            "ESPN_S2": data.get("espn_s2") or creds["espn_id"],
            "SWID": data.get("swid") or creds["s_id"],
            "start": data["start"],
            "end": data["end"],
            "primary_own": data["primary_owner"],
            "key": key,
            "display_name": display,
        }
    return lg_vars


def resolve_league(
    name: str, config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Look up one league by display name or by config key.

    Callers use both spellings interchangeably -- ``"Knights_FFL"`` on the command
    line, ``"knights_ffl"`` in a store path -- so accepting either is the only way
    the two stay interoperable. This lookup was written inline in
    ``Scripts.equivalence.build_league_frame`` and ``Scripts.season_projections.main``
    before; both now call here.

    Args:
        name: Display name (``"12 Dudes one Cup"``) or config key
            (``"twelve_dudes_one_cup"``).
        config: Pre-loaded config. Loaded from disk when omitted.

    Returns:
        dict: The ``build_lg_vars`` entry, including ``key`` and ``display_name``.

    Raises:
        ValueError: When ``name`` matches neither, listing what is configured.
    """
    lg_vars = build_lg_vars(config)
    by_key = {cfg["key"]: cfg for cfg in lg_vars.values()}
    cfg = lg_vars.get(name) or by_key.get(name)
    if cfg is None:
        raise ValueError(
            f"Unknown league {name!r}. Configured display names: "
            f"{sorted(lg_vars)}; keys: {sorted(by_key)}."
        )
    return cfg
