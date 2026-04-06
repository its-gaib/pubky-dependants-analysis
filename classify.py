"""Classify repos by how they depend on a target crate."""

import tomllib
from collections import defaultdict


def classify_cargo_toml(content: str, target_crate: str) -> dict | None:
    """Classify how a Cargo.toml references a target crate.

    Returns:
        dict with "kind" key ("direct" or "feature_flag") and metadata, or
        None if the target crate is not referenced.
    """
    try:
        data = tomllib.loads(content)
    except tomllib.TOMLDecodeError:
        return None

    # Check [dependencies], [dev-dependencies], [build-dependencies]
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        dep = data.get(section, {}).get(target_crate)
        if dep is not None:
            return _parse_direct_dep(dep)

    # Check [workspace.dependencies]
    ws_deps = data.get("workspace", {}).get("dependencies", {})
    dep = ws_deps.get(target_crate)
    if dep is not None:
        return _parse_direct_dep(dep)

    # Check if target_crate appears only in feature strings of other deps
    feature_flag_parent = _find_in_feature_flags(data, target_crate)
    if feature_flag_parent:
        return {"kind": "feature_flag", "parent_crate": feature_flag_parent}

    return None


def _parse_direct_dep(dep) -> dict:
    """Parse a dependency value into a classification result."""
    result = {"kind": "direct"}

    if isinstance(dep, str):
        result["version"] = dep
        result["features"] = []
        result["optional"] = False
        result["default_features"] = True
    else:
        result["version"] = dep.get("version", "workspace")
        result["features"] = dep.get("features", [])
        result["optional"] = dep.get("optional", False)
        result["default_features"] = dep.get("default-features", True)
        if dep.get("workspace"):
            result["version"] = "workspace"

    return result


def _find_in_feature_flags(data: dict, target_crate: str) -> str | None:
    """Check if target_crate appears in feature flags of another dep.

    For example, iroh = { features = ["discovery-pkarr-dht"] } references
    pkarr only as an iroh feature flag. Uses word-boundary matching to
    avoid false positives (e.g. "pk" matching "pkarr").
    """
    all_deps = list(data.get("dependencies", {}).items())
    all_deps += list(data.get("dev-dependencies", {}).items())
    all_deps += list(data.get("build-dependencies", {}).items())
    all_deps += list(
        data.get("workspace", {}).get("dependencies", {}).items()
    )

    for crate_name, dep_val in all_deps:
        if not isinstance(dep_val, dict):
            continue
        for feat in dep_val.get("features", []):
            # Match "pkarr" as a whole segment: "discovery-pkarr-dht",
            # "pkarr/dht", or exactly "pkarr"
            if feat == target_crate or f"-{target_crate}" in feat or f"{target_crate}/" in feat:
                return crate_name

    return None


def trace_chains(cargo_lock_content: str, target_crate: str) -> list[list[str]]:
    """Trace dependency chains from root crates to a target crate in a Cargo.lock.

    Returns a list of chains, where each chain is a list of crate names
    from a root crate to the target crate (inclusive).
    """
    packages = _parse_cargo_lock(cargo_lock_content)

    # Build reverse adjacency: child -> {parents}
    reverse_deps: dict[str, set[str]] = defaultdict(set)
    for pkg_name, pkg_info in packages.items():
        for dep_name in pkg_info["deps"]:
            reverse_deps[dep_name].add(pkg_name)

    roots = {name for name, info in packages.items() if info["source"] is None}

    if target_crate not in packages:
        return []

    chains: list[list[str]] = []
    _find_chains_to_roots(target_crate, reverse_deps, roots, [], chains, set())
    return chains


def _find_chains_to_roots(
    current: str,
    reverse_deps: dict[str, set[str]],
    roots: set[str],
    path: list[str],
    result: list[list[str]],
    visited: set[str],
):
    """DFS from target crate upward through reverse deps to find root crates."""
    if current in visited:
        return
    visited = visited | {current}

    path = [current] + path

    if current in roots:
        result.append(path)
        return

    parents = reverse_deps.get(current, set())
    if not parents:
        result.append(path)
        return

    for parent in sorted(parents):
        _find_chains_to_roots(parent, reverse_deps, roots, path, result, visited)


def _parse_cargo_lock(content: str) -> dict[str, dict]:
    """Parse a Cargo.lock file into a dict of package name -> info."""
    data = tomllib.loads(content)

    packages = {}
    for pkg in data.get("package", []):
        name = pkg["name"]
        deps = []
        for dep in pkg.get("dependencies", []):
            # Deps are "name" or "name version" or "name version source"
            deps.append(dep.split()[0])
        packages[name] = {
            "version": pkg.get("version"),
            "source": pkg.get("source"),
            "deps": deps,
        }
    return packages


def categorize(repos: list[dict], target_crate: str) -> dict[str, list[dict]]:
    """Categorize repos into lists named after direct dependants.

    Each repo dict must have: repo, classification, chain.
    Returns dict of list_name -> [repo entries].
    "direct" list contains repos that directly depend on target_crate.
    Other lists are named after the crate that is the direct parent of
    target_crate in the chain.
    """
    result = defaultdict(list)

    for repo in repos:
        entry = {"repo": repo["repo"], "chain": repo["chain"], "stars": repo.get("stars")}

        classification = repo["classification"]

        if classification and classification["kind"] == "direct":
            entry.update({
                k: v for k, v in classification.items()
                if k != "kind"
            })
            result["direct"].append(entry)
            continue

        # For feature_flag or transitive: use the chain to find which
        # direct dependant of target_crate this repo goes through.
        chain = repo["chain"]
        if target_crate in chain:
            idx = chain.index(target_crate)
            if idx > 0:
                direct_parent = chain[idx - 1]
                result[direct_parent].append(entry)
                continue

        result["unknown"].append(entry)

    return dict(result)
