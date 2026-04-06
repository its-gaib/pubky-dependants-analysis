"""Classify repos by how they depend on a target crate."""

import re
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
        return _classify_cargo_toml_regex(content, target_crate)

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

    # Check target-specific dependencies
    for key, val in data.items():
        if key.startswith("target.") or (key == "target" and isinstance(val, dict)):
            targets = val if isinstance(val, dict) else {key: val}
            for _target_cfg, target_data in targets.items():
                if isinstance(target_data, dict):
                    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
                        dep = target_data.get(section, {}).get(target_crate)
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
    elif isinstance(dep, dict):
        result["version"] = dep.get("version", "workspace")
        result["features"] = dep.get("features", [])
        result["optional"] = dep.get("optional", False)
        result["default_features"] = dep.get("default-features", True)
        if dep.get("workspace"):
            result["version"] = "workspace"
    else:
        result["version"] = str(dep)
        result["features"] = []
        result["optional"] = False
        result["default_features"] = True

    return result


def _find_in_feature_flags(data: dict, target_crate: str) -> str | None:
    """Check if target_crate appears only in feature flags of another dep.

    For example, iroh = { features = ["discovery-pkarr-dht"] } references
    pkarr only as an iroh feature flag.
    """
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        deps = data.get(section, {})
        for crate_name, dep_val in deps.items():
            if not isinstance(dep_val, dict):
                continue
            features = dep_val.get("features", [])
            for feat in features:
                if target_crate in feat:
                    return crate_name

    # Also check workspace deps
    ws_deps = data.get("workspace", {}).get("dependencies", {})
    for crate_name, dep_val in ws_deps.items():
        if not isinstance(dep_val, dict):
            continue
        features = dep_val.get("features", [])
        for feat in features:
            if target_crate in feat:
                return crate_name

    return None


def _classify_cargo_toml_regex(content: str, target_crate: str) -> dict | None:
    """Fallback regex-based classification for malformed TOML."""
    escaped = re.escape(target_crate)

    # Direct dep: target_crate = "version" or target_crate = { ... }
    direct_pattern = rf'^{escaped}\s*=\s*(.+)$'
    match = re.search(direct_pattern, content, re.MULTILINE)
    if match:
        return {"kind": "direct", "version": "unknown", "features": [],
                "optional": False, "default_features": True}

    # Feature flag: "something-target_crate-something" in features list
    if re.search(rf'"{escaped}|{escaped}/', content):
        return {"kind": "feature_flag", "parent_crate": "unknown"}

    return None


def trace_chains(cargo_lock_content: str, target_crate: str) -> list[list[str]]:
    """Trace dependency chains from root crates to a target crate in a Cargo.lock.

    Returns a list of chains, where each chain is a list of crate names
    from a root crate to the target crate (inclusive).
    """
    packages = _parse_cargo_lock(cargo_lock_content)

    # Build adjacency: parent -> [children]
    # and reverse: child -> [parents]
    reverse_deps = defaultdict(set)
    for pkg_name, pkg_info in packages.items():
        for dep_name in pkg_info["deps"]:
            reverse_deps[dep_name].add(pkg_name)

    # Find root crates (no source = local/workspace crate)
    roots = {name for name, info in packages.items() if info["source"] is None}

    if target_crate not in packages:
        return []

    # BFS from target_crate upward to roots
    chains = []
    _find_chains_to_roots(target_crate, reverse_deps, roots, [], chains)
    return chains


def _find_chains_to_roots(
    current: str,
    reverse_deps: dict[str, set[str]],
    roots: set[str],
    path: list[str],
    result: list[list[str]],
    visited: set[str] | None = None,
):
    """DFS from target crate upward through reverse deps to find root crates."""
    if visited is None:
        visited = set()

    if current in visited:
        return
    visited = visited | {current}

    path = [current] + path

    if current in roots:
        result.append(path)
        return

    parents = reverse_deps.get(current, set())
    if not parents:
        # Dead end — not a root but nothing depends on it
        result.append(path)
        return

    for parent in sorted(parents):
        _find_chains_to_roots(parent, reverse_deps, roots, path, result, visited)


def _parse_cargo_lock(content: str) -> dict[str, dict]:
    """Parse a Cargo.lock file into a dict of package name -> info.

    Each info dict has: version, source, deps (list of crate names).
    """
    packages = {}
    current_pkg = None
    in_deps = False
    deps_list = []

    for line in content.splitlines():
        line = line.strip()

        if line == "[[package]]":
            if current_pkg:
                packages[current_pkg["name"]] = {
                    "version": current_pkg.get("version"),
                    "source": current_pkg.get("source"),
                    "deps": deps_list,
                }
            current_pkg = {}
            in_deps = False
            deps_list = []
            continue

        if current_pkg is None:
            continue

        if line.startswith("name = "):
            current_pkg["name"] = _unquote(line.split("=", 1)[1].strip())
            in_deps = False
        elif line.startswith("version = "):
            current_pkg["version"] = _unquote(line.split("=", 1)[1].strip())
            in_deps = False
        elif line.startswith("source = "):
            current_pkg["source"] = _unquote(line.split("=", 1)[1].strip())
            in_deps = False
        elif line == "dependencies = [":
            in_deps = True
        elif line == "]":
            in_deps = False
        elif in_deps and line.startswith('"'):
            dep_str = _unquote(line.rstrip(","))
            # Deps can be "name" or "name version"
            dep_name = dep_str.split()[0]
            deps_list.append(dep_name)

    # Don't forget the last package
    if current_pkg and "name" in current_pkg:
        packages[current_pkg["name"]] = {
            "version": current_pkg.get("version"),
            "source": current_pkg.get("source"),
            "deps": deps_list,
        }

    return packages


def _unquote(s: str) -> str:
    """Remove surrounding quotes from a TOML string value."""
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


def categorize(repos: list[dict], target_crate: str) -> dict[str, list[dict]]:
    """Categorize repos into lists named after direct dependants.

    Each repo dict must have: repo, classification, chain.
    - classification: result of classify_cargo_toml or None
    - chain: list of crate names from some root to target_crate

    Returns dict of list_name -> [repo entries].
    "direct" list contains repos that directly depend on target_crate.
    Other lists are named after the crate that is the direct parent of
    target_crate in the chain.
    """
    # First pass: identify direct dependants (they define the list names)
    direct_crate_to_repo = {}
    for repo in repos:
        if repo["classification"] and repo["classification"]["kind"] == "direct":
            # The crate name that directly depends on target is the one
            # just before target_crate in the chain
            if len(repo["chain"]) >= 2:
                crate_name = repo["chain"][-2]
                direct_crate_to_repo[crate_name] = repo["repo"]

    result = defaultdict(list)

    for repo in repos:
        entry = {"repo": repo["repo"], "chain": repo["chain"]}

        if repo["classification"] and repo["classification"]["kind"] == "direct":
            entry.update({
                k: v for k, v in repo["classification"].items()
                if k != "kind"
            })
            result["direct"].append(entry)
            continue

        # For feature_flag or transitive (None classification):
        # Find which direct dependant of target_crate is in the chain
        if repo["classification"] and repo["classification"]["kind"] == "feature_flag":
            parent = repo["classification"]["parent_crate"]
            if parent in direct_crate_to_repo or parent in ("iroh", "iroh-relay"):
                result[parent].append(entry)
                continue

        # Look at the chain: the crate just before target_crate is the
        # direct dependant
        chain = repo["chain"]
        if target_crate in chain:
            idx = chain.index(target_crate)
            if idx > 0:
                direct_parent = chain[idx - 1]
                result[direct_parent].append(entry)
                continue

        # Fallback: unknown
        result["unknown"].append(entry)

    return dict(result)
