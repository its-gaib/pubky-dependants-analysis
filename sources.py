"""Fetch dependant data from crates.io, GitHub search, and GitHub dependents page."""

import base64
import json
import logging
import re
import subprocess
import time
from dataclasses import dataclass, field

import requests

log = logging.getLogger(__name__)

USER_AGENT = (
    "pubky-dependents-analysis (https://github.com/its-gaib/pubky-dependents-analysis)"
)
CRATES_IO_BASE = "https://crates.io/api/v1"
CRATES_IO_DELAY = 1  # seconds between crates.io requests
SCRAPE_DELAY = 2  # seconds between dependents page requests
SCRAPE_MAX_RETRIES = 3  # retries per page on failure


@dataclass
class RepoMatch:
    """A repository that references the target crate."""

    repo: str  # owner/name
    cargo_toml_paths: list[str] = field(default_factory=list)
    cargo_lock_paths: list[str] = field(default_factory=list)
    source: str = ""  # where we found it


def fetch_crates_io_reverse_deps(crate_name: str) -> list[dict]:
    """Fetch all published crates that depend on target crate from crates.io."""
    results = []
    page = 1
    while True:
        resp = requests.get(
            f"{CRATES_IO_BASE}/crates/{crate_name}/reverse_dependencies",
            params={"per_page": 100, "page": page},
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        if resp.status_code == 404:
            log.warning("Crate %s not found on crates.io", crate_name)
            return results
        resp.raise_for_status()
        data = resp.json()

        for version in data.get("versions", []):
            results.append(
                {
                    "crate": version.get("crate", version.get("num", "")),
                    "version": version.get("num", ""),
                    "description": version.get("description", ""),
                    "repository": version.get("repository", ""),
                }
            )

        total = data.get("meta", {}).get("total", 0)
        if page * 100 >= total:
            break
        page += 1
        time.sleep(CRATES_IO_DELAY)

    return results


def search_github_cargo_toml(crate_name: str) -> list[RepoMatch]:
    """Search GitHub for repos that mention the crate in Cargo.toml files."""
    return _gh_search_code(crate_name, "Cargo.toml", "cargo_toml_paths")


def search_github_cargo_lock(crate_name: str) -> list[RepoMatch]:
    """Search GitHub for repos that mention the crate in Cargo.lock files."""
    return _gh_search_code(crate_name, "Cargo.lock", "cargo_lock_paths")


def _gh_search_code(query: str, filename: str, path_attr: str) -> list[RepoMatch]:
    """Run gh search code and return RepoMatch objects."""
    try:
        result = subprocess.run(
            [
                "gh",
                "search",
                "code",
                query,
                "--filename",
                filename,
                "--limit",
                "100",
                "--json",
                "repository,path",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            log.warning("gh search failed: %s", result.stderr)
            return []

        items = json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
        log.warning("gh search error: %s", e)
        return []

    repo_map: dict[str, RepoMatch] = {}
    for item in items:
        repo = item["repository"]["nameWithOwner"]
        path = item["path"]
        if repo not in repo_map:
            repo_map[repo] = RepoMatch(repo=repo, source=f"github_{filename}")
        getattr(repo_map[repo], path_attr).append(path)

    return list(repo_map.values())


def scrape_github_dependents(github_repo: str) -> list[str]:
    """Scrape the GitHub dependents page across all packages."""
    base_url = f"https://github.com/{github_repo}/network/dependents"

    # Discover all package_ids from the first page.
    html = _fetch_dependents_page(base_url)
    if html is None:
        return []

    package_ids = _extract_package_ids(html)
    if not package_ids:
        # No package selector — scrape the single default view.
        package_ids = [None]

    all_repos: list[str] = []
    seen: set[str] = set()

    for pkg_id in package_ids:
        if pkg_id is None:
            start_url = base_url
        else:
            start_url = f"{base_url}?package_id={pkg_id}"

        for repo in _scrape_dependents_pages(start_url, github_repo):
            if repo not in seen:
                seen.add(repo)
                all_repos.append(repo)

    return all_repos


def _extract_package_ids(html: str) -> list[str]:
    """Extract all package_id values from the dependents page package selector."""
    return re.findall(r'package_id=([A-Za-z0-9=]+)', html)


def _fetch_dependents_page(url: str) -> str | None:
    """Fetch a single dependents page with retries."""
    for attempt in range(SCRAPE_MAX_RETRIES):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < SCRAPE_MAX_RETRIES - 1:
                delay = SCRAPE_DELAY * (attempt + 1)
                log.warning("Dependents page fetch failed (attempt %d): %s — retrying in %ds", attempt + 1, e, delay)
                time.sleep(delay)
            else:
                log.warning("Dependents page fetch failed after %d attempts: %s", SCRAPE_MAX_RETRIES, e)
    return None


def _scrape_dependents_pages(start_url: str, github_repo: str) -> list[str]:
    """Paginate through all dependents pages starting from a URL."""
    repos: list[str] = []
    url = start_url

    page_num = 0
    while True:
        html = _fetch_dependents_page(url)
        if html is None:
            break

        page_repos_before = len(repos)

        for match in re.finditer(
            r'<a[^>]+data-hovercard-type="repository"[^>]+href="/([^"]+)"',
            html,
        ):
            repo = match.group(1)
            if repo != github_repo and repo not in repos:
                repos.append(repo)

        # Find next page link
        next_match = re.search(
            r'<a[^>]*class="[^"]*"[^>]*href="([^"]+)"[^>]*>Next</a>', html
        )
        if not next_match:
            break
        url = next_match.group(1)
        if not url.startswith("http"):
            url = f"https://github.com{url}"

        if page_num > 0 and len(repos) == page_repos_before:
            log.warning(
                "Dependents page %d returned no new repos — HTML structure may have changed",
                page_num + 1,
            )
            break

        time.sleep(SCRAPE_DELAY)
        page_num += 1

    return repos


def fetch_file_content(repo: str, path: str) -> str | None:
    """Fetch a file from a GitHub repo via the API."""
    try:
        result = subprocess.run(
            ["gh", "api", f"repos/{repo}/contents/{path}", "--jq", ".content"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            return _fetch_raw(repo, path)

        return base64.b64decode(result.stdout.strip()).decode("utf-8", errors="replace")
    except Exception:
        return _fetch_raw(repo, path)


def fetch_github_stars(repo: str) -> int | None:
    """Fetch the star count for a GitHub repo."""
    try:
        result = subprocess.run(
            ["gh", "api", f"repos/{repo}", "--jq", ".stargazers_count"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip().isdigit():
            return int(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def search_npm_dependents(package_name: str) -> list[dict]:
    """Search for npm packages that reference the target package."""
    dependents = []
    seen = set()

    # Source 1: npm registry search
    try:
        resp = requests.get(
            "https://registry.npmjs.org/-/v1/search",
            params={"text": package_name, "size": 50},
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            for obj in data.get("objects", []):
                pkg = obj["package"]
                name = pkg["name"]
                if name != package_name and name not in seen:
                    seen.add(name)
                    dependents.append(
                        {
                            "package": name,
                            "description": pkg.get("description", ""),
                            "source": "npm_registry",
                        }
                    )
    except requests.RequestException as e:
        log.warning("npm registry search failed: %s", e)

    # Source 2: GitHub code search for package.json references
    try:
        result = subprocess.run(
            [
                "gh",
                "search",
                "code",
                package_name,
                "--filename",
                "package.json",
                "--limit",
                "50",
                "--json",
                "repository,path",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            items = json.loads(result.stdout)
            for item in items:
                repo = item["repository"]["nameWithOwner"]
                if repo not in seen:
                    seen.add(repo)
                    dependents.append(
                        {
                            "package": repo,
                            "source": "github_package_json",
                        }
                    )
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
        log.warning("gh search for package.json failed: %s", e)

    return dependents


def _fetch_raw(repo: str, path: str) -> str | None:
    """Fetch raw file content from GitHub."""
    for branch in ("main", "master", "develop"):
        try:
            resp = requests.get(
                f"https://raw.githubusercontent.com/{repo}/{branch}/{path}",
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.text
        except requests.RequestException:
            continue
    return None
