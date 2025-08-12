"""
GitHub crawler (config-driven):
- Lists repos for each user/org via HTML (requests + BS4)
- Two modes:
  1) "readme" -> metadata + rendered README text
  2) "repo"   -> shallow clone and collect a file tree {relative_path: file_content}

Output doc (per repo) looks like:
{
  "name": "<repo_name>",
  "link": "https://github.com/<user>/<repo>",
  "owner_id": "<user>",                # mirrors their 'owner_id'
  "topics": [...],                     # available in both modes
  "readme_text": "...",                # present in both modes if discoverable
  "content": {"path/to/file.py": "..."}  # only in mode: "repo"
}
"""

from __future__ import annotations
import os
import re
import time
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Iterable

import requests
from bs4 import BeautifulSoup

BASE = "https://github.com"

# ---------- HTML helpers (for listing repos & optional topics/readme) ----------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
    })
    return s

def _get_soup(s: requests.Session, url: str) -> BeautifulSoup:
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _normalize_ws(text: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()

def list_user_repo_urls(user: str, max_repos: int, delay: float) -> List[str]:
    """Collect repo URLs from the user's repositories tab (source repos only)."""
    s = _session()
    urls: List[str] = []
    page = 1
    while len(urls) < max_repos:
        url = f"{BASE}/{user}?tab=repositories&type=source&page={page}"
        soup = _get_soup(s, url)
        page_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if re.fullmatch(rf"/{re.escape(user)}/[^/]+", href):
                page_urls.append(f"{BASE}{href}")
        # de-dup, preserve order
        seen = set(urls)
        for u in page_urls:
            if u not in seen:
                urls.append(u); seen.add(u)
        if not page_urls:
            break
        page += 1
        time.sleep(delay)
    return urls[:max_repos]

def _parse_repo_topics_and_readme(repo_url: str) -> Dict[str, Optional[str] | List[str]]:
    """Fetch repo page and extract topics + README text (best-effort)."""
    s = _session()
    soup = _get_soup(s, repo_url)
    topics = [a.get_text(strip=True) for a in soup.select("a.topic-tag")]
    readme = soup.select_one("article.markdown-body")
    readme_text = _normalize_ws(readme.get_text("\n")) if readme else None
    return {"topics": topics, "readme_text": readme_text}

# -------------------------- Repo tree extraction (git) -------------------------

def _safe_clone(link: str, dest: Path) -> None:
    """Shallow clone the repository into dest."""
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", link, str(dest)],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    except FileNotFoundError:
        raise RuntimeError("git is not installed or not in PATH.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"git clone failed for {link}: {e.stderr.decode('utf-8', 'ignore')}") from e

def _should_skip_dir(dirname: str, ignore_dirs: Iterable[str]) -> bool:
    return dirname in set(ignore_dirs)

def _should_skip_file(filename: str, ignore_exts: Iterable[str]) -> bool:
    return any(filename.endswith(ext) for ext in ignore_exts)

def _within_size_limit(path: Path, max_bytes: int) -> bool:
    try:
        return path.stat().st_size <= max_bytes
    except OSError:
        return False

def extract_repo_tree_via_git(
    link: str,
    ignore_dirs: List[str],
    ignore_exts: List[str],
    max_file_mb: float
) -> Dict[str, str]:
    """Clone repo shallowly and return {relative_path: text_content} of text-like files."""
    max_bytes = int(max_file_mb * 1024 * 1024)
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        _safe_clone(link, repo_dir)

        tree: Dict[str, str] = {}
        for root, dirs, files in os.walk(repo_dir):
            # prune ignored directories in-place
            dirs[:] = [d for d in dirs if not _should_skip_dir(d, ignore_dirs)]
            root_p = Path(root)
            for fname in files:
                if _should_skip_file(fname, ignore_exts):
                    continue
                fpath = root_p / fname
                if not _within_size_limit(fpath, max_bytes):
                    continue
                rel = str(fpath.relative_to(repo_dir))
                # read as text, skipping binary-ish files by decode errors
                try:
                    with fpath.open("r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    # keep whitespace as-is (do NOT strip spaces like their example)
                    tree[rel] = content
                except Exception:
                    # ignore unreadable files quietly
                    continue
        return tree

# ------------------------------- Public API -----------------------------------

def build_repo_document(
    owner: str,
    repo_url: str,
    mode: str,
    ignore_dirs: List[str],
    ignore_exts: List[str],
    max_file_mb: float,
    delay: float
) -> Dict:
    """Assemble a Repository-like document, similar to their RepositoryDocument model."""
    repo_name = repo_url.rstrip("/").split("/")[-1]
    meta = _parse_repo_topics_and_readme(repo_url)
    doc: Dict = {
        "name": repo_name,
        "link": repo_url,
        "owner_id": owner,
        "topics": meta.get("topics", []),
        "readme_text": meta.get("readme_text"),
    }
    if mode == "repo":
        doc["content"] = extract_repo_tree_via_git(
            link=repo_url,
            ignore_dirs=ignore_dirs,
            ignore_exts=ignore_exts,
            max_file_mb=max_file_mb,
        )
        time.sleep(delay)  # polite pause between clones
    return doc

def save_jsonl(records: List[Dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
