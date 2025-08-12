"""
Main entrypoint.
- Reads config/crawler.yaml
- For each configured GitHub user/org:
  1) Lists repositories
  2) For each repo, builds a document (topics, readme, and optionally file tree)
  3) Appends to JSONL at data/raw/github/<user>/repos.jsonl
"""

from __future__ import annotations
import sys
from pathlib import Path
import yaml  # type: ignore

# ensure 'src' is importable
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_crawling.github_crawler import (
    list_user_repo_urls,
    build_repo_document,
    save_jsonl,
)

def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def run_github(cfg: dict) -> int:
    gh = cfg.get("github", {})
    users = gh.get("users", [])
    if not users:
        print("No users configured in config/crawler.yaml → github.users")
        return 1

    max_repos = int(gh.get("max_repos", 5))
    delay = float(gh.get("delay_seconds", 1.0))
    outdir = Path(gh.get("outdir", "data/raw/github"))
    mode = gh.get("mode", "repo")  # "repo" or "readme"

    ignore_dirs = list(gh.get("ignore_dirs", []))
    ignore_exts = list(gh.get("ignore_exts", []))
    max_file_mb = float(gh.get("max_file_mb", 1.0))

    total_docs = 0
    for user in users:
        print(f"[>] Listing repos for {user} (max_repos={max_repos})")
        repo_urls = list_user_repo_urls(user, max_repos=max_repos, delay=delay)
        docs = []
        for url in repo_urls:
            print(f"    - Processing {url} (mode={mode})")
            doc = build_repo_document(
                owner=user,
                repo_url=url,
                mode=mode,
                ignore_dirs=ignore_dirs,
                ignore_exts=ignore_exts,
                max_file_mb=max_file_mb,
                delay=delay,
            )
            docs.append(doc)
        out_path = outdir / user / "repos.jsonl"
        save_jsonl(docs, out_path)
        print(f"[✓] {user}: saved {len(docs)} repos → {out_path}")
        total_docs += len(docs)

    print(f"Done. Total repos saved: {total_docs}")
    return 0

def main() -> None:
    cfg_path = ROOT / "config" / "crawler.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    cfg = load_config(cfg_path)
    code = run_github(cfg)
    if code != 0:
        sys.exit(code)

if __name__ == "__main__":
    main()
