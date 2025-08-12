"""
Main entrypoint for the project.

Reads config/crawler.yaml and runs the GitHub crawler.
Usage:
  python main.py
"""

from __future__ import annotations
import sys
from pathlib import Path

# Make 'src' importable when running from project root
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml  # type: ignore
from src.data_crawling.github_crawler import crawl_user_repos, save_jsonl


def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def run_github_crawler(cfg: dict) -> int:
    gh = cfg.get("github", {})
    users = gh.get("users", [])
    max_repos = int(gh.get("max_repos", 5))
    delay = float(gh.get("delay_seconds", 1.0))
    outdir = Path(gh.get("outdir", "data/raw/github"))

    if not users:
        print("No users configured in config/crawler.yaml under github.users")
        return 1

    total = 0
    for user in users:
        print(f"[>] Crawling GitHub user/org: {user} (max_repos={max_repos}, delay={delay}s)")
        records = crawl_user_repos(user, max_repos=max_repos, delay=delay)
        out_path = outdir / user / "repo_readmes.jsonl"
        save_jsonl(records, out_path)
        print(f"[✓] {user}: saved {len(records)} repos → {out_path}")
        total += len(records)

    print(f"Done. Total repos saved: {total}")
    return 0


def main() -> None:
    cfg_path = ROOT / "config" / "crawler.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    cfg = load_config(cfg_path)
    exit_code = run_github_crawler(cfg)
    if exit_code != 0:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
