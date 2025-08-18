# main.py
from __future__ import annotations
import sys
from pathlib import Path
import yaml  # type: ignore

ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_crawling.github_crawler import (
    list_user_repo_urls, build_repo_document, save_jsonl,
)
from src.preprocess.make_corpus import run_preprocess  # ← NEW
from src.db import get_db
from src.documents import RepositoryDocument

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
    mode = gh.get("mode", "repo")
    ignore_dirs = list(gh.get("ignore_dirs", []))
    ignore_exts = list(gh.get("ignore_exts", []))
    max_file_mb = float(gh.get("max_file_mb", 1.0))

    st = cfg.get("storage", {})
    sink = st.get("sink", "jsonl").lower()
    jsonl_template = st.get("jsonl_path", "data/raw/github/{user}/repos.jsonl")
    mongo_uri = st.get("mongo_uri", "mongodb://localhost:27017")
    mongo_db = st.get("mongo_db", "llm_twin")
    mongo_coll = st.get("mongo_collection", "repositories")

    if sink == "mongo":
        db = get_db(mongo_uri, mongo_db)
        RepositoryDocument.create_indexes(db, mongo_coll)

    total = 0
    for user in users:
        print(f"[>] Listing repos for {user} (max_repos={max_repos})")
        repo_urls = list_user_repo_urls(user, max_repos=max_repos, delay=delay)
        docs = []
        for url in repo_urls:
            print(f"    - Processing {url} (mode={mode})")
            doc = build_repo_document(
                owner=user, repo_url=url, mode=mode,
                ignore_dirs=ignore_dirs, ignore_exts=ignore_exts,
                max_file_mb=max_file_mb, delay=delay,
            )
            docs.append(doc)

        if sink == "jsonl":
            out_path = Path(jsonl_template.format(user=user))
            save_jsonl(docs, out_path)
            print(f"[✓] {user}: saved {len(docs)} repos → {out_path}")
        elif sink == "mongo":
            for d in docs:
                RepositoryDocument.from_dict(d).save(db, mongo_coll)
            print(f"[✓] {user}: upserted {len(docs)} repos → mongodb://.../{mongo_db}.{mongo_coll}")
        else:
            print(f"[!] Unknown storage.sink={sink}; supported: jsonl, mongo")
            return 2
        total += len(docs)

    print(f"Done. Total repos processed: {total}")
    return 0

def main() -> None:
    cfg_path = ROOT / "config" / "crawler.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    cfg = load_config(cfg_path)

    # 1) Crawl (if enabled)
    pipe = cfg.get("pipeline", {})
    if pipe.get("crawl_github", True):
        code = run_github(cfg)
        if code != 0:
            sys.exit(code)

    # 2) Preprocess (if enabled)
    if pipe.get("preprocess", True):
        n, out_path = run_preprocess(cfg)
        print(f"[✓] Preprocess: wrote {n} chunks → {out_path}")

if __name__ == "__main__":
    main()