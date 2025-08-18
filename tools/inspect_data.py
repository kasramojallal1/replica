# tools/inspect_data.py
from __future__ import annotations
import json
from pathlib import Path
from collections import Counter
import sys

# ---- Resolve project root (../ from this tools/ folder) ----
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml  # type: ignore

try:
    from src.db import get_db  # only used if sink == "mongo"
except Exception:
    get_db = None  # type: ignore


def load_config() -> dict:
    cfg_path = ROOT / "config" / "crawler.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _resolve_under_root(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else (ROOT / p)


def summarize_jsonl(users: list[str], jsonl_template: str) -> None:
    for user in users:
        p = _resolve_under_root(jsonl_template.format(user=user)).resolve()
        if not p.exists():
            print(f"[!] Not found: {p}")
            continue

        repo_names = []
        files_total = 0
        topics_counter = Counter()

        with p.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    print(f"[!] Skipping invalid JSON line {i} in {p}")
                    continue
                repo_names.append(d.get("name", ""))
                content = d.get("content", {})
                if isinstance(content, dict):
                    files_total += len(content)
                for t in d.get("topics", []) or []:
                    topics_counter[t] += 1

        print(f"\n== {user} ==")
        print(f"Source file: {p}")
        print(f"Repos stored: {len(repo_names)}")
        print(f"Total files captured across repos: {files_total}")
        if repo_names:
            sample = ", ".join([r for r in repo_names[:5] if r]) + (" ..." if len(repo_names) > 5 else "")
            if sample:
                print(f"Sample repos: {sample}")
        if topics_counter:
            print("Top topics:", topics_counter.most_common(10))


def summarize_mongo(users: list[str], uri: str, db_name: str, coll_name: str) -> None:
    if get_db is None:
        print("[!] Mongo summary requested but PyMongo helper not available.")
        return
    db = get_db(uri, db_name)
    col = db[coll_name]

    for user in users:
        repo_count = col.count_documents({"owner_id": user})
        files_total = 0
        for doc in col.find({"owner_id": user}, {"content": 1}):
            content = doc.get("content", {})
            if isinstance(content, dict):
                files_total += len(content)
        names = [d.get("name", "") for d in col.find({"owner_id": user}, {"name": 1}).limit(5)]
        sample = ", ".join([n for n in names if n]) + (" ..." if repo_count > 5 else "")

        print(f"\n== {user} ==")
        print(f"MongoDB: {db_name}.{coll_name}")
        print(f"Repos stored: {repo_count}")
        print(f"Total files captured across repos: {files_total}")
        if sample:
            print(f"Sample repos: {sample}")


def main() -> None:
    cfg = load_config()

    gh = cfg.get("github", {})
    users: list[str] = gh.get("users", [])
    if not users:
        print("[!] No users configured at github.users in config/crawler.yaml")
        return

    storage = cfg.get("storage", {})
    sink = (storage.get("sink") or "jsonl").lower()

    if sink == "jsonl":
        jsonl_template = storage.get("jsonl_path", "data/raw/github/{user}/repos.jsonl")
        summarize_jsonl(users, jsonl_template)
    elif sink == "mongo":
        uri = storage.get("mongo_uri", "mongodb://localhost:27017")
        db_name = storage.get("mongo_db", "llm_twin")
        coll_name = storage.get("mongo_collection", "repositories")
        summarize_mongo(users, uri, db_name, coll_name)
    else:
        print(f"[!] Unknown storage.sink={sink}. Use 'jsonl' or 'mongo'.")


if __name__ == "__main__":
    main()