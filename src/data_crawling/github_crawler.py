"""
Minimal, config-driven GitHub crawler (requests + BeautifulSoup)

Exports:
- crawl_user_repos(user: str, max_repos: int, delay: float) -> list[dict]
- save_jsonl(records: list[dict], out_path: Path) -> None
"""

from __future__ import annotations
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

BASE = "https://github.com"

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

def _extract_repo_links_from_repos_tab(soup: BeautifulSoup, user: str) -> List[str]:
    repo_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # matches "/<user>/<repo>"
        if re.fullmatch(rf"/{re.escape(user)}/[^/]+", href):
            repo_urls.append(f"{BASE}{href}")

    # de-duplicate while preserving order
    seen = set()
    out = []
    for u in repo_urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _extract_repo_data_from_repo_page(soup: BeautifulSoup, repo_url: str) -> Dict:
    # repo name
    repo_name = None
    strong = soup.find("strong", attrs={"itemprop": "name"})
    if strong and strong.find("a"):
        repo_name = strong.find("a").get_text(strip=True)
    if not repo_name:
        repo_name = repo_url.rstrip("/").split("/")[-1]

    # topics
    topics = [a.get_text(strip=True) for a in soup.select("a.topic-tag")]

    # README text (rendered on main page)
    readme = soup.select_one("article.markdown-body")
    readme_text: Optional[str] = _normalize_ws(readme.get_text("\n")) if readme else None

    return {
        "repo_name": repo_name,
        "repo_url": repo_url,
        "topics": topics,
        "readme_text": readme_text,
    }

def crawl_user_repos(user: str, max_repos: int, delay: float) -> List[Dict]:
    s = _session()
    collected: List[Dict] = []
    page = 1

    while len(collected) < max_repos:
        url = f"{BASE}/{user}?tab=repositories&type=source&page={page}"
        soup = _get_soup(s, url)
        repo_urls = _extract_repo_links_from_repos_tab(soup, user)
        if not repo_urls:
            break

        for repo_url in repo_urls:
            if len(collected) >= max_repos:
                break
            rsoup = _get_soup(s, repo_url)
            data = _extract_repo_data_from_repo_page(rsoup, repo_url)
            collected.append(data)
            time.sleep(delay)  # polite

        page += 1
        time.sleep(delay)

    return collected

def save_jsonl(records: List[Dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(__import__("json").dumps(r, ensure_ascii=False) + "\n")
