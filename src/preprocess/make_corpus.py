# src/preprocess/make_corpus.py
from __future__ import annotations
import json, hashlib, re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

ROOT = Path(__file__).resolve().parents[2]  # project root (../.. from this file)

def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else (ROOT / p)

def _load_jsonl(path: Path) -> Iterator[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def _normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    # unify newlines, collapse weird whitespace, strip trailing long spaces
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t\f\v]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _printable_ratio(s: str) -> float:
    if not s:
        return 0.0
    printable = sum(1 for ch in s if ch.isprintable() or ch in "\n\t")
    return printable / max(len(s), 1)

def _chunk(text: str, size: int, overlap: int) -> List[str]:
    text = text.strip()
    if not text:
        return []
    if size <= 0:
        return [text]
    chunks, i = [], 0
    step = max(size - overlap, 1)
    while i < len(text):
        chunks.append(text[i:i+size])
        i += step
    return chunks

def _lang_from_ext(ext: str) -> str:
    m = {
        ".py": "python", ".md": "markdown", ".txt": "text",
        ".ipynb": "notebook", ".json": "json", ".yaml": "yaml", ".yml": "yaml",
        ".js": "javascript", ".ts": "typescript", ".cpp": "cpp", ".c": "c",
    }
    return m.get(ext.lower(), ext.lower().lstrip("."))

def _iter_repo_docs(users: List[str], input_template: str) -> Iterator[Tuple[str, dict]]:
    for user in users:
        p = _resolve(input_template.format(user=user))
        for doc in _load_jsonl(p):
            yield user, doc

def _extract_ipynb(text: str) -> str:
    """If given a notebook JSON string, pull out markdown + code cell text."""
    try:
        nb = json.loads(text)
        cells = nb.get("cells", [])
        parts = []
        for c in cells:
            if c.get("cell_type") in ("markdown", "code"):
                src = c.get("source", [])
                parts.append("".join(src) if isinstance(src, list) else str(src))
        return _normalize_text("\n\n".join(parts))
    except Exception:
        return _normalize_text(text)

def build_corpus(
    users: List[str],
    input_template: str,
    include_exts: Iterable[str],
    exclude_exts: Iterable[str],
    max_file_chars: int,
    chunk_size: int,
    chunk_overlap: int,
) -> List[dict]:
    include_exts = {e.lower() for e in include_exts}
    exclude_exts = {e.lower() for e in exclude_exts}
    out: List[dict] = []

    for owner, repo_doc in _iter_repo_docs(users, input_template):
        repo = repo_doc.get("name", "")
        link = repo_doc.get("link", "")
        content: Dict[str, str] = repo_doc.get("content", {}) or {}
        for rel_path, raw in content.items():
            ext = Path(rel_path).suffix.lower()
            if include_exts and ext not in include_exts:
                continue
            if ext in exclude_exts:
                continue

            text = _extract_ipynb(raw) if ext == ".ipynb" else _normalize_text(raw)
            if not text or len(text) > max_file_chars or _printable_ratio(text) < 0.85:
                continue

            pieces = _chunk(text, size=chunk_size, overlap=chunk_overlap)
            lang = _lang_from_ext(ext)
            for idx, piece in enumerate(pieces):
                meta = {
                    "source": "github",
                    "owner": owner,
                    "repo": repo,
                    "path": rel_path,
                    "ext": ext,
                    "lang": lang,
                    "repo_url": link,
                    "chunk_index": idx,
                    "n_chunks": len(pieces),
                }
                base = f"{owner}|{repo}|{rel_path}|{idx}|{len(piece)}"
                rid = hashlib.sha1(base.encode("utf-8")).hexdigest()  # stable deterministic id
                out.append({"id": rid, "text": piece, "metadata": meta})
    return out

def save_jsonl(records: List[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def run_preprocess(cfg: dict) -> Tuple[int, Path]:
    gh = cfg.get("github", {})
    pp = cfg.get("preprocess", {})
    users = gh.get("users", [])
    if not users:
        return 0, Path()

    input_template = pp.get("input_template", "data/raw/github/{user}/repos.jsonl")
    outdir = pp.get("outdir", "data/processed")
    include_exts = pp.get("include_exts", [])
    exclude_exts = pp.get("exclude_exts", [])
    max_file_chars = int(pp.get("max_file_chars", 120000))
    chunk_cfg = pp.get("chunk", {})
    size = int(chunk_cfg.get("size", 1200))
    overlap = int(chunk_cfg.get("overlap", 200))

    records = build_corpus(
        users=users,
        input_template=input_template,
        include_exts=include_exts,
        exclude_exts=exclude_exts,
        max_file_chars=max_file_chars,
        chunk_size=size,
        chunk_overlap=overlap,
    )

    out_path = _resolve(outdir) / "corpus.jsonl"
    save_jsonl(records, out_path)
    return len(records), out_path