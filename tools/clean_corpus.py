from __future__ import annotations
import json, re, sys
from pathlib import Path
from typing import Tuple

# Resolve project root (../ from tools/)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml  # type: ignore

# ------------------------ helpers ------------------------

def _load_cfg() -> dict:
    cfg_path = ROOT / "config" / "crawler.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else (ROOT / p)

def _ascii_ratio(s: str) -> float:
    if not s:
        return 1.0
    return sum(1 for ch in s if ord(ch) < 128) / len(s)

def _letters_and_digits_counts(s: str) -> Tuple[int, int]:
    letters = sum(1 for ch in s if ch.isalpha())
    digits = sum(1 for ch in s if ch.isdigit())
    return letters, digits

def _is_english(text: str, min_chars: int, min_prob: float, ascii_min_ratio: float) -> bool:
    t = text.strip()
    if not t:
        return False
    if len(t) >= max(min_chars, 1):
        # use langdetect
        try:
            from langdetect import detect_langs, DetectorFactory
            DetectorFactory.seed = 0
            for lp in detect_langs(t):
                if lp.lang == "en" and lp.prob >= min_prob:
                    return True
            return False
        except Exception:
            # fall back to ASCII heuristic below
            pass
    # short text or detector failed: use ASCII heuristic
    return _ascii_ratio(t) >= ascii_min_ratio

def _is_mostly_numeric(text: str, max_digit_ratio: float) -> bool:
    # Compute digits / (letters + digits); ignore whitespace/punct for this test
    letters, digits = _letters_and_digits_counts(text)
    denom = letters + digits
    if denom == 0:
        # If we have no letters or digits at all (just punctuation/whitespace), treat as bad
        return True
    return (digits / denom) >= max_digit_ratio

# ------------------------ pipeline ------------------------

def main() -> None:
    cfg = _load_cfg()
    pp = cfg.get("postprocess", {})
    in_path = _resolve(pp.get("input_path", "data/processed/corpus.jsonl"))
    out_path = _resolve(pp.get("output_path", "data/processed/corpus.cleaned.jsonl"))
    lang_min_chars = int(pp.get("lang_min_chars", 200))
    lang_min_prob = float(pp.get("lang_min_prob", 0.80))
    ascii_min_ratio = float(pp.get("ascii_min_ratio", 0.90))
    max_digit_ratio = float(pp.get("max_digit_ratio", 0.60))

    if not in_path.exists():
        print(f"[!] Input not found: {in_path}")
        return

    kept = 0
    dropped_non_en = 0
    dropped_numeric = 0
    dropped_empty = 0
    total = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                dropped_empty += 1
                continue

            text = (obj.get("text") or "").strip()
            if not text:
                dropped_empty += 1
                continue

            # numeric check first (quick reject)
            if _is_mostly_numeric(text, max_digit_ratio=max_digit_ratio):
                dropped_numeric += 1
                continue

            # english check
            if not _is_english(text, lang_min_chars, lang_min_prob, ascii_min_ratio):
                dropped_non_en += 1
                continue

            # keep
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            kept += 1

    print("\n[clean_corpus] Summary")
    print(f"Input:  {in_path}")
    print(f"Output: {out_path}")
    print(f"Total:  {total}")
    print(f"Kept:   {kept}")
    print(f"Dropped (non-English): {dropped_non_en}")
    print(f"Dropped (mostly-numeric): {dropped_numeric}")
    print(f"Dropped (empty/invalid): {dropped_empty}")

if __name__ == "__main__":
    main()