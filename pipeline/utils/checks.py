#!/usr/bin/env python3
from pathlib import Path
import hashlib

def write_sha256(file_path: str) -> str:
    """Write SHA256 checksum next to file and return the .sha256 path."""
    p = Path(file_path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    out = p.with_suffix(p.suffix + ".sha256")
    out.write_text(f"{h.hexdigest()}  {p.name}\n", encoding="utf-8")
    return out.as_posix()
