#!/usr/bin/env python3
from pathlib import Path
import hashlib


def write_sha256(file_path: str) -> str:
    """Compute the SHA256 of a file and write it to a .sha256 file.

    Args:
        file_path: Path to the file to hash.

    Returns:
        The path to the generated .sha256 file as a string.
    """
    p = Path(file_path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    # Write the checksum and filename to a .sha256 file
    out = p.with_suffix(p.suffix + ".sha256")
    out.write_text(f"{h.hexdigest()}  {p.name}\n", encoding="utf-8")
    return out.as_posix()
