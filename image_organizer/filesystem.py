from __future__ import annotations

import mimetypes
from pathlib import Path

__all__ = ['IMG_SUFFIXES', 'mkdirp']

IMG_SUFFIXES = {k for k, v in mimetypes.types_map.items() if v.startswith('image/')}
"""Files with any of these suffixes are considered to be images."""

def mkdirp(path: Path):
    """
    As the Unix command `mkdir -p`, which automatically creates any parent directories
    along the way if necessary.
    """
    return path.mkdir(parents=True)
