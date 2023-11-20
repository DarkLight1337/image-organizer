from __future__ import annotations

import mimetypes
from pathlib import Path

__all__ = ['is_image', 'mkdirp']

def is_image(path: Path) -> bool:
    """
    Tests if a file is an image or not.
    """
    filetype, _ = mimetypes.guess_type(path)
    return filetype is not None and filetype.startswith('image/')

def mkdirp(path: Path) -> None:
    """
    As the Unix command `mkdir -p`, which automatically creates any parent directories
    along the way if necessary.
    """
    return path.mkdir(parents=True)
