from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Literal

from .logger import get_logger

__all__ = ['compute_hash']

logger = get_logger()

def compute_hash(img_path: Path, *, hash_method: Literal['sha256', 'sha512']) -> str:
    """
    Computes the hash from an image file.
    """
    if hash_method == 'sha256':
        hasher = hashlib.sha256()
    elif hash_method == 'sha512':
        hasher = hashlib.sha512()

    try:
        hasher.update(img_path.read_bytes())
    except Exception:
        logger.warning('File (%s) cannot be opened as an image.', img_path, exc_info=True)
        return ''

    return hasher.hexdigest()
