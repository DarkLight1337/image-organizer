from __future__ import annotations

from collections.abc import Collection
from typing import Callable, TypeVar

from pqdm.threads import pqdm

__all__ = ['map_parallel_with_tqdm']

T, R = TypeVar('T'), TypeVar('R')

def map_parallel_with_tqdm(
    arr: Collection[T],
    fn: Callable[[T], R],
    *,
    n_jobs: int,
    desc: str,
) -> list[R]:
    """
    Executes a function in parallel.

    Each element of `arr` contains the arguments that are passed to `fn`
    for one execution.

    The function `fn` is run in parallel using multithreading.
    """
    return pqdm(
        arr,
        fn,
        n_jobs=n_jobs,
        exception_behaviour='immediate',
        desc=desc,
        total=len(arr),
    )
