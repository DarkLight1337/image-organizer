from __future__ import annotations

from collections.abc import Collection
from typing import Callable, TypeVar

from pqdm.processes import pqdm as pqdm_mp
from pqdm.threads import pqdm as pqdm_mt

__all__ = ['map_mt_with_tqdm']

T, R = TypeVar('T'), TypeVar('R')

def map_mp_with_tqdm(
    arr: Collection[T],
    fn: Callable[[T], R],
    *,
    n_jobs: int,
    desc: str,
) -> list[R]:
    """
    Executes a function in parallel using multiprocessing.

    Each element of `arr` contains the arguments that are passed to `fn`
    for one execution.

    Returns a list where the `i`th element corresponds to the return value
    of the `i`th execution (i.e., invoking `fn` with arguments `arr[i]`).
    """
    return pqdm_mp(
        arr,
        fn,
        n_jobs=n_jobs,
        exception_behaviour='immediate',
        desc=desc,
        total=len(arr),
    )

def map_mt_with_tqdm(
    arr: Collection[T],
    fn: Callable[[T], R],
    *,
    n_jobs: int,
    desc: str,
) -> list[R]:
    """
    Executes a function in parallel using multithreading.

    Each element of `arr` contains the arguments that are passed to `fn`
    for one execution.

    Returns a list where the `i`th element corresponds to the return value
    of the `i`th execution (i.e., invoking `fn` with arguments `arr[i]`).
    """
    return pqdm_mt(
        arr,
        fn,
        n_jobs=n_jobs,
        exception_behaviour='immediate',
        desc=desc,
        total=len(arr),
    )
