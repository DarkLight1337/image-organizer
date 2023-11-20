from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Mapping
from datetime import datetime
import mimetypes
import logging
from pathlib import Path
import shutil
import sys
from typing import Any, Callable, Literal, TypeVar

import click
from PIL import ExifTags, Image
from pqdm.threads import pqdm
from tqdm import tqdm

# Which files to consider as images
IMG_SUFFIXES = {k for k, v in mimetypes.types_map.items() if v.startswith('image/')}

# Set up the logger
logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

# Parallel operations
T, R = TypeVar('T'), TypeVar('R')
def map_parallel(
    arr: Collection[T],
    fn: Callable[[T], R],
    *,
    n_jobs: int,
    desc: str,
) -> list[R]:
    return pqdm(
        arr,
        fn,
        n_jobs=n_jobs,
        exception_behaviour='immediate',
        desc=desc,
        total=len(arr),
    )

def read_exif_attrs(img_path: Path) -> Mapping[int, Any]:
    try:
        with Image.open(img_path) as img:
            return img.getexif()
    except Exception:
        logger.warning('File (%s) cannot be opened as an image.', img_path, exc_info=True)
        return {}

def mkdirp(path: Path):
    return path.mkdir(parents=True)

def copyfile(src_dst: tuple[Path, Path]):
    src, dst = src_dst
    return shutil.copyfile(src, dst)

# Utility functions
def get_img_capture_timestamp(img_path: Path, exif_attrs: Mapping[int, Any]) -> datetime | None:
    # Check the following flags in the following order of preference
    if ExifTags.Base.DateTimeOriginal in exif_attrs:
        try:
            return datetime.strptime(exif_attrs[ExifTags.Base.DateTimeOriginal], r'%Y:%m:%d %H:%M:%S')
        except ValueError:
            logger.info('Image (%s) has invalid DateTimeOriginal format.', img_path)
            pass
    if ExifTags.Base.DateTimeDigitized in exif_attrs:
        try:
            return datetime.strptime(exif_attrs[ExifTags.Base.DateTimeDigitized], r'%Y:%m:%d %H:%M:%S')
        except ValueError:
            logger.info('Image (%s) has invalid DateTimeDigitized format.', img_path)
            pass
    if ExifTags.Base.DateTime in exif_attrs:
        try:
            return datetime.strptime(exif_attrs[ExifTags.Base.DateTime], r'%Y:%m:%d %H:%M:%S')
        except ValueError:
            logger.info('Image (%s) has invalid DateTime format.', img_path)
            pass

    logger.info('Image (%s) does not have a timestamp when it was captured.', img_path)
    logger.info('Image (%s) EXIF attributes:\n%s', img_path, exif_attrs)
    return None

def get_dst_dir_name(timestamp: datetime, *, groupby: Literal['year', 'month', 'day']) -> str:
    if groupby == 'year':
        return timestamp.strftime(r'%Y')
    elif groupby == 'month':
        return timestamp.strftime(r'%Y%m')
    elif groupby == 'day':
        return timestamp.strftime(r'%Y%m%d')

@click.command()
@click.argument('src', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('dst', type=click.Path(exists=False))
@click.option('--groupby', '-g', type=click.Choice(['year', 'month', 'day']), default='month',
              help='Represent each group as one year, one month or one day. (Default: month)')
@click.option('--threads', '-t', type=click.IntRange(min=1), default=8,
              help='Number of threads to use in parallel for I/O operations. (Default: 8)')
def groupby_capture_timestamp(
    src: str,
    dst: str,
    *,
    groupby: Literal['year', 'month', 'day'],
    threads: int,
):
    """
    Groups the images in SRC based on the timestamp when they were captured.

    The images are outputted under DST, with one directory per group.
    """
    if Path(dst).exists():
        raise ValueError(f'The destination directory ({dst}) already exists.')

    src_img_paths = [
        path
        for path in Path(src).rglob('*')
        if path.suffix in IMG_SUFFIXES
    ]

    exif_attrs_per_img = map_parallel(
        src_img_paths,
        read_exif_attrs,
        n_jobs=threads,
        desc='Reading images',
    )

    dst_dir_name_to_src_img_paths: defaultdict[str, list[Path]] = defaultdict(list)
    for src_img_path, exif_attrs in tqdm(
        zip(src_img_paths, exif_attrs_per_img),
        desc=f'Grouping images by {groupby}',
        total=len(src_img_paths),
    ):
        timestamp = get_img_capture_timestamp(src_img_path, exif_attrs)
        if timestamp is None:
            dst_dir_name = 'UNKNOWN'
        else:
            dst_dir_name = get_dst_dir_name(timestamp, groupby=groupby)

        dst_dir_name_to_src_img_paths[dst_dir_name].append(src_img_path)

    dst_dir_name_to_dst_dir_path = {
        dst_dir_name: Path(dst) / dst_dir_name
        for dst_dir_name in dst_dir_name_to_src_img_paths.keys()
    }

    map_parallel(
        dst_dir_name_to_dst_dir_path.values(),
        mkdirp,
        n_jobs=threads,
        desc='Creating output directories',
    )

    src_img_path_to_dst_img_path = {
        src_img_path: dst_dir_name_to_dst_dir_path[dst_dir_name] / src_img_path.name
        for dst_dir_name, src_img_paths in dst_dir_name_to_src_img_paths.items()
        for src_img_path in src_img_paths
    }

    map_parallel(
        src_img_path_to_dst_img_path.items(),
        copyfile,
        n_jobs=threads,
        desc='Writing output images',
    )


if __name__ == '__main__':
    groupby_capture_timestamp()
