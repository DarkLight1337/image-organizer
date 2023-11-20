from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import logging
from pathlib import Path
import shutil
from typing import Literal

import click
from tqdm import tqdm

from image_organizer.exif import read_exif_tags, get_img_captured_timestamp
from image_organizer.filesystem import is_image, mkdirp
from image_organizer.func import map_mt_with_tqdm
from image_organizer.logger import set_logger_level

__all__ = ['groupby_captured_timestamp']

def _copyfile(src_dst: tuple[Path, Path]):
    src, dst = src_dst
    return shutil.copyfile(src, dst)

def _get_dst_dir_name(timestamp: datetime, *, groupby: Literal['year', 'month', 'day']) -> str:
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
def groupby_captured_timestamp(
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

    set_logger_level(logging.INFO)

    src_img_paths = [path for path in Path(src).rglob('*') if is_image(path)]

    exif_tags_per_img = map_mt_with_tqdm(
        src_img_paths,
        read_exif_tags,
        n_jobs=threads,
        desc='Reading images',
    )

    dst_dir_name_to_src_img_paths: defaultdict[str, list[Path]] = defaultdict(list)
    for src_img_path, exif_tags in tqdm(
        zip(src_img_paths, exif_tags_per_img),
        desc=f'Grouping images by {groupby}',
        total=len(src_img_paths),
    ):
        timestamp = get_img_captured_timestamp(exif_tags, img_path=src_img_path)
        if timestamp is None:
            dst_dir_name = 'UNKNOWN'
        else:
            dst_dir_name = _get_dst_dir_name(timestamp, groupby=groupby)

        dst_dir_name_to_src_img_paths[dst_dir_name].append(src_img_path)

    dst_dir_name_to_dst_dir_path = {
        dst_dir_name: Path(dst) / dst_dir_name
        for dst_dir_name in dst_dir_name_to_src_img_paths.keys()
    }

    map_mt_with_tqdm(
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

    map_mt_with_tqdm(
        src_img_path_to_dst_img_path.items(),
        _copyfile,
        n_jobs=threads,
        desc='Writing output images',
    )


if __name__ == '__main__':
    groupby_captured_timestamp()
