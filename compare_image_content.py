from __future__ import annotations

import logging
from pathlib import Path
import shutil
from typing import Literal

import click

from image_organizer.filesystem import is_image, mkdirp
from image_organizer.func import map_mt_with_tqdm
from image_organizer.hash import compute_hash
from image_organizer.logger import set_logger_level

__all__ = ['compare_image_content']

def _copyfile(src_dst: tuple[Path, Path]):
    src, dst = src_dst
    return shutil.copyfile(src, dst)

@click.command()
@click.argument('src1', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('src2', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('dst', type=click.Path(exists=False))
@click.option('--hasher', '-h', type=click.Choice(['sha256', 'sha512']), default='sha256',
              help='The algorithm used to hash each file. (Default: sha256)')
@click.option('--threads', '-t', type=click.IntRange(min=1), default=8,
              help='Number of threads to use in parallel for I/O operations. (Default: 8)')
def compare_image_content(
    src1: str,
    src2: str,
    dst: str,
    *,
    hasher: Literal['sha256', 'sha512'],
    threads: int,
):
    """
    Given two sets of images contained in SRC1 and SRC2,
    finds the images that exist in both sets, and those that do not.

    Images are matched according to their content and have no relation to their filename.

    The images are outputted under DST, with one directory for each of the following:

    - `both`: Contains the images that exist in both sets.
      The path of each output image is based on that in SRC1.

    - `src1_only`: Contains the images that only exist in SRC1 but not in SRC2.
      The path of each output image is based on that in SRC1.

    - `src2_only`: Contains the images that only exist in SRC2 but not in SRC1.
      The path of each output image is based on that in SRC2.
    """
    if Path(dst).exists():
        raise ValueError(f'The destination directory ({dst}) already exists.')

    set_logger_level(logging.INFO)

    src1_img_paths = [path for path in Path(src1).rglob('*') if is_image(path)]
    src1_img_hashes = map_mt_with_tqdm(
        src1_img_paths,
        lambda path: compute_hash(path, hash_method=hasher),
        n_jobs=threads,
        desc='Hashing images from src1',
    )
    src1_img_hash_to_path = {
        img_hash: img_path
        for img_path, img_hash in zip(src1_img_paths, src1_img_hashes)
    }

    src2_img_paths = [path for path in Path(src2).rglob('*') if is_image(path)]
    src2_img_hashes = map_mt_with_tqdm(
        src2_img_paths,
        lambda path: compute_hash(path, hash_method=hasher),
        n_jobs=threads,
        desc='Hashing images from src2',
    )
    src2_img_hash_to_path = {
        img_hash: img_path
        for img_path, img_hash in zip(src2_img_paths, src2_img_hashes)
    }

    hashes_in_src1 = set(src1_img_hashes)
    hashes_in_src2 = set(src2_img_hashes)
    hashes_in_both = hashes_in_src1 & hashes_in_src2
    hashes_in_src1_only = hashes_in_src1 - hashes_in_src2
    hashes_in_src2_only = hashes_in_src2 - hashes_in_src1

    in_both_src_path_to_dst_path = {
        src1_img_hash_to_path[img_hash]: Path(dst) / 'both' / src1_img_hash_to_path[img_hash].relative_to(src1)
        for img_hash in hashes_in_both
    }

    map_mt_with_tqdm(
        {path.parent for path in in_both_src_path_to_dst_path.values()},
        mkdirp,
        n_jobs=threads,
        desc='Creating output directories for images that exist in both',
    )

    map_mt_with_tqdm(
        in_both_src_path_to_dst_path.items(),
        _copyfile,
        n_jobs=threads,
        desc='Writing output images that exist in both',
    )

    in_src1_only_src_path_to_dst_path = {
        src1_img_hash_to_path[img_hash]: Path(dst) / 'src1_only' / src1_img_hash_to_path[img_hash].relative_to(src1)
        for img_hash in hashes_in_src1_only
    }

    map_mt_with_tqdm(
        {path.parent for path in in_src1_only_src_path_to_dst_path.values()},
        mkdirp,
        n_jobs=threads,
        desc='Creating output directories for images that exist in src1 only',
    )

    map_mt_with_tqdm(
        in_src1_only_src_path_to_dst_path.items(),
        _copyfile,
        n_jobs=threads,
        desc='Writing output images that exist in src1 only',
    )

    in_src2_only_src_path_to_dst_path = {
        src2_img_hash_to_path[img_hash]: Path(dst) / 'src2_only' / src2_img_hash_to_path[img_hash].relative_to(src2)
        for img_hash in hashes_in_src2_only
    }

    map_mt_with_tqdm(
        {path.parent for path in in_src2_only_src_path_to_dst_path.values()},
        mkdirp,
        n_jobs=threads,
        desc='Creating output directories for images that exist in src2 only',
    )

    map_mt_with_tqdm(
        in_src2_only_src_path_to_dst_path.items(),
        _copyfile,
        n_jobs=threads,
        desc='Writing output images that exist in src2 only',
    )


if __name__ == '__main__':
    compare_image_content()
