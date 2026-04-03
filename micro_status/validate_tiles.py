#!/usr/bin/env python3
"""
Stitch tiles into one TIFF per color channel, and also write a 4x-downscaled PNG preview.

Filename pattern example:
  MAX_elastase_mock_gfp_rfp_Mag8x_Tile0_Ch488_Sh0_Rot0.btf.tiff

Features:
- Groups tiles by channel (_Ch###) and stitches one BigTIFF per channel.
- Supports row-major or column-major tile index mapping.
- Optional per-tile flips for stage direction along X and Y.
- Writes a 4x downscaled PNG preview per stitched image using scikit-image.

Assumptions:
- Each tile is a single 2D plane (H x W). If multi-page, we take the first page.
- Grid has NR rows and NC columns.
- Default index mapping is COLUMN-MAJOR (Tile0..Tile(NR-1) down first column).
- Missing tiles are filled with zeros.

Requires: numpy, tifffile, scikit-image
"""

import re
import os
import sys
import glob
import argparse
import numpy as np
import tifffile as tiff

from skimage.transform import resize
from skimage.exposure import rescale_intensity
from skimage.util import img_as_ubyte
from skimage import io as skio

FNAME_RE = re.compile(r"""
    (?P<prefix>.*?)
    _Tile(?P<tile>\d+)
    _Ch(?P<ch>\d+)
    (?:_Sh(?P<sh>\d+))?
    (?:_Rot(?P<rot>\d+))?
    (?P<suffix>.*?)
    (\.btf\.tiff|\.ome\.zarr\.tif)$
""", re.VERBOSE | re.IGNORECASE)

def parse_args():
    ap = argparse.ArgumentParser(
        description="Stitch tiles into one image per channel (BigTIFF output) and save a 4x-downscaled PNG preview."
    )
    ap.add_argument("input_glob", help="Glob for input files, e.g. './*.btf.tiff'")
    ap.add_argument("--rows", "-R", type=int, required=True, help="Number of tile rows (NR)")
    ap.add_argument("--cols", "-C", type=int, required=True, help="Number of tile cols (NC)")
    ap.add_argument("--outdir", "-o", default="stitched", help="Output directory")
    ap.add_argument(
        "--compression",
        default="lzw",
        choices=["none","lzw","deflate","zlib","zstd","jpeg","webp"],
        help="TIFF compression (default: lzw)"
    )
    ap.add_argument(
        "--dtype",
        default=None,
        help="Force output dtype (e.g. uint16). Default: inferred from tiles"
    )
    ap.add_argument(
        "--order",
        choices=["row-major","col-major"],
        default="col-major",
        help="Tile index traversal to (row,col) mapping (default: col-major)"
    )
    ap.add_argument(
        "--stage-direction-y",
        type=int,
        choices=[1, -1],
        default=1,
        help="If -1, flip each tile vertically before stitching. If 1, leave as-is. (default: 1)"
    )
    ap.add_argument(
        "--stage-direction-x",
        type=int,
        choices=[1, -1],
        default=1,
        help="If -1, flip each tile horizontally before stitching. If 1, leave as-is. (default: 1)"
    )
    ap.add_argument("--dry-run", action="store_true", help="Parse & report only, no writing")
    return ap.parse_args()

def find_tiles(pattern):
    files = sorted(glob.glob(pattern))
    records = []
    for f in files:
        m = FNAME_RE.search(os.path.basename(f))
        if not m:
            continue
        tile = int(m.group("tile"))
        ch = m.group("ch")
        records.append((f, tile, ch))
    return records

def idx_to_rc(tile_idx, NR, NC, order):
    if order == "row-major":
        # fill rows first (left->right within a row), then go down
        r = tile_idx // NC
        c = tile_idx % NC
    elif order == "col-major":
        # fill columns first (top->bottom within a column), then go right
        r = tile_idx % NR
        c = tile_idx // NR
    else:
        raise ValueError(f"Unknown order {order}")
    return r, c

def read_tile_first_plane(path):
    with tiff.TiffFile(path) as tf:
        arr = tf.asarray()
    # Squeeze any singleton dims; ensure 2D
    arr = np.squeeze(arr)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D tile, got shape {arr.shape} in {path}")
    return arr

def maybe_flip(arr, stage_direction_y, stage_direction_x):
    # Use view-based flips (no extra copy where possible).
    if stage_direction_y == -1 and stage_direction_x == -1:
        return arr[::-1, ::-1]
    elif stage_direction_y == -1:
        return arr[::-1, :]
    elif stage_direction_x == -1:
        return arr[:, ::-1]
    return arr

def save_downscaled_png(canvas_memmap, out_tiff_path, downscale_factor=4):
    """
    Save a 4x downscaled PNG (contrast-stretched to 8-bit) next to the stitched TIFF.
    Uses 1st–99th percentile of intensity for scaling.
    """
    H, W = canvas_memmap.shape
    target_h = max(1, H // downscale_factor)
    target_w = max(1, W // downscale_factor)

    print(f"Creating 1/{downscale_factor} preview: {target_h}x{target_w} PNG")

    # Resize with anti-aliasing; preserve original range
    small = resize(
        canvas_memmap,
        (target_h, target_w),
        order=1,              # bilinear
        anti_aliasing=True,
        preserve_range=True
    )

    # Compute 1% and 99% percentiles for contrast stretching
    p1, p99 = np.percentile(small, (1, 99))
    if p1 == p99:
        # avoid divide by zero if flat
        p1, p99 = small.min(), small.max()
    small = rescale_intensity(small, in_range=(p1, p99), out_range=(0, 1))

    # Convert to 8-bit
    png8 = img_as_ubyte(small)

    png_path = out_tiff_path.replace(".btf.tiff", "_down4.png")
    if png_path == out_tiff_path:
        png_path = out_tiff_path + "_down4.png"

    skio.imsave(png_path, png8, check_contrast=False)
    print(f"Wrote preview PNG -> {png_path}")

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    recs = find_tiles(args.input_glob)
    if not recs:
        print("No matching files found for pattern:", args.input_glob, file=sys.stderr)
        sys.exit(1)

    # Group files by channel
    by_ch = {}
    for f, tile, ch in recs:
        by_ch.setdefault(ch, []).append((tile, f))

    # Peek at first image to get size & dtype
    sample_path = recs[0][0]
    sample = read_tile_first_plane(sample_path)
    tile_h, tile_w = sample.shape
    dtype = np.dtype(args.dtype) if args.dtype else sample.dtype

    NR, NC = args.rows, args.cols
    total_tiles = NR * NC

    print(f"Detected tile size: {tile_h}x{tile_w}, dtype={dtype}")
    print(f"Grid: {NR} rows x {NC} cols (expect {total_tiles} tiles per channel)")
    print(f"Channels found: {', '.join(sorted(by_ch.keys()))}")
    print(f"Index order: {args.order}")
    print(f"Stage direction Y: {args.stage_direction_y} (use -1 to flip vertically)")
    print(f"Stage direction X: {args.stage_direction_x} (use -1 to flip horizontally)")

    for ch in sorted(by_ch.keys(), key=lambda x: int(x) if x.isdigit() else x):
        entries = by_ch[ch]
        tile_map = {tile: f for tile, f in entries}

        missing = sorted(set(range(total_tiles)) - set(tile_map.keys()))
        if missing:
            example = ", ".join(map(str, missing[:10]))
            print(f"[WARN] Channel {ch}: missing {len(missing)}/{total_tiles} tiles (e.g., {example})")

        # Prepare canvas via memmap to avoid huge RAM spikes
        canvas_h = NR * tile_h
        canvas_w = NC * tile_w
        mmap_path = os.path.join(args.outdir, f"__tmp_ch{ch}.dat")

        print(f"Channel {ch}: creating canvas {canvas_h}x{canvas_w}")
        canvas = np.memmap(mmap_path, dtype=dtype, mode="w+", shape=(canvas_h, canvas_w))
        canvas[:] = 0  # fill with zeros for missing tiles
        del canvas  # ensure header is written before we reopen

        # Reopen writable
        canvas = np.memmap(mmap_path, dtype=dtype, mode="r+", shape=(canvas_h, canvas_w))

        placed = 0
        for tile_idx in range(total_tiles):
            path = tile_map.get(tile_idx, None)
            r, c = idx_to_rc(tile_idx, NR, NC, args.order)
            r0, r1 = r * tile_h, (r + 1) * tile_h
            c0, c1 = c * tile_w, (c + 1) * tile_w

            if path is None:
                # Leave zeros
                continue

            arr = read_tile_first_plane(path)
            if arr.shape != (tile_h, tile_w):
                raise ValueError(f"Tile shape mismatch for {path}: {arr.shape} vs {(tile_h, tile_w)}")

            # Apply per-tile flips if requested
            arr = maybe_flip(arr, args.stage_direction_y, args.stage_direction_x)

            if arr.dtype != dtype:
                arr = arr.astype(dtype, copy=False)

            canvas[r0:r1, c0:c1] = arr
            placed += 1
            if placed % 250 == 0:
                canvas.flush()

        canvas.flush()
        del canvas  # close memmap

        out_path = os.path.join(args.outdir, f"stitched_Ch{ch}.btf.tiff")
        print(f"Channel {ch}: placed {placed}/{total_tiles} tiles -> {out_path}")

        if not args.dry_run:
            # Write BigTIFF
            canvas_ro = np.memmap(mmap_path, dtype=dtype, mode="r", shape=(canvas_h, canvas_w))
            # tiff.imwrite(
            #     out_path,
            #     canvas_ro,
            #     bigtiff=True,
            #     compression=None if args.compression == "none" else args.compression,
            # )

            # Also write a 4x-downscaled PNG preview
            try:
                save_downscaled_png(canvas_ro, out_path, downscale_factor=4)
            except MemoryError:
                print("[WARN] Ran out of memory while generating downscaled PNG; skipping.")
            except Exception as e:
                print(f"[WARN] Failed to generate downscaled PNG: {e}")

            del canvas_ro

        # cleanup temp
        try:
            os.remove(mmap_path)
        except OSError:
            pass

    print("Done.")

if __name__ == "__main__":
    main()
