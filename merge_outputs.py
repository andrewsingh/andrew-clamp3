#!/usr/bin/env python3
"""
Merge per-chunk embedding directories back into a single folder.

Usage
-----
python merge_outputs.py \
       --chunks   10 \
       --in_root  ./mp3_clamp3_embeds \
       --dest     ./mp3_clamp3_embeds_merged
"""

import argparse
import shutil
from pathlib import Path

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--chunks",   type=int, required=True)
    p.add_argument("--in-root",  required=True)
    p.add_argument("--dest",     required=True)
    p.add_argument("--overwrite", action="store_true",
                   help="Overwrite files if name collisions happen")
    return p.parse_args()

def main():
    a = parse_args()
    in_root = Path(a.in_root).expanduser().resolve()
    dest    = Path(a.dest).expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)

    for idx in range(a.chunks):
        chunk_dir = in_root / f"chunk_{idx:02d}"
        if not chunk_dir.exists():
            print(f"Warning: {chunk_dir} missing, skipping")
            continue

        for f in chunk_dir.iterdir():
            target = dest / f.name
            if target.exists() and not a.overwrite:
                print(f"Collision: {target} exists – skipping "
                      "(use --overwrite to clobber)")
                continue
            shutil.copy2(f, target)

    print("Merge complete!")

if __name__ == "__main__":
    main()
