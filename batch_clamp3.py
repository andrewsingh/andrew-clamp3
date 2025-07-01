#!/usr/bin/env python3
"""
Partition a directory of MP3s into N chunks, run clamp3_embd.py on each chunk,
and resume safely if interrupted.

Usage
-----
python batch_clamp3.py \
       --src_dir   ../mp3s \
       --work_dir  ./mp3_work           # temporary chunk folders will live here
       --out_root  ./mp3_clamp3_embeds  # final embeddings will live here
       --chunks    10                   # default 10
"""

import argparse
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--src-dir",  required=True,
                   help="Folder that contains *all* original mp3s")
    p.add_argument("--work-dir", required=True,
                   help="Folder that will hold the per-chunk mp3 folders")
    p.add_argument("--out-root", required=True,
                   help="Folder that will hold the per-chunk output folders")
    p.add_argument("--chunks",   type=int, default=10,
                   help="How many chunks to create (default 10)")
    p.add_argument("--move",     action="store_true",
                   help="Move files instead of copying (saves space)")
    return p.parse_args()

def chunkify(files, n):
    """Yield n approximately equal slices of *files*"""
    k = math.ceil(len(files) / n)
    for i in range(n):
        yield files[i*k : (i+1)*k]

def ensure_empty(path: Path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)

def main():
    args = parse_args()
    src_dir  = Path(args.src_dir).expanduser().resolve()
    work_dir = Path(args.work_dir).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    num_chunks = args.chunks

    # 1️⃣ discover MP3s
    mp3s = sorted([p for p in src_dir.rglob("*.mp3")])
    if not mp3s:
        sys.exit(f"No mp3 files found in {src_dir}")

    # 2️⃣ create chunk folders (skip if they already contain the correct files)
    work_dir.mkdir(parents=True, exist_ok=True)
    for idx, group in enumerate(chunkify(mp3s, num_chunks)):
        chunk_in  = work_dir / f"chunk_{idx:02d}"
        if not chunk_in.exists():
            chunk_in.mkdir(parents=True)
            for f in group:
                target = chunk_in / f.name
                if args.move:
                    shutil.move(str(f), target)
                else:     # copy (default, leaves originals intact)
                    shutil.copy2(f, target)

    # 3️⃣ run clamp3_embd.py on each chunk
    out_root.mkdir(parents=True, exist_ok=True)
    for idx in range(num_chunks):
        chunk_in  = work_dir / f"chunk_{idx:02d}"
        chunk_out = out_root / f"chunk_{idx:02d}"
        if chunk_out.exists():        # already done – skip
            print(f"✓ chunk {idx:02d} already processed, skipping")
            continue

        print(f"→ processing chunk {idx:02d} ({len(list(chunk_in.glob('*.mp3')))} files)")
        try:
            subprocess.run(
                [
                    "python",               
                    "clamp3_embd.py",
                    str(chunk_in),
                    str(chunk_out),
                    "--get-global"        
                ],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"✗ embedding script failed on chunk {idx:02d}: {e}")
            print("   Fix the issue and re-run batch_clamp3.py – "
                  "completed chunks will be skipped.")
            sys.exit(1)

    print("\nAll chunks finished!")
    print("Now run `python merge_outputs.py "
          f"--chunks {num_chunks} --in_root {out_root} "
          f"--dest {out_root}_merged`")

if __name__ == "__main__":
    main()
