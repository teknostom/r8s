#!/usr/bin/env python3
"""Compare per-spec durations of two conformance runs.

    scripts/compare-junit.py R8S_JUNIT... -- OTHER_JUNIT... [--min SECONDS] [--top N]

Each side takes junit XML files or directories of them (ginkgo --procs
writes one per worker). Specs that passed on both sides are matched by
name; those where either side took at least --min seconds (default 5) are
listed by how much slower the first side was, as a ratio and a difference.
"""

import argparse
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET


def durations(paths):
    files = []
    for path in paths:
        if os.path.isdir(path):
            files += glob.glob(os.path.join(path, "*.xml"))
        else:
            files.append(path)
    out = {}
    for f in sorted(files, key=os.path.getmtime):
        for tc in ET.parse(f).iter("testcase"):
            name = tc.get("name", "").removeprefix("[It] ")
            if not name.startswith("[sig"):
                continue
            if tc.find("skipped") is not None or tc.find("failure") is not None:
                continue
            out[spec_key(name)] = float(tc.get("time", 0))
    return out


def spec_key(name):
    """The name without `[...]` tags; ginkgo's --junit-report appends labels."""
    return " ".join(re.sub(r"\[[^\]]*\]", " ", name).split())


def main():
    argv = sys.argv[1:]
    if "--" not in argv:
        sys.exit(__doc__)
    split = argv.index("--")
    left_paths, rest = argv[:split], argv[split + 1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--min", type=float, default=5.0)
    parser.add_argument("--top", type=int, default=30)
    args = parser.parse_args(rest)

    ours, theirs = durations(left_paths), durations(args.paths)
    common = [n for n in ours if n in theirs]
    rows = []
    for name in common:
        a, b = ours[name], theirs[name]
        if max(a, b) < args.min:
            continue
        rows.append((a - b, a / b if b > 0 else float("inf"), a, b, name))
    rows.sort(reverse=True)

    total_a = sum(ours[n] for n in common)
    total_b = sum(theirs[n] for n in common)
    print(f"{len(common)} specs passed on both sides; summed spec time "
          f"{total_a / 60:.1f} min vs {total_b / 60:.1f} min")
    print(f"{'diff':>7} {'ratio':>6} {'ours':>7} {'theirs':>7}  spec")
    for diff, ratio, a, b, name in rows[: args.top]:
        print(f"{diff:+6.1f}s {ratio:5.2f}x {a:6.1f}s {b:6.1f}s  {name[:100]}")


if __name__ == "__main__":
    main()
