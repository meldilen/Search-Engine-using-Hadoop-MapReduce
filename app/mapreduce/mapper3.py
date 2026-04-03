#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if line and line.startswith("!DOCLEN!"):
        parts = line.split('\t')
        if len(parts) == 3:
            print(f"{parts[1]}\t{parts[2]}")