#!/usr/bin/env python3
import sys

current_term = None
postings = []

def flush():
    if current_term:
        postings_str = ','.join([f"{doc}:{tf}" for doc, tf in postings])
        print(f"{current_term}\t{postings_str}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    term, doc_id, tf = line.split('\t')
    tf = int(tf)
    
    if term == current_term:
        postings.append((doc_id, tf))
    else:
        flush()
        current_term = term
        postings = [(doc_id, tf)]

flush()