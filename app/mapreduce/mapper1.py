#!/usr/bin/env python3
import sys
import re

def clean_term(term):
    term = term.lower()
    term = re.sub(r'[^a-z]', '', term)
    return term

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    # format: id \t title \t text
    parts = line.split('\t', 2)
    if len(parts) != 3:
        continue
    
    doc_id, title, text = parts
    
    # tokenize
    words = re.findall(r'[a-zA-Z]+', text.lower())
    
    # calculate tf
    tf_dict = {}
    for word in words:
        cleaned = clean_term(word)
        if cleaned and len(cleaned) > 2:
            tf_dict[cleaned] = tf_dict.get(cleaned, 0) + 1
    
    for term, tf in tf_dict.items():
        print(f"{term}\t{doc_id}\t{tf}")

    print(f"!DOCLEN!\t{doc_id}\t{len(words)}")