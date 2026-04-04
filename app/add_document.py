#!/usr/bin/env python3
import sys
import re
import os
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

def clean_wiki_text(text):
    if not text:
        return ""
    
    # delete links [[link]] or [[link|display]]
    text = re.sub(r'\[\[[^\]]+\|([^\]]+)\]\]', r'\1', text)  # [[display|link]] -> display
    text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)          # [[link]] -> link
    
    # delete HTML-tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # delete {{template}}
    text = re.sub(r'\{\{[^}]+\}\}', '', text)
    
    # delete ''italic'' and '''bold'''
    text = re.sub(r"'{2,}", '', text)
    
    # delete links [http://example.com]
    text = re.sub(r'\[https?://[^\s]+\s([^\]]+)\]', r'\1', text)
    text = re.sub(r'\[https?://[^\]]+\]', '', text)

    text = re.sub(r'={2,}[^=]+={2,}', '', text)
    text = re.sub(r'Category:[^\n]+', '', text)
    text = re.sub(r'^\*[^\n]*\n?', '', text, flags=re.MULTILINE)
    
    # delete newlines and multiple spaces
    text = re.sub(r'\n\s*\n', '\n', text)
    text = re.sub(r' +', ' ', text)

    text = re.sub(r'==\s*(References|External links|See also|Discography|External links)\s*==.*$', '', text, flags=re.MULTILINE | re.IGNORECASE)
    text = re.sub(r'\[|\]', '', text)
    
    return text.strip()

def extract_terms(text):
    words = re.findall(r'[a-zA-Z]+', text.lower())
    tf = {}
    for w in words:
        if len(w) > 2:
            tf[w] = tf.get(w, 0) + 1
    return tf

def main():
    if len(sys.argv) != 2:
        print("Usage: add_document.py <hdfs_file_path>")
        print("File name format: {id}_{title}.txt")
        sys.exit(1)
    
    hdfs_path = sys.argv[1]
    
    # extract document ID
    filename = os.path.basename(hdfs_path)
    doc_id = filename.split('_')[0]

    # Extract title from filename (remove id and .txt)
    title_part = filename.split('_', 1)[1].replace('.txt', '').replace('_', ' ')
    
    print(f"Processing document ID: {doc_id}, Title: {title_part}")
    
    spark = SparkSession.builder.appName("Add document to index").master("local").getOrCreate()
    
    text_rdd = spark.sparkContext.textFile(hdfs_path)
    raw_text = " ".join(text_rdd.collect())
    
    clean_text = clean_wiki_text(raw_text)
    doc_len = len(clean_text.split())
    
    tf_dict = extract_terms(clean_text)
    
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    N_row = session.execute("SELECT stat_value FROM global_stats WHERE stat_name='N'").one()
    N = int(N_row.stat_value) if N_row else 0
    avg_row = session.execute("SELECT stat_value FROM global_stats WHERE stat_name='avg_doc_len'").one()
    avg_doc_len = float(avg_row.stat_value) if avg_row else 0
    
    new_N = N + 1
    new_avg = (avg_doc_len * N + doc_len) / new_N
    
    session.execute(f"UPDATE global_stats SET stat_value='{new_N}' WHERE stat_name='N'")
    session.execute(f"UPDATE global_stats SET stat_value='{new_avg}' WHERE stat_name='avg_doc_len'")
    
    for term, tf in tf_dict.items():
        existing = session.execute(f"SELECT postings FROM term_index WHERE term='{term}'").one()
        if existing:
            new_postings = f"{existing.postings},{doc_id}:{tf}"
            session.execute(f"UPDATE term_index SET postings='{new_postings}' WHERE term='{term}'")
        else:
            session.execute(f"INSERT INTO term_index (term, postings) VALUES ('{term}', '{doc_id}:{tf}')")
    
    session.execute(f"INSERT INTO doc_stats (doc_id, title, doc_len) VALUES ('{doc_id}', '{title_part}', {doc_len})")
    
    print(f"Document {doc_id} added successfully")
    print(f"New N: {new_N}, New avg_doc_len: {new_avg:.2f}")
    print(f"Terms added: {len(tf_dict)}")
    
    cluster.shutdown()
    spark.stop()

if __name__ == "__main__":
    main()