#!/usr/bin/env python3
import sys
import re
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster
import math

# BM25 parameters
K1 = 1
B = 0.75

def clean_term(term):
    term = term.lower()
    term = re.sub(r'[^a-z]', '', term)
    return term

def parse_postings(postings_str):
    """Parse string of format 'doc1:tf,doc2:tf' to dict {doc_id: tf}"""
    result = {}
    if not postings_str:
        return result
    for pair in postings_str.split(','):
        if ':' in pair:
            doc_id, tf = pair.split(':')
            result[doc_id] = int(tf)
    return result

def get_global_stats():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    N_row = session.execute("SELECT stat_value FROM global_stats WHERE stat_name='N'").one()
    avg_row = session.execute("SELECT stat_value FROM global_stats WHERE stat_name='avg_doc_len'").one()
    
    N = int(N_row.stat_value) if N_row else 0
    avg_doc_len = float(avg_row.stat_value) if avg_row else 0
    
    session.shutdown()
    cluster.shutdown()
    return N, avg_doc_len

def get_doc_data():
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    rows = session.execute("SELECT doc_id, title, doc_len FROM doc_stats")
    doc_lens = {}
    doc_titles = {}
    for row in rows:
        doc_lens[row.doc_id] = row.doc_len
        doc_titles[row.doc_id] = row.title if row.title else "Unknown"
    
    session.shutdown()
    cluster.shutdown()
    return doc_lens, doc_titles

def main():
    # read query from stdin
    query = sys.stdin.read().strip()
    if not query:
        print("No query provided")
        sys.exit(1)
    
    print(f"Query: {query}\n")
    
    # clean and tokenize query
    query_terms = []
    for word in re.findall(r'[a-zA-Z]+', query.lower()):
        cleaned = clean_term(word)
        if cleaned and len(cleaned) > 2:
            query_terms.append(cleaned)
    
    if not query_terms:
        print("No valid terms in query")
        sys.exit(1)
    
    print(f"Query terms: {query_terms}\n")
    
    # get global stats
    N, avg_doc_len = get_global_stats()
    print(f"N: {N}, avg_doc_len: {avg_doc_len:.2f}\n")
    
    # get document data
    doc_lens, doc_titles = get_doc_data()
    print(f"Total documents in index: {len(doc_lens)}")

    # Get term data from Cassandra
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    
    term_postings = {}
    term_df = {}
    for term in query_terms:
        row = session.execute(f"SELECT postings FROM term_index WHERE term='{term}'").one()
        if row:
            postings = parse_postings(row.postings)
            term_postings[term] = postings
            term_df[term] = len(postings)
        else:
            term_postings[term] = {}
            term_df[term] = 0
    
    session.shutdown()
    cluster.shutdown()

    # Initialize Spark
    conf = SparkConf().setAppName("BM25 Search").setMaster("local")
    sc = SparkContext(conf=conf)
    
    doc_ids = list(doc_lens.keys())
    doc_rdd = sc.parallelize(doc_ids)
    
    # Broadcast values
    bc_N = sc.broadcast(N)
    bc_avg = sc.broadcast(avg_doc_len)
    bc_term_postings = sc.broadcast(term_postings)
    bc_term_df = sc.broadcast(term_df)
    bc_doc_lens = sc.broadcast(doc_lens)

    def compute_score(doc_id):
        score = 0.0
        doc_len = bc_doc_lens.value.get(doc_id, 0)
        if doc_len == 0:
            return (doc_id, 0.0)
        
        for term in query_terms:
            postings = bc_term_postings.value.get(term, {})
            tf = postings.get(doc_id, 0)
            if tf == 0:
                continue
            
            df = bc_term_df.value.get(term, 0)
            idf = math.log((bc_N.value - df + 0.5) / (df + 0.5) + 1) if df > 0 else 0
            
            norm_len = doc_len / bc_avg.value
            numerator = tf * (K1 + 1)
            denominator = tf + K1 * (1 - B + B * norm_len)
            term_score = idf * (numerator / denominator) if denominator != 0 else 0
            score += term_score
        
        return (doc_id, score)
    
    scores = doc_rdd.map(compute_score)
    top_docs = scores.takeOrdered(10, key=lambda x: -x[1])
    
    print("\nTop 10 documents")
    if not top_docs:
        print("No documents found")
    else:
        for rank, (doc_id, score) in enumerate(top_docs, 1):
            title = doc_titles.get(doc_id, "Unknown")
            print(f"{rank}. doc_id: {doc_id}, title: {title}, score: {score:.4f}")
    
    sc.stop()

if __name__ == "__main__":
    main()