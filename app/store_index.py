#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg as spark_avg
from cassandra.cluster import Cluster

CASSANDRA_HOST = "cassandra-server"
CASSANDRA_KEYSPACE = "search_engine"


print("=== Creating keyspace and tables in Cassandra ===")

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace(CASSANDRA_KEYSPACE)

session.execute("""
    CREATE TABLE IF NOT EXISTS term_index (
        term TEXT PRIMARY KEY,
        postings TEXT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id TEXT PRIMARY KEY,
        doc_len INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS global_stats (
        stat_name TEXT PRIMARY KEY,
        stat_value TEXT
    )
""")

print("Tables ready")


spark = SparkSession.builder \
    .appName("Store index to Cassandra") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .master("local") \
    .getOrCreate()


print("=== Loading term_index from HDFS ===")

df_index = spark.read.option("delimiter", "\t").csv("/indexer/index/part-*")
df_index = df_index.toDF("term", "postings")

count_index = df_index.count()
print(f"Term index: {count_index} rows")

df_index.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="term_index", keyspace=CASSANDRA_KEYSPACE) \
    .save()

print("Term index saved to Cassandra")


print("=== Loading doc_stats from HDFS ===")

df_stats = spark.read.option("delimiter", "\t").csv("/indexer/doc_stats/part-*")
df_stats = df_stats.toDF("doc_id", "doc_len")

df_stats = df_stats.withColumn("doc_len", col("doc_len").cast("int"))

count_stats = df_stats.count()
print(f"Document stats: {count_stats} rows")

df_stats.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="doc_stats", keyspace=CASSANDRA_KEYSPACE) \
    .save()

print("Document stats saved to Cassandra")


print("=== Loading global stats ===")

# Read N from HDFS
n_lines = spark.read.text("/indexer/N/part-*").collect()
N = int(n_lines[0][0]) if n_lines else 0
print(f"Total documents (N): {N}")

# Calculate avg_doc_len from doc_stats
avg_doc_len = df_stats.select(spark_avg("doc_len")).collect()[0][0]
print(f"Average document length: {avg_doc_len}")

session.execute(
    "INSERT INTO global_stats (stat_name, stat_value) VALUES (%s, %s)",
    ("N", str(N))
)
session.execute(
    "INSERT INTO global_stats (stat_name, stat_value) VALUES (%s, %s)",
    ("avg_doc_len", str(avg_doc_len))
)

print("Global stats saved to Cassandra")


print("\n=== Verification ===")

rows = session.execute("SELECT * FROM term_index LIMIT 5")
for row in rows:
    print(f"term: {row.term}, postings: {row.postings[:50]}...")

rows = session.execute("SELECT * FROM doc_stats LIMIT 5")
for row in rows:
    print(f"doc_id: {row.doc_id}, doc_len: {row.doc_len}")

rows = session.execute("SELECT * FROM global_stats")
for row in rows:
    print(f"{row.stat_name}: {row.stat_value}")


session.shutdown()
cluster.shutdown()
spark.stop()