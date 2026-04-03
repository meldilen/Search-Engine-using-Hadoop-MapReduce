#!/usr/bin/env python3
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re


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


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/data/d.parquet")
n = 1100
total = df.count()
df = df.select(['id', 'title', 'text']).sample(fraction=min(1.0, n / total), seed=42).limit(n)

clean_udf = udf(clean_wiki_text, StringType())
df_clean = df.withColumn("clean_text", clean_udf(df["text"]))


def create_doc(row):
    title = sanitize_filename(row['title']).replace(" ", "_")
    filename = f"data/{row['id']}_{title}.txt"
    if row['clean_text'] and len(row['clean_text']) > 0:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(row['clean_text'])

df_clean.foreach(create_doc)

df_clean.select("id", "title", "clean_text").coalesce(1).write.csv("/input/data", sep="\t", mode="overwrite")

spark.stop()