#!/usr/bin/env python3
"""CS 511 Project 1, Part 3: WikiIndexing.

Build an inverted index over Wikipedia articles stored in HDFS.

    spark-submit --master spark://main:7077 \
        /wikiindex.py <input_uri> <output_uri>

Both URIs must start with hdfs://main:9000/.

Input: one JSON article per line, e.g.

    {"id":10,"title":"MapReduce","text":"Map maps data ...","links":[20]}

`id` identifies the article and `text` holds the words to index; `title` and
`links` are ignored. Lines that are not valid JSON articles must be dropped.

Output: one line per word,

    word<TAB>df<TAB>cf<TAB>id:tf,id:tf,...

where tf is the number of occurrences of the word in that article, df is the
number of distinct articles containing the word, cf is the sum of the tf
values, and the postings are sorted by ascending numeric article id.

Only the three functions marked TODO need to be written. Do not modify
run-wikiindex.sh; the test scripts call it to submit this file.
"""
import json
import re
import sys

from pyspark.sql import SparkSession

# Tokenization rules (Part 3.1.1).
TOKEN_PATTERN = re.compile(r"[a-z]+(?:'[a-z]+)?")
MIN_TOKEN_LENGTH = 2
STOPWORDS = frozenset(
    "a an and are as at be by for from in is it of on or that the to was with".split()
)


def parse_article(line):
    """Parse one input line into an (article_id, text) pair.

    Return None when the line is not a valid JSON article, so that invalid
    lines (such as the trailing "not-json" line of the sample) are dropped
    instead of failing the job.
    """
    # TODO: implement.
    # raise NotImplementedError("parse_article")
    try: 
        info = json.loads(line) #takes the single string line and attempts to parse it into a Python dictionary
        if "id" in info and "text" in info:
            return (info["id"], info["text"]) #if the line is valid JSON and contains the required id and text fields, it ignores 
            # continuation: the extra data and returns only a tuple of (article_id, text)
        return None
    except (json.JSONDecodeError, TypeError): #if the line is invalid and throws an error, we can catch it and return None instead of
    # continuation: crashing the program
        return None


def tokenize(text):
    """Return the index terms of `text`, in order of appearance.

    Apply, in this order: lowercase the text, take all non-overlapping matches
    of TOKEN_PATTERN, drop tokens shorter than MIN_TOKEN_LENGTH, drop
    STOPWORDS. Repeated words are kept: the caller counts them.
    """
    # TODO: implement.
    # raise NotImplementedError("tokenize")
    text_in_lower = text.lower() #make the text in lowercase
    tokens = TOKEN_PATTERN.findall(text_in_lower); #extract all valid words into a list while automatically 
    # continuation: discarding punctuation and extra spaces
    return [t for t in tokens #iterate through the extracted words and discard any token that is shorter than 2
    # continuation: characters or belongs to the STOPWORDS list
        if len(t)>= MIN_TOKEN_LENGTH and t not in STOPWORDS
    ]


def build_index(lines):
    """Turn an RDD of raw input lines into an RDD of (word, postings) pairs.

    `postings` is an iterable of (article_id, tf) pairs, one per article that
    contains the word; their order does not matter, format_record sorts them.
    """
    # TODO: parse the lines, tokenize the text, count each (word, article_id)
    # pair, then group the counts by word.
    # raise NotImplementedError("build_index")
    articles = lines.map(parse_article).filter(lambda x: x is not None) #parse lines, check every item and keep it only if x is not None
    word_pairs = articles.flatMap(lambda article: [((word, article[0]), 1) for word in tokenize(article[1])]) #tokenize the text and add one occurrence for each word; flatMap flattens the list of tuples returned for each article
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b) #calculate the term frequency for each word in each document
    postings = word_counts.map(lambda item: (item[0][0], (item[0][1], item[1]))) #rearrange the key-value pairs (grouping by word)
    return postings.groupByKey() #group all tuples by word


def format_record(word, postings):
    """Format one index record: word<TAB>df<TAB>cf<TAB>id:tf,id:tf,..."""
    postings = sorted(postings)
    df = len(postings)
    cf = sum(tf for _, tf in postings)
    joined = ",".join("%d:%d" % (article_id, tf) for article_id, tf in postings)
    return "%s\t%d\t%d\t%s" % (word, df, cf, joined)


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    input_uri, output_uri = argv[1], argv[2]

    spark = SparkSession.builder.appName("WikiIndexing").getOrCreate()
    sc = spark.sparkContext
    try:
        # Use one output partition for a single, sorted index file.
        records = build_index(sc.textFile(input_uri)) \
            .map(lambda kv: format_record(kv[0], kv[1])) \
            .sortBy(lambda record: record, numPartitions=1)
        records.saveAsTextFile(output_uri)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
