from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .master("local")\
        .appName("PythonWordCount")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    count = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = count.sortBy(lambda x : x[1], False).collect()
    with open('results.txt', 'w', encoding='utf-8') as g:
    	for (word, count) in output[0:100]:
        	print("{}: {}".format(word, count), file=g)

    spark.stop()