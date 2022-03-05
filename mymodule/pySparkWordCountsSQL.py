from pyspark.sql import SparkSession
from pyspark.sql.functions import  *

dataFileBasePath = "/Users/prabhakara/prabhakara/books code/DataAnalysisWithPythonAndPySpark-master/data/gutenberg_books/"

if __name__ == "__main__":

    sparkSession = SparkSession\
                    .builder \
                    .appName("word count") \
                    .getOrCreate() \


    book = sparkSession.read.text(dataFileBasePath+"11-0.txt")

    book.show(10,False)

    lines = book.select(split(col("value")," ").alias("line"))

    lines.show(10,False)

    words  = lines.select(explode(col("line")).alias("word"))

    words.show(10,False)

    lower_words = words.select(lower(col("word")).alias("lower_word"))

    lower_words.show(10,False)

    """
    remove functuations
    """

    pure_words = lower_words.select(regexp_extract(col("lower_word"),"[a-z]*",0).alias("pure_word"))

    pure_words_non_empty = pure_words.where(col("pure_word") != "").alias("pure_word_non_empty")

    pure_words_non_empty.show(10,False)

    wordCounts = pure_words_non_empty.groupBy(col("pure_word")).count()

    wordCounts.show(10,False)

    sortedWords = wordCounts.orderBy(col("count").desc())

    sortedWords.show(10,False)
