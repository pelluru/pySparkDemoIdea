from pyspark.sql import SparkSession

dataFileBasePath = "/Users/prabhakara/prabhakara/books code/DataAnalysisWithPythonAndPySpark-master/data/gutenberg_books/"

if __name__ == "__main__":

    sparkSession = SparkSession\
                    .builder \
                    .appName("word count") \
                    .getOrCreate() \


    book = sparkSession.read.text(dataFileBasePath+"11-0.txt")

    book.show(10,False)

    """
    create temp table in Spark, so that you can  use spark SQL
    """
    book.createOrReplaceTempView("book_table")

    lines_split = sparkSession.sql("select split(value,' ') as line from book_table")

    lines_split.show(10,False)

    lines_split.createOrReplaceTempView("lines_table")

    words = sparkSession.sql("select explode(line) from lines_table")

    words.show(10,False)

