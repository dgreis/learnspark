from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import pandas
import subprocess
from datetime import datetime
import os


def load_data(spark):

    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("hdfs://172.17.0.2:54310/user/root/movies.dat")

    parts = lines.map(lambda l: l.split("::"))
    # Each line is converted to a tuple.
    movies = parts.map(lambda p: (p[0], p[1].split('(')[0].strip(), p[1].split('(')[1].strip(')'), p[2]))

    # The schema is encoded in a string.
    schemaString = "movie_id title year genres"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaMovies = spark.createDataFrame(movies, schema)

    # Creates a temporary view using the DataFrame
    schemaMovies.createOrReplaceTempView("movies")

    ###Ratings

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("hdfs://172.17.0.2:54310/user/root/ratings.dat")

    parts = lines.map(lambda l: l.split("::"))
    # Each line is converted to a tuple.
    ratings = parts.map(lambda p: (p[0], int(p[1].strip()),int(p[2]), datetime.fromtimestamp(int(p[3]))))

    #ratings.cache()

    # The schema is encoded in a string.
    schemaString = "user_id movie_id rating timestamp"

    #fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    fields = [StructField('user_id', StringType(), True), StructField('movie_id',IntegerType(),True), StructField('rating',IntegerType(),True),\
             StructField('timestamp',TimestampType(),True)]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaRatings = spark.createDataFrame(ratings, schema)
    #schemaRatings.write.saveAsTable("ratings")


    # Creates a temporary view using the DataFrame
    schemaRatings.createOrReplaceTempView("ratings")

    ###Users

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("hdfs://172.17.0.2:54310/user/root/users.dat")

    parts = lines.map(lambda l: l.split("::"))
    # Each line is converted to a tuple.
    users = parts.map(lambda p: (p[0], p[1].strip(), p[2], p[3], p[4]))


    # The schema is encoded in a string.
    schemaString = "user_id gender age occupation zip_code"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaUsers = spark.createDataFrame(users, schema)

    # Creates a temporary view using the DataFrame
    schemaUsers.createOrReplaceTempView("users")

    #Plots

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("hdfs://172.17.0.2:54310/user/root/plots.dat")

    parts = lines.map(lambda l: l.split("\t"))
    # Each line is converted to a tuple.
    plots = parts.map(lambda p: (p[0], p[1]))

    # The schema is encoded in a string.
    schemaString = "title plot_summary"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPlots = spark.createDataFrame(plots, schema)

    # Creates a temporary view using the DataFrame
    schemaPlots.createOrReplaceTempView("plots")

    sq = HiveContext(spark)
    return (sq.table("ratings"),sq.table("users"),sq.table("movies"),sq.table("plots"))