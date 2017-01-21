from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import subprocess
import os

#conf = SparkConf().setAppName('appName').setMaster('local')
#sc = SparkContext(conf=conf)

mach_id = subprocess.check_output(["cat", "/proc/1/cgroup"
# , "|", "grep", "'docker/'","|" ,"tail" , "-1", "|", "sed", "'s/^.*\///'","|","cut", "-c", "1-12"])
                           ])
#mach_id = os.system("cat /proc/1/cgroup | grep 'docker/' | tail -1 | sed 's/^.*\///' | cut -c 1-12")
cmd = "cat /proc/1/cgroup | grep 'docker/' | tail -1 | sed 's/^.*\///' | cut -c 1-12"
ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
mach_id = ps.communicate()[0].strip()

spark = SparkSession.builder \
       .master("spark://1c451c55a047:7077") \
       .appName("Third Word Count") \
       .config("spark.some.config.option", "some-value") \
       .getOrCreate()

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("hdfs://"+mach_id+":9000/user/root/movies.dat")

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
lines = sc.textFile("hdfs://"+mach_id+":9000/user/root/ratings.dat")

parts = lines.map(lambda l: l.split("::"))
# Each line is converted to a tuple.
ratings = parts.map(lambda p: (p[0], p[1].strip(), p[2], p[3]))

# The schema is encoded in a string.
schemaString = "user_id movie_id rating timestamp"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaRatings = spark.createDataFrame(ratings, schema)

# Creates a temporary view using the DataFrame
schemaRatings.createOrReplaceTempView("ratings")

###Users

# Load a text file and convert each line to a Row.
lines = sc.textFile("hdfs://"+mach_id+":9000/user/root/users.dat")

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

print("at the end")