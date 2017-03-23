from __future__ import division
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
    ratings = parts.map(lambda p: (int(p[0]), int(p[1].strip()),int(p[2]), datetime.fromtimestamp(int(p[3]))))

    #ratings.cache()

    # The schema is encoded in a string.
    schemaString = "user_id movie_id rating timestamp"

    #fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    fields = [StructField('user_id', IntegerType(), True), StructField('movie_id',IntegerType(),True), StructField('rating',IntegerType(),True),\
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


def create_topic_vector(v, tmat, k):
    idxs = v.indices
    cnts = v.values
    idxs_cnts = dict(zip(idxs, cnts))
    # arr = sV.toArray()
    # vocab_size = len(arr)
    ntmat = tmat / tmat.sum(axis=0)
    # ntmat = tmat
    ntops = tmat.shape[1]
    # top_vec = dict([(k,0) for k in range(ntops)])
    top_vec = dict([(k, 0)])
    for w in idxs:
        # for k in range(ntops):
        multiplier = idxs_cnts[w]
        weight = ntmat[w, k]
        val = multiplier * weight
        top_vec[k] = float(top_vec[k] + val)
    # r = [list(x) for x in top_vec.items()]
    r = top_vec[k]
    return r


def UDF_create_topic_vector(k, tmat):
    return udf(lambda v: create_topic_vector(v, tmat, k), FloatType())

##  Sample Usage

#topic_vectors = X.withColumn('topic_val_0', UDF_create_topic_vector(0,tmat)(col('features')))
#for k in range(1,num_topics):
#    topic_vectors = topic_vectors.withColumn('topic_val_'+str(k), UDF_create_topic_vector(k,tmat)(col('features')))
#topic_cols = [column('topic_val_'+str(k)) for k in range(num_topics)]
#mv = topic_vectors.select(['title'] + [array(topic_cols).alias("movie_topic_vector")])

def cosine_distance(ui,vi):
    from pyspark.mllib.linalg import Vectors
    u = Vectors.dense(ui)
    v = Vectors.dense(vi)
    dot = u.dot(v)
    u_norm = u.norm(2)
    v_norm = v.norm(2)
    r = float(dot/(u_norm*v_norm))
    return r

UDF_cosine_distance = udf(lambda ui,vi: cosine_distance(ui,vi), FloatType())

##  Sample Usage
#df = df.withColumn('cosine_sim', UDF_cosine_distance(col('user_topic_vector'),col('movie_topic_vector')))

UDF_max_arg = udf(lambda x: x if x > 0 else 0, IntegerType())

##  Sample Usage
#rj = rj.withColumn('rating_shifted', UDF_max_arg(col('rating_shifted')))


def precision_at_k(p,t,k):
    if len(t) < k:
        return np.nan
    else:
        tks = set(t[:k])
        #print tks
        pks = set(p[:k])
        tp = len(tks.intersection(pks))
        return tp/k

def UDF_precision_at_k(k):
    return udf(lambda x,y: precision_at_k(x,y,k), FloatType())

##  Sample Usage
#k=10
#user_preds = user_preds.withColumn('bst_precision_at_'+str(k), UDF_precision_at_k(k)(col('collect_list(rank_bst_preds)')
#                                                                                ,(col('collect_list(rank_ratings)')))
#                                  )
#user_preds = user_preds.withColumn('reg_precision_at_'+str(k), UDF_precision_at_k(k)(col('collect_list(rank_reg_preds)')
#                                                                                ,(col('collect_list(rank_ratings)')))
#                                  )