from misc import load_data
import pandas
from pyspark.sql import SparkSession, SQLContext
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import *
import string
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import Column as col
from pyspark.sql.types import StringType, ArrayType
import nltk
from nltk.stem.porter import *
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA
from operator import add
#nltk.download('punkt')



def build_topic_model(spark,num_topics=8):

    plots = spark.sql("""Select * from plots where title != 'title'
                              AND plot_summary != 'Algo search did not find movie article'
                              AND plot_summary != '' """)

    def remove_punc(s):
        #return s.translate(None, string.punctuation)
        exclude = set(string.punctuation)
        return ''.join(ch for ch in s if ch not in exclude)

    UDF_remove_punc = udf(remove_punc, StringType())

    no_punc = plots.withColumn("no_punc", UDF_remove_punc(plots['plot_summary']))

    tokenizer = RegexTokenizer(inputCol="no_punc", outputCol="words",toLowercase=False)
    tokenized_plots = tokenizer.transform(no_punc)

    # def filter_proper_nouns(wordlist):
    def filter_pos(wordlist):
        import nltk
        #nltk.download('averaged_perceptron_tagger')
        clean_wordlist = filter(lambda x: x not in [''], wordlist)
        # return [nltk.pos_tag(word) for word in wordlist]
        tagged_wordlist = nltk.pos_tag(clean_wordlist)
        # nonpropernouns = [word.lower() for word,pos in tagged_wordlist if pos != 'NNP']
        filtered_pos = [word.lower() for word, pos in tagged_wordlist if pos in ['NN', 'NNS']]
        return filtered_pos
        # return [list(x) for x in nltk.pos_tag(filtered_wordlist)]

    # UDF_tag_wordlist =  udf(lambda c: filter_proper_nouns(c), ArrayType(StructType([
    #                                                        StructField("word", StringType(), True),
    #                                                        StructField("pos", StringType(), True)
    #                                                        ])))
    # UDF_filter_proper_nouns =  udf(lambda c: filter_proper_nouns(c),ArrayType(StringType(),True))
    UDF_filter_pos = udf(lambda c: filter_pos(c), ArrayType(StringType(), True))

    # prop_filtered = tokenized_plots.withColumn("no_props", UDF_filter_proper_nouns(tokenized_plots['words']))
    pos_filtered = tokenized_plots.withColumn("filt_pos", UDF_filter_pos(tokenized_plots['words']))


    remover = StopWordsRemover(inputCol="filt_pos", outputCol="no_stops")
    no_stop = remover.transform(pos_filtered)


    def stem_wordlist(wordlist):
        myStemmer = PorterStemmer()
        return [myStemmer.stem(word) for word in wordlist]
        #return wordlist

    #UDF_stem_wordlist = udf(stem_wordlist, ArrayType(s))
    UDF_stem_wordlist = udf(lambda c: stem_wordlist(c),ArrayType(StringType(),True)) ##YAY!!

    stemmed = no_stop.withColumn("stemmed", UDF_stem_wordlist(no_stop['no_stops']))

    stemmed_rdd = stemmed[['title', 'stemmed']].rdd.flatMap(lambda row: \
                                                                [((row.title, x), 1) for x in row.stemmed]).reduceByKey(add)
    fields = [StructField('key', ArrayType(StringType()), True), StructField('count', IntegerType(), True)]
    schema = StructType(fields)
    stemmed_df = spark.createDataFrame(stemmed_rdd.map(lambda x: (x[0], int(x[1]))), schema)

    sdf = stemmed_df.toPandas()
    sdf['title'] = sdf['key'].apply(lambda x: x[0])
    sdf['token'] = sdf['key'].apply(lambda x: x[1])
    sdf['present'] = 1
    # sdf[['title','token','count']].to_csv('./wc_by_title.csv',encoding='utf-8')
    kdf = sdf.groupby(['title', 'token'], as_index=False).agg({'present': max}) \
        .groupby('token', as_index=False).agg({'present': sum})
    kdf['perc_present'] = kdf['present'] / len(sdf['title'].unique())

    cutoff = 1

    saturating_words = kdf[kdf['perc_present'] > cutoff]['token'].tolist()

    def filter_saturating_words(wlist, slist):
        r = filter(lambda w: w not in slist, wlist)
        return r

    def UDF_filter_saturating_words(sw):
        return udf(lambda wl: filter_saturating_words(wl, sw), ArrayType(StringType(), True))

    unsaturated = stemmed.withColumn('unsaturated', UDF_filter_saturating_words(saturating_words)(stemmed['stemmed']))

    cv = CountVectorizer(inputCol="unsaturated", outputCol="features",minDF=5)
    cvmodel = cv.fit(unsaturated)

    design_matrix = cvmodel.transform(unsaturated)

    lda = LDA(k=num_topics, seed=1, optimizer="em")
    model = lda.fit(design_matrix[['features']])

    return {'model': model, 'count_vectorizor': cvmodel, 'design_matrix': design_matrix}
