from misc import load_data
import pandas
from pyspark.sql import SparkSession, SQLContext
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import string
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import Column as col
from pyspark.sql.types import StringType, ArrayType
import nltk
from nltk.stem.porter import *
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA
nltk.download('punkt')



def create_topic_model(spark):

    plots = spark.sql("""Select * from plots where title != 'title'
                              AND plot_summary != 'Algo search did not find movie article'""")

    def remove_punc(s):
        #return s.translate(None, string.punctuation)
        exclude = set(string.punctuation)
        return ''.join(ch for ch in s if ch not in exclude)

    UDF_remove_punc = udf(remove_punc, StringType())

    no_punc = plots.withColumn("no_punc", UDF_remove_punc(plots['plot_summary']))

    tokenizer = RegexTokenizer(inputCol="no_punc", outputCol="words",toLowercase=False)
    tokenized_plots = tokenizer.transform(no_punc)


    def filter_proper_nouns(wordlist):
        import nltk
        nltk.download('averaged_perceptron_tagger')
        clean_wordlist = filter(lambda x: x not in [''], wordlist)
        #return [nltk.pos_tag(word) for word in wordlist]
        tagged_wordlist = nltk.pos_tag(clean_wordlist)
        nonpropernouns = [word.lower() for word,pos in tagged_wordlist if pos != 'NNP']
        return nonpropernouns
        #return [list(x) for x in nltk.pos_tag(filtered_wordlist)]

    UDF_filter_proper_nouns =  udf(lambda c: filter_proper_nouns(c),ArrayType(StringType(),True))


    prop_filtered = tokenized_plots.withColumn("no_props", UDF_filter_proper_nouns(tokenized_plots['words']))
    ##TODO: See if I can pass a single stemmer?


    remover = StopWordsRemover(inputCol="no_props", outputCol="no_stops")
    no_stop = remover.transform(prop_filtered)


    def stem_wordlist(wordlist):
        myStemmer = PorterStemmer()
        return [myStemmer.stem(word) for word in wordlist]
        #return wordlist

    #UDF_stem_wordlist = udf(stem_wordlist, ArrayType(s))
    UDF_stem_wordlist = udf(lambda c: stem_wordlist(c),ArrayType(StringType(),True)) ##YAY!!

    stemmed = no_stop.withColumn("stemmed", UDF_stem_wordlist(no_stop['no_stops']))
    ##TODO: See if I can pass a single stemmer?


    cv = CountVectorizer(inputCol="stemmed", outputCol="features")
    cvmodel = cv.fit(stemmed)

    design_matrix = cvmodel.transform(stemmed)

    lda = LDA(k=5, seed=1, optimizer="em")
    model = lda.fit(design_matrix[['features']])

    return model
