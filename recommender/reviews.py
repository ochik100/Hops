import multiprocessing
import string
import warnings

import numpy as np
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import word_tokenize

import pyspark as ps
from pyspark.ml.feature import IDF, CountVectorizer, Normalizer, Tokenizer
from pyspark.sql.functions import collect_list, count, udf
from pyspark.sql.types import ArrayType, DoubleType, StringType

cpu = 'local[{}]'.format(multiprocessing.cpu_count())
sc = ps.SparkContext(cpu)
print "Just created a SparkContext"
sql_context = ps.SQLContext(sc)
print "Just created a SQLContext"


def preprocess_review_text(text):
    stopwords_ = set(stopwords.words('english')
                     + [str(i) for i in xrange(100)])
    stemmer_ = SnowballStemmer('english')

    if not text:
        return []

    if (len(text) < 1):
        return []

    if (type(text) == unicode):
        text = text.encode('utf-8')

    replace_punctuation = string.maketrans(
        string.punctuation, ' ' * len(string.punctuation))
    unpunctuated_text = text.translate(replace_punctuation)
    # reviewer username info lost here via array slice
    tokens = word_tokenize(unpunctuated_text)[:-5:]
    lowercased_tokens = [token.lower() for token in tokens]
    filtered_tokens = [w for w in lowercased_tokens if not w in stopwords_]

    stemmed = []
    for token in filtered_tokens:
        try:
            stemmed.append(stemmer_.stem(token))
        except:
            # continue when stemmer doesn't work
            continue

    return stemmed


def tokenizer(df_tokens):

    def get_all_tokens(tokens):
        return reduce(lambda x, y: x + y, tokens)

    get_all_tokens_udf = udf(get_all_tokens, ArrayType(StringType()))

    df_reviews_stemmed = df_tokens.groupby('brewery_name', 'beer_name', 'state', 'beer_style') \
        .agg(get_all_tokens_udf(collect_list('tokens'))
             .alias('stemmed_tokens'))

    return df_reviews_stemmed

if __name__ == '__main__':
    df = sql_context.read.json('../data/reviews_100.json')
    token_udf = udf(lambda x: preprocess_review_text(x), ArrayType(StringType()))
    df_reviews = df.select('brewery_name', 'beer_name', 'state', 'beer_style', 'text')

    df_tokens = df_reviews.withColumn("tokens", token_udf('text'))
    df_reviews_stemmed = tokenizer(df_tokens)

    # sc.stop()
