
import string

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

import pyspark as ps
from pyspark.sql.functions import avg, col, collect_list, count, udf
from pyspark.sql.types import ArrayType, StringType


def preprocess_review_text(text):
    stopwords_ = set(stopwords.words('english') + [str(i) for i in xrange(100)])
    # stemmer_ = SnowballStemmer('english')
    lemma_ = WordNetLemmatizer()

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
    filtered_tokens = [w for w in lowercased_tokens if w not in stopwords_]

    # stemmed = []
    lemmatized = []
    for token in filtered_tokens:
        try:
            lemmatized.append(lemma_.lemmatize(token))
            # stemmed.append(stemmer_.stem(token))
        except:
            # continue when stemmer doesn't work
            continue

    return lemmatized


def group_tokens_by_beer(df_tokens):

    def get_all_tokens(tokens):
        return reduce(lambda x, y: x + y, tokens)

    get_all_tokens_udf = udf(get_all_tokens, ArrayType(StringType()))

    df_beer_reviews = df_tokens.groupby('brewery_name', 'beer_name', 'state', 'beer_style') \
        .agg(get_all_tokens_udf(collect_list('tokens')).alias('lemmatized_tokens'), count("*").alias("count"), avg("avg_rating").alias("avg")) \
        .where((col("count") > 25) & (col("avg") >= 3.5))

    return df_beer_reviews


def get_beer_reviews_dataframe(df_reviews):
    lemmatize_review_udf = udf(lambda x: preprocess_review_text(x), ArrayType(StringType()))
    df_tokens = df_reviews.withColumn("tokens", lemmatize_review_udf('text'))
    df_tokens.persist(ps.StorageLevel.MEMORY_AND_DISK)
    df_beer_reviews = group_tokens_by_beer(df_tokens)
    return df_beer_reviews
