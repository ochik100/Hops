import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics.pairwise import cosine_similarity

from pyspark.ml.feature import IDF, CountVectorizer, Normalizer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType


class LatentSemanticAnalysis(object):

    def __init__(self, df_beer_reviews):
        self.beer_reviews = df_beer_reviews
        self.vocabulary = None
        self.tfidf = None
        self.vt = None
        self.similarity_matrix = None

    def term_frequency(self):
        cv = CountVectorizer(inputCol='lemmatized_tokens', outputCol='features_tf')
        cv_model = cv.fit(self.beer_reviews)
        self.beer_reviews = cv_model.transform(self.beer_reviews)
        self.vocabulary = cv_model.vocabulary

        normalizer = Normalizer(inputCol='features_tf', outputCol='features_normalized')
        self.beer_reviews = normalizer.transform(self.beer_reviews)

    def inverse_document_frequency(self):
        idf = IDF(inputCol='features_normalized', outputCol='features')
        idf_model = idf.fit(self.beer_reviews)
        self.beer_reviews = idf_model.transform(self.beer_reviews)
        self.tfidf = self.beer_reviews.select(
            'brewery_name', 'beer_name', 'state', 'beer_style', 'features')

        # def get_top_features(features):
        #     return [self.vocabulary[idx] for idx in np.argsort(features.toArray())[::-1][:10]]
        # top_features_udf = udf(lambda beer: get_top_features(beer), ArrayType(StringType()))
        # self.tfidf = self.tfidf.withColumn('top_features', top_features_udf('features'))

    def perform_tfidf(self):
        self.term_frequency()
        self.inverse_document_frequency()
        return self.tfidf

    def singular_value_decomposition(self, n_components):
        A = np.array([x.features.toArray() for x in self.tfidf.rdd.toLocalIterator()])
        svd = TruncatedSVD(n_components=n_components)
        svd_model = svd.fit(A)
        self.vt = svd_model.transform(A)
        self.similarity_matrix = cosine_similarity(self.vt)
        np.save('../data/similarity_matrix.npy', self.similarity_matrix)

    def transform(self, n_components):
        self.perform_tfidf()
        self.singular_value_decomposition(n_components=n_components)
    # def fit(self, df_beer_reviews):
    #     self.beer_reviews = df_beer_reviews
    #
    # def transform(self):
    #     self.term_frequency()
    #     self.inverse_document_frequency()
    #     return self.tfidf
    #
    # def fit_transform(self, df_beer_reviews):
    #     self.fit(df_beer_reviews)
    #     self.transform()

    def get_vocabulary(self):
        return self.vocabulary
