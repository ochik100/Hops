import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

import database
from pyspark.ml.feature import IDF, CountVectorizer, Normalizer
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import col, udf
from singular_value_decomposition import computeSVD


class LatentSemanticAnalysis(object):

    def __init__(self, spark_context, sql_context, df_beer_reviews):
        self.spark_context = spark_context
        self.sql_context = sql_context
        self.beer_reviews = df_beer_reviews
        self.vocabulary = None
        self.tfidf = None
        self.vt = None
        self.similarity_matrix = None
        self.five_most_similar_beers = None
        self.token = None
        self.db = None

    def term_frequency(self):
        # TODO: save vocabulary to firebase
        cv = CountVectorizer(inputCol='lemmatized_tokens', outputCol='features_tf', vocabSize=2500)
        cv_model = cv.fit(self.beer_reviews)
        self.beer_reviews = cv_model.transform(self.beer_reviews)
        self.vocabulary = {idx: val.encode('utf-8') for idx, val in enumerate(cv_model.vocabulary)}

        normalizer = Normalizer(inputCol='features_tf', outputCol='features_normalized')
        self.beer_reviews = normalizer.transform(self.beer_reviews)

    def inverse_document_frequency(self):
        idf = IDF(inputCol='features_normalized', outputCol='features')
        idf_model = idf.fit(self.beer_reviews)
        self.beer_reviews = idf_model.transform(self.beer_reviews)

        self.find_beer_top_features()
        self.lookup_top_features()

    def find_beer_top_features(self):
        self.tfidf = self.beer_reviews.select(
            'brewery_name', 'beer_name', 'state', 'beer_style', 'features')

        self.tfidf = self.tfidf.rdd.zipWithIndex().map(lambda (y, id_): [y[0], y[1], y[2], y[3], y[4], id_]).toDF(
            ['brewery_name', 'beer_name', 'state', 'beer_style', 'features', 'id'])

        def get_top_features(x):
            return [x.id] + np.argsort(x.features.toArray())[::-1][:7].tolist()
        rdd_ = self.tfidf.rdd.map(lambda x: get_top_features(x))
        df_features = self.sql_context.createDataFrame(
            rdd_, ['id', 'top1', 'top2', 'top3', 'top4', 'top5', 'top6', 'top7'])

        self.tfidf = self.tfidf.join(df_features, ['id'], 'inner')

    def lookup_top_features(self):
        broadcastVocab = self.spark_context.broadcast(self.vocabulary)

        def lookup(x):
            if broadcastVocab.value.get(x) is None:
                return ""
            else:
                return broadcastVocab.value.get(x)
        lookup_udf = udf(lookup)
        cols = [column for column in self.tfidf.columns if "top" in column]
        self.tfidf = self.tfidf.select(*(lookup_udf(col(c)).alias(c)
                                         if c in cols else c for c in self.tfidf.columns))

    def perform_tfidf(self):
        self.term_frequency()
        self.inverse_document_frequency()
        # return self.tfidf

    def rdd_transpose(self, rdd):
        # http://www.data-intuitive.com/2015/01/transposing-a-spark-rdd/
        rddT1 = rdd.zipWithIndex().flatMap(lambda (x, i): [(i, j, e) for (j, e) in enumerate(x)])
        rddT2 = rddT1.map(lambda (i, j, e): (j, (i, e))).groupByKey().sortByKey()
        rddT3 = rddT2.map(lambda (i, x): sorted(
            list(x), cmp=lambda (i1, e1), (i2, e2): cmp(i1, i2)))
        rddT4 = rddT3.map(lambda x: map(lambda (i, y): y, x))
        return rddT4.map(lambda x: np.asarray(x))

    def singular_value_decomposition(self, n_components):
        # mat = RowMatrix(self.spark_context.parallelize(np.asarray(self.tfidf.select(
        #     'id', 'features').rdd.map(lambda row: row[1].toArray()).collect()).T))

        rdd = self.tfidf.select('id', 'features').rdd.map(lambda row: row[1].toArray())
        mat = RowMatrix(self.rdd_transpose(rdd))

        # rdd_ = self.tfidf.select(
        #     'id', 'features').rdd
        # rdd_ = rdd_.map(lambda row: row[1].toArray())
        svd = computeSVD(mat, n_components, computeU=True)
        print svd.U.numCols(), svd.U.numRows()
        print type(svd.V)
        self.vt = svd.V
        self.similarity_matrix = cosine_similarity(svd.V.toArray())
        # try:
        #     A = np.array([x.features.toArray() for x in self.tfidf.rdd.toLocalIterator()])
        # except:
        #     pass
        # A = np.array([x.features.toArray() for x in self.tfidf.rdd.toLocalIterator()])
        # svd = TruncatedSVD(n_components=n_components)
        # svd_model = svd.fit(A)
        # self.vt = svd_model.transform(A)
        # self.similarity_matrix = cosine_similarity(self.vt)

        # self.five_most_similar_beers = map(
        # lambda x: np.argsort(x)[::-1][:6], self.similarity_matrix)

        self.five_most_similar_beers = self.sql_context.createDataFrame(map(lambda x: np.argsort(
            x)[:: -1][: 6].tolist(), self.similarity_matrix), ['id', 'first', 'second', 'third', 'fourth', 'fifth'])

        self.tfidf = self.tfidf.join(self.five_most_similar_beers, ['id'], 'inner')

        #     , 'attr1': x.attr1, 'attr2': x.attr2, 'attr3': x.attr3, 'attr4': x.attr4, 'attr5': x.attr5
        #

        self.token, self.db = database.connect_to_database()

        # use default arguments to avvoid closure of the environment of the token and db variables
        def save_to_firebase(x, token=self.token, db=self.db):
            data = {'brewery_name': x.brewery_name, 'beer_name': x.beer_name, 'state': x.state, 'beer_style': x.beer_style, 'first': x.first, 'second': x.second, 'third': x.third,
                    'fourth': x.fourth, 'fifth': x.fifth, 'top1': x.top1, 'top2': x.top2, 'top3': x.top3, 'top4': x.top4, 'top5': x.top5, 'top6': x.top6, 'top7': x.top7}
            # name = {'brewery_name': x.brewery_name, 'beer_name': x.beer_name}
            db.child('beers').child(x.id).set(data, token)
            # db.child('beer_names').child(x.id).set(name, token)
            # sleep.(0.1)

        self.tfidf.rdd.foreach(lambda x: save_to_firebase(x))

        # for x in self.tfidf.rdd.toLocalIterator():
        #     data = {'brewery_name': x.brewery_name, 'beer_name': x.beer_name, 'state': x.state, 'beer_style': x.beer_style,
        #             'first': x.first, 'second': x.second, 'third': x.third, 'fourth': x.fourth, 'fifth': x.fifth}
        #     name = {'brewery_name': x.brewery_name, 'beer_name': x.beer_name}
        #     db.child('beers').child(x.id).set(data, token)
        #     db.child('beer_names').child(x.id).set(name, token)
        # np.save('../data/five_most_similar_beers.npy', self.five_most_similar_beers)

    def transform(self, n_components):
        self.perform_tfidf()
        # self.singular_value_decomposition(n_components=n_components)

    def get_vocabulary(self):
        return self.vocabulary
