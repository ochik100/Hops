import multiprocessing

import pyspark as ps
from lsa import LatentSemanticAnalysis
from reviews import get_beer_reviews_dataframe

if __name__ == '__main__':
    cpu = 'local[{}]'.format(multiprocessing.cpu_count())
    sc = ps.SparkContext(cpu)
    print "Just created a SparkContext"
    sql_context = ps.SQLContext(sc)
    print "Just created a SQLContext"
    df = sql_context.read.json('../data/reviews.json')
    print "df"
    df_reviews = df.select('brewery_name', 'beer_name', 'state', 'beer_style', 'avg_rating', 'text')
    print "df_reviews"

    try:
        df_beer_reviews = get_beer_reviews_dataframe(df_reviews)
    except:
        pass
    df_beer_reviews = get_beer_reviews_dataframe(df_reviews)
    df_beer_reviews.persist(ps.StorageLevel.MEMORY_AND_DISK)
    print "df_beer_reviews"
    print df_beer_reviews.count()

    lsa_ = LatentSemanticAnalysis(sc, sql_context, df_beer_reviews)
    lsa_.transform(n_components=500)
    # sc.stop()
