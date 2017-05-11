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
    df_reviews = df.select('brewery_name', 'beer_name', 'state', 'beer_style', 'text')
    print "df_reviews"

    try:
        df_beer_reviews = get_beer_reviews_dataframe(df_reviews)
    except:
        pass
    df_beer_reviews = get_beer_reviews_dataframe(df_reviews)
    print "df_beer_reviews"

    lsa_ = LatentSemanticAnalysis(df_beer_reviews, sql_context)
    lsa_.transform(n_components=300)
    # sc.stop()
