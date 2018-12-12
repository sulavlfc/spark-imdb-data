from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import desc, udf
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession\
        .builder\
        .master('local')\
        .appName('imdb-app')\
        .config("spark.executor.memory", "1gb")\
        .getOrCreate()

sc = spark.sparkContext
imdb_data = spark.read.csv('imdb.csv', header='true', mode='DROPMALFORMED')

median = imdb_data.sort('imdbRating').select('imdbRating').filter('imdbRating is NOT NULL')
median_count = median.count()

median_location = round((median_count + 1 ) / 2)
first_quartile = round(median_count / 4)
third_quartile =  round(0.75 * median_count)

median_panda = median.toPandas()
median_rating = median_panda.iloc[int(median_location)]['imdbRating']
first_quartile_rating = median_panda.iloc[int(first_quartile)]['imdbRating']
third_quartile_rating = median_panda.iloc[int(third_quartile)]['imdbRating']


def categorize_movies(imdbRating):
    # print(median_rating)
    if (imdbRating > median_rating):
        category  =  1 if (imdbRating > third_quartile_rating) else 2
    else:
        category = 3 if (imdbRating > first_quartile_rating) else 4
    return category


#udf to categorize the moveies based on ratings and quartile score
category_udf = udf(lambda imdbRating : categorize_movies(imdbRating))

median_category = median.withColumn("category", category_udf(median.imdbRating))

category_count = median_category.groupBy('category').count().toDF('category', 'count') #.orderBy(desc('count')).show()

category = category_count.select('category', 'count').collect()

category_list = list(map(lambda x: int(x['category']), category))
count_list = list(map(lambda x: int(x['count']), category))


plt.figure(1)

plt.bar(category_list, count_list)
plt.show()