import sys
import csv
import json
import numpy as np
import pandas as pd

from pyproj import Transformer
import shapely
from shapely.geometry import Point

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions  import date_format
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)



#read the file 

df_supermarket = pd.read_csv('nyc_supermarkets.csv')
df_supermarket = spark.createDataFrame(df_supermarket)
df_pattern = spark.read.option("header","true").option("escape", "\"").csv("/tmp/bdm/weekly-patterns-nyc-2019-2020/*")
df_cbg = pd.read_csv("nyc_cbg_centroids.csv")
df_cbg = spark.createDataFrame(df_cbg)

#Filter the palce
placeList = df_supermarket.select("safegraph_placekey").rdd.flatMap(lambda x: x).collect()
df_pattern = df_pattern.filter(F.col("placekey").isin(placeList))

df_pattern1 = df_pattern.select("placekey","poi_cbg","visitor_home_cbgs", date_format("date_range_start", "yyyy-MM").alias("Formatted_start_date"),date_format("date_range_end", "yyyy-MM").alias("Formatted_end_date"))
dateList = ['2019-03','2019-10','2020-03','2020-10']
df_pattern2 = df_pattern1.filter(F.col("Formatted_start_date").isin(dateList) | F.col("Formatted_end_date").isin(dateList))

def extract_date(col1,col2):
  res = ''
  if col1 == col2:
    res = col1 
  elif col1 in ['2019-03','2019-10','2020-03','2020-10']:
    res = col1
  elif col2 in ['2019-03','2019-10','2020-03','2020-10']:
    res = col2 
  return res

dateUdf = F.udf(extract_date,T.StringType())
df_pattern_date = df_pattern2.withColumn('date',dateUdf('Formatted_start_date','Formatted_end_date'))

def f_key(col1):
  visitor_home_cbg = json.loads(col1)
  res = []
  for i in visitor_home_cbg:
    res.append(i)
  return res

def f_value(col1):
  visitor_home_cbg = json.loads(col1)
  value_res = []
  for i in visitor_home_cbg.values():
    value_res.append(i)
  return 
  
keyUdf = F.udf(f_key,T.ArrayType(T.StringType()))
valueUdf = F.udf(f_value,T.ArrayType(T.IntegerType()))
df_pattern3 = df_pattern_date.withColumn('res_key',keyUdf("visitor_home_cbgs")).withColumn('res_value',valueUdf("visitor_home_cbgs"))

df_pattern5 = df_pattern3.withColumn("new", F.arrays_zip("res_key", "res_value"))\
       .withColumn("new", F.explode("new"))\
       .select('date',"poi_cbg", F.col("new.res_key").alias("home_key"), F.col("new.res_value").alias("people_number"))

df_pattern5 = df_pattern5.filter(F.col('home_key').substr(1, 2) == '36')

join_df1 = df_pattern5.join(df_cbg,F.col('poi_cbg')==F.col('cbg_fips'),'inner')\
.select(['date','poi_cbg','home_key','people_number','latitude','longitude']).withColumnRenamed('latitude','poi_lat').withColumnRenamed('longitude','poi_long')

join_df2 = join_df1.join(df_cbg,F.col('home_key')==F.col('cbg_fips'),'inner')\
.select(['date','poi_cbg','home_key','people_number','poi_lat','poi_long','latitude','longitude']).withColumnRenamed('latitude','home_lat').withColumnRenamed('longitude','home_long')

'''def cal_distance(col1,col2,col3,col4):
  t = Transformer.from_crs(4326, 2263)
  tuple1 = t.transform(col1,col2)
  tuple2 = t.transform(col3,col4)
  distance = Point(tuple1).distance(Point(tuple2))/5280
  return distance'''

def weigted_distance(col1,col2,col3,col4,col5):
  t = Transformer.from_crs(4326, 2263)
  tuple1 = t.transform(col1,col2)
  tuple2 = t.transform(col3,col4)
  weighted_distance = col5*(Point(tuple1).distance(Point(tuple2))/5280)
  
  return weighted_distance

#distanceUdf = F.udf(cal_distance,T.DoubleType())
weight_distanceUdf = F.udf(weigted_distance,T.DoubleType())
#distance_res = join_df2.withColumn('distance',distanceUdf('poi_lat','poi_long','home_lat','home_long')).withColumn('weighted_distance',weight_distanceUdf('poi_lat','poi_long','home_lat','home_long','people_number'))
distance_res = join_df2.withColumn('weighted_distance',weight_distanceUdf('poi_lat','poi_long','home_lat','home_long','people_number'))


res_df = distance_res.select('date','poi_cbg','people_number','distance','weighted_distance')

grouped_df = res_df.groupby('date','poi_cbg').agg(
    F.sum('people_number'),
    F.avg('distance').alias('distance'),
    (F.sum('weighted_distance')/F.sum('people_number')).alias('weighted_distance')
)
df_pivot_res = grouped_df.groupBy("poi_cbg").pivot('date').sum('weighted_distance').sort('poi_cbg').rdd.saveAsTextFile(sys.argv[1])


