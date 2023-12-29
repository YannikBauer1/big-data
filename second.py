import os, findspark
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import pandas as pd

spark = SparkSession\
        .builder\
        .master('local[*]')\
        .getOrCreate()

from pyspark.sql.functions import *

sc = spark.read.csv("ICUSTAYS.csv", header=True)

df= sc.withColumn("INTIME",to_timestamp("INTIME")).withColumn("INTIME",to_date("INTIME")).show()
#print(df.dtypes)

t0 = time.time()
sc.toPandas()
#print(sc.toPandas())
print(time.time()-t0)
t0 = time.time()
sc.collect()
#print(sc.collect())
print(time.time()-t0)

t0 = time.time()
sc.cache().collect()
#print(sc.collect())
print(time.time()-t0)

rdd = spark.sparkContext.parallelize(sc)
t0 = time.time()
rdd.collect()
#print(sc.collect())
print(time.time()-t0)

#sc.select("SUBJECT_ID","HADM_ID").groupBy("SUBJECT_ID","HADM_ID").agg().show()
#sc.withColumn("ROW_ID",sc.ROW_ID.cast('int')).groupby("SUBJECT_ID","HADM_ID").sum("ROW_ID").show(False)
#dataframe = sc.groupBy('SUBJECT_ID',"HADM_ID").agg(f.first(sc['ROW_ID'])).select("SUBJECT_ID","HADM_ID")

print("aaaa")




