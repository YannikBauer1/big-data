#def setupSpark():
  # Spark needs to run with Java 8 ...
#  !pip install -q findspark
#  !apt-get install openjdk-8-jdk-headless > /dev/null
#  !echo 2 | update-alternatives --config java > /dev/null
#  !java -version
#  import os, findspark
#  os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
#  # !echo JAVA_HOME=$JAVA_HOME
#  !pip install -q pyspark
#  findspark.init(spark_home='/usr/local/lib/python3.7/dist-packages/pyspark')
#  !pyspark --version

import os, findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import time
import matplotlib.pyplot as plt
import numpy as np

t = time.time()
spark = SparkSession\
        .builder\
        .master('local[*]')\
   .    getOrCreate()
sc = spark.read.csv("CHARTEVENTS.csv", header=True)

sc = sc.withColumn("CHARTTIME",to_timestamp("CHARTTIME"))\
        .withColumn("STORETIME",to_timestamp("STORETIME"))\
        .withColumn("VALUE",col("VALUE").cast("int"))\
        .withColumn("ROW_ID",col("ROW_ID").cast("int"))\
        .withColumn("SUBJECT_ID",col("SUBJECT_ID").cast("int"))\
        .withColumn("HADM_ID",col("HADM_ID").cast("int"))\
        .withColumn("ICUSTAY_ID",col("ICUSTAY_ID").cast("float"))\
        .withColumn("ITEMID",col("ITEMID").cast("int"))\
        .withColumn("CGID",col("CGID").cast("float"))\
        .withColumn("VALUENUM",col("VALUENUM").cast("float"))
print("Reading time: ",time.time()-t)

# if patients.csv does not exists, it takes ~433 sec or ~7 min
def createPatients():
        files=[]
        for subdir, dirs, files in os.walk('./'):
                break
        if not "patients.csv" in files:
                t1 = time.time()
                df = sc.groupBy('SUBJECT_ID', "HADM_ID")\
                        .agg(f.first(sc['ROW_ID']))\
                        .select("SUBJECT_ID", "HADM_ID")
                l = []
                for i in df.cache().collect():
                        l.append(tuple(i))
                pd.DataFrame(l,columns=["SUBJECT_ID","HADM_ID"]).to_csv("patients.csv")
                print("Patients Dataframe creation time", time.time()-t1)
        return pd.read_csv("patients.csv",index_col=[0])
patients=createPatients()
print("Patients dataframe created!")
#print(patients)

# it needs ~500 sec or ~8 min
def plotPatientToValue(patient,date=0):
        t0 = time.time()
        grouped = sc.filter((sc.SUBJECT_ID==str(patient[0])) & (sc.HADM_ID==str(patient[1])))\
                .withColumn("DATE",to_date("CHARTTIME"))
        if date==0:
                date = grouped.select("DATE").first().DATE
        data = grouped.filter(grouped.DATE==date)\
                .select("VALUE","CHARTTIME","ITEMID")\
                .withColumn("CHARTTIME",(hour("CHARTTIME")*60+minute("CHARTTIME"))/(60*24))
        l_value = []
        l_time = []
        l_itemid =[]
        for i in data.cache().collect():
                l_value.append(i.VALUE)
                l_time.append(i.CHARTTIME)
                l_itemid.append(i.ITEMID)
        print("Time needed to plot: ",time.time()-t0)
        for i in range(len(l_value)-1,-1,-1):
                if not isinstance(l_value[i],int):
                        l_value.pop(i)
                        l_time.pop(i)
                        l_itemid.pop(i)
        #pd.DataFrame([(l_value[i],l_time[i],l_itemid[i]) for i in range(len(l_time))], columns=["VALUE", "TIME","ITEMID"]).to_csv("plot1.csv")
        cm = plt.cm.get_cmap("winter")
        sca = plt.scatter(l_time, l_value, c=l_itemid, cmap=cm)
        plt.colorbar(sca, label="Item_id")
        a, b, c = np.polyfit(l_time, l_value, deg=2)
        xseq = np.linspace(0, 1, num=100)
        plt.plot(xseq, a + b * xseq + c * (xseq ** 2), color="k", lw=2.5)
        plt.xlabel("Time of a day in [0,1]")
        plt.ylabel("Value of item")
        plt.title("Values of items in one day ("+str(date)+") of patiente "+str(patient[0]))
        plt.show()
#plotPatientToValue(list(patients.loc[0]))

# 600 sec / 10 min
def plot1ItemPatienteTime(patient,item_id=0):
        t0 = time.time()
        grouped = sc.filter((sc.SUBJECT_ID == str(patient[0])) & (sc.HADM_ID == str(patient[1])))
        if item_id == 0:
                item_id = grouped.select("ITEMID").first().ITEMID
        data = grouped.filter((sc.ITEMID==str(item_id)))\
                .select("CHARTTIME","VALUE").toPandas()
        data = data.sort_values(by="CHARTTIME")
        print("Time to create plot: ",time.time()-t0)
        #data.to_csv("plot2.csv")
        plt.scatter(list(data.CHARTTIME),list(data.VALUE))
        plt.plot(list(data.CHARTTIME),list(data.VALUE))
        plt.xlabel("Time")
        plt.ylabel("Value")
        plt.title("Value per time of patiente "+str(patient[0])+" of item "+str(item_id))
        plt.show()
#plot1ItemPatienteTime(list(patients.loc[0]),223834)
#plot1ItemPatienteTime(list(patients.loc[0]))

# ~ 600 sec / 10 min
def plotPatientesTime():
        t0 = time.time()
        print(t0)
        data = sc.groupBy("SUBJECT_ID","HADM_ID").agg(max("CHARTTIME"),min("CHARTTIME")).toPandas()
        # data.to_csv("plot3.csv")
        data = list(data.apply(lambda row: row[2] - row[3], axis=1))
        data = [i.total_seconds() / (24 * 60 * 60) for i in data]
        plt.hist(data)
        plt.yscale('symlog')
        plt.show()
        print("time needed",time.time()-t0)
#plotPatientesTime()


#--- Data Preprocessing

def ajuda(patient,pa2):
        g = sc.filter(((sc.SUBJECT_ID==str(patient[0])) & (sc.HADM_ID==str(patient[1]))) | ((sc.SUBJECT_ID==str(pa2[0])) & (sc.HADM_ID==str(pa2[1])))).toPandas()
        g.to_csv("1patiente.csv")
#ajuda(list(patients.loc[0]),list(patients.loc[1]))

def testPrePro():
        sc2 = spark.read.csv("1patiente.csv", header=True)
        sc.show()
        sc2.select("SUBJECT_ID", "HADM_ID", "ITEMID", "VALUENUM","CHARTTIME").dropna().groupBy("CHARTTIME","SUBJECT_ID", "HADM_ID")\
                .agg(collect_list(struct("ITEMID", "VALUENUM"))).orderBy(col("CHARTTIME").asc())\
                .show()
testPrePro()

def preprocessing():
        sc.select("SUBJECT_ID","HADM_ID","ITEMID","VALUENUM","CHARTTIME")

#sc.show()
#print(sc.dtypes)




