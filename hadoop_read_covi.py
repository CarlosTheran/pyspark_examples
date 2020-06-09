from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


sparkSession = SparkSession.builder.appName("rwHDFS").getOrCreate()
sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("rwHDFS"))

# Read csv file from HDFS as DataFrame
df = sparkSession.read.csv('hdfs://namenode:8020/covi/csv_covi19')

# Read json file from HDFS as DataFrame
df2 = sparkSession.read.json('hdfs://namenode:8020/covi/json_covi19')


#To verify DataFrame type
df.dtypes

#To show the first element of your DataFrame
df.show()


#To print the number of record on your dataset
df.cout()

#Show n first observation
df.head(5)

#Get the summary statistic (mean, standard deviation, min, max, count)
df.describe().show()

#Get the DF which will not have duplicate rows of given DataFrame
df.select('_c6').dropDuplicates().show() 


# To summit spark-submit --master yarn --deploy-mode client <py file>
