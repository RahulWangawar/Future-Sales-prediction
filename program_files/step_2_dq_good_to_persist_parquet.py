from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

master = 'local'
appName = 'PySpark_Data parquet converter'
config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
sqlContext = SQLContext(sc)
ss = SparkSession(sc)

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

baseDir = 'hdfs://localhost:9090/mini_project/raw/persist/'

salesSchema = StructType([StructField('year', IntegerType(), True),
                          StructField('month', IntegerType(), True),
                          StructField('day', IntegerType(), True),
                          StructField('date_block_num', IntegerType(), True),
                          StructField('shop_id', IntegerType(), True),
                          StructField('item_id', IntegerType(), True),
                          StructField('item_price', FloatType(), True),
                          StructField('item_cnt_day', IntegerType(), True)])

df = ss.read.schema(salesSchema)\
    .option('header',True)\
    .csv("hdfs://localhost:9090/mini_project/raw/dq_good/part-r-00000")
df.show(truncate=False)
df.write.mode('overwrite').parquet(baseDir)
print("done")

#
# df.write.option('header', True).csv(baseDir + "csv")
#
# df.write.mode('overwrite').option('header', False).csv(baseDir + "csv")
#
# df.write.mode('overwrite').parquet(baseDir + 'parquet')
#
# df.write.mode('overwrite').orc(baseDir + 'orc')
#
# df.select('_c0').write.text(baseDir + 'text')

