from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, date
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df.show()


myschema = 'patient_id INT, examination STRING'
df_ = spark.createDataFrame([(44521, 'ECG')], schema=myschema)
df = df_.filter(~F.col('examination').isin(['ECG', 'ENT GENERAL']))
df1 = df.groupBy(F.col('patient_id')).agg(F.array(F.col('patient_id')))
df2 = df1.select()
