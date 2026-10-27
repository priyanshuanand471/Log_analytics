import os
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType

SCHEMA = StructType([StructField(x,StringType(),True) for x in
    ('event_id','event_time','service','level','message','host')])

def spark_session(name):
    return (SparkSession.builder.appName(name)
            .config('spark.sql.shuffle.partitions','2')
            .config('spark.hadoop.fs.defaultFS',os.getenv('HDFS_URI','hdfs://namenode:9000'))
            .getOrCreate())

def parse_json(df):
    return (df.select(F.from_json(F.col('value').cast('string'),SCHEMA).alias('r'))
              .select('r.*')
              .withColumn('event_time',F.to_timestamp('event_time'))
              .filter(F.col('event_id').isNotNull() & F.col('event_time').isNotNull()
                      & F.col('service').isNotNull() & F.col('level').isin('INFO','WARN','ERROR')))

def cassandra():
    cluster=Cluster([os.getenv('CASSANDRA_HOST','cassandra')],port=9042)
    return cluster,cluster.connect('log_analytics')
