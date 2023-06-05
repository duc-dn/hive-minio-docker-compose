from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("spark application")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.2.3,"
        "com.amazonaws:aws-java-sdk:1.11.375,"
    )
    .config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.warehouse.dir", "s3a://spark/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()
)

columns = ["id", "name","age","gender"]

# Create DataFrame 
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create Hive Internal table
sampleDF.write.mode('overwrite') \
         .saveAsTable("employeee")