# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from deltalake_func import DeltaLakeFunctions

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'{name}')  # Press Ctrl+F8 to toggle the breakpoint.


def create_spark_session():
    return SparkSession.\
        builder.\
        appName("streamingExampleRead").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.0'). \
        getOrCreate()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Lendo arquivo parquet local')
    path = "/home/marcos/Documents/Projects/Others/eng-dados/stream-mongodb/resource"
    spark = create_spark_session();
    df_stream = DeltaLakeFunctions.read_delta_stream(create_spark_session(), 'parquet', path)

    # spark.sql("set spark.sql.streaming.schemaInference=true")
    # df_stream.createOrReplaceTempView("stream_rdf")

    # query = spark.sql("select * from stream_rdf")\
    #     .writeStream \
    #     .outputMode("append") \
    #     .option("forceDeleteTempCheckpointLocation", "true") \
    #     .format("console") \
    #     .start().awaitTermination()
    CONNECTION_STRING = 'mongodb://bsopin:bs123@localhost:20000/?authSource=admin'

    dsw = df_stream\
        .writeStream\
        .format("mongodb")\
        .queryName("ToMDB")\
        .option("checkpointLocation", "/tmp/pyspark7/")\
        .option("forceDeleteTempCheckpointLocation", "true")\
        .option('spark.mongodb.connection.uri', CONNECTION_STRING)\
        .option('spark.mongodb.database', 'local')\
        .option('spark.mongodb.collection', 'Sink')\
        .outputMode("append")\
        .start().awaitTermination()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
