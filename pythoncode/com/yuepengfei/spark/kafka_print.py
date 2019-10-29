from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os


def main():
    # 这种方式添加不起作用
    # os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars D:\jars\spark-streaming-kafka-0-8-assembly_2.11-2.1.3.jar pyspark-shell"

    spark = SparkSession.builder\
        .appName("kafka_print") \
        .master("local[*]") \
        .getOrCreate()

        # 这种方式也不行
        # config("spark.executor.extraClassPath", "D:\jars\spark-streaming-kafka-0-8-assembly_2.11-2.1.3.jar")

    sc = spark.sparkContext
    sc.setLogLevel('warn')
    ssc = StreamingContext(sc, 1)

    kafkaParam = {"metadata.broker.list": "192.168.240.131:9092"}
    stream = KafkaUtils.createDirectStream(ssc, ["test"], kafkaParam)
    stream.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()