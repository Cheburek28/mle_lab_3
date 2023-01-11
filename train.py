import os
from logger import Logger

from pyspark_cassandra import *
from pyspark.conf import SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql.functions import *
from math import sqrt

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster("local") \
    .set("spark.cassandra.connection.host", "cassandra-1")\
    .set("spark.sql.codegen.wholeStage", "false")

sc = CassandraSparkContext(conf=conf).getOrCreate()

# TEST_SIZE = 0.2
SHOW_LOG = True


class KMeans:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.model_path = os.path.join(os.getcwd(), "")

        # self.log.info("DataLoader is ready")

    def kmeans(self):
        rdd_one = sc.cassandraTable("learn_cassandra", "test",
                                    connection_config={"spark_cassandra_connection_host": "cassandra-1"})

        clusters = KMeans.train(rdd_one, 2, maxIterations=10, initializationMode="random")

        # Evaluate clustering by computing Within Set Sum of Squared Errors
        def error(point):
            center = clusters.centers[clusters.predict(point)]
            return sqrt(sum([x ** 2 for x in (point - center)]))

        WSSSE = rdd_one.map(lambda point: error(point)).reduce(lambda x, y: x + y)
        print("Within Set Sum of Squared Error = " + str(WSSSE))

        # Save and load model
        clusters.save(sc, os.path.join(self.model_path, "KMeans"))


sc.stop()

# spark-submit --packages anguenot/pyspark-cassandra:2.4.1 --conf spark.cassandra.connection.host=cassandra-1 train.py
if __name__ == "__main__":
    km = KMeans()
    km.kmeans()

