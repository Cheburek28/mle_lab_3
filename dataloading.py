import os
from logger import Logger
import pandas as pd

from pyspark_cassandra import *
from pyspark.conf import SparkConf


conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster("local") \
    .set("spark.cassandra.connection.host", "cassandra-1")

sc = CassandraSparkContext(conf=conf).getOrCreate()

# TEST_SIZE = 0.2
SHOW_LOG = True


class DataLoader:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.data_path = os.path.join(os.getcwd(), "data/en.openfoodfacts.org.products.csv")

        # self.log.info("DataLoader is ready")

    def load_data(self):
        df = pd.read_csv(self.data_path, sep="	", nrows=30000)
        df.to_csv("data/new_data.csv", index=False)

        # rdd = sc.parallelize(df)
        # rdd = sc.textFile("data/new_data.csv")
        # rdd.cache()

        # df = rdd.toDF()
        df.show()

        df.write.format("org.apache.spark.sql.cassandra").options(
            table="test", keyspace="learn_cassandra").save()
        # rdd.toDF().write.format("org.apache.spark.sql.cassandra").options(
        #     table="mle_lab_3", keyspace="learn_cassandra").save()


sc.stop()

if __name__ == "__main__":
    pass
    # dl = DataLoader()
    # dl.load_data()

    # Loading data by cqlsh
#     docker run --name cassandra-1 -p 9042:9042 -d cassandra:3.7
    # sudo docker exec -it cassandra-1 cqlsh
#   sudo docker cp new_data.csv 1393ac3e18fe:/new_data.csv
# COPY learn_cassandra.test FROM '/new_data.csv' WITH HEADER=TRUE AND NULL=TRUE;

