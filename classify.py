import os

import pandas as pd

from logger import Logger

from pyspark.conf import SparkConf
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import *
from model_data_mart import DataMart
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

# TEST_SIZE = 0.2
SHOW_LOG = True


class KMeansModel:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.model_path = os.path.join(os.getcwd(), "")
        self.datamart = DataMart()

        self.spark = SparkSession.builder \
            .master("local[4]") \
            .appName("KMeansModel") \
            .getOrCreate()

        # self.log.info("DataLoader is ready")

    def kmeans(self):
        """The class method which cloistering data into 7 classes. The reason to choose this number of classes is shown
        in silhouette_plot.png"""

        df = self.datamart.get_unclassified_data()

        sparkDF = self.spark.createDataFrame(df)
        self.log.info("Data loaded!")
        sparkDF.printSchema()
        sparkDF.show()

        assemble = VectorAssembler(inputCols=sparkDF.columns, outputCol='features')
        assembled_data = assemble.transform(sparkDF)
        assembled_data.show(2)

        scale = StandardScaler(inputCol='features', outputCol='standardized', )
        data_scale = scale.fit(assembled_data)
        data_scale_output = data_scale.transform(assembled_data)
        data_scale_output.show(2)

        evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized',
                                        metricName='silhouette', distanceMeasure='squaredEuclidean')

        silhouette_score = []

        # for i in range(2, 10):
        kmeans = KMeans(featuresCol='standardized', k=7)
        model = kmeans.fit(data_scale_output)

        # Make predictions
        predictions = model.transform(data_scale_output)
        # predictions.printSchema()
        # predictions.show()

        score = evaluator.evaluate(predictions)
        print("Silhouette Score:", score)
        silhouette_score.append(score)
        #
        # import matplotlib.pyplot as plt
        # fig, ax = plt.subplots(1, 1, figsize=(8, 6))
        # ax.plot(range(2, 10), silhouette_score)
        # ax.set_xlabel('k')
        # ax.set_ylabel('cost')

        # plt.show()

        # centers = model.clusterCenters()
        # print("Cluster Centers: ")
        # for center in centers:
        #     print(center)

        self.save_model(predictions.toPandas())

    def save_model(self, df: pd.DataFrame):
        """Saves clustered data"""
        self.datamart.set_classified_data(df)


if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("MLE lab 3") \
        .setMaster("local")

    sc = SparkContext(conf=conf).getOrCreate()

    km = KMeansModel()
    km.kmeans()
    sc.stop()

