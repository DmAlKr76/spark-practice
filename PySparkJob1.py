import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    data = spark.read.parquet(flights_path)
    df = data.where(F.col('TAIL_NUMBER').isNotNull()) \
        .groupBy(F.col('TAIL_NUMBER')) \
        .agg(F.count(F.col('TAIL_NUMBER')).alias('count')) \
        .select(F.col('TAIL_NUMBER'), F.col('count')) \
        .orderBy(F.col('count').desc()) \
        .limit(10)

    df.write.parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result_my', help='Please set result_my path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)

