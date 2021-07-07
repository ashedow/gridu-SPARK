import time

from pyspark.sql import SparkSession
from jobs.utils import save_to_parquet, load_clickstream_data, load_user_purchases_data
from jobs.env import TEST_TASK_1_PATH
from jobs.task1 import transform_data, transform_data_udf
from jobs.task2 import (
    top_campaigns_df, top_campaigns_sql,
    channels_engagement_df, channels_engagement_sql
)


def time_of_func(function):
    def wrapped(*args):
        start_time = time.perf_counter_ns()
        res = function(*args)
        print(time.perf_counter_ns() - start_time)
        return res
    return wrapped


@time_of_func
def test_transform_data():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data(clickstream_data, user_purchases_data)
    transformed_data.createOrReplaceTempView('confBillingCost')

    save_to_parquet(transformed_data, TEST_TASK_1_PATH)
    spark.stop()


@time_of_func
def test_transform_udf_data():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.createOrReplaceTempView('confBillingCost')

    save_to_parquet(transformed_data, TEST_TASK_1_PATH)
    spark.stop()


@time_of_func
def test_top_campaigns_spark_df():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('confBillingCost')
    save_to_parquet(transformed_data, TEST_TASK_1_PATH)

    top_campaigns_df(transformed_data)

    spark.stop()

@time_of_func
def test_top_campaigns_spark_sql():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('confBillingCost')
    save_to_parquet(transformed_data, TEST_TASK_1_PATH)

    top_campaigns_sql(spark)

    spark.stop()


@time_of_func
def test_channels_engagement_spark_df():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('confBillingCost')
    save_to_parquet(transformed_data, TEST_TASK_1_PATH)

    channels_engagement_df(transformed_data)

    spark.stop()


@time_of_func
def test_channels_engagement_sql():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('confBillingCost')
    save_to_parquet(transformed_data, TEST_TASK_1_PATH)

    channels_engagement_sql(transformed_data)

    spark.stop()


if __name__ == '__main__':
    test_transform_data()
    test_transform_udf_data()
    test_top_campaigns_spark_df()
    test_top_campaigns_spark_sql()
    test_channels_engagement_spark_df()
    test_channels_engagement_sql()
