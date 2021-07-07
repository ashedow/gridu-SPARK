from pyspark.sql import SparkSession

from jobs.env import RESULT_TRANSFORM
from jobs.utils import (
    save_to_parquet,
    load_clickstream_data,
    load_user_purchases_data,
)
from jobs.task1 import (
    # transform_data,
    transform_data_udf
)
from jobs.task2 import (
#   top_campaigns_spark_df,
  top_campaigns_sql,
#   channels_engagement_spark_df,
  channels_engagement_sql,
)


def main():
    spark = SparkSession.builder.appName('mobile_app_click_stream').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    clickstream_data = load_clickstream_data(spark)
    user_purchases_data = load_user_purchases_data(spark)

    transformed_data = transform_data_udf(clickstream_data, user_purchases_data)
    transformed_data.createOrReplaceTempView('confBillingCost')
    save_to_parquet(transformed_data, RESULT_TRANSFORM)

    top_campaigns_sql(spark)
    channels_engagement_sql(spark)

    spark.stop()


if __name__ == '__main__':
    main()
