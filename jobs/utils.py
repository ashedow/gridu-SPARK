from pyspark.sql.types import (
    StringType,
    StructType,
    BooleanType,
    DoubleType,
    TimestampType,
)

from jobs.env import CAPSTONE_USER_PURCHASES, CAPSTONE_MOBILE_APP_CLICKS


def save_to_parquet(df, path, mode='overwrite'):
    df.write.parquet(path, mode=mode)


clickstream_schema = (
    StructType()
    .add('userId', StringType(), False)
    .add('eventId', StringType(), False)
    .add('eventType', StringType(), False)
    .add('eventTime', TimestampType(), False)
    .add('attributes', StringType(), True)
)


purchase_schema = (
    StructType()
    .add('purchaseId', StringType(), False)
    .add('purchaseTime', TimestampType(), False)
    .add('billingCost', DoubleType(), True)
    .add('isConfirmed', BooleanType(), True)
)


def load_clickstream_data(spark):
    return (
      spark.read.format('csv').options(header='True').schema(clickstream_schema)
        .load(f'{CAPSTONE_MOBILE_APP_CLICKS}/mobile_app_clickstream_0.csv.gz')
    )


def load_user_purchases_data(spark):
    return (
      spark.read.format('csv').options(header='True').schema(purchase_schema)
        .load(f'{CAPSTONE_USER_PURCHASES}/user_purchases_0.csv.gz')
    )
