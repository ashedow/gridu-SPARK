from enum import Enum
from pyspark.sql.functions import (
    col, when, udf, sum, last, get_json_object, lit,
    length, regexp_replace, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

JOIN_TYPE = 'left'
DEFALT_APP_IS_OPEN = 0
COUNT_SESSIONS = 0


class EventType(Enum):
    OPEN = 'app_open'
    SEARCH = 'search_product'
    PRODUCT_DETAILS = 'view_product_details'
    PURCHASE = 'purchase'
    CLOSE = 'app_close'


# Task 1.1 Implement it by utilizing default Spark SQL capabilities.
def transform_data(clickstream_data, user_purchases_data):
    window1 = Window.partitionBy('userId').orderBy('eventTime')
    window2 = Window.orderBy('sessionId')

    clickstream_data = (
      clickstream_data
        .withColumn('appOpenFlag',
          when(
            col('eventType') == EventType.OPEN.value,
            monotonically_increasing_id()
          ).otherwise(DEFALT_APP_IS_OPEN)
        )
        .withColumn('sessionId', sum(col('appOpenFlag')).over(window1))
        .withColumn('attrs',
          when(
            col('eventType') == EventType.PURCHASE.value,
            clickstream_data['attributes'].substr(lit(2), length('attributes') - lit(2))
          ).otherwise(col('attributes'))
        )
        .withColumn('attr',
          when(
            col('eventType') == EventType.PURCHASE.value,
            regexp_replace(col('attrs'), '""', "'")
          ).otherwise(col('attrs'))
        )
        .withColumn('campaign_id',
          when(
            get_json_object('attr', '$.campaign_id').isNotNull(),
            get_json_object('attr', '$.campaign_id')
          ).otherwise(None)
        )
        .withColumn('channel_id',
          when(
            get_json_object('attr', '$.channel_id').isNotNull(),
            get_json_object('attr', '$.channel_id')
          ).otherwise(None)
        )
        .withColumn('purchase_id',
          when(
            get_json_object('attr', '$.purchase_id').isNotNull(),
            get_json_object('attr', '$.purchase_id')
          ).otherwise(None)
        )
        .withColumn('campaignId', last(col('campaign_id'), ignorenulls=True).over(window2.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn('channelId', last(col('channel_id'), ignorenulls=True).over(window2.rowsBetween(Window.unboundedPreceding, 0)))
    )

    target_df = clickstream_data.join(
      user_purchases_data,
      clickstream_data['purchase_id'] == user_purchases_data['purchaseId'],
      JOIN_TYPE
    )
    return target_df.select(
      col('purchaseId'),
      col('purchaseTime'),
      col('billingCost'),
      col('isConfirmed'),
      col('sessionId'),
      col('campaignId'),
      col('channelId')
    )


# Task 1.2 Implement it by using a custom UDF.
def increase_session_count(event_type):
    global COUNT_SESSIONS
    if event_type == EventType.OPEN:
        session_id = COUNT_SESSIONS
        COUNT_SESSIONS += 1
        return session_id
    else:
        return None


def clear_attributes(event_type, attributes):
    attr = attributes
    if event_type == EventType.PURCHASE:
        attr = attributes[1:len(attributes) - 1].replace('""', "'")
    return attr

# Define UDF 
app_open_udf = udf(increase_session_count, IntegerType())
attributes_udf = udf(clear_attributes, StringType())


def transform_data_udf(clickstream_data, purchase_data):
    window1 = Window.partitionBy('userId').orderBy('eventTime')
    window2 = Window.orderBy('sessionId')

    clickstream_data = (clickstream_data
        .withColumn('appOpenFlag', app_open_udf(clickstream_data['eventType']))
        .withColumn('sessionId', sum(col('appOpenFlag')).over(window1))
        .withColumn('attr', attributes_udf(clickstream_data['eventType'], clickstream_data['attributes']))
        .withColumn('campaign_id',
          when(
            get_json_object('attr', '$.campaign_id').isNotNull(),
            get_json_object('attr', '$.campaign_id')
          ).otherwise(None)
        )
        .withColumn('channel_id',
          when(
            get_json_object('attr', '$.channel_id').isNotNull(),
            get_json_object('attr', '$.channel_id')
          ).otherwise(None)
        )
        .withColumn('purchase_id',
          when(
            get_json_object('attr', '$.purchase_id').isNotNull(),
            get_json_object('attr', '$.purchase_id')
          ).otherwise(None)
        )
        .withColumn('campaignId',last(col('campaign_id'), ignorenulls=True)
          .over(window2.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn('channelId',last(col('channel_id'), ignorenulls=True)
          .over(window2.rowsBetween(Window.unboundedPreceding, 0)))
      )

    target_df = clickstream_data.join(
      purchase_data,
      clickstream_data['purchase_id'] == purchase_data['purchaseId'], 
      JOIN_TYPE
    )

    return target_df.select(
      col('purchaseId'),
      col('purchaseTime'),
      col('billingCost'),
      col('isConfirmed'),
      col('sessionId'),
      col('campaignId'),
      col('channelId')
    )
