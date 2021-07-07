from pyspark.sql.functions import sum, desc, first, countDistinct, max

from jobs.sql_queue import TOP_CAMPAIGNS, CHANNELS_ENGAGEMENT
from jobs.utils import save_to_parquet
from jobs.env import SQL_DEFAULT_LIMIT, TOP_CAMPAIGNS_OUTPUT, CHANNELS_ENGAGEMENT_OUTPUT


def top_campaigns_sql(spark):
    top_campaigns = spark.sql(TOP_CAMPAIGNS)
    save_to_parquet(top_campaigns, f'{TOP_CAMPAIGNS_OUTPUT}/sql')
    return top_campaigns


def top_campaigns_df(df):
    top_campaigns = (df.where('isConfirmed = "TRUE"')
        .groupBy('campaignId')
        .agg(sum('billingCost').alias('revenue'))
        .orderBy(desc('revenue'))
        .limit(SQL_DEFAULT_LIMIT)
        .select('campaignId', 'revenue'))
    save_to_parquet(top_campaigns, f'{TOP_CAMPAIGNS_OUTPUT}/spark_df')
    return top_campaigns


def channels_engagement_sql(spark):
    result = spark.sql(CHANNELS_ENGAGEMENT)
    save_to_parquet(result, f'{CHANNELS_ENGAGEMENT_OUTPUT}/sql')
    return result


def channels_engagement_df(df):
    channels_engagement_df = (
      df.groupBy('campaignId', 'channelId')
        .agg(countDistinct('sessionId').alias('sessionCount'))
        .orderBy('campaignId', desc('sessionCount'))
        .select('campaignId', 'channelId', 'sessionCount')
      )

    channels_engagement_df = (channels_engagement_df.groupBy('campaignId')
        .agg(max('sessionCount').alias('maxSessions'), first('channelId').alias('channelId'))
        .orderBy(desc('maxSessions'))
        .select('campaignId', 'channelId', 'maxSessions'))

    save_to_parquet(channels_engagement_df, f'{CHANNELS_ENGAGEMENT_OUTPUT}/spark_df')
    return channels_engagement_df
