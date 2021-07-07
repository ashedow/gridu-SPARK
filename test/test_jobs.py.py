import unittest
from pyspark import Row
from pyspark.sql import SparkSession

from jobs.task1 import (
        transform_data,
        transform_data_udf
)
from jobs.task2 import (
        top_campaigns_df,
        top_campaigns_sql,
        channels_engagement_df,
        channels_engagement_sql
)
from jobs.env import (
    TEST_TASK_1_PATH,
    TEST_CAPSTONE_USER_PURCHASES,
    TEST_CAPSTONE_MOBILE_APP_CLICKS
)


class SparkJobTests(unittest.TestCase):

        def setUp(self):
            self.spark = (
                SparkSession.builder.appName('test_app').getOrCreate()
            )

            self.expected_result_top = [
                Row(campaignId='cmp1', channelId='Google Ads', performance=2),
                Row(campaignId='cmp2', channelId='Yandex Ads', performance=2)
            ]

            self.expected_result_engagement = [
                Row(campaignId='cmp2', channelId='Yandex Ads', maxSessions=3),
                Row(campaignId='cmp1', channelId='Google Ads', maxSessions=2),
            ]

            self.mobile_clicks_data = (
                self.spark.read.csv(TEST_CAPSTONE_MOBILE_APP_CLICKS, sep=r'\t', header=True)
            )

            self.user_purchases_data = (
                self.spark.read.csv(TEST_CAPSTONE_USER_PURCHASES, sep=r'\t', header=True)
            )


        def test_transform_data(self):
            result = transform_data(self.mobile_clicks_data, self.user_purchases_data)
            self.assertEqual(result.count(), 39)
            self.assertEqual(len(result.columns), 7)


        def test_transform_data_udf(self):
            result = transform_data_udf(self.mobile_clicks_data, self.user_purchases_data)
            self.assertEqual(result.count(), 39)
            self.assertEqual(len(result.columns), 7)


        def test_top_campaigns_sql(self):
            result = transform_data(self.mobile_clicks_data, self.user_purchases_data)
            result.createOrReplaceTempView('confBillingCost')
            result = top_campaigns_sql(result)
            self.assertTrue([col in self.expected_result_top for col in result])


        def test_top_campaigns_df(self):
            result = transform_data(self.mobile_clicks_data, self.user_purchases_data)
            result.createOrReplaceTempView('confBillingCost')
            result = top_campaigns_df(result).collect()
            self.assertTrue([col in self.expected_result_top for col in result])


        def test_channels_engagement_df(self):
            result = transform_data(self.mobile_clicks_data, self.user_purchases_data)
            result.createOrReplaceTempView('confBillingCost')
            result = channels_engagement_df(result).collect()
            self.assertTrue([col in self.expected_result_engagement for col in result])


        def test_channels_engagement_df(self):
            result = transform_data(self.mobile_clicks_data, self.user_purchases_data)
            result.createOrReplaceTempView('confBillingCost')
            result = channels_engagement_sql(result).collect()
            self.assertTrue([col in self.expected_result_engagement for col in result])


if __name__ == '__main__':
        unittest.main()
