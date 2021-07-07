from jobs.env import SQL_DEFAULT_LIMIT

#       SELECT DISTINCT campaignId, sum(billingCost) as revenue FROM confBillingCost
#       WHERE isConfirmed = true
#       GROUP BY campaignId
#       ORDER BY revenue DESC

TOP_CAMPAIGNS = f"""
      SELECT campaignId, SUM(billingCost) as revenue 
      FROM purchase_attribution 
      WHERE isConfirmed = "TRUE" 
      GROUP BY campaignId 
      ORDER BY revenue DESC 
      LIMIT {SQL_DEFAULT_LIMIT}
      """

CHANNELS_ENGAGEMENT = """
      SELECT campaignId, first(channelId) as channelId, max(sessionCount) AS maxSessions
      FROM (
        SELECT campaignId, channelId, count(distinct sessionId) AS sessionCount
        FROM confBillingCost
        GROUP BY campaignId, channelId
        ORDER BY campaignId, sessionCount DESC
      )
      GROUP BY campaignId
      ORDER BY maxSessions DESC
      """
