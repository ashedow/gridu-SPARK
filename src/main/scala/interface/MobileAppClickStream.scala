package interface

import java.sql.Timestamp

case class MobAppClickstream (
    userId: String,
    eventId: String,
    eventTime: Timestamp,
    eventType: String,
    attributes: Option[Map[String, String]]
)