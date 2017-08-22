package com.knmi.climatology.connect.config

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util

import com.knmi.climatology.WeatherStation
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import scala.collection.JavaConverters._

case class KnmiClimatologySourceConfig(props: java.util.Map[String, String])
  extends AbstractConfig(KnmiClimatologySourceConfig.definition, props) {
  import KnmiClimatologySourceConfig._

  val topicName: String = getString(TOPIC_NAME)

  val weatherStations: Set[WeatherStation] = getList(WEATHERSTATIONS).asScala.map(id => WeatherStation(id)).toSet

  val startTimestamp: OffsetDateTime = {
    if (getBoolean(START_NOW)) OffsetDateTime.now(ZoneOffset.UTC)
    else Instant.ofEpochSecond(getLong(START_EPOCHSECOND)).atOffset(ZoneOffset.UTC)
  }

  val interval: Duration = Duration.ofSeconds(getLong(INTERVAL_SECONDS))

  def splitTasksConfig(maxTasks: Int): util.List[util.Map[String, String]] = {
    weatherStations
      .zipWithIndex
      .groupBy(kv => kv._2 % maxTasks)
      .map(stns => {
        val map = new util.HashMap[String, String](props)
        map.put(WEATHERSTATIONS, stns._2.map(_._1.id).mkString(","))
        map.asInstanceOf[util.Map[String, String]]
      })
      .toList
      .asJava
  }

  assert(interval.toHours >= 1, "Interval should be at least 1 hour")
}

object KnmiClimatologySourceConfig {
  final val TOPIC_NAME = "topic.name"
  final val WEATHERSTATIONS = "connect.knmi.climatology.weatherstations"
  final val START_NOW = "connect.knmi.climatology.start.now"
  final val START_EPOCHSECOND = "connect.knmi.climatology.start.epochsecond"
  final val INTERVAL_SECONDS = "connect.knmi.climatology.intervalseconds"

  val definition: ConfigDef = new ConfigDef()
    .define(TOPIC_NAME, Type.STRING, "knmi-climatology", Importance.HIGH, "Name of the Kafka topic to write to")
    .define(WEATHERSTATIONS, Type.LIST, Importance.HIGH, "Weather Station ID's, see: http://projects.knmi.nl/klimatologie/metadata/index.html")
    .define(START_NOW, Type.BOOLEAN, true, Importance.MEDIUM, "Start pulling data from current time onwards")
    .define(START_EPOCHSECOND, Type.LONG, 0, Importance.MEDIUM, "Start pulling data of timestamp")
    .define(INTERVAL_SECONDS, Type.LONG, 21600, Importance.HIGH, "Data gathering interval in seconds")
}