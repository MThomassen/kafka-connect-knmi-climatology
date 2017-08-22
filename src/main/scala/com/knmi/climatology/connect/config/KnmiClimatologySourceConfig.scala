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

  val startTimestamp: OffsetDateTime = Instant.ofEpochSecond(getLong(START_EPOCHSECOND)).atOffset(ZoneOffset.UTC)

  val maxPollingInterval: Duration = Duration.ofSeconds(getLong(MAX_POLLING_INTERVAL_SECONDS))

  val maxDataInterval: Duration = Duration.ofSeconds(getLong(MAX_DATA_INTERVAL_SECONDS))

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

  assert(maxPollingInterval.toHours >= 3, "Interval should be at least 3 hour (don't stress the KNMI API)")
}

object KnmiClimatologySourceConfig {
  final val TOPIC_NAME = "topic.name"
  final val WEATHERSTATIONS = "connect.knmi.climatology.weatherstations"
  final val START_EPOCHSECOND = "connect.knmi.climatology.start.epochsecond"
  final val MAX_POLLING_INTERVAL_SECONDS = "connect.knmi.climatology.maxpollingintervalseconds"
  final val MAX_DATA_INTERVAL_SECONDS = "connect.knmi.climatology.maxdataintervalseconds"


  val definition: ConfigDef = new ConfigDef()
    .define(TOPIC_NAME, Type.STRING, "knmi-climatology", Importance.HIGH, "Name of the Kafka topic to write to")
    .define(WEATHERSTATIONS, Type.LIST, Importance.HIGH, "Weather Station ID's, see: http://projects.knmi.nl/klimatologie/metadata/index.html")
    .define(START_EPOCHSECOND, Type.LONG, OffsetDateTime.now().toEpochSecond, Importance.MEDIUM, "Start pulling data since timestamp (now if empty)")
    .define(MAX_POLLING_INTERVAL_SECONDS, Type.LONG, 21600, Importance.HIGH, "Max data polling interval in seconds")
    .define(MAX_DATA_INTERVAL_SECONDS, Type.LONG, 86400, Importance.HIGH, "Max data interval in seconds")
}