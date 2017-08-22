package com.knmi.climatology.connect.config

import java.time.{Instant, OffsetDateTime, ZoneOffset}

import com.knmi.climatology.WeatherStation
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.JavaConverters._

class KnmiKlimatologieSourceConfigSuite extends WordSpecLike with Matchers {

  "KnmiKlimatologieSourceConfig parsing of configfile" should {

    val props = new java.util.HashMap[String, String]()
    props.put(KnmiClimatologySourceConfig.TOPIC_NAME, "knmi")
    props.put(KnmiClimatologySourceConfig.WEATHERSTATIONS, "1,2,3,4,5")
    props.put(KnmiClimatologySourceConfig.START_EPOCHSECOND, "1483232400")
    props.put(KnmiClimatologySourceConfig.MAX_POLLING_INTERVAL_SECONDS, "21600")
    props.put(KnmiClimatologySourceConfig.MAX_DATA_INTERVAL_SECONDS, "86400")

    "parse provided weather station id's" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.weatherStations === Set(
        WeatherStation("1"),
        WeatherStation("2"),
        WeatherStation("3"),
        WeatherStation("4"),
        WeatherStation("5")
      ))
    }

    "parse provided topic name" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.topicName === "knmi")
    }

    "parse provided polling interval" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.maxPollingInterval.getSeconds === 21600)
    }

    "parse provided data interval" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.maxDataInterval.getSeconds === 86400)
    }

    "parse start timestamp" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.startTimestamp.isEqual(Instant.ofEpochSecond(1483232400).atOffset(ZoneOffset.UTC)))
    }

    "interpret optional configs" in {
      val emptyProps = new java.util.HashMap[String, String]()
      emptyProps.put(KnmiClimatologySourceConfig.TOPIC_NAME, "knmi")
      emptyProps.put(KnmiClimatologySourceConfig.WEATHERSTATIONS, "1,2,3,4,5")

      val config = KnmiClimatologySourceConfig(emptyProps)

      val diff = java.time.Duration.between(
        config.startTimestamp,
        OffsetDateTime.now()
      )

      assert(diff.getSeconds < 3)
    }
  }

  "KnmiKlimatologieSourceConfig splitting taskConfigs" should {

    val props = new java.util.HashMap[String, String]()
    props.put(KnmiClimatologySourceConfig.TOPIC_NAME, "knmi")
    props.put(KnmiClimatologySourceConfig.WEATHERSTATIONS, "1,2,3,4,5")

    val config = KnmiClimatologySourceConfig(props)

    "split all weather stations over 3 tasks" in {
      val taskConfigs = config.splitTasksConfig(3)
        .asScala
        .map(splitProps => KnmiClimatologySourceConfig(splitProps))

      assert(taskConfigs.size === 3)
    }
  }

}
