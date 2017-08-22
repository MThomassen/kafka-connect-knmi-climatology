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
    props.put(KnmiClimatologySourceConfig.START_NOW, "false")
    props.put(KnmiClimatologySourceConfig.START_EPOCHSECOND, "1483232400")
    props.put(KnmiClimatologySourceConfig.INTERVAL_SECONDS, "21600")

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

    "parse provided interval" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.interval.getSeconds === 21600)
    }

    "interpret start timestamp for 'start.now=false'" in {
      val config = KnmiClimatologySourceConfig(props)

      assert(config.startTimestamp.isEqual(Instant.ofEpochSecond(1483232400).atOffset(ZoneOffset.UTC)))
    }

    "interpret start timestamp for 'start.now=true'" in {
      val newProps = new java.util.HashMap[String, String](props)
      newProps.put(KnmiClimatologySourceConfig.START_NOW, "true")

      val config = KnmiClimatologySourceConfig(newProps)

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
    props.put(KnmiClimatologySourceConfig.START_NOW, "false")
    props.put(KnmiClimatologySourceConfig.START_EPOCHSECOND, "1483232400")

    val config = KnmiClimatologySourceConfig(props)

    "split all weather stations over 3 tasks" in {
      val taskConfigs = config.splitTasksConfig(3)
        .asScala
        .map(splitProps => KnmiClimatologySourceConfig(splitProps))

      assert(taskConfigs.size === 3)
    }
  }

}
