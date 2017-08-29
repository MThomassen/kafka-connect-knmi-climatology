package com.knmi.climatology.connect

import java.time.{Instant, OffsetDateTime}
import java.util

import akka.actor.ActorSystem
import com.knmi.climatology.WeatherStation
import com.knmi.climatology.client._
import com.knmi.climatology.connect.config.KnmiClimatologySourceConfig
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await

class KnmiClimatologySourceTask extends SourceTask with StrictLogging {
  implicit val system = ActorSystem("knmiclimatology")
  val client: KnmiClimatologyClient = new AkkaKnmiClimatologyClient()

  /**
    * Task-local offset storage
    */
  val offsets: TrieMap[WeatherStation, Long] = TrieMap.empty

  var taskConfig: Option[KnmiClimatologySourceConfig] = None

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Task starting")
    taskConfig = Some(KnmiClimatologySourceConfig(props))
  }

  override def stop(): Unit = {
    logger.info("Task stopping")
    taskConfig = None
  }

  override def poll(): util.List[SourceRecord] = {
    val sourceMeasurements = taskConfig match {
      case None => throw new ConnectException("Config not initialized")
      case Some(config) => config.weatherStations.par
        .flatMap(stn => pollWeatherStation(stn))
    }

    sourceMeasurements match {
      case measurements if measurements.isEmpty =>
        logger.info(s"Sleeping for ${taskConfig.get.maxPollingInterval.getSeconds} seconds..")

        Thread.sleep(taskConfig.get.maxPollingInterval.toMillis)

        util.Collections.emptyList()

      case measurements =>
        logger.info(s"Committing ${sourceMeasurements.size} records")

        measurements
          .groupBy(_.weerstation)
          .mapValues(_.map(_.timestamp).max)
          .foreach{
            case (stn, offset) => offsets.put(stn, offset)
          }

        measurements
          .map(measurement => KnmiClimatologyConnectModel.toSourceRecord(measurement, taskConfig.get.topicName))
          .toList
          .asJava
    }
  }

  /**
    * Poll data for a single WeatherStation. This methods tries to pull data for the configured `maxdataintervalseconds`
    * since either;
    *  - Last known task-local measurement timestamp
    *  - Last known committed source offset for sourcePartition = weatherStation
    *  - configured `start.epochsecond`
    *  Note that when the `maxdataintervalseconds` spans more than one day, only part of this interval for a single day
    *  is requested.
    * @param stn WeatherStation
    * @return KNMI Climatology Measurements
    */
  def pollWeatherStation(stn: WeatherStation): Seq[KnmiClimatologyMeasurement] = {
    val startTsp: OffsetDateTime = offsets.lift(stn) match {
      case None =>
        val offset = context.offsetStorageReader
          .offset(KnmiClimatologyConnectModel.weatherStationSourcePartitionKey(stn.id))

        if (offset == null) taskConfig.get.startTimestamp
        else OffsetDateTime.ofInstant(
          Instant.ofEpochSecond(offset.get(KnmiClimatologyConnectModel.OFFSET_KEY).asInstanceOf[Long]),
          KnmiDateTimeFormatter.zone
        )

      case Some(epochSecond) =>
        OffsetDateTime.ofInstant(
          Instant.ofEpochSecond(epochSecond),
          KnmiDateTimeFormatter.zone
        )
    }

    if (startTsp.plus(taskConfig.get.maxDataInterval).isBefore(OffsetDateTime.now())) {
      import system.dispatcher

      val command = KnmiClimatologyHourDataCommand.forRange(
        weatherStation = stn,
        fromTsp = startTsp,
        toTsp = startTsp.plus(taskConfig.get.maxDataInterval)
      ).head

      val commandResponse = client.handle(command)
        .recover{
          case ex => Seq()
        }

      Await.result(commandResponse, client.timeout)
    }
    else {
      Seq[KnmiClimatologyMeasurement]()
    }
  }
}
