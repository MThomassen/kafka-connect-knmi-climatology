package com.knmi.climatology.connect

import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util

import akka.actor.ActorSystem
import com.knmi.climatology.WeatherStation
import com.knmi.climatology.client.{AkkaKnmiClimatologyClient, KnmiClimatologyClient, KnmiClimatologyHourDataCommand}
import com.knmi.climatology.connect.config.KnmiClimatologySourceConfig
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

class KnmiClimatologySourceTask extends SourceTask with StrictLogging {

  var taskConfig: Option[KnmiClimatologySourceConfig] = None

  implicit val system = ActorSystem("knmiclimatology")
  val client: KnmiClimatologyClient = new AkkaKnmiClimatologyClient()

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
    val sourceRecords = taskConfig match {
      case None => throw new ConnectException("Config not initialized")
      case Some(config) => config.weatherStations.par
        .flatMap(stn => pollWeatherStation(stn))
    }

    if (sourceRecords.isEmpty) {
      logger.info(s"Sleeping for ${taskConfig.get.maxPollingInterval.getSeconds} seconds..")

      Thread.sleep(taskConfig.get.maxPollingInterval.toMillis)

      util.Collections.emptyList()
    } else {
      sourceRecords
        .toList.asJava
    }
  }

  def pollWeatherStation(stn: WeatherStation): Seq[SourceRecord] = {
    val offset = context.offsetStorageReader
      .offset(KnmiClimatologyConnectModel.weatherStationSourcePartitionKey(stn.id))

    val startTsp: OffsetDateTime = if (offset == null) taskConfig.get.startTimestamp
      else OffsetDateTime.ofInstant(
        Instant.ofEpochSecond(offset.get(KnmiClimatologyConnectModel.OFFSET_KEY).asInstanceOf[Long]),
        ZoneId.systemDefault
      ).plusHours(1)

    if (startTsp.plus(taskConfig.get.maxDataInterval).isBefore(OffsetDateTime.now())) {
      import system.dispatcher

      val commands = KnmiClimatologyHourDataCommand.forRange(
        weatherStation = stn,
        fromTsp = startTsp,
        toTsp = startTsp.plus(taskConfig.get.maxDataInterval))

      val commandResponses = Future.sequence(commands
            .map(cmd => client.handle(cmd)
              .recover{
                  case ex => Seq()
              }
            ))

      Await.result(commandResponses, client.timeout)
        .flatten
        .map(KnmiClimatologyConnectModel.toSourceRecord(_, taskConfig.get.topicName))
    } else {
      Seq[SourceRecord]()
    }
  }
}
