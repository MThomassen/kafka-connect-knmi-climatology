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

class KnmiKlimatologieSourceTask extends SourceTask with StrictLogging {

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
      case Some(config) => config.weatherStations
          .flatMap(stn => pollWeatherStation(stn))
    }

    if (sourceRecords.isEmpty) {
      logger.info(s"Sleeping for ${taskConfig.get.interval.getSeconds} seconds..")

      Thread.sleep(taskConfig.get.interval.toMillis)

      util.Collections.emptyList()
    } else {
      sourceRecords
        .toList.asJava
    }
  }

  def pollWeatherStation(stn: WeatherStation): Seq[SourceRecord] = {
    val startTsp: OffsetDateTime = {
      val offset = context.offsetStorageReader
        .offset(KnmiClimatologyConnectModel.weatherStationPartitionKey(stn.id))

      if (offset == null) taskConfig.get.startTimestamp
      else OffsetDateTime.ofInstant(Instant.ofEpochSecond(
        offset.get(KnmiClimatologyConnectModel.OFFSET_KEY).asInstanceOf[Long]),
        ZoneId.systemDefault)
    }

    if (startTsp.plus(taskConfig.get.interval).isBefore(OffsetDateTime.now())) {
      import system.dispatcher

      val apiResults = Future.sequence(
        KnmiClimatologyHourDataCommand.forRange(
          weatherStation = stn,
          fromTsp = startTsp,
          toTsp = startTsp.plus(taskConfig.get.interval))
        .map(cmd => {logger.info(s"Pulling KNMI Climatology data [${cmd.weatherStation.id} - ${cmd.date} - ${cmd.fromHour}:${cmd.toHour}]"); cmd})
        .map(client.handle)
      )

      val records = Await.result(apiResults,  client.timeout)
        .flatten
        .map(m => KnmiClimatologyConnectModel.toSourceRecord(m, taskConfig.get.topicName))

      logger.info(s"Committing ${records.size} records [${stn.id}]")

      records
    } else {
      Seq[SourceRecord]()
    }
  }
}
