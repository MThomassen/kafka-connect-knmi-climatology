package com.knmi.climatology.connect

import java.util

import com.knmi.climatology.connect.config.KnmiClimatologySourceConfig
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.util.{Failure, Success, Try}

class KnmiClimatologySourceConnector extends SourceConnector with StrictLogging {

  var sourceConfig: Option[KnmiClimatologySourceConfig] = None

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config(): ConfigDef = KnmiClimatologySourceConfig.definition

  override def taskClass(): Class[_ <: Task] = classOf[KnmiClimatologySourceTask]

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Connector starting")
    sourceConfig = Try(KnmiClimatologySourceConfig(props)) match {
      case Success(config) => Some(config)
      case Failure(ex) => throw new ConnectException("Couldn't start due to configuration error: " + ex.getMessage, ex)
    }
  }

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    sourceConfig match {
      case None => throw new ConnectException("Cannot provide taskConfigs without being initialised")
      case Some(config) => config.splitTasksConfig(maxTasks)
    }
  }

  override def stop(): Unit = {
    logger.info("Connector stopping")
    sourceConfig = None
  }

}
