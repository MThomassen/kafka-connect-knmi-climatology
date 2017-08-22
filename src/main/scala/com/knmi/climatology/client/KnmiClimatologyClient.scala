package com.knmi.climatology.client

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait KnmiClimatologyClient {
  def timeout: Duration
  def handle(command: KnmiClimatologyHourDataCommand): Future[Seq[KnmiClimatologyMeasurement]]
}