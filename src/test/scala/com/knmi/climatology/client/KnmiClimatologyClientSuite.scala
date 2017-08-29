package com.knmi.climatology.client

import java.time._

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.knmi.climatology.WeatherStation
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await

class KnmiClimatologyClientSuite extends TestKit(ActorSystem("KnmiClimatologyClientSuite"))
  with WordSpecLike with Matchers with BeforeAndAfterAll  {

  val weatherStation: WeatherStation = WeatherStation("310")

  "A KnmiClimatologyClient" should {

    val client: KnmiClimatologyClient = new AkkaKnmiClimatologyClient()

    "handle a request" in {
      val command = KnmiClimatologyHourDataCommand(
        weatherStation = WeatherStation("310"),
        date = LocalDate.of(2017,1,1),
        fromHour = 1,
        toHour = 24
      )

      val measurements = Await.result(client.handle(command), client.timeout)

      assert(measurements.size === 24)

      val maxReceivedTimestamp = measurements.map(_.timestamp).max

      assert(maxReceivedTimestamp === LocalDate.of(2017,1,2).atStartOfDay(KnmiDateTimeFormatter.zone).toEpochSecond)

      val nextFrom = OffsetDateTime.ofInstant(Instant.ofEpochSecond(maxReceivedTimestamp), KnmiDateTimeFormatter.zone)
      val nextTo = OffsetDateTime.ofInstant(Instant.ofEpochSecond(maxReceivedTimestamp), KnmiDateTimeFormatter.zone)
        .plusDays(1)

      val nextCommands = KnmiClimatologyHourDataCommand.forRange(weatherStation, nextFrom, nextTo)

      assert(nextCommands.toSet === Set(KnmiClimatologyHourDataCommand(weatherStation,LocalDate.of(2017,1,2),1,24)))
    }
  }
}
