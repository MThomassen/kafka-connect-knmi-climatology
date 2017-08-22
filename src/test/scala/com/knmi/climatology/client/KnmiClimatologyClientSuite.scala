package com.knmi.climatology.client

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}

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
      val from = OffsetDateTime.of(2017,1,1,3,0,0,0,ZoneOffset.UTC)
      val to = OffsetDateTime.of(2017,1,2,0,0,0,0,ZoneOffset.UTC)

      val command = KnmiClimatologyHourDataCommand(
        weatherStation = WeatherStation("310"),
        date = LocalDate.of(2017,1,1),
        fromHour = 1,
        toHour = 24
      )

      val measurements = Await.result(client.handle(command), client.timeout)

      assert(measurements.size === 24)
    }
  }
}
