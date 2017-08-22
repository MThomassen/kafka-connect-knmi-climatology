package com.knmi.climatology.client

import java.time._

import com.knmi.climatology.WeatherStation
import org.scalatest.{Matchers, WordSpecLike}

class KnmiClimatologyHourDataCommandSuite extends WordSpecLike with Matchers {
  "KNMI ClimatologyHourDataCommand" should {

    val weatherStation: WeatherStation = WeatherStation("310")

    "Range subset of one day" in {
      val fromTsp = OffsetDateTime.of(2017,8,1,14,30,0,0,ZoneOffset.ofHours(2))
      val toTsp = OffsetDateTime.of(2017,8,1,18,30,0,0,ZoneOffset.ofHours(2))

      val commands = KnmiClimatologyHourDataCommand.forRange(weatherStation, fromTsp, toTsp)

      assert(commands.toSet == Set(
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,1), 15, 19)
      ))
    }

    "Range of one day" in {
      val fromTsp = OffsetDateTime.of(2017,8,1,0,0,0,0,ZoneOffset.ofHours(2))
      val toTsp = OffsetDateTime.of(2017,8,2,0,0,0,0,ZoneOffset.ofHours(2))

      val commands = KnmiClimatologyHourDataCommand.forRange(weatherStation, fromTsp, toTsp)

      assert(commands.toSet === Set(
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,1), 1, 24)
      ))
    }

    "Range of three whole days" in {
      val fromTsp = OffsetDateTime.of(2017,8,1,0,0,0,0,ZoneOffset.ofHours(2))
      val toTsp = OffsetDateTime.of(2017,8,4,0,0,0,0,ZoneOffset.ofHours(2))

      val commands = KnmiClimatologyHourDataCommand.forRange(weatherStation, fromTsp, toTsp)

      assert(commands.toSet === Set(
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,1), 1, 24),
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,2), 1, 24),
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,3), 1, 24)
      ))
    }

    "Range of three non-whole days" in {
      val fromTsp = OffsetDateTime.of(2017,8,1,14,0,0,0,ZoneOffset.ofHours(2))
      val toTsp = OffsetDateTime.of(2017,8,4,16,0,0,0,ZoneOffset.ofHours(2))

      val commands = KnmiClimatologyHourDataCommand.forRange(weatherStation, fromTsp, toTsp)

      assert(commands.toSet === Set(
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,1), 15, 24),
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,2), 1, 24),
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,3), 1, 24),
        KnmiClimatologyHourDataCommand(weatherStation, LocalDate.of(2017,8,4), 1, 17)
      ))
    }
  }
}
