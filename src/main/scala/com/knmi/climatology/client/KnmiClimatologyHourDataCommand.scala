package com.knmi.climatology.client

import java.time.{LocalDate, OffsetDateTime}

import com.knmi.climatology.WeatherStation

case class KnmiClimatologyHourDataCommand(
  weatherStation: WeatherStation,
  date: LocalDate,
  fromHour: Int,
  toHour: Int) {
  require(fromHour > 0 && fromHour <= 24)
  require(toHour > 0 && fromHour <= 24)

  def formattedStart: String = s"${KnmiDateTimeFormatter.dateFormatter.format(date)}${f"$fromHour%02d"}"
  def formattedEnd: String = s"${KnmiDateTimeFormatter.dateFormatter.format(date)}${f"$toHour%02d"}"
}

object KnmiClimatologyHourDataCommand {
  def forRange(weatherStation: WeatherStation, fromTsp: OffsetDateTime, toTsp: OffsetDateTime): Seq[KnmiClimatologyHourDataCommand] = {
    val fromDateTime = fromTsp.atZoneSameInstant(KnmiDateTimeFormatter.zone)
    val toDateTime = toTsp.atZoneSameInstant(KnmiDateTimeFormatter.zone)

    val days = Stream
      .iterate(fromDateTime.toLocalDate)(day => day.plusDays(1))
      .takeWhile(date => date.atStartOfDay().isBefore(toDateTime.toLocalDateTime))

    def knmiEndOfIntervalHour(hour: Int): Int = hour match {
      case 0 => 24
      case x => x + 1
    }

    days match {
      case day #:: Stream.Empty =>
        KnmiClimatologyHourDataCommand(weatherStation, day, fromDateTime.getHour + 1, knmiEndOfIntervalHour(toDateTime.getHour)) #::
          Stream.Empty
      case firstDay +: rest :+ lastDay =>
        KnmiClimatologyHourDataCommand(weatherStation, firstDay, fromDateTime.getHour + 1, 24) #::                      // First part
          KnmiClimatologyHourDataCommand(weatherStation, lastDay, 1, knmiEndOfIntervalHour(toDateTime.getHour)) #::     // Last part
          rest.map(day => KnmiClimatologyHourDataCommand(weatherStation, day, 1, 24))                                   // middle parts
    }
  }
}