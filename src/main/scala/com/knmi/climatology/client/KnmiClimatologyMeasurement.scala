package com.knmi.climatology.client

import java.time._
import java.time.format.DateTimeFormatter

import akka.util.ByteString
import com.knmi.climatology.WeatherStation

case class KnmiClimatologyMeasurement(
  weerstation: WeatherStation,
  timestamp: Long,
  windrichting: Option[Int],
  windsnelheidLaatsteUur: Option[Int],
  windsnelheidLaatste10Minuten: Option[Int],
  hoogsteWindstoot: Option[Int],
  temperatuur: Option[Int],
  minimumTemperatuurLaatste6Uur: Option[Int],
  dauwpuntsTemperatuur: Option[Int],
  duurZonneschijnLaatsteUur: Option[Int],
  globaleStraling: Option[Int],
  duurNeerslagLaatsteUur: Option[Int],
  neerslagLaatsteUur: Option[Int],
  luchtdrukZeeniveau: Option[Int],
  horizontaalZicht: Option[Int],
  bewolking: Option[Int],
  relatieveVochtigheid: Option[Int],
  weercode: Option[Int],
  weercodeIndicator: Option[Int],
  isMistLaatsteUur: Option[Boolean],
  isRegenLaatsteUur: Option[Boolean],
  isSneeuwLaatsteUur: Option[Boolean],
  isOnweerLaatsteUur: Option[Boolean],
  isIjsvormingLaatsteUur: Option[Boolean]) {
}

object KnmiKlimatologieMeasurementFactory {
  def createEntity(byteString: ByteString) : Option[KnmiClimatologyMeasurement] = {
    Some(byteString)
      .map(_.utf8String)
      .filterNot(isHeaderLine)
      .map(splitFields)
      .filter(isNrOfFieldsCorrect)
      .map(parseIntFields)
      .filter(isMandatoryFieldsDefined)
      .filter(isBooleanFields)
      .map(allocateFields)
  }

  private def isHeaderLine(line: String): Boolean = {
    line.startsWith("#")
  }

  private def splitFields(line: String): Array[String] = {
    //Note we set limit to 26, so that íf an extra field is present, it will be in the resulting array, but if the last field is empty "1,0,", it will also be in the array
    line.split(",",26)
      .map(field => field.trim)
  }

  private def isNrOfFieldsCorrect(fields: Array[String]): Boolean = {
    fields.length == 25
  }

  private def parseIntFields(fields: Array[String]): Array[Option[Int]] = {
    fields.map{
      case x if x == null || x.isEmpty => None
      case x => Some(x.toInt)
    }
  }

  private def isMandatoryFieldsDefined(fields: Array[Option[Int]]): Boolean = {
    fields.isDefinedAt(0) && fields.isDefinedAt(1)&& fields.isDefinedAt(2)
  }

  private def isBooleanFields(fields: Array[Option[Int]]): Boolean = {
    def isBooleanField(field: Option[Int]): Boolean = {
      field.isEmpty || field.get == 1 || field.get == 0
    }

    isBooleanField(fields(20)) &&
      isBooleanField(fields(21)) &&
      isBooleanField(fields(22)) &&
      isBooleanField(fields(23)) &&
      isBooleanField(fields(24))
  }

  private def parseDateTime(dateStr: String, hour: Int): LocalDateTime = {
    val localDate = LocalDate.parse(dateStr, KnmiDateTimeFormatter.dateFormatter)
    if (hour == 24) localDate.plusDays(1).atStartOfDay
    else {
      val localTime = LocalTime.of(hour, 0)
      LocalDateTime.of(localDate, localTime)
    }
  }

  private def allocateFields(fields: Array[Option[Int]]): KnmiClimatologyMeasurement = {
    def toBoolean(field: Option[Int]): Option[Boolean] = {
      if (field.isEmpty) None
      else if (field.get == 1) Some(true)
      else Some(false)
    }
    //391|20161231|1|220|10|10|20|-36||-39|0|0|0|0||||97||6|||||
    val weerstationId = fields(0).get.toString
    val date = KnmiDateTimeFormatter.parse(fields(1).get.toString, fields(2).get.toString)
    val windrichting = fields(3)
    val windsnelheidLaatsteUur = fields(4)
    val windsnelheidLaatste10Minuten = fields(5)
    val hoogsteWindstoot = fields(6)
    val temperatuur = fields(7)
    val minimumTemperatuurLaatste6Uur = fields(8)
    val dauwpuntsTemperatuur = fields(9)
    val duurZonneschijnLaatsteUur = fields(10)
    val globaleStraling = fields(11)
    val duurNeerslagLaatsteUur = fields(12)
    val neerslagLaatsteUur = fields(13)
    val luchtdrukZeeniveau = fields(14)
    val horizontaalZicht = fields(15)
    val bewolking = fields(16)
    val relatieveVochtigheid = fields(17)
    val weercode = fields(18)
    val weercodeIndicator = fields(19)
    val isMistLaatsteUur = toBoolean(fields(20))
    val isRegenLaatsteUur = toBoolean(fields(21))
    val isSneeuwLaatsteUur = toBoolean(fields(22))
    val isOnweerLaatsteUur = toBoolean(fields(23))
    val isIjsvormingLaatsteUur = toBoolean(fields(24))

    KnmiClimatologyMeasurement(WeatherStation(weerstationId), date, windrichting, windsnelheidLaatsteUur, windsnelheidLaatste10Minuten, hoogsteWindstoot, temperatuur, minimumTemperatuurLaatste6Uur, dauwpuntsTemperatuur, duurZonneschijnLaatsteUur, globaleStraling, duurNeerslagLaatsteUur, neerslagLaatsteUur, luchtdrukZeeniveau, horizontaalZicht, bewolking, relatieveVochtigheid, weercode, weercodeIndicator, isMistLaatsteUur, isRegenLaatsteUur, isSneeuwLaatsteUur, isOnweerLaatsteUur, isIjsvormingLaatsteUur)
  }
}

object KnmiDateTimeFormatter {
  case class KnmiDataCommand(
                              weatherStation: WeatherStation,
                              from: String,
                              to: String) {
    require(from.matches("[0-9]{10}"))
    require(to.matches("[0-9]{10}"))
  }

  val zone: ZoneId = ZoneId.of("Europe/Amsterdam")
  val dateFormatter: DateTimeFormatter = DateTimeFormatter
    .ofPattern("yyyyMMdd")

  def parse(date: String, hour: String): Long = {
    require(date.matches("[0-9]{8}"), "Specify 'date' as 'yyyyMMdd'")
    require(hour.matches("[0-9]{1,2}"), "Specify 'hour' as 'HH'")

    val zonedDate: ZonedDateTime = ZonedDateTime.of(
      LocalDate.parse(date, dateFormatter).atStartOfDay(),
      zone
    )

    hour.toInt match {
      case 24 => zonedDate.plusDays(1).toEpochSecond
      case x => zonedDate.plusHours(x).toEpochSecond
    }
  }

  def format(dateTime: OffsetDateTime): String = {
    val zonedDateTime: ZonedDateTime = dateTime.atZoneSameInstant(zone)
    zonedDateTime.getHour match {
      case 0 => s"${dateFormatter.format(zonedDateTime.minusDays(1))}24"
      case h => s"${dateFormatter.format(zonedDateTime)}${f"$h%02d"}"
    }
  }

  def format(dateTime: LocalDateTime): String = {
    dateTime.getHour match {
      case 0 => s"${dateFormatter.format(dateTime.minusDays(1))}24"
      case h => s"${dateFormatter.format(dateTime)}${f"$h%02d"}"
    }
  }
}