package com.knmi.climatology.connect

import java.util
import java.util.Collections

import com.knmi.climatology.client.KnmiClimatologyMeasurement
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

object KnmiClimatologyConnectModel {
  final val keySchema: Schema = SchemaBuilder.struct().name("knmiclimatologymeasurementkey")
    .field("weerstationId", Schema.STRING_SCHEMA)
    .field("timestamp", Schema.INT64_SCHEMA)

  final val valueSchema: Schema = SchemaBuilder.struct().name("knmiclimatologymeasurement")
    .field("windrichting", Schema.OPTIONAL_INT32_SCHEMA)
    .field("windsnelheidLaatsteUur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("windsnelheidLaatste10Minuten", Schema.OPTIONAL_INT32_SCHEMA)
    .field("hoogsteWindstoot", Schema.OPTIONAL_INT32_SCHEMA)
    .field("temperatuur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("minimumTemperatuurLaatste6Uur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("dauwpuntsTemperatuur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("duurZonneschijnLaatsteUur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("globaleStraling", Schema.OPTIONAL_INT32_SCHEMA)
    .field("duurNeerslagLaatsteUur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("neerslagLaatsteUur", Schema.OPTIONAL_INT32_SCHEMA)
    .field("luchtdrukZeeniveau", Schema.OPTIONAL_INT32_SCHEMA)
    .field("horizontaalZicht", Schema.OPTIONAL_INT32_SCHEMA)
    .field("bewolking", Schema.OPTIONAL_INT32_SCHEMA)
    .field("relatieveVochtigheid", Schema.OPTIONAL_INT32_SCHEMA)
    .field("weercode", Schema.OPTIONAL_INT32_SCHEMA)
    .field("weercodeIndicator", Schema.OPTIONAL_INT32_SCHEMA)
    .field("isMistLaatsteUur", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("isRegenLaatsteUur", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("isSneeuwLaatsteUur", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("isOnweerLaatsteUur", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("isIjsvormingLaatsteUur", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .build()

  def toSourceRecord(measurement: KnmiClimatologyMeasurement, topic: String):  SourceRecord = {
    val keyStruct = new Struct(keySchema)
      .put("weerstationId", measurement.weerstationId)
      .put("timestamp", measurement.timestamp)

    val valueStruct = new Struct(valueSchema)
      .put("windrichting", measurement.windrichting.getOrElse(null))
      .put("windsnelheidLaatsteUur", measurement.windsnelheidLaatsteUur.getOrElse(null))
      .put("windsnelheidLaatste10Minuten", measurement.windsnelheidLaatste10Minuten.getOrElse(null))
      .put("hoogsteWindstoot", measurement.hoogsteWindstoot.getOrElse(null))
      .put("temperatuur", measurement.temperatuur.getOrElse(null))
      .put("minimumTemperatuurLaatste6Uur", measurement.minimumTemperatuurLaatste6Uur.getOrElse(null))
      .put("dauwpuntsTemperatuur", measurement.dauwpuntsTemperatuur.getOrElse(null))
      .put("duurZonneschijnLaatsteUur", measurement.duurZonneschijnLaatsteUur.getOrElse(null))
      .put("globaleStraling", measurement.globaleStraling.getOrElse(null))
      .put("duurNeerslagLaatsteUur", measurement.duurNeerslagLaatsteUur.getOrElse(null))
      .put("neerslagLaatsteUur", measurement.neerslagLaatsteUur.getOrElse(null))
      .put("luchtdrukZeeniveau", measurement.luchtdrukZeeniveau.getOrElse(null))
      .put("horizontaalZicht", measurement.horizontaalZicht.getOrElse(null))
      .put("bewolking", measurement.bewolking.getOrElse(null))
      .put("relatieveVochtigheid", measurement.relatieveVochtigheid.getOrElse(null))
      .put("weercode", measurement.weercode.getOrElse(null))
      .put("weercodeIndicator", measurement.weercodeIndicator.getOrElse(null))
      .put("isMistLaatsteUur", measurement.isMistLaatsteUur.getOrElse(null))
      .put("isRegenLaatsteUur", measurement.isRegenLaatsteUur.getOrElse(null))
      .put("isSneeuwLaatsteUur", measurement.isSneeuwLaatsteUur.getOrElse(null))
      .put("isOnweerLaatsteUur", measurement.isOnweerLaatsteUur.getOrElse(null))
      .put("isIjsvormingLaatsteUur", measurement.isIjsvormingLaatsteUur.getOrElse(null))

    new SourceRecord(
      weatherStationSourcePartitionKey(measurement.weerstationId),
      offset(measurement),
      topic,
      keySchema,
      keyStruct,
      valueSchema,
      valueStruct
    )
  }

  final val WEATHERSTATION_SOURCE_PARTITION_KEY = "stn"
  final val OFFSET_KEY = "tsp"

  def weatherStationSourcePartitionKey(weatherStationId: String): util.Map[String, String] = {
    Collections.singletonMap(WEATHERSTATION_SOURCE_PARTITION_KEY, weatherStationId)
  }

  def offset(measurement: KnmiClimatologyMeasurement): util.Map[String, Long] = {
    Collections.singletonMap(OFFSET_KEY, measurement.timestamp)
  }
}
