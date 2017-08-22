package com.knmi.climatology.client

import java.time._

import org.scalatest.{Matchers, WordSpecLike}

class KnmiDateTimeFormatterSuite extends WordSpecLike with Matchers {
  "KNMI DateTime Formatter" should {

    "format 2017-08-01T14:30:00Z" in {
      val tsp = OffsetDateTime.of(2017,8,1,14,30,0,0,ZoneOffset.UTC)
      val formattedTsp = KnmiDateTimeFormatter.format(tsp)

      assert(formattedTsp === "2017080116")
    }

    "format 2017-08-01T14:30:00+0200" in {
      val tsp = OffsetDateTime.of(2017,8,1,14,30,0,0,ZoneOffset.ofHours(2))
      val formattedTsp = KnmiDateTimeFormatter.format(tsp)

      assert(formattedTsp === "2017080114")
    }

    "format 2017-08-01T00:00:00Z" in {
      val tsp = OffsetDateTime.of(2017,8,1,0,0,0,0,ZoneOffset.UTC)
      val formattedTsp = KnmiDateTimeFormatter.format(tsp)

      assert(formattedTsp === "2017080102")
    }

    "format 2017-08-01T00:00:00+0200" in {
      val tsp = OffsetDateTime.of(2017,8,1,0,0,0,0,ZoneOffset.ofHours(2))
      val formattedTsp = KnmiDateTimeFormatter.format(tsp)

      assert(formattedTsp === "2017073124")
    }

    "parse 2017080116" in {
      val dateTime = KnmiDateTimeFormatter.parse("20170801","16")
      val expectedDateTime = ZonedDateTime.of(
        LocalDateTime.of(2017,8,1,16,0,0),
        ZoneId.of("Europe/Amsterdam")
      )

      assert(dateTime ===expectedDateTime.toEpochSecond)
    }

    "parse 2017080124" in {
      val dateTime = KnmiDateTimeFormatter.parse("20170801","24")
      val expectedDateTime = ZonedDateTime.of(
        LocalDateTime.of(2017,8,2,0,0,0),
        ZoneId.of("Europe/Amsterdam")
      )

      assert(dateTime === expectedDateTime.toEpochSecond)
    }
  }
}
