package com.knmi.climatology.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Framing, Sink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration._

class AkkaKnmiClimatologyClient(implicit system: ActorSystem) extends KnmiClimatologyClient {

  import system.dispatcher

  final private implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))
  final private val http = Http(system)
  final private val knmiDataUri = Uri("http://projects.knmi.nl/klimatologie/uurgegevens/getdata_uur.cgi")

  val timeout: Duration = 300 seconds

  def handle(command: KnmiClimatologyHourDataCommand): Future[Seq[KnmiClimatologyMeasurement]] = {
    val start = command.formattedStart
    val end = command.formattedEnd

    val formEntity = FormData(Map(
      "start" -> start,
      "end" -> end,
      "stns" -> command.weatherStation.id,
      "vars" -> "ALL"
    )).toEntity

    val request = HttpRequest(
      HttpMethods.POST,
      uri = knmiDataUri,
      entity = formEntity
    )

    http.singleRequest(request)
      .flatMap{
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entity.dataBytes
            .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 512))
            .map(KnmiKlimatologieMeasurementFactory.createEntity)
            .collect{ case Some(e) => e }
            .runWith(Sink.seq)
        case HttpResponse(statusCode,_,entity,_) =>
          entity.discardBytes()
          throw new Exception(s"KNMI GetHourData Faulted; HTTP$statusCode")
      }
  }
}
