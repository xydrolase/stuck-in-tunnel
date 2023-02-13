package stunnel.njtransit.api

import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.effect.Concurrent
import org.http4s.EntityDecoder
import org.http4s.Method.GET
import org.http4s.client.Client
import org.http4s.Header
import org.http4s.Uri
import org.http4s.client.dsl.Http4sClientDsl

import stunnel.njtransit.*
import org.http4s.Header.ToRaw

object MyBusNowApiClient {
  val ApiV3Root = Uri.unsafeFromString("https://mybusnow.njtransit.com/bustime/api/v3/")
  val RequestIdHeader = "X-Request-Id"

  // all headers required by the MyBusNow API
  val mandatoryHeaders = List(
    Header("Sec-Fetch-Mode", "cors"),
    Header("Sec-Fetch-Site", "same-origin"),
    Header("Sec-Fetch-Dest", "empty")
  )
}

trait MyBusNowApiClient[F[_]] {
  // def getRoutes: F[Seq[Route]]
  // def getVehicles(route: String): F[Seq[VehicleLocation]]
  def getPatterns(route: String): F[Seq[Pattern]]
}

class Http4sMyBusNowApiClient[F[_]: Concurrent](client: Client[F], clock: Clock, keyProvider: KeyProvider[F]) extends MyBusNowApiClient[F] {
  val dsl = new Http4sClientDsl[F] {}
  import dsl.*
  
  /**
    * Provides a base implementation for all HTTP GET requests to the MyBusNow API, which
    * injects all required query parameters (e.g. `key` and `xtime`) plus the headers.
    *
    * @param uri
    * @return
    */
  protected def httpGet[A](uri: org.http4s.Uri)(using EntityDecoder[F, A]): F[A] = {
    val xTime = clock.currentTimeMillis
    val xDate = KeyProvider.formatTimestamp(xTime)

    for {
      apiKey <- keyProvider.getKey

      uriWithKeyAndTime = uri
        .withQueryParam("key", apiKey)
        .withQueryParam("xtime", xTime.toString)

      // the request must be signed using the URI and request time.
      requestId <- keyProvider.sign(uriWithKeyAndTime, xTime)
      headers = (List(
          Header("X-Request-Id", requestId),
          Header("X-Date", xDate),
        ) ::: MyBusNowApiClient.mandatoryHeaders).map(ToRaw.rawToRaw)
      request = GET.apply(
        uriWithKeyAndTime, 
        headers: _*
      )
      response <- client.expect[A](request)
    } yield response
  }

  override def getPatterns(route: String): F[Seq[Pattern]] = {
    val uri = List(
      ("rt", route),
      ("format", "json"),
      ("locale", "en"),
      ("requestType", "getpatterns"),
      ("rtpidatafeed", "bustime")
    ).foldLeft(MyBusNowApiClient.ApiV3Root / "getpatterns") { case (uri, kv) => uri +? kv }

    httpGet(uri)
  }
}
