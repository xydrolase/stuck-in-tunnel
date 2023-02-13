package stunnel

import cats.effect.IOApp
import cats.effect.IO
import org.http4s.ember.client.EmberClientBuilder

import stunnel.njtransit.api.*

object Application extends IOApp.Simple {
  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val emberClient = EmberClientBuilder.default[IO].build
    val clock = new Clock {
      override def setOffset(offset: Long): Unit = ()
      override def currentTimeMillis: Long = System.currentTimeMillis()
    }

    emberClient.use { client =>
      val apiClient = new Http4sMyBusNowApiClient[IO](client, clock, keyProvider)
      apiClient.getPatterns("126").flatMap { patterns =>
        IO.println(patterns)
      }
    }.void
  }
}
