package stunnel.njtransit.api

import cats.syntax.functor.*
import cats.Applicative
import org.http4s.Uri

import stunnel.util.HMACUtil

import java.time.format.DateTimeFormatter
import java.time.Instant
import java.time.ZoneOffset


object KeyProvider {
  // the date time format to use for the signature is RFC 1123 (e.g. "Mon, Jan 23 2023 11:05:30 GMT")
  val dtFormatter = DateTimeFormatter.RFC_1123_DATE_TIME

  def formatTimestamp(epochMilli: Long): String = {
    dtFormatter.format(Instant.ofEpochMilli(epochMilli).atZone(ZoneOffset.UTC))
  }
}

/**
  * Provide the API key and signature private key for the MyBusNow API.
  */
trait KeyProvider[F[_]: Applicative] {
  def getKey: F[String]
  def getSignaturePrivateKey: F[String]

  def sign(uri: Uri, timestamp: Long): F[String] = {
    // compute the HMAC SHA256 hash of the uri using the signature private key
    getSignaturePrivateKey.map { privKey =>
      val offset = uri.toString.indexOf("/api")
      val message = uri.toString.substring(offset) + KeyProvider.formatTimestamp(timestamp)
      HMACUtil.hmacSha256Hex(message, privKey)
    }
  }
}


case class StaticKeyProvider[F[_]: Applicative](key: String, privateKey: String) extends KeyProvider[F] {
  def getKey: F[String] = summon[Applicative[F]].pure(key)
  def getSignaturePrivateKey: F[String] = summon[Applicative[F]].pure(privateKey)
}
  

