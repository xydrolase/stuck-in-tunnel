package stunnel

package config 

import fs2.aws.s3.models.Models.BucketName
import software.amazon.awssdk.regions.Region
import io.circe.Decoder
import io.circe.generic.semiauto.*
import eu.timepit.refined.types.string.NonEmptyString

case class EncodingConfig(batchSize: Int, fileRecordsLimit: Int)
object EncodingConfig {
  given Decoder[EncodingConfig] = deriveDecoder
}

/**
 * Configuration for bus location tracking
 */
case class TrackingConfig(routes: List[String], maxTrips: Int)
object TrackingConfig {
  given Decoder[TrackingConfig] = deriveDecoder
}

case class PersistenceConfig(encoding: EncodingConfig, bucketName: BucketName, region: Region)
object PersistenceConfig {
  given Decoder[BucketName] = Decoder.decodeString.emap { name =>
    NonEmptyString.from(name) match
      case Left(invalid) => Left(s"Invalid bucket name '$invalid'")
      case Right(value) => Right(BucketName(value))
  }

  given Decoder[Region] = Decoder.decodeString.map { region => Region.of(region) }

  given Decoder[PersistenceConfig] = deriveDecoder
}

case class AppConfig(tracking: TrackingConfig, persistence: Map[String, PersistenceConfig])
object AppConfig {
  given Decoder[AppConfig] = deriveDecoder
}
