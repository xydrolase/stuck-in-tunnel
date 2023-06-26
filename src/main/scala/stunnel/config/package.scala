package stunnel

package config 

import fs2.aws.s3.models.Models.BucketName
import software.amazon.awssdk.regions.Region
import io.circe.Decoder
import eu.timepit.refined.types.string.NonEmptyString

case class EncodingConfig(batchSize: Int, fileRecordsLimit: Int)

/**
 * Configuration for bus location tracking
 */
case class TrackingConfig(routes: List[String], maxTrips: Int)

case class PersistenceConfig(bucketName: BucketName, region: Region)
object PersistenceConfig {
  given Decoder[PersistenceConfig] = Decoder.forProduct2("bucket-name", "region") { 
    (bucket: String, region: String) => PersistenceConfig(
      BucketName(NonEmptyString.from(bucket)
        .fold(invalid => throw new IllegalArgumentException(s"Invalid bucket name '$invalid'"), identity)),
      Region.of(region)
    )
  }
}

case class AppConfig(encoding: EncodingConfig, tracking: TrackingConfig, persist: PersistenceConfig)
