package stunnel

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import stunnel.njtransit.*
import java.time.LocalDateTime

class OpsSpec extends AnyWordSpec with Matchers {
    "an Ops" when {
        "tracking vehicle movements" should {
            "return a stream of vehicle movements" in {
                val vehicles = Stream(
                    VehicleLocation(1440, LocalDateTime.now(), 34f, -73f, 123, "126", 217, 8,
                        "NEW YORK", false, "light", 133579, LocalDateTime.now()
                    ),
                    VehicleLocation(1440, LocalDateTime.now(), 34.51f, -72.8f, 123, "126", 217, 8,
                        "NEW YORK", false, "light", 133579, LocalDateTime.now()
                    ),
                    VehicleLocation(1733, LocalDateTime.now(), 34.11f, -73.8f, 159, "159", 217, 8,
                        "NEW YORK", false, "light", 672579, LocalDateTime.now()
                    )
                ).covary[IO]

                val movements = vehicles.through(Ops.trackMovement(5)).compile.toList.unsafeRunSync() 
                movements.length shouldBe 1
                movements.head.currentLocation.tripId shouldBe 133579
            }
        }
    }
}