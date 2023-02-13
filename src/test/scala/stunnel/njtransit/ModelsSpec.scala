package stunnel.njtransit

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class ModelsSpec extends AnyWordSpec with Matchers {
  "the Pattern data model" should {
    "be decodeable from API response" in {
      val response = """
      {
	"bustime-response": {
		"ptr": [
			{
				"pid": 1368,
				"ln": 23422.0,
				"rtdir": "New York",
				"pt": [
					{
						"seq": 1,
						"lat": 40.735589999998716,
						"lon": -74.0291100000008,
						"typ": "S",
						"stpid": "20496",
						"stpnm": "HOBOKEN TERMINAL",
						"pdist": 0.0
					},
					{
						"seq": 2,
						"lat": 40.7355,
						"lon": -74.02945,
						"typ": "W",
						"pdist": 0.0
					},
					{
						"seq": 3,
						"lat": 40.73542,
						"lon": -74.03034,
						"typ": "W",
						"pdist": 0.0
					}
				]
			}
		]
	}
} 
      """

      val patterns = (for {
        json <- io.circe.parser.parse(response)
        patterns <- Pattern.given_Decoder_Seq.decodeJson(json)
      } yield patterns).fold(ex => throw ex, identity)

      patterns.length shouldBe 1
      patterns.head.id shouldBe 1368
      patterns.head.routeDirection shouldBe "New York"
      patterns.head.points.length shouldBe 3
      val pt = patterns.head.points.head
      pt.latitude shouldBe 40.7355f +- 1e-4f
      pt.longitude shouldBe -74.0291f +- 1e-4f
      pt.pointType shouldBe Stop 
      pt.stopId shouldBe Some(20496)

      val pt2 = patterns.head.points(1)
      pt2.pointType shouldBe Waypoint
      pt2.latitude shouldBe 40.7355f +- 1e-4f
      pt2.longitude shouldBe -74.02945f +- 1e-4f
    }
  }  
}
