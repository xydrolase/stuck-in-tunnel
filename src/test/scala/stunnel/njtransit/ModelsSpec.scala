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

  "the VehicleLocation data model" should {
    "be decodeable from API response" in {
      val response = """
    {
      "bustime-response": {
        "vehicle": [
          {
            "vid": "5254",
            "tmstmp": "20230214 01:40",
            "lat": "40.759281158447266",
            "lon": "-74.0279769897461",
            "hdg": "192",
            "pid": 519,
            "rt": "126",
            "des": "126 HOBOKEN-PATH",
            "pdist": 14140,
            "dly": false,
            "spd": 25,
            "tatripid": "16",
            "origtatripno": "19045150",
            "tablockid": "126GV019",
            "zone": "",
            "mode": 0,
            "psgld": "EMPTY",
            "srvtmstmp": "20230214 01:40",
            "oid": "549130",
            "or": false,
            "rid": "8",
            "blk": 157179502,
            "tripid": 3897020,
            "tripdyn": 0,
            "stst": 91800,
            "stsd": "2023-02-13"
          },
          {
            "vid": "5243",
            "tmstmp": "20230214 01:40",
            "lat": "40.759401148015804",
            "lon": "-74.02794450702089",
            "hdg": "13",
            "pid": 1368,
            "rt": "126",
            "des": "126 NEW YORK",
            "pdist": 9686,
            "dly": false,
            "spd": 24,
            "tatripid": "16",
            "origtatripno": "19045084",
            "tablockid": "126GV020",
            "zone": "",
            "mode": 0,
            "psgld": "EMPTY",
            "srvtmstmp": "20230214 01:40",
            "oid": "545711",
            "or": false,
            "rid": "9",
            "blk": 157180202,
            "tripid": 14141020,
            "tripdyn": 0,
            "stst": 91800,
            "stsd": "2023-02-13"
          }
        ]
      }
    }
      """

      val vecLocs = (for {
        json <- io.circe.parser.parse(response)
        locs <- json.as[Seq[VehicleLocation]]
      } yield locs).fold(ex => throw ex, identity)

      vecLocs.length shouldBe 2
      val loc = vecLocs.head
      loc.vehicleId shouldBe 5254
      loc.timestamp.toString shouldBe "2023-02-14T01:40"
      loc.scheduledStartDt.toString shouldBe "2023-02-14T01:30"
      loc.delayed shouldBe false
      loc.heading shouldBe 192
      loc.speed shouldBe 25
      loc.latitude shouldBe 40.75928f +- 1e-4f
      loc.longitude shouldBe -74.02797f +- 1e-4f
    }
  }
}
