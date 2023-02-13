package stunnel.util

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64


object HMACUtil {
  val HMAC_SHA_256 = "HmacSHA256"
  val HEX_ARRAY = "0123456789abcdef".toCharArray()

  /**
   * Compute the hex digest of HMAC SHA256 signature of the given `data` using the provided `key`.
   *
   * @param data
   * @param key
   * @return
   */
  def hmacSha256Hex(data: String, key: String): String = {
    val sha256Hmac = Mac.getInstance(HMAC_SHA_256)
    val secretKey = new SecretKeySpec(key.getBytes(), HMAC_SHA_256)
    sha256Hmac.init(secretKey)
    val macData = sha256Hmac.doFinal(data.getBytes())

    bytesToHex(macData)
  }

  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Char](bytes.length * 2)
    bytes.iterator.zipWithIndex.foreach { case (byte, i) =>
      val v = byte & 0xFF
      hexChars(i * 2) = HEX_ARRAY(v >>> 4)
      hexChars(i * 2 + 1) = HEX_ARRAY(v & 0x0F)
    }

    new String(hexChars)
  }
}
