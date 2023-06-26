package stunnel.persist

import fs2.io.file.Path
import fs2.aws.s3.models.Models.FileKey
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import eu.timepit.refined.types.string.NonEmptyString

trait ObjectKeyMaker {
  def createKey(path: Path): FileKey
}

object ObjectKeyMaker {
  val dtFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")

  def datedFolder(root: Option[String] = None): ObjectKeyMaker = (path: Path) => {
    val date = dtFormatter.format(LocalDateTime.now(ZoneId.of("UTC")))
    FileKey(NonEmptyString.unsafeFrom((Path(root.getOrElse("/")) / date / path.fileName).toString))
  }

  def datedWithPrefixSubdir(root: Option[String] = None, delimiter: String = "_"): ObjectKeyMaker = (path: Path) => {
    val date = dtFormatter.format(LocalDateTime.now(ZoneId.of("UTC")))
    val prefix = path.fileName.toString.split(delimiter).head

    FileKey(NonEmptyString.unsafeFrom((Path(root.getOrElse("/")) / prefix / date / path.fileName).toString))
  }
}
