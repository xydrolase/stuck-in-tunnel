package sarrow

import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

class ArrowVSRReader(vsr: VectorSchemaRoot, allocator: BufferAllocator) extends ArrowReader(allocator) {
  @volatile private var read: Boolean = false

  override def loadNextBatch(): Boolean = {
    if (!read) {
      read = true
      true
    } else false
  }

  override def bytesRead(): Long = 0L

  override def getVectorSchemaRoot(): VectorSchemaRoot = vsr

  override protected def closeReadSource(): Unit = ()

  override protected def readSchema(): Schema = vsr.getSchema()

}
