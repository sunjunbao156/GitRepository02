package top.doe.spark.util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

//ByteArrayOutputStream的扩展增强类
class ByteBufferOutputStream(capacity: Int) extends ByteArrayOutputStream(capacity) {

  def this() = this(32)

  def getCount(): Int = count

  private[this] var closed: Boolean = false

  override def write(b: Int): Unit = {
    super.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    super.write(b, off, len)
  }

  override def reset(): Unit = {
    super.reset()
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  def toByteBuffer: ByteBuffer = {
    ByteBuffer.wrap(buf, 0, count)
  }
}