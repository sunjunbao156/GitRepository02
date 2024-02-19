package top.doe.spark.serializer

import top.doe.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

import java.io.{Externalizable, InputStream, NotSerializableException, ObjectInput, ObjectInputStream, ObjectOutput, ObjectOutputStream, ObjectStreamClass, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag


class JavaSerializationStream(
  out: OutputStream,
  counterReset: Int,
  extraDebugInfo: Boolean
) extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo => throw new Exception(e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush(): Unit = {
    objOut.flush()
  }

  def close(): Unit = {
    objOut.close()
  }
}

class JavaDeserializationStream(
  in: InputStream,
  loader: ClassLoader
) extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {

    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        Class.forName(desc.getName, false, loader)
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }

    override def resolveProxyClass(ifaces: Array[String]): Class[_] = {
      val resolved = ifaces.map(iface => Class.forName(iface, false, loader))
      java.lang.reflect.Proxy.getProxyClass(loader, resolved: _*)
    }

  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]

  def close(): Unit = {
    objIn.close()
  }
}

private object JavaDeserializationStream {

  //Java和Scala类型的映射关系
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Unit])
}

class JavaSerializerInstance(
  counterReset: Int,
  extraDebugInfo: Boolean,
  defaultClassLoader: ClassLoader
) extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }

}

class JavaSerializer() extends Serializer with Externalizable {
  private var counterReset = 100
  private var extraDebugInfo = true

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit =
    Utils.tryOrIOException {
      out.writeInt(counterReset)
      out.writeBoolean(extraDebugInfo)
    }

  override def readExternal(in: ObjectInput): Unit =
    Utils.tryOrIOException {
      counterReset = in.readInt()
      extraDebugInfo = in.readBoolean()
    }

}