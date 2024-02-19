package top.doe.spark.serializer


import java.io.{Closeable, InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

//序列化器抽象类，以后可以根据配置，创建具体的实现类
abstract class Serializer {

  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  //这种默认的ClassLoader
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  //创建序列化器的实例
  def newInstance(): SerializerInstance

}

abstract class SerializerInstance {

  //将指定类型的对象序列化返回ByteBuffer
  def serialize[T: ClassTag](t: T): ByteBuffer

  //将ByteBuffer进行反序列化返回指定的类型
  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  //使用指定的ClassLoader将ByteBuffer进行反序列化
  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  //对OutputStream流进行序列化，返回SerializationStream
  def serializeStream(s: OutputStream): SerializationStream

  //对InputStream流进行反序列化，返回DeserializationStream
  def deserializeStream(s: InputStream): DeserializationStream
}


abstract class SerializationStream extends Closeable {

  //将指定类型的对象写入到SerializationStream，返回SerializationStream
  def writeObject[T: ClassTag](t: T): SerializationStream

  //将指定类型的key写入到SerializationStream，返回SerializationStream
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)

  //将指定类型的value写入到SerializationStream，返回SerializationStream
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)

  def flush(): Unit

  override def close(): Unit

}

abstract class DeserializationStream extends Closeable {

  //从DeserializationStream流中读取一个对象，并返回指定的类型
  def readObject[T: ClassTag](): T

  //从DeserializationStream流中读取一个key，并返回指定的类型
  def readKey[T: ClassTag](): T = readObject[T]()

  //从DeserializationStream流中读取一个value，并返回指定的类型
  def readValue[T: ClassTag](): T = readObject[T]()

  override def close(): Unit

}